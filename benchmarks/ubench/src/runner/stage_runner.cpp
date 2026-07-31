#include "stage_runner.hpp"
#include "abi_ubench.hpp"

#include "plume/common/trace.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/csv.hpp"
#include "plume/functions/parquet.hpp"
#include "plume/functions/stage.hpp"

#include <exception>

namespace plume::ubench {

namespace {

using EntryFn = Result<void> (*)();

// Copy the calling thread's captured outputs into a partitioned block set (keyed
// by the OutputSplit partition each block was tagged with). Copies the bytes out
// before the next Reset frees the thread-local buffers.
PartitionedBlocks CaptureKeyed() {
    PartitionedBlocks parts;
    for (auto &o : rt::Outputs()) {
        if (o.set_idx != 0) {
            continue; // stage output is always set 0
        }
        auto &set = parts[static_cast<uint32_t>(o.key)];
        set.emplace_back(o.buffer.data(), o.buffer.data() + o.buffer.size());
    }
    return parts;
}

// Run one stage invocation on the calling (worker) thread: install its ABI input
// sets, call the function entrypoint under trace, and capture its output blocks.
// Never throws; a failure (Result error or a DuckDB kernel exception) is written
// to `err` and an empty result returned.
PartitionedBlocks InvokeOne(const std::vector<uint8_t> &templ, const std::vector<InItem> &set1,
                            const std::vector<InItem> &set2, uint16_t stage_id, uint32_t invocation,
                            uint16_t partition, uint16_t num_ops, EntryFn entry, std::string &err) {
    rt::Reset();
    rt::AddInput(0, templ.data(), templ.size());
    // Every fn::Run*Stage entrypoint fetches sets 1 and 2 unconditionally (the join
    // right side is gated on HasJoin(), but once gated-in it's still an unconditional
    // GetInputSet) — register both as present even when a partition's blocks are
    // empty, or GetInputSet fails with "out of range" instead of returning empty.
    rt::EnsureInputSet(1);
    rt::EnsureInputSet(2);
    for (auto &it : set1) {
        rt::AddInput(1, it.data, it.size, it.ident, it.key);
    }
    for (auto &it : set2) {
        rt::AddInput(2, it.data, it.size, it.ident, it.key);
    }

    trace::BeginInvocation(stage_id, invocation, partition, num_ops);
    Result<void> r = Ok();
    try {
        r = entry();
    } catch (const std::exception &e) {
        err = std::string("exception: ") + e.what();
        trace::EndInvocation();
        return {};
    } catch (...) {
        err = "unknown exception";
        trace::EndInvocation();
        return {};
    }
    trace::EndInvocation();

    if (r.is_error()) {
        err = r.error().message();
        return {};
    }
    return CaptureKeyed();
}

// Merge every partition of `src` into `dst`.
void MergeInto(PartitionedBlocks &dst, PartitionedBlocks &&src) {
    for (auto &kv : src) {
        auto &into = dst[kv.first];
        for (auto &b : kv.second) {
            into.push_back(std::move(b));
        }
    }
}

// The blocks for partition `p` of a producer's output (empty if none).
BlockSet PartitionOf(const PartitionedBlocks &parts, uint32_t p) {
    auto it = parts.find(p);
    return it == parts.end() ? BlockSet{} : it->second;
}

std::vector<InItem> Views(const BlockSet &blocks) {
    std::vector<InItem> v;
    v.reserve(blocks.size());
    for (auto &b : blocks) {
        v.push_back(InItem{b.data(), b.size(), "", 0});
    }
    return v;
}

} // namespace

StageGraphRunner::StageGraphRunner(const UbPlan &plan, LocalFetcher &fetch, ThreadPool &pool)
    : plan_(plan), fetch_(fetch), pool_(pool) {
    for (const auto &s : plan_.stages) {
        by_id_[s.id] = &s;
        // Count operators once (off the measured path) so BeginInvocation can size
        // its accumulator; a bad blob just leaves the count at 0 (auto-resized later).
        auto templ = DeserializePipeline(s.pipeline_blob);
        num_ops_[s.id] = templ.is_ok() ? static_cast<uint16_t>(templ.unwrap().operators.size()) : 0;
    }
}

uint32_t StageGraphRunner::ProducerPartitions(int stage_id) const {
    auto it = by_id_.find(stage_id);
    uint32_t n = (it == by_id_.end()) ? 1 : it->second->partitions;
    return n < 1 ? 1 : n;
}

Result<PartitionedBlocks> StageGraphRunner::RunTableStage(const UbStage &s) {
    std::vector<InItem> set1;
    set1.reserve(s.table_blocks.size());
    for (auto &blk : s.table_blocks) {
        set1.push_back(InItem{blk.data.data(), blk.data.size(), "", 0});
    }
    std::string err;
    PartitionedBlocks out = InvokeOne(s.pipeline_blob, set1, {}, static_cast<uint16_t>(s.id), 0, 0, num_ops_[s.id],
                                      &fn::RunStage, err);
    if (!err.empty()) {
        return Error("ubench: table stage " + std::to_string(s.id) + " failed: " + err);
    }
    return out;
}

Result<PartitionedBlocks> StageGraphRunner::RunBlockStage(const UbStage &s) {
    const PartitionedBlocks &lprod = outputs_.at(s.input_stages.at(0));
    const PartitionedBlocks *rprod = s.leads_with_join ? &outputs_.at(s.input_stages.at(1)) : nullptr;
    const uint32_t parts = ProducerPartitions(s.input_stages.at(0));

    std::vector<PartitionedBlocks> results(parts);
    std::vector<std::string> errs(parts);
    pool_.ParallelFor(parts, [&](size_t p) {
        BlockSet left = PartitionOf(lprod, static_cast<uint32_t>(p));
        BlockSet right = rprod ? PartitionOf(*rprod, static_cast<uint32_t>(p)) : BlockSet{};
        // A fanned-out stage skips wholly-empty partitions; a single-partition stage
        // must still fire once (e.g. a global aggregate over empty input emits a row).
        if (parts > 1 && left.empty() && right.empty()) {
            return;
        }
        std::vector<InItem> set1 = Views(left);
        std::vector<InItem> set2 = Views(right);
        results[p] = InvokeOne(s.pipeline_blob, set1, set2, static_cast<uint16_t>(s.id), static_cast<uint32_t>(p),
                               static_cast<uint16_t>(p), num_ops_[s.id], &fn::RunStage, errs[p]);
    });

    PartitionedBlocks out;
    for (uint32_t p = 0; p < parts; p++) {
        if (!errs[p].empty()) {
            return Error("ubench: stage " + std::to_string(s.id) + " invocation " + std::to_string(p) +
                         " failed: " + errs[p]);
        }
        MergeInto(out, std::move(results[p]));
    }
    return out;
}

Result<PartitionedBlocks> StageGraphRunner::RunSourceStage(const UbStage &s) {
    EntryFn entry = s.source == UbSourceKind::CSV ? &fn::RunCSVStage : &fn::RunParquetStage;

    // dandelion's anyKeyed sharding: one invocation per distinct source key
    // (region / csv chunk). Group the fetch requests by key up front.
    std::unordered_map<uint64_t, std::vector<const UbItem *>> reqs_by_key;
    for (auto &req : s.source_reqs) {
        reqs_by_key[req.key].push_back(&req);
    }
    // Preserve source_info order for deterministic invocation ids.
    const size_t n = s.source_info.size();

    std::vector<PartitionedBlocks> results(n);
    std::vector<std::string> errs(n);
    pool_.ParallelFor(n, [&](size_t i) {
        const UbItem &info = s.source_info[i];
        std::vector<InItem> set1 = {InItem{info.data.data(), info.data.size(), info.ident, info.key}};

        // Resolve this key's fetch requests against the local filesystem. The
        // resolved bytes must outlive InvokeOne, so hold them here.
        std::vector<std::vector<uint8_t>> resolved;
        std::vector<InItem> set2;
        auto it = reqs_by_key.find(info.key);
        if (it != reqs_by_key.end()) {
            resolved.reserve(it->second.size());
            for (const UbItem *req : it->second) {
                auto bytes = fetch_.Resolve(req->data.data(), req->data.size());
                if (bytes.is_error()) {
                    errs[i] = bytes.error().message();
                    return;
                }
                resolved.push_back(std::move(bytes).unwrap());
                set2.push_back(InItem{resolved.back().data(), resolved.back().size(), req->ident, req->key});
            }
        }

        results[i] = InvokeOne(s.pipeline_blob, set1, set2, static_cast<uint16_t>(s.id), static_cast<uint32_t>(i),
                               static_cast<uint16_t>(i), num_ops_[s.id], entry, errs[i]);
    });

    PartitionedBlocks out;
    for (size_t i = 0; i < n; i++) {
        if (!errs[i].empty()) {
            return Error("ubench: source stage " + std::to_string(s.id) + " key-invocation " + std::to_string(i) +
                         " failed: " + errs[i]);
        }
        MergeInto(out, std::move(results[i]));
    }
    return out;
}

Result<PartitionedBlocks> StageGraphRunner::RunStage(const UbStage &s) {
    switch (s.source) {
    case UbSourceKind::TABLE_BLOCKS:
        return RunTableStage(s);
    case UbSourceKind::CSV:
    case UbSourceKind::PARQUET:
        return RunSourceStage(s);
    case UbSourceKind::STAGE_OUTPUT:
        return RunBlockStage(s);
    }
    return Error("ubench: unknown source kind");
}

Result<BlockSet> StageGraphRunner::Run() {
    // The plan is topologically ordered, so a single forward pass suffices.
    for (const auto &s : plan_.stages) {
        auto out = RunStage(s);
        if (out.is_error()) {
            return Error(out.error().message());
        }
        outputs_[s.id] = std::move(out).unwrap();
    }

    BlockSet result;
    auto it = outputs_.find(plan_.root_stage);
    if (it == outputs_.end()) {
        return Error("ubench: root stage " + std::to_string(plan_.root_stage) + " produced no output");
    }
    for (auto &kv : it->second) {
        for (auto &b : kv.second) {
            result.push_back(std::move(b));
        }
    }
    return result;
}

} // namespace plume::ubench
