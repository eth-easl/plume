#include "mock_runtime.hpp"
#include "abi_mock.hpp"

#include "plume/catalog/csv.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/common/serial.hpp"
#include "plume/csv/csv.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/csv.hpp"
#include "plume/functions/parquet.hpp"
#include "plume/functions/stage.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/parquet/parquet.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <cstdio>
#include <fstream>
#include <string>

namespace plume::client {

using catalog::BuildCSVStageInputs;
using catalog::BuildParquetStageInputs;
using catalog::DataSourceType;
using catalog::RemoteCSVDataSource;
using catalog::RemoteParquetDataSource;
using parser::LeafStage;
using parser::PhysicalPlan;
using parser::Stage;

namespace {

std::string Quote(const std::string &identifier) {
    return duckdb::KeywordHelper::WriteQuoted(identifier, '"');
}

std::vector<uint8_t> CopyBytes(const DataBuffer &buf) {
    return std::vector<uint8_t>(buf.data(), buf.data() + buf.size());
}

std::vector<DataBuffer> AsBuffers(const BlockSet &blocks) {
    std::vector<DataBuffer> bufs;
    bufs.reserve(blocks.size());
    for (auto &b : blocks) {
        bufs.push_back(mock::MakeBuffer(b.data(), b.size()));
    }
    return bufs;
}

// Read a whole file into memory (the runtime would fetch ranges over HTTP; the
// mock reads the local file). A "file://" URL — what a remote source stage carries
// for a fixture file — is resolved to its local path.
std::vector<uint8_t> ReadFile(const std::string &path) {
    std::string local = path.rfind("file://", 0) == 0 ? path.substr(7) : path;
    std::ifstream f(local, std::ios::binary);
    if (!f) {
        throw duckdb::InvalidInputException("Plume mock runtime: cannot open source file '%s'", local);
    }
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

// Mimic the runtime resolving an S3 GET request: parse "Range: bytes=START-END"
// into the half-open file byte range [START, END+1).
std::pair<uint64_t, uint64_t> ParseRange(const DataBuffer &req) {
    std::string s(reinterpret_cast<const char *>(req.data()), req.size());
    auto pos = s.find("bytes=");
    if (pos == std::string::npos) {
        throw duckdb::InvalidInputException("Plume mock runtime: fetch request has no Range header");
    }
    pos += 6; // strlen("bytes=")
    auto dash = s.find('-', pos);
    uint64_t start = std::stoull(s.substr(pos, dash - pos));
    uint64_t end_inclusive = std::stoull(s.substr(dash + 1));
    return {start, end_inclusive + 1};
}

// A captured abi output (ident/key/bytes), copied out before the next Reset.
struct CapturedOutput {
    std::string ident;
    size_t key = 0;
    std::vector<uint8_t> bytes;
};

std::vector<CapturedOutput> Capture(size_t set_idx) {
    std::vector<CapturedOutput> out;
    for (auto *o : mock::OutputsForSet(set_idx)) {
        out.push_back({o->ident, o->key, CopyBytes(o->buffer)});
    }
    return out;
}

// Capture output set 0 grouped by the partition index each block was tagged with
// (the AddOutput key the stage's OutputSplit assigned). At a single partition all
// blocks land under key 0.
PartitionedBlocks CaptureKeyed() {
    PartitionedBlocks parts;
    for (auto *o : mock::OutputsForSet(0)) {
        parts[static_cast<uint32_t>(o->key)].push_back(CopyBytes(o->buffer));
    }
    return parts;
}

// Append every partition's blocks from `src` onto `dst` (gathering one stage's
// invocations into its combined partitioned output).
void MergeInto(PartitionedBlocks &dst, PartitionedBlocks &&src) {
    for (auto &kv : src) {
        auto &into = dst[kv.first];
        for (auto &b : kv.second) {
            into.push_back(std::move(b));
        }
    }
}

// The blocks for partition `p` of a producer's output, or an empty set if that
// partition received no rows.
BlockSet PartitionOf(const PartitionedBlocks &parts, uint32_t p) {
    auto it = parts.find(p);
    return it == parts.end() ? BlockSet{} : it->second;
}

} // namespace

BlockSet MockRuntime::MaterializeTable(const LeafStage &stage) {
    const Schema &schema = stage.pipeline.input_schema;
    if (schema.columns.empty()) {
        throw duckdb::InvalidInputException("Plume mock runtime: source stage has no input columns");
    }

    std::string select = "SELECT ";
    for (size_t i = 0; i < schema.columns.size(); i++) {
        if (i > 0) {
            select += ", ";
        }
        select += Quote(schema.columns[i].name);
    }
    const std::string &table = stage.data_source->name;
    select += " FROM " + Quote(table);

    auto result = con_.Query(select);
    if (result->HasError()) {
        throw duckdb::InvalidInputException("Plume mock runtime: failed to read table '%s': %s", table,
                                            result->GetError());
    }

    memory::Allocator alloc;
    BlockSet blocks;
    while (auto chunk = result->Fetch()) {
        if (chunk->size() == 0) {
            continue;
        }
        auto block = memory::ExportChunk(schema, *chunk, alloc).unwrap();
        blocks.push_back(std::vector<uint8_t>(block.data, block.data + block.size));
        alloc.Free(block.data, block.size);
    }
    return blocks;
}

uint32_t MockRuntime::ProducerPartitions(size_t stage_idx) const {
    uint32_t n = plan_->stages[stage_idx]->pipeline.output_split.partitions;
    return n < 1 ? 1 : n;
}

PartitionedBlocks MockRuntime::RunStage(const Stage &stage) {
    if (stage.IsLeaf()) {
        const auto &leaf = static_cast<const LeafStage &>(stage);
        switch (leaf.data_source->type) {
        case DataSourceType::REMOTE_CSV:
            return RunCsvStage(leaf);
        case DataSourceType::REMOTE_PARQUET:
            return RunParquetStage(leaf);
        case DataSourceType::LOCAL_TABLE:
            break; // handled by RunBlockStage below
        }
    }
    return RunBlockStage(stage);
}

// One plume_stage invocation: feed the template + this partition's input blocks,
// run the pipeline, and capture the output partitioned by the stage's OutputSplit.
PartitionedBlocks MockRuntime::InvokeBlockStage(const Stage &stage, const BlockSet &left, const BlockSet &right) {
    auto blob = plume::SerializePipeline(stage.pipeline);

    mock::Reset();
    std::vector<DataBuffer> templ;
    templ.push_back(mock::MakeBuffer(blob.data(), blob.size()));
    mock::SetInput(0, std::move(templ));
    mock::SetInput(1, AsBuffers(left));
    if (stage.LeadsWithJoin()) {
        mock::SetInput(2, AsBuffers(right));
    }

    auto status = fn::RunStage();
    if (status.is_error()) {
        throw duckdb::InvalidInputException("Plume mock runtime: stage %zu failed: %s", stage.idx,
                                            status.error().message());
    }

    PartitionedBlocks out = CaptureKeyed();
    mock::Reset(); // releases the mock-owned output buffers; we copied what we need
    return out;
}

// LOCAL_TABLE / non-leaf: run plume_stage. A LOCAL_TABLE source reads the whole
// table in one invocation; a non-leaf stage runs one invocation per probe (left)
// partition. The build (right) side is either co-partitioned on the join key
// (same partition count as the probe side — invocation p joins probe[p] with
// build[p]) or, for a broadcast join (see Converter::BuildCrossProduct),
// single-partitioned — its one partition is then wired as an "all" edge in
// composition.cpp, reaching every probe invocation, so it is fed whole to every p.
PartitionedBlocks MockRuntime::RunBlockStage(const Stage &stage) {
    PartitionedBlocks out;

    if (stage.IsLeaf()) {
        const auto &leaf = static_cast<const LeafStage &>(stage);
        auto it = provided_table_blocks_.find(stage.idx);
        BlockSet table = (it != provided_table_blocks_.end()) ? it->second : MaterializeTable(leaf);
        MergeInto(out, InvokeBlockStage(stage, table, {}));
        return out;
    }

    const PartitionedBlocks &lprod = stage_outputs_.at(stage.input_stages.at(0));
    const PartitionedBlocks *rprod =
        stage.LeadsWithJoin() ? &stage_outputs_.at(stage.input_stages.at(1)) : nullptr;
    const uint32_t parts = ProducerPartitions(stage.input_stages.at(0));
    const bool right_broadcast =
        stage.LeadsWithJoin() && ProducerPartitions(stage.input_stages.at(1)) <= 1;

    for (uint32_t p = 0; p < parts; p++) {
        BlockSet left = PartitionOf(lprod, p);
        BlockSet right = rprod ? PartitionOf(*rprod, right_broadcast ? 0 : p) : BlockSet{};
        // Skip empty partitions only when fanned out: a single-partition stage must
        // still invoke once so a global aggregate over empty input emits its row.
        if (parts > 1 && left.empty() && right.empty()) {
            continue;
        }
        MergeInto(out, InvokeBlockStage(stage, left, right));
    }
    return out;
}

// Resolve one client-built fetch request against the local file(s) it targets: the
// GET request line carries the url, its Range header the byte range. Reads the bytes
// the request asks for (standing in for the runtime's HTTP fetch).
std::vector<uint8_t> MockRuntime::ResolveRequest(const dandelion::DataItem &req) {
    std::string s(req.data.begin(), req.data.end());
    auto url_begin = s.find(' ') + 1;
    auto url_end = s.find(' ', url_begin);
    std::string url = s.substr(url_begin, url_end - url_begin);
    auto file = ReadFile(url);
    auto [start, end] = ParseRange(mock::MakeBuffer(req.data.data(), req.data.size()));
    end = std::min<uint64_t>(end, file.size());
    return std::vector<uint8_t>(file.begin() + start, file.begin() + end);
}

// CSV file source: the client precomputes the chunk infos + fetch requests
// (BuildCSVStageInputs, the former csv_prepare) -> resolve each request's byte range
// from the local file -> csv_stage (parse + run the pipeline). csv_stage itself runs
// the stage pipeline, so its output IS the stage output (partitioned per the stage's
// OutputSplit).
PartitionedBlocks MockRuntime::RunCsvStage(const LeafStage &stage) {
    auto src = std::static_pointer_cast<RemoteCSVDataSource>(stage.data_source);
    auto inputs = BuildCSVStageInputs(*src, *stage.projection, stage.source_splits).unwrap();

    auto blob = plume::SerializePipeline(stage.pipeline);
    mock::Reset();
    std::vector<DataBuffer> templ;
    templ.push_back(mock::MakeBuffer(blob.data(), blob.size()));
    mock::SetInput(0, std::move(templ));
    for (auto &ci : inputs.chunk_info) {
        mock::AddInput(1, mock::MakeBuffer(ci.data.data(), ci.data.size()), ci.identifier, ci.key);
    }
    for (auto &req : inputs.chunk_reqs) {
        auto bytes = ResolveRequest(req);
        mock::AddInput(2, mock::MakeBuffer(bytes.data(), bytes.size()), req.identifier, req.key);
    }

    auto st = fn::RunCSVStage();
    if (st.is_error()) {
        throw duckdb::InvalidInputException("Plume mock runtime: csv_stage (stage %zu) failed: %s", stage.idx,
                                            st.error().message());
    }
    PartitionedBlocks out = CaptureKeyed();
    mock::Reset();
    return out;
}

// Parquet file source: the client precomputes the region infos + fetch requests
// (BuildParquetStageInputs, the former pq_prepare: prune -> regions -> requests) ->
// resolve each request from the local file -> pq_stage (decode + run the pipeline,
// partitioning the output per the stage's OutputSplit).
PartitionedBlocks MockRuntime::RunParquetStage(const LeafStage &stage) {
    auto src = std::static_pointer_cast<RemoteParquetDataSource>(stage.data_source);
    const expr::ExprNode *pushed = stage.pushed_filter.get();
    auto inputs = BuildParquetStageInputs(*src, *stage.projection, pushed, stage.source_splits).unwrap();

    auto blob = plume::SerializePipeline(stage.pipeline);
    mock::Reset();
    std::vector<DataBuffer> templ;
    templ.push_back(mock::MakeBuffer(blob.data(), blob.size()));
    mock::SetInput(0, std::move(templ));
    for (auto &ro : inputs.region_info) {
        mock::AddInput(1, mock::MakeBuffer(ro.data.data(), ro.data.size()), ro.identifier, ro.key);
    }
    for (auto &req : inputs.chunk_reqs) {
        auto bytes = ResolveRequest(req);
        mock::AddInput(2, mock::MakeBuffer(bytes.data(), bytes.size()), req.identifier, req.key);
    }

    auto st = fn::RunParquetStage();
    if (st.is_error()) {
        throw duckdb::InvalidInputException("Plume mock runtime: pq_stage (stage %zu) failed: %s", stage.idx,
                                            st.error().message());
    }
    PartitionedBlocks out = CaptureKeyed();
    mock::Reset();
    return out;
}

BlockSet MockRuntime::Run(const PhysicalPlan &plan, const std::unordered_map<size_t, BlockSet> &provided) {
    plan_ = &plan;
    provided_table_blocks_ = provided;
    // Stages are emitted in dependency order (a stage's inputs always have smaller
    // indices), so a single forward pass suffices.
    for (const auto &stage : plan.stages) {
        stage_outputs_[stage->idx] = RunStage(*stage);
    }
    // The root gathers into a single result; flatten its partitions (the default
    // single partition is the common case) into one block set.
    BlockSet result;
    for (auto &kv : stage_outputs_.at(plan.root_idx)) {
        for (auto &b : kv.second) {
            result.push_back(std::move(b));
        }
    }
    return result;
}

} // namespace plume::client
