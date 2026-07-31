// plume_stage through the mock host ABI: load a pipeline (set 0) + input blocks
// (set 1), run, and verify the output is filtered and hash-partitioned by key.

#include "abi_mock.hpp"
#include "test_util.hpp"

#include "plume/execution/operators/filter.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/expression/expression.hpp"
#include "plume/functions/output_split.hpp"
#include "plume/functions/stage.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

using namespace plume;
using namespace plume::exec;
using namespace plume::expr;
using namespace plume::memory;
using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema RegionAmount() {
    Schema s;
    s.columns = {{"region", I32(), false}, {"amount", I32(), false}};
    return s;
}

// Build one Plume block (as an owning DataBuffer) from column-major int data.
DataBuffer MakeBlock(Allocator &alloc, const std::vector<int32_t> &region, const std::vector<int32_t> &amount) {
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::INTEGER});
    for (idx_t r = 0; r < region.size(); r++) {
        chunk.SetValue(0, r, Value::INTEGER(region[r]));
        chunk.SetValue(1, r, Value::INTEGER(amount[r]));
    }
    chunk.SetCardinality(region.size());
    auto block = ExportChunk(RegionAmount(), chunk, alloc).unwrap();
    auto buf = mock::MakeBuffer(block.data, block.size);
    alloc.Free(block.data, block.size);
    return buf;
}

uint32_t ExpectedPartition(int32_t region, uint32_t n) {
    return static_cast<uint32_t>(Value::INTEGER(region).Hash() % n);
}

} // namespace

TEST_CASE("plume_stage: filter + hash-partitioned output") {
    mock::Reset();
    Allocator alloc;
    const uint32_t N = 3;

    // Pipeline: WHERE amount > 0, output split by region into N partitions.
    PipelineTemplate desc;
    desc.input_schema = RegionAmount();
    auto filter = std::make_shared<FilterTemplate>();
    filter->type = OpType::FILTER;
    filter->filter = ExprNode::Comparison(duckdb::ExpressionType::COMPARE_GREATERTHAN,
                                          ExprNode::Reference(1, I32()), ExprNode::Constant(Value::INTEGER(0), I32()));
    desc.operators = {filter};
    desc.output_split.key_columns = {0};
    desc.output_split.partitions = N;
    auto blob = plume::SerializePipeline(desc);

    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(blob.data(), blob.size()));
        return v;
    }());
    mock::SetInput(1, [&] {
        std::vector<DataBuffer> v;
        v.push_back(MakeBlock(alloc, {1, 2, 1, 3, 2, 1, 3, 1}, {100, 50, -5, 30, 25, 0, 70, 12}));
        return v;
    }());

    CHECK(fn::RunStage().is_ok());

    // Collect all output rows with their partition key.
    int total = 0;
    for (auto *out : mock::OutputsForSet(0)) {
        Schema schema;
        auto chunks = ImportBlockChunks(const_cast<uint8_t *>(out->buffer.data()), out->buffer.size(), schema).unwrap();
        for (auto &c : chunks) {
            for (idx_t r = 0; r < c->size(); r++) {
                int32_t region = c->GetValue(0, r).GetValue<int32_t>();
                int32_t amount = c->GetValue(1, r).GetValue<int32_t>();
                CHECK(amount > 0);                                  // filter applied
                CHECK(out->key == ExpectedPartition(region, N));    // routed to hash(region)%N
                total++;
            }
        }
    }
    CHECK(total == 6); // 6 of 8 rows have amount > 0
}

namespace {

// Build a ChunkList source of `n` rows (region = `region_val`, amount = row idx),
// spanning multiple STANDARD_VECTOR_SIZE chunks — the operator-chain output the
// split now consumes directly.
ChunkList MakeChunks(Allocator &alloc, uint32_t n, int32_t region_val) {
    ChunkList chunks;
    for (uint32_t base = 0; base < n; base += STANDARD_VECTOR_SIZE) {
        auto c = std::make_unique<DataChunk>();
        c->Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::INTEGER});
        idx_t k = std::min<uint32_t>(STANDARD_VECTOR_SIZE, n - base);
        for (idx_t r = 0; r < k; r++) {
            c->SetValue(0, r, Value::INTEGER(region_val));
            c->SetValue(1, r, Value::INTEGER(static_cast<int32_t>(base + r)));
        }
        c->SetCardinality(k);
        chunks.push_back(std::move(c));
    }
    return chunks;
}

// Count total rows across the split blocks and the per-partition block count.
struct SplitStats {
    uint32_t total_rows = 0;
    std::map<uint32_t, uint32_t> blocks_per_key;
};

SplitStats Inspect(const std::vector<fn::SplitBlock> &parts) {
    SplitStats s;
    for (auto &sb : parts) {
        s.blocks_per_key[sb.key]++;
        Schema schema;
        auto chunks = ImportBlockChunks(const_cast<uint8_t *>(sb.buffer.data()), sb.buffer.size(), schema).unwrap();
        for (auto &c : chunks) {
            s.total_rows += c->size();
        }
    }
    return s;
}

} // namespace

TEST_CASE("SplitOutput: coalesces a partition into one ~target block") {
    Allocator alloc;
    exec::OutputSplit split;
    split.key_columns = {0};
    split.partitions = 2;

    // 5000 rows, all region 7 -> all route to a single partition.
    const uint32_t kRows = 5000;
    ChunkList chunks = MakeChunks(alloc, kRows, 7);

    // Large cap: the whole partition coalesces into ONE multi-chunk block.
    auto big = fn::SplitOutput(split, RegionAmount(), chunks, 1u << 20).unwrap();
    auto bs = Inspect(big);
    CHECK(bs.total_rows == kRows);
    CHECK(bs.blocks_per_key.size() == 1);            // a single partition received rows
    CHECK(bs.blocks_per_key.begin()->second == 1);   // coalesced into one block

    // Tiny cap: the same rows spill across several blocks (still all data).
    auto small = fn::SplitOutput(split, RegionAmount(), chunks, 4096).unwrap();
    auto ss = Inspect(small);
    CHECK(ss.total_rows == kRows);
    CHECK(ss.blocks_per_key.begin()->second > 1);    // flushed at the byte cap
}

namespace {

// A (region, varchar payload) schema for exercising the heap-gather path.
Schema RegionPayload() {
    Schema s;
    s.columns = {{"region", I32(), false}, {"payload", {TypeId::VARCHAR}, false}};
    return s;
}

} // namespace

TEST_CASE("SplitOutput: hash round-trip preserves rows and routes by key") {
    Allocator alloc;
    exec::OutputSplit split;
    split.key_columns = {0};
    const uint32_t N = 4;
    split.partitions = N;

    // Varied regions so all N partitions receive rows; amount is unique per row.
    const uint32_t kRows = 5000;
    ChunkList chunks;
    std::multiset<std::pair<int32_t, int32_t>> expected;
    for (uint32_t base = 0; base < kRows; base += STANDARD_VECTOR_SIZE) {
        auto c = std::make_unique<DataChunk>();
        c->Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::INTEGER});
        idx_t k = std::min<uint32_t>(STANDARD_VECTOR_SIZE, kRows - base);
        for (idx_t r = 0; r < k; r++) {
            int32_t region = static_cast<int32_t>((base + r) % 37);
            int32_t amount = static_cast<int32_t>(base + r);
            c->SetValue(0, r, Value::INTEGER(region));
            c->SetValue(1, r, Value::INTEGER(amount));
            expected.insert({region, amount});
        }
        c->SetCardinality(k);
        chunks.push_back(std::move(c));
    }

    auto parts = fn::SplitOutput(split, RegionAmount(), chunks, 1u << 20).unwrap();
    std::multiset<std::pair<int32_t, int32_t>> got;
    for (auto &sb : parts) {
        Schema schema;
        auto cs = ImportBlockChunks(const_cast<uint8_t *>(sb.buffer.data()), sb.buffer.size(), schema).unwrap();
        for (auto &c : cs) {
            for (idx_t r = 0; r < c->size(); r++) {
                int32_t region = c->GetValue(0, r).GetValue<int32_t>();
                int32_t amount = c->GetValue(1, r).GetValue<int32_t>();
                CHECK(sb.key == ExpectedPartition(region, N)); // routed to hash(region)%N
                got.insert({region, amount});
            }
        }
    }
    CHECK(got == expected); // every input row present exactly once
}

TEST_CASE("SplitOutput: coalesced blocks respect the byte cap") {
    Allocator alloc;
    exec::OutputSplit split;
    split.key_columns = {0};
    split.partitions = 1;

    const uint32_t kRows = 10000;
    ChunkList chunks = MakeChunks(alloc, kRows, 7);

    const size_t cap = 8192;
    auto parts = fn::SplitOutput(split, RegionAmount(), chunks, cap).unwrap();
    CHECK(parts.size() > 1); // spilled across many blocks, not one giant block

    // Flushing is chunk-granular (a whole input chunk is the smallest unit that gets
    // cut), so a block may overshoot the cap by at most one source chunk's worth.
    const size_t one_chunk = STANDARD_VECTOR_SIZE * 2 * sizeof(int32_t) + 512; // 2 int32 cols + slack
    uint32_t total = 0;
    for (auto &sb : parts) {
        Schema schema;
        auto cs = ImportBlockChunks(const_cast<uint8_t *>(sb.buffer.data()), sb.buffer.size(), schema).unwrap();
        uint32_t rows = 0;
        for (auto &c : cs) {
            rows += c->size();
        }
        total += rows;
        CHECK(sb.buffer.size() <= cap + one_chunk);
    }
    CHECK(total == kRows);
}

TEST_CASE("SplitOutput: varchar heap gather round-trips") {
    Allocator alloc;
    exec::OutputSplit split;
    split.key_columns = {0};
    const uint32_t N = 3;
    split.partitions = N;
    Schema schema = RegionPayload();

    auto mkstr = [](uint32_t i) {
        // > string_t::INLINE_LENGTH (12) so the value lives in the block heap.
        return std::string("payload-value-number-") + std::to_string(i);
    };

    const uint32_t kRows = 3000;
    ChunkList chunks;
    std::multiset<std::pair<int32_t, std::string>> expected;
    for (uint32_t base = 0; base < kRows; base += STANDARD_VECTOR_SIZE) {
        auto c = std::make_unique<DataChunk>();
        c->Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::VARCHAR});
        idx_t k = std::min<uint32_t>(STANDARD_VECTOR_SIZE, kRows - base);
        for (idx_t r = 0; r < k; r++) {
            int32_t region = static_cast<int32_t>((base + r) % 7);
            std::string s = mkstr(base + r);
            c->SetValue(0, r, Value::INTEGER(region));
            c->SetValue(1, r, Value(s));
            expected.insert({region, s});
        }
        c->SetCardinality(k);
        chunks.push_back(std::move(c));
    }

    auto parts = fn::SplitOutput(split, schema, chunks, 1u << 20).unwrap();
    std::multiset<std::pair<int32_t, std::string>> got;
    for (auto &sb : parts) {
        Schema out_schema;
        auto cs = ImportBlockChunks(const_cast<uint8_t *>(sb.buffer.data()), sb.buffer.size(), out_schema).unwrap();
        for (auto &c : cs) {
            for (idx_t r = 0; r < c->size(); r++) {
                int32_t region = c->GetValue(0, r).GetValue<int32_t>();
                std::string s = c->GetValue(1, r).ToString();
                CHECK(sb.key == ExpectedPartition(region, N));
                got.insert({region, s});
            }
        }
    }
    CHECK(got == expected); // strings survive the heap gather intact
}

int main() { return plume_test::RunAll(); }
