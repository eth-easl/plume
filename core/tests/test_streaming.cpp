// Push-based execution (Executor::Push / Finish): push the source one chunk at a
// time and verify results match batch execution across chunk boundaries. Covers a
// reducing filter, cross-chunk aggregation, limit/offset spanning chunks, and a
// blocking sort (buffers its input in Push, orders + emits in Finish).

#include "test_util.hpp"

#include "plume/execution/executor.hpp"
#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/limit.hpp"
#include "plume/execution/operators/output.hpp"
#include "plume/execution/operators/sort.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace plume;
using namespace plume::exec;
using namespace plume::expr;
using namespace plume::memory;
using duckdb::DataChunk;
using duckdb::ExpressionType;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema AB() {
    Schema s;
    s.columns = {{"a", I32(), false}, {"b", I32(), false}};
    return s;
}

ExprNode Ref(uint32_t i) { return ExprNode::Reference(i, I32()); }
ExprNode Lit(int32_t v) { return ExprNode::Constant(Value::INTEGER(v), I32()); }

// One source chunk from column-major int data.
std::unique_ptr<DataChunk> MakeChunk(Allocator &alloc, const std::vector<int32_t> &a, const std::vector<int32_t> &b) {
    auto chunk = std::make_unique<DataChunk>();
    chunk->Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::INTEGER});
    for (idx_t r = 0; r < a.size(); r++) {
        chunk->SetValue(0, r, Value::INTEGER(a[r]));
        chunk->SetValue(1, r, Value::INTEGER(b[r]));
    }
    chunk->SetCardinality(a.size());
    return chunk;
}

// Build `exec` from `desc` with a block sink, push `chunks` one at a time, finish,
// and return the emitted blocks (self-freeing DataBuffers).
std::vector<DataBuffer> StreamToBlocks(Executor &exec, const PipelineTemplate &desc, ChunkList chunks) {
    std::vector<DataBuffer> out;
    OutputEmit emit = [&](const std::string &, size_t, DataBuffer buf, size_t) { out.push_back(std::move(buf)); };
    exec.Build(desc, emit).unwrap();
    for (auto &c : chunks) {
        CHECK(exec.Push(std::move(c)).is_ok());
    }
    exec.Finish().unwrap();
    return out;
}

// Push `chunks` through `exec` one at a time and collect the (a,b) output rows.
std::vector<std::pair<int32_t, int32_t>> StreamRows(Executor &exec, const PipelineTemplate &desc, ChunkList chunks) {
    auto outputs = StreamToBlocks(exec, desc, std::move(chunks));
    std::vector<std::pair<int32_t, int32_t>> rows;
    for (auto &ob : outputs) {
        DataChunk c;
        Schema s;
        ImportBlock(ob.mutable_data(), ob.size(), c, s).unwrap();
        for (idx_t r = 0; r < c.size(); r++) {
            rows.emplace_back(c.GetValue(0, r).GetValue<int32_t>(), c.GetValue(1, r).GetValue<int32_t>());
        }
    }
    return rows;
}

} // namespace

TEST_CASE("streaming: filter reduces data chunk-by-chunk") {
    Allocator alloc;
    PipelineTemplate desc;
    desc.input_schema = AB();
    auto filter = std::make_shared<FilterTemplate>();
    filter->type = OpType::FILTER;
    filter->filter = ExprNode::Comparison(ExpressionType::COMPARE_GREATERTHAN, Ref(1), Lit(0)); // b > 0
    desc.operators = {filter};

    Executor exec(alloc);


    ChunkList src;
    src.push_back(MakeChunk(alloc, {1, 2, 3}, {10, -1, 20}));
    src.push_back(MakeChunk(alloc, {4, 5}, {-5, 30}));
    src.push_back(MakeChunk(alloc, {6}, {0}));

    auto rows = StreamRows(exec, desc, std::move(src));
    std::vector<std::pair<int32_t, int32_t>> want = {{1, 10}, {3, 20}, {5, 30}};
    CHECK(rows == want);
}

TEST_CASE("streaming: aggregation accumulates across chunks") {
    Allocator alloc;
    // SELECT a, sum(b) GROUP BY a
    PipelineTemplate desc;
    desc.input_schema = AB();
    auto agg = std::make_shared<AggregateTemplate>();
    agg->type = OpType::AGGREGATE;
    agg->group_keys = {Ref(0)};
    AggregateSpec sum;
    sum.func_name = "sum";
    sum.return_type = {TypeId::HUGEINT}; // sum(int32) -> hugeint (int128)
    sum.arguments = {Ref(1)};
    agg->aggregates = {sum};
    desc.operators = {agg};

    Executor exec(alloc);


    ChunkList src;
    src.push_back(MakeChunk(alloc, {1, 2, 1}, {10, 5, 20}));
    src.push_back(MakeChunk(alloc, {2, 1}, {7, 3}));
    src.push_back(MakeChunk(alloc, {3}, {100}));

    auto outputs = StreamToBlocks(exec, desc, std::move(src));

    std::map<int32_t, int64_t> got;
    for (auto &ob : outputs) {
        DataChunk c;
        Schema s;
        ImportBlock(ob.mutable_data(), ob.size(), c, s).unwrap();
        for (idx_t r = 0; r < c.size(); r++) {
            // sum is hugeint; the values fit in int64 here.
            got[c.GetValue(0, r).GetValue<int32_t>()] = c.GetValue(1, r).GetValue<int64_t>();
        }
    }
    std::map<int32_t, int64_t> want = {{1, 33}, {2, 12}, {3, 100}};
    CHECK(got == want);
}

TEST_CASE("streaming: limit/offset spans chunk boundaries") {
    Allocator alloc;
    // LIMIT 3 OFFSET 2 over the concatenated source.
    PipelineTemplate desc;
    desc.input_schema = AB();
    auto limit = std::make_shared<LimitTemplate>();
    limit->type = OpType::LIMIT;
    limit->has_limit = true;
    limit->limit = 3;
    limit->offset = 2;
    desc.operators = {limit};

    Executor exec(alloc);


    ChunkList src;
    src.push_back(MakeChunk(alloc, {1, 2}, {1, 2}));    // rows 0,1
    src.push_back(MakeChunk(alloc, {3, 4, 5}, {3, 4, 5})); // rows 2,3,4
    src.push_back(MakeChunk(alloc, {6, 7}, {6, 7}));    // rows 5,6

    auto rows = StreamRows(exec, desc, std::move(src));
    // Skip rows 0,1; emit rows 2,3,4.
    std::vector<std::pair<int32_t, int32_t>> want = {{3, 3}, {4, 4}, {5, 5}};
    CHECK(rows == want);
}

TEST_CASE("streaming: sort buffers pushed chunks and orders on finish") {
    Allocator alloc;
    // ORDER BY a ASC.
    PipelineTemplate desc;
    desc.input_schema = AB();
    auto sort = std::make_shared<SortTemplate>();
    sort->type = OpType::ORDER_BY;
    SortKey key;
    key.expr = Ref(0);
    key.order = SortOrder::ASCENDING;
    sort->sort_keys = {key};
    desc.operators = {sort};

    Executor exec(alloc);


    ChunkList src;
    src.push_back(MakeChunk(alloc, {3, 1}, {30, 10}));
    src.push_back(MakeChunk(alloc, {2, 5, 4}, {20, 50, 40}));

    auto rows = StreamRows(exec, desc, std::move(src));
    std::vector<std::pair<int32_t, int32_t>> want = {{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}};
    CHECK(rows == want);
}

int main() { return plume_test::RunAll(); }
