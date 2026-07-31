// TOP_N (fused ORDER BY + LIMIT) tests: verifies the bounded-heap operator
// against the same scenarios test_sort.cpp exercises for plain ORDER BY,
// plus limit/offset interplay and stability under ties.

#include "exec_test_util.hpp"
#include "test_util.hpp"

#include "plume/execution/executor.hpp"
#include "plume/execution/operators/top_n.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

using namespace plume;
using namespace plume::exec;
using namespace plume::expr;
using namespace plume::memory;
using duckdb::Value;
using duckdb::DataChunk;
using duckdb::LogicalType;
using duckdb::FlatVector;
using duckdb::idx_t;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema AB() {
    Schema s;
    s.columns = {{"a", I32(), true}, {"b", I32(), false}};
    return s;
}

// Build a block; a negative sentinel (INT32_MIN) means NULL for column a.
OwnedBlock MakeBlock(Allocator &alloc, const Schema &schema, const std::vector<int32_t> &a,
                     const std::vector<int32_t> &b) {
    duckdb::vector<LogicalType> types = {LogicalType::INTEGER, LogicalType::INTEGER};
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), types);
    for (idx_t r = 0; r < a.size(); r++) {
        if (a[r] == INT32_MIN) {
            FlatVector::SetNull(chunk.data[0], r, true);
        } else {
            chunk.SetValue(0, r, Value::INTEGER(a[r]));
        }
        chunk.SetValue(1, r, Value::INTEGER(b[r]));
    }
    chunk.SetCardinality(a.size());
    return ExportChunk(schema, chunk, alloc).unwrap();
}

std::vector<std::pair<int32_t, int32_t>> RunTopN(Allocator &alloc, const std::vector<InputBlock> &blocks,
                                                  std::shared_ptr<TopNTemplate> top_n_op) {
    PipelineTemplate desc;
    desc.input_schema = AB();
    desc.operators = {std::move(top_n_op)};
    Executor exec(alloc);
    auto out = plume_test::RunBlocks(exec, desc, blocks, alloc);

    std::vector<std::pair<int32_t, int32_t>> rows;
    for (auto &ob : out) {
        DataChunk c;
        Schema s;
        ImportBlock(ob.data, ob.size, c, s).unwrap();
        for (idx_t r = 0; r < c.size(); r++) {
            Value va = c.GetValue(0, r);
            int32_t a_val = va.IsNull() ? INT32_MIN : va.GetValue<int32_t>();
            rows.emplace_back(a_val, c.GetValue(1, r).GetValue<int32_t>());
        }
    }
    return rows;
}

SortKey Key(uint32_t col, SortOrder ord, NullOrder no) {
    SortKey k;
    k.expr = ExprNode::Reference(col, I32());
    k.order = ord;
    k.null_order = no;
    return k;
}

std::shared_ptr<TopNTemplate> TopN(std::vector<SortKey> keys, uint64_t limit, uint64_t offset = 0) {
    auto t = std::make_shared<TopNTemplate>();
    t->type = OpType::TOP_N;
    t->sort_keys = std::move(keys);
    t->limit = limit;
    t->offset = offset;
    return t;
}

} // namespace

TEST_CASE("TOP_N: limit smaller than input, across two blocks") {
    Allocator alloc;
    auto b0 = MakeBlock(alloc, AB(), {5, 1, 3}, {50, 10, 30});
    auto b1 = MakeBlock(alloc, AB(), {2, 4}, {20, 40});

    auto top_n = TopN({Key(0, SortOrder::ASCENDING, NullOrder::NULLS_LAST)}, /*limit=*/3);
    auto rows = RunTopN(alloc, {{b0.data, b0.size}, {b1.data, b1.size}}, top_n);

    std::vector<std::pair<int32_t, int32_t>> want = {{1, 10}, {2, 20}, {3, 30}};
    CHECK(rows == want);
    alloc.Free(b0.data, b0.size);
    alloc.Free(b1.data, b1.size);
}

TEST_CASE("TOP_N: limit larger than input emits all rows sorted") {
    Allocator alloc;
    auto b0 = MakeBlock(alloc, AB(), {3, 1, 2}, {30, 10, 20});

    auto top_n = TopN({Key(0, SortOrder::ASCENDING, NullOrder::NULLS_LAST)}, /*limit=*/100);
    auto rows = RunTopN(alloc, {{b0.data, b0.size}}, top_n);

    std::vector<std::pair<int32_t, int32_t>> want = {{1, 10}, {2, 20}, {3, 30}};
    CHECK(rows == want);
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("TOP_N: limit 0 emits nothing") {
    Allocator alloc;
    auto b0 = MakeBlock(alloc, AB(), {3, 1, 2}, {30, 10, 20});

    auto top_n = TopN({Key(0, SortOrder::ASCENDING, NullOrder::NULLS_LAST)}, /*limit=*/0);
    auto rows = RunTopN(alloc, {{b0.data, b0.size}}, top_n);

    CHECK(rows.empty());
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("TOP_N: offset skips leading rows after ordering") {
    Allocator alloc;
    auto b0 = MakeBlock(alloc, AB(), {5, 1, 3, 2, 4}, {50, 10, 30, 20, 40});

    auto top_n = TopN({Key(0, SortOrder::ASCENDING, NullOrder::NULLS_LAST)}, /*limit=*/2, /*offset=*/2);
    auto rows = RunTopN(alloc, {{b0.data, b0.size}}, top_n);

    // Sorted: 1,2,3,4,5 -> skip first 2 (1,2) -> emit 3,4
    std::vector<std::pair<int32_t, int32_t>> want = {{3, 30}, {4, 40}};
    CHECK(rows == want);
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("TOP_N: offset beyond total row count emits nothing") {
    Allocator alloc;
    auto b0 = MakeBlock(alloc, AB(), {1, 2}, {10, 20});

    auto top_n = TopN({Key(0, SortOrder::ASCENDING, NullOrder::NULLS_LAST)}, /*limit=*/5, /*offset=*/10);
    auto rows = RunTopN(alloc, {{b0.data, b0.size}}, top_n);

    CHECK(rows.empty());
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("TOP_N: descending with NULLS LAST, limit keeps the largest") {
    Allocator alloc;
    // a: 3, NULL, 1, NULL, 2
    auto b0 = MakeBlock(alloc, AB(), {3, INT32_MIN, 1, INT32_MIN, 2}, {1, 2, 3, 4, 5});

    auto top_n = TopN({Key(0, SortOrder::DESCENDING, NullOrder::NULLS_LAST)}, /*limit=*/3);
    auto rows = RunTopN(alloc, {{b0.data, b0.size}}, top_n);

    CHECK(rows.size() == 3);
    CHECK(rows[0].first == 3);
    CHECK(rows[1].first == 2);
    CHECK(rows[2].first == 1);
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("TOP_N: two keys, a asc then b desc") {
    Allocator alloc;
    auto b0 = MakeBlock(alloc, AB(), {1, 1, 2, 2}, {10, 20, 5, 7});

    auto top_n = TopN({Key(0, SortOrder::ASCENDING, NullOrder::NULLS_LAST),
                        Key(1, SortOrder::DESCENDING, NullOrder::NULLS_LAST)},
        /*limit=*/3);
    auto rows = RunTopN(alloc, {{b0.data, b0.size}}, top_n);

    std::vector<std::pair<int32_t, int32_t>> want = {{1, 20}, {1, 10}, {2, 7}};
    CHECK(rows == want);
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("TOP_N: ties keep earliest-arriving rows (stable like ORDER BY + LIMIT)") {
    Allocator alloc;
    // All rows share the same key; b distinguishes arrival order.
    auto b0 = MakeBlock(alloc, AB(), {1, 1, 1}, {1, 2, 3});
    auto b1 = MakeBlock(alloc, AB(), {1, 1}, {4, 5});

    auto top_n = TopN({Key(0, SortOrder::ASCENDING, NullOrder::NULLS_LAST)}, /*limit=*/3);
    auto rows = RunTopN(alloc, {{b0.data, b0.size}, {b1.data, b1.size}}, top_n);

    std::vector<std::pair<int32_t, int32_t>> want = {{1, 1}, {1, 2}, {1, 3}};
    CHECK(rows == want);
    alloc.Free(b0.data, b0.size);
    alloc.Free(b1.data, b1.size);
}

int main() {
    return plume_test::RunAll();
}
