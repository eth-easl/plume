// Milestone-5 ORDER BY tests (no DuckDB instance): single/multi-key, asc/desc,
// nulls first/last, across multiple input blocks.

#include "exec_test_util.hpp"
#include "test_util.hpp"

#include "plume/execution/executor.hpp"
#include "plume/execution/operators/sort.hpp"
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

std::vector<std::pair<int32_t, int32_t>> RunSort(Allocator &alloc, const std::vector<InputBlock> &blocks,
                                                  std::shared_ptr<SortTemplate> sort_op) {
    PipelineTemplate desc;
    desc.input_schema = AB();
    desc.operators = {std::move(sort_op)};
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

} // namespace

TEST_CASE("ORDER BY single key ascending, across two blocks") {
    Allocator alloc;
    auto b0 = MakeBlock(alloc, AB(), {5, 1, 3}, {50, 10, 30});
    auto b1 = MakeBlock(alloc, AB(), {2, 4}, {20, 40});

    auto sort = std::make_shared<SortTemplate>();
    sort->type = OpType::ORDER_BY;
    sort->sort_keys = {Key(0, SortOrder::ASCENDING, NullOrder::NULLS_LAST)};

    auto rows = RunSort(alloc, {{b0.data, b0.size}, {b1.data, b1.size}}, sort);
    CHECK(rows.size() == 5);
    int32_t expect[5] = {1, 2, 3, 4, 5};
    for (size_t i = 0; i < 5; i++) {
        CHECK(rows[i].first == expect[i]);
        CHECK(rows[i].second == expect[i] * 10);
    }
    alloc.Free(b0.data, b0.size);
    alloc.Free(b1.data, b1.size);
}

TEST_CASE("ORDER BY descending with NULLS LAST") {
    Allocator alloc;
    // a: 3, NULL, 1, NULL, 2
    auto b0 = MakeBlock(alloc, AB(), {3, INT32_MIN, 1, INT32_MIN, 2}, {1, 2, 3, 4, 5});

    auto sort = std::make_shared<SortTemplate>();
    sort->type = OpType::ORDER_BY;
    sort->sort_keys = {Key(0, SortOrder::DESCENDING, NullOrder::NULLS_LAST)};

    auto rows = RunSort(alloc, {{b0.data, b0.size}}, sort);
    CHECK(rows.size() == 5);
    // Descending non-nulls 3,2,1 then NULLs last (stable: original order of nulls)
    CHECK(rows[0].first == 3);
    CHECK(rows[1].first == 2);
    CHECK(rows[2].first == 1);
    CHECK(rows[3].first == INT32_MIN); // null
    CHECK(rows[4].first == INT32_MIN); // null
    // stability: the two nulls keep their original relative order (b=2 then b=4)
    CHECK(rows[3].second == 2);
    CHECK(rows[4].second == 4);
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("ORDER BY two keys: a asc, b desc") {
    Allocator alloc;
    auto b0 = MakeBlock(alloc, AB(), {1, 1, 2, 2}, {10, 20, 5, 7});

    auto sort = std::make_shared<SortTemplate>();
    sort->type = OpType::ORDER_BY;
    sort->sort_keys = {Key(0, SortOrder::ASCENDING, NullOrder::NULLS_LAST),
                       Key(1, SortOrder::DESCENDING, NullOrder::NULLS_LAST)};

    auto rows = RunSort(alloc, {{b0.data, b0.size}}, sort);
    CHECK(rows.size() == 4);
    // (1,20),(1,10),(2,7),(2,5)
    CHECK((rows[0] == std::make_pair(1, 20)));
    CHECK((rows[1] == std::make_pair(1, 10)));
    CHECK((rows[2] == std::make_pair(2, 7)));
    CHECK((rows[3] == std::make_pair(2, 5)));
    alloc.Free(b0.data, b0.size);
}

int main() {
    return plume_test::RunAll();
}
