// Milestone-5 aggregation tests (no DuckDB instance): ungrouped SUM/COUNT/MIN/
// MAX/AVG and grouped aggregation, reusing DuckDB aggregate kernels with a
// Plume-owned hash map.

#include "exec_test_util.hpp"
#include "test_util.hpp"

#include "plume/execution/executor.hpp"
#include "plume/execution/operators/aggregate.hpp"
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
using duckdb::idx_t;

namespace {

ColumnType I32() { return {TypeId::INT32}; }
ColumnType I64() { return {TypeId::INT64}; }
ColumnType F64() { return {TypeId::DOUBLE}; }

// Schema: g INT32, a INT32, c DOUBLE
Schema GAC() {
    Schema s;
    s.columns = {{"g", I32(), false}, {"a", I32(), false}, {"c", F64(), false}};
    return s;
}

OwnedBlock MakeBlock(Allocator &alloc, const Schema &schema,
                     const std::vector<int32_t> &g, const std::vector<int32_t> &a,
                     const std::vector<double> &c) {
    duckdb::vector<LogicalType> types;
    for (auto &col : schema.columns) {
        types.push_back(ToLogicalType(col.type));
    }
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), types);
    idx_t n = g.size();
    for (idx_t r = 0; r < n; r++) {
        chunk.SetValue(0, r, Value::INTEGER(g[r]));
        chunk.SetValue(1, r, Value::INTEGER(a[r]));
        chunk.SetValue(2, r, Value::DOUBLE(c[r]));
    }
    chunk.SetCardinality(n);
    return ExportChunk(schema, chunk, alloc).unwrap();
}

ExprNode RefI32(uint32_t i) { return ExprNode::Reference(i, I32()); }
ExprNode RefF64(uint32_t i) { return ExprNode::Reference(i, F64()); }

AggregateSpec Agg(std::string name, ColumnType ret, std::vector<ExprNode> args, bool distinct = false) {
    AggregateSpec a;
    a.func_name = std::move(name);
    a.return_type = ret;
    a.arguments = std::move(args);
    a.distinct = distinct;
    return a;
}

} // namespace

TEST_CASE("ungrouped aggregation: count*/min/max/sum/avg") {
    Allocator alloc;
    auto schema = GAC();
    auto blk = MakeBlock(alloc, schema, {1, 1, 2, 2, 2}, {10, 20, 30, 40, 50}, {1.5, 2.5, 3.5, 4.5, 5.5});

    PipelineTemplate desc;
    desc.input_schema = schema;
    auto agg = std::make_shared<AggregateTemplate>();
    agg->type = OpType::AGGREGATE;
    agg->aggregates = {
        Agg("count_star", I64(), {}),
        Agg("min", I32(), {RefI32(1)}),
        Agg("max", I32(), {RefI32(1)}),
        Agg("sum", F64(), {RefF64(2)}),
        Agg("avg", F64(), {RefI32(1)}),
    };
    desc.operators = {agg};

    Executor exec(alloc);

    auto out = plume_test::RunBlocks(exec, desc, {{blk.data, blk.size}}, alloc);
    CHECK(out.size() == 1);

    DataChunk c;
    Schema s;
    ImportBlock(out[0].data, out[0].size, c, s).unwrap();
    CHECK(c.size() == 1);
    CHECK(c.GetValue(0, 0).GetValue<int64_t>() == 5);    // count(*)
    CHECK(c.GetValue(1, 0).GetValue<int32_t>() == 10);   // min(a)
    CHECK(c.GetValue(2, 0).GetValue<int32_t>() == 50);   // max(a)
    CHECK(c.GetValue(3, 0).GetValue<double>() == 17.5);  // sum(c)
    CHECK(c.GetValue(4, 0).GetValue<double>() == 30.0);  // avg(a)

    alloc.Free(out[0].data, out[0].size);
    alloc.Free(blk.data, blk.size);
}

TEST_CASE("grouped aggregation: GROUP BY g -> sum(c), count(*)") {
    Allocator alloc;
    auto schema = GAC();
    auto blk = MakeBlock(alloc, schema, {1, 1, 2, 2, 2}, {10, 20, 30, 40, 50}, {1.0, 3.0, 4.0, 4.5, 5.0});

    PipelineTemplate desc;
    desc.input_schema = schema;
    auto agg = std::make_shared<AggregateTemplate>();
    agg->type = OpType::AGGREGATE;
    agg->group_keys = {RefI32(0)};
    agg->aggregates = {Agg("sum", F64(), {RefF64(2)}), Agg("count_star", I64(), {})};
    desc.operators = {agg};

    Executor exec(alloc);

    auto out = plume_test::RunBlocks(exec, desc, {{blk.data, blk.size}}, alloc);

    // Collect (g -> sum, count) from output rows.
    int total_rows = 0;
    double sum_g1 = -1, sum_g2 = -1;
    int64_t cnt_g1 = -1, cnt_g2 = -1;
    for (auto &ob : out) {
        DataChunk c;
        Schema s;
        ImportBlock(ob.data, ob.size, c, s).unwrap();
        for (idx_t r = 0; r < c.size(); r++) {
            int32_t g = c.GetValue(0, r).GetValue<int32_t>();
            double sum = c.GetValue(1, r).GetValue<double>();
            int64_t cnt = c.GetValue(2, r).GetValue<int64_t>();
            if (g == 1) { sum_g1 = sum; cnt_g1 = cnt; }
            if (g == 2) { sum_g2 = sum; cnt_g2 = cnt; }
            total_rows++;
        }
    }
    CHECK(total_rows == 2);
    CHECK(sum_g1 == 4.0);
    CHECK(cnt_g1 == 2);
    CHECK(sum_g2 == 13.5);
    CHECK(cnt_g2 == 3);

    for (auto &ob : out) alloc.Free(ob.data, ob.size);
    alloc.Free(blk.data, blk.size);
}

TEST_CASE("DISTINCT aggregation: ungrouped, dedup persists across chunks") {
    Allocator alloc;
    auto schema = GAC();
    // a = [10, 10, 20] / c = [1.5, 1.5, 2.5] in chunk 1, a = [30, 30, 10] / c =
    // [3.5, 3.5, 1.5] in chunk 2 -> distinct a {10, 20, 30}, distinct c {1.5, 2.5,
    // 3.5}. The repeated 10/1.5 pair spans both chunks, so this only passes if the
    // dedup state is kept across Push calls, not just within one chunk. (sum is
    // deduped over `c`, not `a`: DuckDB's sum(INT32) widens to HUGEINT, which this
    // low-level operator test doesn't model — see Converter::BuildAggregate.)
    auto blk1 = MakeBlock(alloc, schema, {1, 1, 1}, {10, 10, 20}, {1.5, 1.5, 2.5});
    auto blk2 = MakeBlock(alloc, schema, {1, 1, 1}, {30, 30, 10}, {3.5, 3.5, 1.5});

    PipelineTemplate desc;
    desc.input_schema = schema;
    auto agg = std::make_shared<AggregateTemplate>();
    agg->type = OpType::AGGREGATE;
    agg->aggregates = {
        Agg("count", I64(), {RefI32(1)}, /*distinct=*/true),
        Agg("sum", F64(), {RefF64(2)}, /*distinct=*/true),
        Agg("count_star", I64(), {}), // non-distinct sanity check alongside distinct aggregates
    };
    desc.operators = {agg};

    Executor exec(alloc);
    auto out = plume_test::RunBlocks(exec, desc, {{blk1.data, blk1.size}, {blk2.data, blk2.size}}, alloc);
    CHECK(out.size() == 1);

    DataChunk c;
    Schema s;
    ImportBlock(out[0].data, out[0].size, c, s).unwrap();
    CHECK(c.size() == 1);
    CHECK(c.GetValue(0, 0).GetValue<int64_t>() == 3);  // count(distinct a): {10, 20, 30}
    CHECK(c.GetValue(1, 0).GetValue<double>() == 7.5);  // sum(distinct c): 1.5+2.5+3.5
    CHECK(c.GetValue(2, 0).GetValue<int64_t>() == 6);   // count(*): all 6 rows

    alloc.Free(out[0].data, out[0].size);
    alloc.Free(blk1.data, blk1.size);
    alloc.Free(blk2.data, blk2.size);
}

TEST_CASE("DISTINCT aggregation: grouped, dedup is per-group not global") {
    Allocator alloc;
    auto schema = GAC();
    // g=1: a = [10, 10, 20]     / c = [1.5, 1.5, 2.5] -> distinct a {10, 20}, distinct c {1.5, 2.5}
    // g=2: a = [10, 5, 5, 5, 7] / c = [9.0, 6.0, 6.0, 6.0, 8.0] -> distinct a {5, 7, 10}
    //      (the "10" also appears in g=1, which must not suppress it here -> the
    //      dedup key must include the group), distinct c {6.0, 8.0, 9.0}.
    auto blk1 = MakeBlock(alloc, schema, {1, 1, 1, 2}, {10, 10, 20, 10}, {1.5, 1.5, 2.5, 9.0});
    auto blk2 = MakeBlock(alloc, schema, {2, 2, 2}, {5, 5, 5}, {6.0, 6.0, 6.0});
    auto blk3 = MakeBlock(alloc, schema, {2}, {7}, {8.0});

    PipelineTemplate desc;
    desc.input_schema = schema;
    auto agg = std::make_shared<AggregateTemplate>();
    agg->type = OpType::AGGREGATE;
    agg->group_keys = {RefI32(0)};
    agg->aggregates = {
        Agg("count", I64(), {RefI32(1)}, /*distinct=*/true),
        Agg("sum", F64(), {RefF64(2)}, /*distinct=*/true),
        Agg("count_star", I64(), {}),
    };
    desc.operators = {agg};

    Executor exec(alloc);
    auto out = plume_test::RunBlocks(
        exec, desc, {{blk1.data, blk1.size}, {blk2.data, blk2.size}, {blk3.data, blk3.size}}, alloc);

    int total_rows = 0;
    int64_t dcnt_g1 = -1, dcnt_g2 = -1;
    double dsum_g1 = -1, dsum_g2 = -1;
    int64_t star_g1 = -1, star_g2 = -1;
    for (auto &ob : out) {
        DataChunk c;
        Schema s;
        ImportBlock(ob.data, ob.size, c, s).unwrap();
        for (idx_t r = 0; r < c.size(); r++) {
            int32_t g = c.GetValue(0, r).GetValue<int32_t>();
            if (g == 1) {
                dcnt_g1 = c.GetValue(1, r).GetValue<int64_t>();
                dsum_g1 = c.GetValue(2, r).GetValue<double>();
                star_g1 = c.GetValue(3, r).GetValue<int64_t>();
            } else if (g == 2) {
                dcnt_g2 = c.GetValue(1, r).GetValue<int64_t>();
                dsum_g2 = c.GetValue(2, r).GetValue<double>();
                star_g2 = c.GetValue(3, r).GetValue<int64_t>();
            }
            total_rows++;
        }
    }
    CHECK(total_rows == 2);
    CHECK(dcnt_g1 == 2);  // {10, 20}
    CHECK(dsum_g1 == 4.0);  // 1.5+2.5
    CHECK(star_g1 == 3);
    CHECK(dcnt_g2 == 3);  // {10, 5, 7}
    CHECK(dsum_g2 == 23.0);  // 9.0+6.0+8.0
    CHECK(star_g2 == 5);

    for (auto &ob : out) alloc.Free(ob.data, ob.size);
    alloc.Free(blk1.data, blk1.size);
    alloc.Free(blk2.data, blk2.size);
    alloc.Free(blk3.data, blk3.size);
}

TEST_CASE("grouped aggregation: many groups force rehash + multi-chunk accumulation") {
    Allocator alloc;
    auto schema = GAC();

    // 6000 rows, g = r % 3000 → 3000 distinct groups (forces several rehashes of
    // the group directory) each seen exactly twice, split across multiple input
    // blocks so duplicate keys accumulate across Sink calls.
    const int N = 6000;
    const int G = 3000;
    std::vector<OwnedBlock> blocks;
    for (int base = 0; base < N; base += 2000) {
        const int n = std::min(2000, N - base);
        std::vector<int32_t> g(n), a(n);
        std::vector<double> c(n);
        for (int i = 0; i < n; i++) {
            const int r = base + i;
            g[i] = r % G;
            a[i] = r;
            c[i] = 1.0;
        }
        blocks.push_back(MakeBlock(alloc, schema, g, a, c));
    }
    std::vector<InputBlock> inputs;
    for (auto &b : blocks) {
        inputs.push_back({b.data, b.size});
    }

    PipelineTemplate desc;
    desc.input_schema = schema;
    auto agg = std::make_shared<AggregateTemplate>();
    agg->type = OpType::AGGREGATE;
    agg->group_keys = {RefI32(0)};
    agg->aggregates = {Agg("sum", F64(), {RefF64(2)}), Agg("count_star", I64(), {})};
    desc.operators = {agg};

    Executor exec(alloc);

    auto out = plume_test::RunBlocks(exec, desc, inputs, alloc);

    std::vector<int64_t> cnt(G, 0);
    std::vector<double> sum(G, -1.0);
    int rows = 0;
    for (auto &ob : out) {
        DataChunk c;
        Schema s;
        ImportBlock(ob.data, ob.size, c, s).unwrap();
        for (idx_t r = 0; r < c.size(); r++) {
            const int32_t g = c.GetValue(0, r).GetValue<int32_t>();
            cnt[g] = c.GetValue(2, r).GetValue<int64_t>();
            sum[g] = c.GetValue(1, r).GetValue<double>();
            rows++;
        }
    }
    CHECK(rows == G);
    bool all_ok = true;
    for (int g = 0; g < G; g++) {
        all_ok = all_ok && cnt[g] == 2 && sum[g] == 2.0;
    }
    CHECK(all_ok);

    for (auto &ob : out) alloc.Free(ob.data, ob.size);
    for (auto &b : blocks) alloc.Free(b.data, b.size);
}

int main() {
    return plume_test::RunAll();
}
