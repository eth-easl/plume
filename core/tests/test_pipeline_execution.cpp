// Milestone-4 end-to-end test: input blocks -> (filter, projection, limit) ->
// output blocks, with no DuckDB instance. Also runs the pipeline through
// serialize/deserialize first, exercising the full M3+M4 chain.

#include "exec_test_util.hpp"
#include "test_util.hpp"

#include "plume/execution/executor.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/limit.hpp"
#include "plume/execution/operators/projection.hpp"
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
using duckdb::ExpressionType;
using duckdb::idx_t;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema AB() {
    Schema s;
    s.columns = {{"a", I32(), false}, {"b", I32(), false}};
    return s;
}

// Build an int32, non-nullable block from column-major data.
OwnedBlock MakeBlock(Allocator &alloc, const Schema &schema, const std::vector<std::vector<int32_t>> &cols) {
    duckdb::vector<LogicalType> types;
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }
    idx_t rows = cols.empty() ? 0 : cols[0].size();
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), types);
    for (size_t c = 0; c < cols.size(); c++) {
        for (idx_t r = 0; r < rows; r++) {
            chunk.SetValue(c, r, Value::INTEGER(cols[c][r]));
        }
    }
    chunk.SetCardinality(rows);
    return ExportChunk(schema, chunk, alloc).unwrap();
}

ExprNode Ref(uint32_t i) { return ExprNode::Reference(i, I32()); }
ExprNode Lit(int32_t v) { return ExprNode::Constant(Value::INTEGER(v), I32()); }

} // namespace

TEST_CASE("filter + projection + limit/offset across multiple blocks") {
    Allocator alloc;
    auto schema = AB();

    // 2 input blocks (limit/offset state must span them).
    auto b0 = MakeBlock(alloc, schema, {{0, 30, 60, 90, 120, 150}, {-2, -1, 0, 1, 2, 3}});
    auto b1 = MakeBlock(alloc, schema, {{10, 200, 40}, {5, 6, -7}});

    // Pipeline: WHERE a<100 AND b>0 ; SELECT a+b, a ; LIMIT 3 OFFSET 1
    PipelineTemplate desc;
    desc.input_schema = schema;

    auto filter = std::make_shared<FilterTemplate>();
    filter->type = OpType::FILTER;
    filter->filter = ExprNode::Conjunction(
        ExpressionType::CONJUNCTION_AND,
        {ExprNode::Comparison(ExpressionType::COMPARE_LESSTHAN, Ref(0), Lit(100)),
         ExprNode::Comparison(ExpressionType::COMPARE_GREATERTHAN, Ref(1), Lit(0))});

    auto proj = std::make_shared<ProjectionTemplate>();
    proj->type = OpType::PROJECTION;
    proj->projections = {ExprNode::Function("+", I32(), {Ref(0), Ref(1)}), Ref(0)};

    auto limit = std::make_shared<LimitTemplate>();
    limit->type = OpType::LIMIT;
    limit->has_limit = true;
    limit->limit = 3;
    limit->offset = 1;

    desc.operators = {filter, proj, limit};

    // Round-trip the description through serialization before executing.
    auto bytes = plume::SerializePipeline(desc);
    auto desc2 = plume::DeserializePipeline(bytes).unwrap();

    Executor exec(alloc);

    auto outputs = plume_test::RunBlocks(exec, desc2, {{b0.data, b0.size}, {b1.data, b1.size}}, alloc);

    // Surviving rows after filter (in order): (90,1),(10,5)
    // Projected (a+b, a): (91,90),(15,10)
    // After OFFSET 1, LIMIT 3: [(15,10)]
    idx_t total = 0;
    int32_t got_sum = -1, got_a = -1;
    for (auto &ob : outputs) {
        DataChunk c;
        Schema s;
        ImportBlock(ob.data, ob.size, c, s).unwrap();
        for (idx_t r = 0; r < c.size(); r++) {
            got_sum = c.GetValue(0, r).GetValue<int32_t>();
            got_a = c.GetValue(1, r).GetValue<int32_t>();
            total++;
        }
    }
    CHECK_MSG(total == 1, "expected exactly 1 output row after limit/offset");
    CHECK(got_sum == 15);
    CHECK(got_a == 10);

    // Output schema: a+b -> "expr0", bare ref -> input name "a"
    CHECK(exec.OutputSchema().columns.size() == 2);
    CHECK(exec.OutputSchema().columns[0].name == "expr0");
    CHECK(exec.OutputSchema().columns[1].name == "a");

    for (auto &ob : outputs) {
        alloc.Free(ob.data, ob.size);
    }
    alloc.Free(b0.data, b0.size);
    alloc.Free(b1.data, b1.size);
}

TEST_CASE("reference-only projection: zero-copy column drop + reorder") {
    Allocator alloc;
    Schema schema;
    schema.columns = {{"a", I32(), false}, {"b", I32(), false}, {"c", I32(), false}};
    auto blk = MakeBlock(alloc, schema, {{1, 2, 3}, {10, 20, 30}, {100, 200, 300}});

    // SELECT c, a  — pure column selection (drops b, reorders). This is the shape
    // pushdown's projection_ids emits; it must take the zero-copy Reference path.
    PipelineTemplate desc;
    desc.input_schema = schema;
    auto proj = std::make_shared<ProjectionTemplate>();
    proj->type = OpType::PROJECTION;
    proj->projections = {Ref(2), Ref(0)};
    desc.operators = {proj};

    Executor exec(alloc);

    auto outputs = plume_test::RunBlocks(exec, desc, {{blk.data, blk.size}}, alloc);

    std::vector<std::pair<int32_t, int32_t>> rows;
    for (auto &ob : outputs) {
        DataChunk c;
        Schema s;
        ImportBlock(ob.data, ob.size, c, s).unwrap();
        for (idx_t r = 0; r < c.size(); r++) {
            rows.emplace_back(c.GetValue(0, r).GetValue<int32_t>(), c.GetValue(1, r).GetValue<int32_t>());
        }
    }
    CHECK(rows.size() == 3);
    CHECK(rows[0] == std::make_pair(100, 1));
    CHECK(rows[1] == std::make_pair(200, 2));
    CHECK(rows[2] == std::make_pair(300, 3));
    CHECK(exec.OutputSchema().columns[0].name == "c");
    CHECK(exec.OutputSchema().columns[1].name == "a");

    for (auto &ob : outputs) {
        alloc.Free(ob.data, ob.size);
    }
    alloc.Free(blk.data, blk.size);
}

TEST_CASE("projection arithmetic over a full block, no filtering") {
    Allocator alloc;
    auto schema = AB();
    auto b0 = MakeBlock(alloc, schema, {{1, 2, 3, 4}, {10, 20, 30, 40}});

    PipelineTemplate desc;
    desc.input_schema = schema;
    auto proj = std::make_shared<ProjectionTemplate>();
    proj->type = OpType::PROJECTION;
    proj->projections = {ExprNode::Function("*", I32(), {Ref(0), Ref(1)})}; // a*b
    desc.operators = {proj};

    Executor exec(alloc);

    auto outputs = plume_test::RunBlocks(exec, desc, {{b0.data, b0.size}}, alloc);
    CHECK(outputs.size() == 1);

    DataChunk c;
    Schema s;
    ImportBlock(outputs[0].data, outputs[0].size, c, s).unwrap();
    CHECK(c.size() == 4);
    int32_t expected[4] = {10, 40, 90, 160};
    for (idx_t r = 0; r < 4; r++) {
        CHECK(c.GetValue(0, r).GetValue<int32_t>() == expected[r]);
    }

    alloc.Free(outputs[0].data, outputs[0].size);
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("projection cast int32 -> double, no instance") {
    Allocator alloc;
    auto schema = AB();
    auto b0 = MakeBlock(alloc, schema, {{1, 2, 3, 4}, {10, 20, 30, 40}});

    // SELECT CAST(a AS DOUBLE), b  — exercises Plume's instance-free CAST builder
    // (DefaultCasts) over a real chunk, no DuckDB instance anywhere.
    PipelineTemplate desc;
    desc.input_schema = schema;
    auto proj = std::make_shared<ProjectionTemplate>();
    proj->type = OpType::PROJECTION;
    proj->projections = {ExprNode::Cast(Ref(0), {TypeId::DOUBLE}, /*try_cast=*/false), Ref(1)};
    desc.operators = {proj};

    // Round-trip through serialization to cover the CAST node on the wire too.
    auto bytes = plume::SerializePipeline(desc);
    Executor exec(alloc);
    auto outputs = plume_test::RunBlocks(exec, plume::DeserializePipeline(bytes).unwrap(), {{b0.data, b0.size}}, alloc);
    CHECK(outputs.size() == 1);

    DataChunk c;
    Schema s;
    ImportBlock(outputs[0].data, outputs[0].size, c, s).unwrap();
    CHECK(c.size() == 4);
    CHECK(c.data[0].GetType() == LogicalType::DOUBLE);
    double expected[4] = {1.0, 2.0, 3.0, 4.0};
    for (idx_t r = 0; r < 4; r++) {
        CHECK(c.GetValue(0, r).GetValue<double>() == expected[r]);
        CHECK(c.GetValue(1, r).GetValue<int32_t>() == (int32_t)((r + 1) * 10));
    }

    alloc.Free(outputs[0].data, outputs[0].size);
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("projection float division, instance-free bind shim") {
    Allocator alloc;
    // Two DOUBLE columns x, y.
    Schema schema;
    schema.columns = {{"x", {TypeId::DOUBLE}, false}, {"y", {TypeId::DOUBLE}, false}};

    DataChunk chunk;
    chunk.Initialize(alloc.Get(), {LogicalType::DOUBLE, LogicalType::DOUBLE});
    std::vector<double> xs = {10.0, 9.0, 7.0, 1.0};
    std::vector<double> ys = {4.0, 3.0, 2.0, 8.0};
    for (idx_t r = 0; r < xs.size(); r++) {
        chunk.SetValue(0, r, Value::DOUBLE(xs[r]));
        chunk.SetValue(1, r, Value::DOUBLE(ys[r]));
    }
    chunk.SetCardinality(xs.size());
    auto b0 = ExportChunk(schema, chunk, alloc).unwrap();

    // SELECT x / y  — '/'(DOUBLE,DOUBLE) has a null kernel + bind callback in
    // DuckDB; Plume must bind the kernel with no instance.
    PipelineTemplate desc;
    desc.input_schema = schema;
    auto proj = std::make_shared<ProjectionTemplate>();
    proj->type = OpType::PROJECTION;
    proj->projections = {ExprNode::Function("/", {TypeId::DOUBLE},
                                            {ExprNode::Reference(0, {TypeId::DOUBLE}),
                                             ExprNode::Reference(1, {TypeId::DOUBLE})})};
    desc.operators = {proj};

    Executor exec(alloc);

    auto outputs = plume_test::RunBlocks(exec, plume::DeserializePipeline(plume::SerializePipeline(desc)).unwrap(),
                                         {{b0.data, b0.size}}, alloc);
    CHECK(outputs.size() == 1);

    DataChunk c;
    Schema s;
    ImportBlock(outputs[0].data, outputs[0].size, c, s).unwrap();
    CHECK(c.size() == 4);
    double expected[4] = {2.5, 3.0, 3.5, 0.125};
    for (idx_t r = 0; r < 4; r++) {
        CHECK(c.GetValue(0, r).GetValue<double>() == expected[r]);
    }

    alloc.Free(outputs[0].data, outputs[0].size);
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("projection CASE expression, instance-free") {
    Allocator alloc;
    auto schema = AB();
    auto b0 = MakeBlock(alloc, schema, {{1, 200, 3, 40}, {10, 20, 30, 4}});

    // SELECT CASE WHEN a < b THEN a ELSE b END  (i.e. min(a, b)).
    // children layout: [when, then, else].
    PipelineTemplate desc;
    desc.input_schema = schema;
    auto proj = std::make_shared<ProjectionTemplate>();
    proj->type = OpType::PROJECTION;
    proj->projections = {ExprNode::Case(
        {ExprNode::Comparison(ExpressionType::COMPARE_LESSTHAN, Ref(0), Ref(1)), Ref(0), Ref(1)}, I32())};
    desc.operators = {proj};

    Executor exec(alloc);

    auto outputs = plume_test::RunBlocks(exec, plume::DeserializePipeline(plume::SerializePipeline(desc)).unwrap(),
                                         {{b0.data, b0.size}}, alloc);
    CHECK(outputs.size() == 1);

    DataChunk c;
    Schema s;
    ImportBlock(outputs[0].data, outputs[0].size, c, s).unwrap();
    CHECK(c.size() == 4);
    int32_t expected[4] = {1, 20, 3, 4};
    for (idx_t r = 0; r < 4; r++) {
        CHECK(c.GetValue(0, r).GetValue<int32_t>() == expected[r]);
    }

    alloc.Free(outputs[0].data, outputs[0].size);
    alloc.Free(b0.data, b0.size);
}

int main() {
    return plume_test::RunAll();
}
