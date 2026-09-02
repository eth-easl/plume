// Dynamic filter tests:
//  (1) DynamicFilterBuildTemplate survives pipeline serialize/deserialize
//  (2) DynamicFilterBuildOperator passes every row through unchanged while
//      accumulating a running min/max, retrievable via Executor::FindDynamicFilterBuild()
//  (3) DynamicFilterBounds itself round-trips through the wire format
//  (4) BuildRangeFilter produces a predicate that actually prunes as expected

#include "exec_test_util.hpp"
#include "test_util.hpp"

#include "plume/common/serial.hpp"
#include "plume/execution/executor.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/expression/expression.hpp"
#include "plume/expression/expression_builder.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <cstring>
#include <optional>
#include <string>
#include <vector>

using namespace plume;
using namespace plume::exec;
using namespace plume::expr;
using namespace plume::memory;
using duckdb::DataChunk;
using duckdb::ExpressionExecutor;
using duckdb::FlatVector;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema AB() {
    Schema s;
    s.columns = {{"a", I32(), false}, {"b", I32(), true}};
    return s;
}

// Build an int32 block from column-major data; a negative sentinel in the (nullable)
// second column marks a NULL.
OwnedBlock MakeBlock(Allocator &alloc, const Schema &schema, const std::vector<std::vector<int32_t>> &cols,
                     int32_t null_sentinel = -1000) {
    duckdb::vector<LogicalType> types;
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }
    idx_t rows = cols.empty() ? 0 : cols[0].size();
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), types);
    for (size_t c = 0; c < cols.size(); c++) {
        for (idx_t r = 0; r < rows; r++) {
            if (cols[c][r] == null_sentinel) {
                chunk.SetValue(c, r, Value(LogicalType::INTEGER));
            } else {
                chunk.SetValue(c, r, Value::INTEGER(cols[c][r]));
            }
        }
    }
    chunk.SetCardinality(rows);
    return ExportChunk(schema, chunk, alloc).unwrap();
}

ColumnType Varchar() { return {TypeId::VARCHAR}; }

Schema OneVarchar() {
    Schema s;
    s.columns = {{"s", Varchar(), true}};
    return s;
}

OwnedBlock MakeVarcharBlock(Allocator &alloc, const Schema &schema,
                           const std::vector<std::optional<std::string>> &vals) {
    duckdb::vector<LogicalType> types = {LogicalType::VARCHAR};
    idx_t rows = vals.size();
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), types);
    for (idx_t r = 0; r < rows; r++) {
        if (vals[r].has_value()) {
            chunk.SetValue(0, r, Value(*vals[r]));
        } else {
            chunk.SetValue(0, r, Value(LogicalType::VARCHAR));
        }
    }
    chunk.SetCardinality(rows);
    return ExportChunk(schema, chunk, alloc).unwrap();
}

} // namespace

TEST_CASE("DynamicFilterBuildTemplate: pipeline serialize/deserialize round-trip") {
    PipelineTemplate p;
    p.input_schema = AB();

    auto dyn = std::make_shared<DynamicFilterBuildTemplate>(1);
    p.operators = {dyn};

    auto bytes = plume::SerializePipeline(p);
    auto q = plume::DeserializePipeline(bytes).unwrap();

    CHECK(q.operators.size() == 1);
    CHECK(q.operators[0]->type == OpType::DYNAMIC_FILTER_BUILD);
    auto &t = static_cast<const DynamicFilterBuildTemplate &>(*q.operators[0]);
    CHECK(t.column == 1);
    CHECK(t.Equals(*dyn));

    auto bytes2 = plume::SerializePipeline(q);
    CHECK_MSG(bytes == bytes2, "re-serialized pipeline differs");
}

TEST_CASE("DynamicFilterBuildOperator: passthrough + running min/max across blocks") {
    Allocator alloc;
    auto schema = AB();

    // column 0 is the one we're building bounds over.
    auto b0 = MakeBlock(alloc, schema, {{10, -5, 30}, {1, 2, 3}});
    auto b1 = MakeBlock(alloc, schema, {{100, -1000, 7}, {4, 5, 6}}); // row 1's col0 is NULL

    PipelineTemplate desc;
    desc.input_schema = schema;
    desc.operators = {std::make_shared<DynamicFilterBuildTemplate>(0)};

    // Drive the Executor by hand (rather than exec_test_util's RunBlocks, which also
    // calls Finish()) so we can inspect Bounds() mid-stream and after Finish() — bounds
    // accumulate incrementally as each chunk is pushed (no separate finalize step), so
    // the host is only expected to read them once all input has been pushed.
    std::vector<OwnedBlock> out;
    exec::OutputEmit emit = [&](const std::string &, size_t, DataBuffer buf, size_t) {
        OwnedBlock ob;
        ob.size = buf.size();
        ob.data = alloc.Allocate(buf.size());
        std::memcpy(ob.data, buf.data(), buf.size());
        out.push_back(ob);
    };

    Executor exec(alloc);
    exec.Build(desc, emit).unwrap();

    auto *dyn = exec.FindDynamicFilterBuild();
    CHECK(dyn != nullptr);
    CHECK_MSG(!dyn->Bounds().valid, "bounds should not be valid before any row was pushed");

    exec.PushBlocks({{b0.data, b0.size}}).unwrap();
    // already correct for what's been seen so far: min/max over {10, -5, 30} = -5 / 30
    CHECK(dyn->Bounds().valid);
    CHECK(dyn->Bounds().min == Value::INTEGER(-5));
    CHECK(dyn->Bounds().max == Value::INTEGER(30));

    exec.PushBlocks({{b1.data, b1.size}}).unwrap();
    exec.Finish().unwrap();

    const auto &bounds = dyn->Bounds();
    CHECK(bounds.valid);
    // NULL (-1000 sentinel) skipped; min/max over {10, -5, 30, 100, 7} = -5 / 100
    CHECK(bounds.min == Value::INTEGER(-5));
    CHECK(bounds.max == Value::INTEGER(100));

    idx_t total_rows = 0;
    for (auto &block : out) {
        Schema out_schema;
        auto chunks = ImportBlockChunks(block.data, block.size, out_schema).unwrap();
        for (auto &chunk : chunks) {
            total_rows += chunk->size();
        }
    }
    CHECK(total_rows == 6);

    for (auto &block : out) {
        alloc.Free(block.data, block.size);
    }
    alloc.Free(b0.data, b0.size);
    alloc.Free(b1.data, b1.size);
}

TEST_CASE("DynamicFilterBuildOperator: VARCHAR min/max, including a non-inlined string") {
    Allocator alloc;
    auto schema = OneVarchar();

    // string_t inlines up to 12 bytes; this one is long enough to force a heap-backed
    // string, exercising the copy-into-an-owned-Value path across chunk lifetimes.
    std::string long_str(40, 'z');
    auto b0 = MakeVarcharBlock(alloc, schema, {std::string("banana"), std::nullopt, std::string("apple"), long_str});

    PipelineTemplate desc;
    desc.input_schema = schema;
    desc.operators = {std::make_shared<DynamicFilterBuildTemplate>(0)};

    Executor exec(alloc);
    auto out = plume_test::RunBlocks(exec, desc, {{b0.data, b0.size}}, alloc);

    auto *dyn = exec.FindDynamicFilterBuild();
    CHECK(dyn != nullptr);
    const auto &bounds = dyn->Bounds();
    CHECK(bounds.valid);
    CHECK(bounds.min == Value(std::string("apple")));
    CHECK(bounds.max == Value(long_str));

    for (auto &block : out) {
        alloc.Free(block.data, block.size);
    }
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("DynamicFilterBuildOperator: empty input yields an invalid bounds") {
    Allocator alloc;
    auto schema = AB();

    PipelineTemplate desc;
    desc.input_schema = schema;
    desc.operators = {std::make_shared<DynamicFilterBuildTemplate>(0)};

    Executor exec(alloc);
    plume_test::RunBlocks(exec, desc, {}, alloc);

    auto *dyn = exec.FindDynamicFilterBuild();
    CHECK(dyn != nullptr);
    CHECK_MSG(!dyn->Bounds().valid, "no rows ever pushed -> bounds must stay invalid");
}

TEST_CASE("DynamicFilterBounds: wire round-trip") {
    DynamicFilterBounds b;
    b.valid = true;
    b.min = Value::INTEGER(-5);
    b.max = Value::INTEGER(100);

    auto buf = SerializeToBuffer(b);
    auto b2 = DeserializeFromBytes<DynamicFilterBounds>(buf.data(), buf.size());

    CHECK(b2.valid);
    CHECK(b2.min == Value::INTEGER(-5));
    CHECK(b2.max == Value::INTEGER(100));

    DynamicFilterBounds invalid;
    auto buf2 = SerializeToBuffer(invalid);
    auto invalid2 = DeserializeFromBytes<DynamicFilterBounds>(buf2.data(), buf2.size());
    CHECK(!invalid2.valid);
}

TEST_CASE("BuildRangeFilter: invalid bounds -> no filter") {
    DynamicFilterBounds b;
    CHECK(!BuildRangeFilter(0, I32(), b).has_value());
}

TEST_CASE("BuildRangeFilter: min == max -> single equality, prunes correctly") {
    DynamicFilterBounds b;
    b.valid = true;
    b.min = b.max = Value::INTEGER(42);

    auto node = BuildRangeFilter(0, I32(), b);
    CHECK(node.has_value());
    CHECK(node->kind == ExprKind::COMPARISON);

    Allocator alloc;
    DataChunk input;
    input.Initialize(alloc.Get(), {LogicalType::INTEGER});
    const idx_t n = 3;
    auto col = FlatVector::GetData<int32_t>(input.data[0]);
    col[0] = 41;
    col[1] = 42;
    col[2] = 43;
    input.SetCardinality(n);

    auto expr = BuildExpression(*node).unwrap();
    ExpressionExecutor executor;
    executor.AddExpression(*expr);
    DataChunk result;
    result.Initialize(alloc.Get(), {LogicalType::BOOLEAN});
    executor.Execute(input, result);
    auto pred = FlatVector::GetData<bool>(result.data[0]);
    CHECK(!pred[0]);
    CHECK(pred[1]);
    CHECK(!pred[2]);
}

TEST_CASE("BuildRangeFilter: min != max -> closed range, prunes correctly") {
    DynamicFilterBounds b;
    b.valid = true;
    b.min = Value::INTEGER(10);
    b.max = Value::INTEGER(20);

    auto node = BuildRangeFilter(0, I32(), b);
    CHECK(node.has_value());
    CHECK(node->kind == ExprKind::CONJUNCTION);
    CHECK(node->children.size() == 2);

    Allocator alloc;
    DataChunk input;
    input.Initialize(alloc.Get(), {LogicalType::INTEGER});
    const idx_t n = 5;
    auto col = FlatVector::GetData<int32_t>(input.data[0]);
    col[0] = 5;
    col[1] = 10;
    col[2] = 15;
    col[3] = 20;
    col[4] = 25;
    input.SetCardinality(n);

    auto expr = BuildExpression(*node).unwrap();
    ExpressionExecutor executor;
    executor.AddExpression(*expr);
    DataChunk result;
    result.Initialize(alloc.Get(), {LogicalType::BOOLEAN});
    executor.Execute(input, result);
    auto pred = FlatVector::GetData<bool>(result.data[0]);
    CHECK(!pred[0]);
    CHECK(pred[1]);
    CHECK(pred[2]);
    CHECK(pred[3]);
    CHECK(!pred[4]);
}

int main() { return plume_test::RunAll(); }
