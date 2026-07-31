// Milestone-3 tests:
//  (1) pipeline serialize -> deserialize round-trip (structural equality)
//  (2) R2 end-to-end proof: build Plume IR, reconstruct DuckDB bound
//      expressions via the registry (no instance), run them through
//      ExpressionExecutor over a DataChunk, and verify results.

#include "test_util.hpp"

#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/limit.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/execution/operators/top_n.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/expression/expression.hpp"
#include "plume/expression/expression_builder.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"

using namespace plume;
using namespace plume::exec;
using namespace plume::expr;
using namespace plume::memory;
using duckdb::Value;
using duckdb::DataChunk;
using duckdb::FlatVector;
using duckdb::LogicalType;
using duckdb::ExpressionExecutor;
using duckdb::ExpressionType;
using duckdb::idx_t;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

// col0 + col1   (both INT32)
ExprNode AddCols() {
    return ExprNode::Function("+", I32(), {ExprNode::Reference(0, I32()), ExprNode::Reference(1, I32())});
}

// (col0 < 100) AND (col1 > 0)
ExprNode Predicate() {
    auto lt = ExprNode::Comparison(ExpressionType::COMPARE_LESSTHAN, ExprNode::Reference(0, I32()),
                                   ExprNode::Constant(Value::INTEGER(100), I32()));
    auto gt = ExprNode::Comparison(ExpressionType::COMPARE_GREATERTHAN, ExprNode::Reference(1, I32()),
                                   ExprNode::Constant(Value::INTEGER(0), I32()));
    return ExprNode::Conjunction(ExpressionType::CONJUNCTION_AND, {std::move(lt), std::move(gt)});
}

PipelineTemplate SamplePipeline() {
    PipelineTemplate p;
    p.input_schema.columns = {{"a", I32(), false}, {"b", I32(), true}};

    auto filter = std::make_shared<FilterTemplate>();
    filter->type = OpType::FILTER;
    filter->filter = Predicate();

    auto proj = std::make_shared<ProjectionTemplate>();
    proj->type = OpType::PROJECTION;
    proj->projections = {AddCols(), ExprNode::Reference(0, I32())};

    auto limit = std::make_shared<LimitTemplate>();
    limit->type = OpType::LIMIT;
    limit->has_limit = true;
    limit->limit = 10;
    limit->offset = 2;

    p.operators = {filter, proj, limit};
    return p;
}

} // namespace

TEST_CASE("pipeline serialize/deserialize round-trip") {
    auto p = SamplePipeline();
    auto bytes = plume::SerializePipeline(p);
    CHECK(bytes.size() > 8);
    auto q = plume::DeserializePipeline(bytes).unwrap();

    CHECK(q.input_schema.columns.size() == 2);
    CHECK(q.input_schema.columns[0].name == "a");
    CHECK(q.input_schema.columns[1].nullable == true);
    CHECK(q.operators.size() == 3);

    CHECK(q.operators[0]->type == OpType::FILTER);
    auto &f = static_cast<const FilterTemplate &>(*q.operators[0]);
    CHECK(f.filter.kind == ExprKind::CONJUNCTION);
    CHECK(f.filter.children.size() == 2);
    CHECK(f.filter.children[0].kind == ExprKind::COMPARISON);
    // constant survived
    CHECK(f.filter.children[0].children[1].constant.GetValue<int32_t>() == 100);

    CHECK(q.operators[1]->type == OpType::PROJECTION);
    auto &pr = static_cast<const ProjectionTemplate &>(*q.operators[1]);
    CHECK(pr.projections.size() == 2);
    CHECK(pr.projections[0].kind == ExprKind::FUNCTION);
    CHECK(pr.projections[0].func_name == "+");

    CHECK(q.operators[2]->type == OpType::LIMIT);
    auto &lm = static_cast<const LimitTemplate &>(*q.operators[2]);
    CHECK(lm.has_limit == true);
    CHECK(lm.limit == 10);
    CHECK(lm.offset == 2);

    // re-serialize must be byte-identical (deterministic format)
    auto bytes2 = plume::SerializePipeline(q);
    CHECK_MSG(bytes == bytes2, "re-serialized pipeline differs");
}

TEST_CASE("R2: IR -> bound expression -> ExpressionExecutor, no instance") {
    Allocator alloc;

    DataChunk input;
    input.Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::INTEGER});
    const idx_t n = 6;
    auto a = FlatVector::GetData<int32_t>(input.data[0]);
    auto b = FlatVector::GetData<int32_t>(input.data[1]);
    for (idx_t i = 0; i < n; i++) {
        a[i] = int32_t(i) * 30; // 0 30 60 90 120 150
        b[i] = int32_t(i) - 2;  // -2 -1 0 1 2 3
    }
    input.SetCardinality(n);

    // Reconstruct bound expressions from Plume IR (registry-resolved '+').
    auto add_res = plume::expr::BuildExpression(AddCols());
    auto pred_res = plume::expr::BuildExpression(Predicate());
    CHECK(add_res.is_ok());
    CHECK(pred_res.is_ok());
    auto add_expr = std::move(add_res).unwrap();
    auto pred_expr = std::move(pred_res).unwrap();

    ExpressionExecutor executor; // context-less
    CHECK(!executor.HasContext());
    executor.AddExpression(*add_expr);
    executor.AddExpression(*pred_expr);

    DataChunk result;
    result.Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::BOOLEAN});
    executor.Execute(input, result);

    auto sum = FlatVector::GetData<int32_t>(result.data[0]);
    auto pred = FlatVector::GetData<bool>(result.data[1]);
    for (idx_t i = 0; i < n; i++) {
        CHECK_MSG(sum[i] == a[i] + b[i], "addition kernel result wrong");
        bool expected = (a[i] < 100) && (b[i] > 0);
        CHECK_MSG(pred[i] == expected, "predicate result wrong");
    }
}

TEST_CASE("TOP_N pipeline serialize/deserialize round-trip") {
    PipelineTemplate p;
    p.input_schema.columns = {{"a", I32(), false}, {"b", I32(), true}};

    auto top_n = std::make_shared<TopNTemplate>();
    top_n->type = OpType::TOP_N;
    SortKey key;
    key.expr = ExprNode::Reference(0, I32());
    key.order = SortOrder::DESCENDING;
    key.null_order = NullOrder::NULLS_FIRST;
    top_n->sort_keys = {key};
    top_n->limit = 10;
    top_n->offset = 2;
    p.operators = {top_n};

    auto bytes = plume::SerializePipeline(p);
    auto q = plume::DeserializePipeline(bytes).unwrap();

    CHECK(q.operators.size() == 1);
    CHECK(q.operators[0]->type == OpType::TOP_N);
    auto &tn = static_cast<const TopNTemplate &>(*q.operators[0]);
    CHECK(tn.sort_keys.size() == 1);
    CHECK(tn.sort_keys[0].order == SortOrder::DESCENDING);
    CHECK(tn.sort_keys[0].null_order == NullOrder::NULLS_FIRST);
    CHECK(tn.limit == 10);
    CHECK(tn.offset == 2);

    auto bytes2 = plume::SerializePipeline(q);
    CHECK_MSG(bytes == bytes2, "re-serialized pipeline differs");
}

int main() {
    return plume_test::RunAll();
}
