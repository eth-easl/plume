#include "plume/execution/operators/filter.hpp"

#include "plume/expression/expression_builder.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

#include <algorithm>

namespace plume::exec {

using expr::BuildExpression;

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::SelectionVector;
using duckdb::unique_ptr;

namespace {

void CollectRefs(const duckdb::Expression &e, std::vector<uint32_t> &out) {
    if (e.type == duckdb::ExpressionType::BOUND_REF) {
        out.push_back(static_cast<uint32_t>(e.Cast<duckdb::BoundReferenceExpression>().index));
    }
    duckdb::ExpressionIterator::EnumerateChildren(
        e, [&](const duckdb::Expression &child) { CollectRefs(child, out); });
}

} // namespace

//===----------------------------------------------------------------------===//
// FilterTemplate
//===----------------------------------------------------------------------===//

void FilterTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(101, "filter", filter);
}

//===----------------------------------------------------------------------===//
// FilterOperator
//===----------------------------------------------------------------------===//

FilterOperator::FilterOperator(unique_ptr<duckdb::Expression> predicate, duckdb::vector<LogicalType> output_types)
    : Operator(std::move(output_types)), predicate_(std::move(predicate)) {
    executor_.AddExpression(*predicate_);
    CollectRefs(*predicate_, filter_cols_);
    std::sort(filter_cols_.begin(), filter_cols_.end());
    filter_cols_.erase(std::unique(filter_cols_.begin(), filter_cols_.end()), filter_cols_.end());
}

Result<void> FilterOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    if (chunk->size() == 0) {
        return Ok();
    }

    SelectionVector sel(STANDARD_VECTOR_SIZE);
    idx_t count = executor_.SelectExpression(*chunk, sel);
    if (count == 0) {
        return Ok(); // empty `chunk` dropped here
    }
    if (count < chunk->size()) {
        chunk->Slice(sel, count);
    }

    return next_->Push(std::move(chunk));
}

Result<void> FilterOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    // not a blocking operator -> just forward the signal to the next operator
    return next_->Finish();
}

idx_t FilterOperator::Select(DataChunk &input, SelectionVector &sel) {
    return executor_.SelectExpression(input, sel);
}

//===----------------------------------------------------------------------===//
// Build
//===----------------------------------------------------------------------===//

Result<std::unique_ptr<Operator>> BuildFilterTemplate(std::shared_ptr<FilterTemplate> templ,
        duckdb::vector<LogicalType> col_types) {
    TRY(auto predicate, BuildExpression(templ->filter));
    return std::unique_ptr<Operator>(
        std::make_unique<FilterOperator>(std::move(predicate), std::move(col_types)));
}

} // namespace plume::exec
