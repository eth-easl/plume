#include "plume/execution/operators/projection.hpp"

#include "plume/expression/expression_builder.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace plume::exec {

using plume::memory::Allocator;
using expr::BuildExpression;

using duckdb::DataChunk;
using duckdb::LogicalType;
using duckdb::unique_ptr;

//===----------------------------------------------------------------------===//
// ProjectionTemplate
//===----------------------------------------------------------------------===//

void ProjectionTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteList(101, "projections", projections.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteElement(projections[i]);
    });
}

//===----------------------------------------------------------------------===//
// ProjectionOperator
//===----------------------------------------------------------------------===//

ProjectionOperator::ProjectionOperator(duckdb::vector<unique_ptr<duckdb::Expression>> expressions,
        duckdb::vector<LogicalType> output_types, memory::Allocator &alloc)
    : Operator(std::move(output_types)), expressions_(std::move(expressions)), alloc_(alloc) {
    references_only_ = true;
    for (auto &expr : expressions_) {
        if (expr->type == duckdb::ExpressionType::BOUND_REF) {
            ref_indices_.push_back(expr->Cast<duckdb::BoundReferenceExpression>().index);
        } else {
            references_only_ = false;
        }
    }
    if (references_only_) {
        return; // no ExpressionExecutor needed
    }
    ref_indices_.clear();
    for (auto &expr : expressions_) {
        executor_.AddExpression(*expr);
    }
}

Result<void> ProjectionOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    if (chunk->size() == 0) {
        return Ok();
    }
    auto result = std::make_unique<DataChunk>();
    if (references_only_) {
        // Reference (zero-copy) the selected input columns.
        result->InitializeEmpty(output_types_);
        for (size_t i = 0; i < ref_indices_.size(); i++) {
            result->data[i].Reference(chunk->data[ref_indices_[i]]);
        }
        result->SetCardinality(chunk->size());
    } else {
        result->Initialize(alloc_.Get(), output_types_);
        executor_.Execute(*chunk, *result);
    }
    return next_->Push(std::move(result));
}

Result<void> ProjectionOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    // not a blocking operator -> just forward the signal to the next operator
    return next_->Finish();
}

//===----------------------------------------------------------------------===//
// Build / schema
//===----------------------------------------------------------------------===//

Result<std::unique_ptr<Operator>> BuildProjectionTemplate(std::shared_ptr<ProjectionTemplate> templ,
        duckdb::vector<LogicalType> col_types, memory::Allocator &alloc) {
    (void)col_types; // output types derive from the projection expressions
    duckdb::vector<unique_ptr<duckdb::Expression>> exprs;
    duckdb::vector<LogicalType> out_types;
    exprs.reserve(templ->projections.size());
    for (auto &pe : templ->projections) {
        TRY(auto built, BuildExpression(pe));
        out_types.push_back(built->return_type);
        exprs.push_back(std::move(built));
    }
    return std::unique_ptr<Operator>(std::make_unique<ProjectionOperator>(std::move(exprs), out_types, alloc));
}

Schema ProjectionSchema(const std::vector<Expression> &projections, const Schema &input_schema) {
    Schema out;
    out.columns.reserve(projections.size());
    for (size_t i = 0; i < projections.size(); i++) {
        const auto &p = projections[i];
        Column col;
        if (p.kind == expr::ExprKind::REFERENCE && p.ref_index < input_schema.columns.size()) {
            col.name = input_schema.columns[p.ref_index].name;
        } else {
            col.name = "expr" + std::to_string(i);
        }
        col.type = p.return_type;
        col.nullable = true;
        out.columns.push_back(std::move(col));
    }
    return out;
}

} // namespace plume::exec
