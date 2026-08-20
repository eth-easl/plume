#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/execution/expression_executor.hpp"

namespace plume::exec {

struct ProjectionTemplate : OperatorTemplate {
    std::vector<expr::ExprNode> projections;

    ProjectionTemplate() : OperatorTemplate(OpType::PROJECTION) {}
    ProjectionTemplate(std::vector<expr::ExprNode> projections)
        : OperatorTemplate(OpType::PROJECTION), projections(std::move(projections)) {}

    bool Equals(const OperatorTemplate &other) const override;
    void Serialize(duckdb::Serializer &s) const override;
};

class ProjectionOperator : public Operator {
public:
    ProjectionOperator(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions,
                       duckdb::vector<duckdb::LogicalType> output_types, memory::Allocator &alloc);

    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;
    Result<void> Finish() override;

private:
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions_;
    duckdb::ExpressionExecutor executor_;
    memory::Allocator alloc_;

    bool references_only_ = false; // fast path for pure column-selection projection (-> zero-copy)
    std::vector<duckdb::idx_t> ref_indices_; // input column index per output column
};

Result<std::unique_ptr<Operator>> BuildProjectionTemplate(std::shared_ptr<ProjectionTemplate> templ,
    duckdb::vector<duckdb::LogicalType> col_types, memory::Allocator &alloc);

Schema ProjectionSchema(const std::vector<expr::ExprNode> &projections, const Schema &input_schema);

} // namespace plume::exec
