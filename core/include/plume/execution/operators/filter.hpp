#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"

#include "duckdb/execution/expression_executor.hpp"

namespace plume::exec {

struct FilterTemplate : OperatorTemplate {
    expr::ExprNode filter;

    FilterTemplate() : OperatorTemplate(OpType::FILTER) {}
    FilterTemplate(expr::ExprNode filter) 
        : OperatorTemplate(OpType::FILTER), filter(std::move(filter)) {}

    void Serialize(duckdb::Serializer &s) const override;
};

class FilterOperator : public Operator {
public:
    FilterOperator(duckdb::unique_ptr<duckdb::Expression> predicate,
                   duckdb::vector<duckdb::LogicalType> output_types);

    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;
    Result<void> Finish() override;

    // Evaluates the predicate over the input, creates a selection vector `sel`, and return the 
    // surviving count.
    duckdb::idx_t Select(duckdb::DataChunk &input, duckdb::SelectionVector &sel);

    // The input columns indices the predicate references.
    const std::vector<uint32_t> &FilterColumns() const { return filter_cols_; }

private:
    duckdb::unique_ptr<duckdb::Expression> predicate_;
    duckdb::ExpressionExecutor executor_;
    std::vector<uint32_t> filter_cols_;
};

Result<std::unique_ptr<Operator>> BuildFilterTemplate(std::shared_ptr<FilterTemplate> templ,
                                                      duckdb::vector<duckdb::LogicalType> col_types);

} // namespace plume::exec
