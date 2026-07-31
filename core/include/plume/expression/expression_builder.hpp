#pragma once

#include "plume/common/result.hpp"
#include "plume/expression/expression.hpp"

#include "duckdb/planner/expression.hpp"

namespace plume::expr {

// Recursively builds a DuckDB bound Expression from a Plume ExprNode.
Result<duckdb::unique_ptr<duckdb::Expression>> BuildExpression(const ExprNode &node);

} // namespace plume::expr
