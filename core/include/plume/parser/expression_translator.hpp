#pragma once

#include "plume/common/result.hpp"
#include "plume/expression/expression.hpp"

#include "duckdb/planner/expression.hpp"

namespace plume::parser {

Result<expr::ExprNode> TranslateExpression(const duckdb::Expression &expr);

} // namespace plume::parser
