#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/expression/expression.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/joinside.hpp"

#include <cstdint>

namespace plume::parser {

Result<expr::ExprNode> TranslateExpression(const duckdb::Expression &expr);

Result<expr::ExprNode> CombinePredicates(const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &predicates);

Result<exec::AggregateSpec> TranslateAggregate(const duckdb::BoundAggregateExpression &agg);

Result<exec::JoinKind> JoinKindFromType(duckdb::JoinType type);

Result<uint32_t> JoinKeyIndex(const duckdb::Expression &side);

bool IsEquiCondition(const duckdb::JoinCondition &cond);

} // namespace plume::parser
