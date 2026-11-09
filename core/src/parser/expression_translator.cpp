#include "plume/parser/expression_translator.hpp"

#include "plume/common/result.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace plume::parser {

using duckdb::Expression;
using duckdb::ExpressionClass;

namespace {

Result<std::vector<expr::ExprNode>> TranslateChildren(const duckdb::vector<duckdb::unique_ptr<Expression>> &children) {
    std::vector<expr::ExprNode> out;
    out.reserve(children.size());
    for (auto &child : children) {
        TRY(auto node, TranslateExpression(*child));
        out.push_back(std::move(node));
    }
    return out;
}

} // namespace

Result<expr::ExprNode> TranslateExpression(const Expression &expr) {
    switch (expr.GetExpressionClass()) {
    case ExpressionClass::BOUND_REF: {
        auto &ref = expr.Cast<duckdb::BoundReferenceExpression>();
        return expr::ExprNode::Reference(static_cast<uint32_t>(ref.index), FromLogicalType(ref.return_type));
    }
    case ExpressionClass::BOUND_CONSTANT: {
        auto &c = expr.Cast<duckdb::BoundConstantExpression>();
        return expr::ExprNode::Constant(c.value, FromLogicalType(c.return_type));
    }
    case ExpressionClass::BOUND_FUNCTION: {
        auto &fn = expr.Cast<duckdb::BoundFunctionExpression>();
        TRY(auto children, TranslateChildren(fn.children));
        return expr::ExprNode::Function(fn.function.name, FromLogicalType(fn.return_type), std::move(children));
    }
    case ExpressionClass::BOUND_COMPARISON: {
        auto &cmp = expr.Cast<duckdb::BoundComparisonExpression>();
        TRY(auto left, TranslateExpression(*cmp.left));
        TRY(auto right, TranslateExpression(*cmp.right));
        return expr::ExprNode::Comparison(cmp.GetExpressionType(), std::move(left), std::move(right));
    }
    case ExpressionClass::BOUND_CONJUNCTION: {
        auto &conj = expr.Cast<duckdb::BoundConjunctionExpression>();
        TRY(auto children, TranslateChildren(conj.children));
        return expr::ExprNode::Conjunction(conj.GetExpressionType(), std::move(children));
    }
    case ExpressionClass::BOUND_OPERATOR: {
        auto &op = expr.Cast<duckdb::BoundOperatorExpression>();
        TRY(auto children, TranslateChildren(op.children));
        return expr::ExprNode::Operator(op.GetExpressionType(), FromLogicalType(op.return_type), std::move(children));
    }
    case ExpressionClass::BOUND_CASE: {
        auto &c = expr.Cast<duckdb::BoundCaseExpression>();
        std::vector<expr::ExprNode> children;
        children.reserve(c.case_checks.size() * 2 + 1);
        for (auto &check : c.case_checks) {
            TRY(auto when, TranslateExpression(*check.when_expr));
            TRY(auto then, TranslateExpression(*check.then_expr));
            children.push_back(std::move(when));
            children.push_back(std::move(then));
        }
        TRY(auto else_node, TranslateExpression(*c.else_expr));
        children.push_back(std::move(else_node));
        return expr::ExprNode::Case(std::move(children), FromLogicalType(c.return_type));
    }
    case ExpressionClass::BOUND_BETWEEN: {
        // Lower to `input >= lower AND input <= upper` (inclusivity from the
        // between flags). Reuses Plume's comparison/conjunction IR.
        auto &b = expr.Cast<duckdb::BoundBetweenExpression>();
        TRY(auto input_ir, TranslateExpression(*b.input));
        TRY(auto lower, TranslateExpression(*b.lower));
        TRY(auto upper, TranslateExpression(*b.upper));
        auto lower_cmp = expr::ExprNode::Comparison(b.LowerComparisonType(), input_ir, std::move(lower));
        auto upper_cmp = expr::ExprNode::Comparison(b.UpperComparisonType(), std::move(input_ir), std::move(upper));
        std::vector<expr::ExprNode> args;
        args.push_back(std::move(lower_cmp));
        args.push_back(std::move(upper_cmp));
        return expr::ExprNode::Conjunction(duckdb::ExpressionType::CONJUNCTION_AND, std::move(args));
    }
    case ExpressionClass::BOUND_CAST: {
        auto &cast = expr.Cast<duckdb::BoundCastExpression>();
        TRY(auto child, TranslateExpression(*cast.child));
        return expr::ExprNode::Cast(std::move(child), FromLogicalType(cast.return_type), cast.try_cast);
    }
    case ExpressionClass::BOUND_COLUMN_REF:
        return Error("Unresolved BoundColumnRefExpression — ColumnBindingResolver must run first.");
    default:
        return Error(
            "Unsupported expression class in scalar context: " +
                std::string(duckdb::ExpressionTypeToString(expr.GetExpressionType())),
            ErrorKind::NotImplemented);
    }
}

Result<expr::ExprNode> CombinePredicates(const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &predicates) {
    if (predicates.size() == 1) {
        return TranslateExpression(*predicates[0]);
    }
    std::vector<expr::ExprNode> children;
    children.reserve(predicates.size());
    for (auto &p : predicates) {
        TRY(auto child, TranslateExpression(*p));
        children.push_back(std::move(child));
    }
    return expr::ExprNode::Conjunction(duckdb::ExpressionType::CONJUNCTION_AND, std::move(children));
}

Result<exec::AggregateSpec> TranslateAggregate(const duckdb::BoundAggregateExpression &agg) {
    if (agg.filter) {
        return Error("Aggregate FILTER clause is not supported.", ErrorKind::NotImplemented);
    }
    if (agg.order_bys) {
        return Error("Ordered aggregates are not supported.", ErrorKind::NotImplemented);
    }
    exec::AggregateSpec spec;
    // statistics_propagation can swap sum -> sum_no_overflow when it proves no
    // overflow; Plume only registers plain sum (same result), so normalize back.
    spec.func_name = (agg.function.name == "sum_no_overflow") ? "sum" : agg.function.name;
    spec.return_type = FromLogicalType(agg.return_type);
    spec.distinct = agg.IsDistinct();
    spec.arguments.reserve(agg.children.size());
    for (auto &child : agg.children) {
        TRY(auto arg, TranslateExpression(*child));
        spec.arguments.push_back(std::move(arg));
    }
    return spec;
}

Result<exec::JoinKind> JoinKindFromType(duckdb::JoinType type) {
    switch (type) {
    case duckdb::JoinType::INNER:
        return exec::JoinKind::INNER;
    case duckdb::JoinType::LEFT:
        return exec::JoinKind::LEFT;
    case duckdb::JoinType::RIGHT:
        return exec::JoinKind::RIGHT;
    case duckdb::JoinType::OUTER:
        return exec::JoinKind::OUTER;
    case duckdb::JoinType::SEMI:
        return exec::JoinKind::SEMI;
    case duckdb::JoinType::ANTI:
        return exec::JoinKind::ANTI;
    case duckdb::JoinType::SINGLE:
        return exec::JoinKind::SINGLE;
    case duckdb::JoinType::MARK:
        return exec::JoinKind::MARK;
    case duckdb::JoinType::RIGHT_SEMI:
        return exec::JoinKind::RIGHT_SEMI;
    case duckdb::JoinType::RIGHT_ANTI:
        return exec::JoinKind::RIGHT_ANTI;
    default:
        return Error("Unsupported join type.", ErrorKind::NotImplemented);
    }
}

Result<uint32_t> JoinKeyIndex(const duckdb::Expression &side) {
    if (side.GetExpressionClass() != duckdb::ExpressionClass::BOUND_REF) {
        return Error("Join keys must be plain columns (expression join keys not supported).",
                     ErrorKind::NotImplemented);
    }
    return static_cast<uint32_t>(side.Cast<duckdb::BoundReferenceExpression>().index);
}

bool IsEquiCondition(const duckdb::JoinCondition &cond) {
    return cond.comparison == duckdb::ExpressionType::COMPARE_EQUAL ||
           cond.comparison == duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

} // namespace plume::parser
