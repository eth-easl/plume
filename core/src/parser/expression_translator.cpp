#include "plume/parser/expression_translator.hpp"

#include "plume/common/result.hpp"

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

} // namespace plume::parser
