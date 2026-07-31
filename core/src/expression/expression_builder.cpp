#include "plume/expression/expression_builder.hpp"

#include "plume/expression/function_registry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace plume::expr {

using duckdb::Expression;
using duckdb::ExpressionType;
using duckdb::LogicalType;
using duckdb::ScalarFunction;
using duckdb::unique_ptr;

namespace {

ExpressionType ToExprType(uint8_t raw) { return static_cast<ExpressionType>(raw); }

Result<unique_ptr<Expression>> BuildFunction(const ExprNode &node) {
    duckdb::vector<unique_ptr<Expression>> args;
    std::vector<LogicalType> arg_types;
    args.reserve(node.children.size());
    arg_types.reserve(node.children.size());
    for (auto &child : node.children) {
        TRY(auto built, BuildExpression(child));
        arg_types.push_back(built->return_type);
        args.push_back(std::move(built));
    }

    // The function may still carry a (now-unused) bind pointer, but execution uses only the kernel,
    // so bind_info stays null.
    TRY(ScalarFunction function, ResolveScalarFunction(node.func_name, arg_types, ToLogicalType(node.return_type)));

    return duckdb::make_uniq<duckdb::BoundFunctionExpression>(
        ToLogicalType(node.return_type), std::move(function), std::move(args), nullptr, /*is_operator=*/true);
}

} // namespace

Result<unique_ptr<Expression>> BuildExpression(const ExprNode &node) {
    switch (node.kind) {
    case ExprKind::REFERENCE:
        return duckdb::make_uniq<duckdb::BoundReferenceExpression>(
            ToLogicalType(node.return_type), duckdb::storage_t(node.ref_index));
    case ExprKind::CONSTANT:
        return duckdb::make_uniq<duckdb::BoundConstantExpression>(node.constant);
    case ExprKind::FUNCTION:
        return BuildFunction(node);
    case ExprKind::COMPARISON: {
        if (node.children.size() != 2) {
            return Error("Comparison expects 2 children", ErrorKind::InvalidInput);
        }
        TRY(auto lhs, BuildExpression(node.children[0]));
        TRY(auto rhs, BuildExpression(node.children[1]));
        return unique_ptr<Expression>(duckdb::make_uniq<duckdb::BoundComparisonExpression>(
            ToExprType(node.expr_type), std::move(lhs), std::move(rhs)));
    }
    case ExprKind::CONJUNCTION: {
        unique_ptr<Expression> result = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
            ToExprType(node.expr_type));
        auto &conj = result->Cast<duckdb::BoundConjunctionExpression>();
        for (auto &child : node.children) {
            TRY(auto built, BuildExpression(child));
            conj.children.push_back(std::move(built));
        }
        return result;
    }
    case ExprKind::OPERATOR: {
        unique_ptr<Expression> result = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
            ToExprType(node.expr_type), ToLogicalType(node.return_type));
        auto &op = result->Cast<duckdb::BoundOperatorExpression>();
        for (auto &child : node.children) {
            TRY(auto built, BuildExpression(child));
            op.children.push_back(std::move(built));
        }
        return result;
    }
    case ExprKind::CAST: {
        if (node.children.size() != 1) {
            return Error("Cast expects 1 child", ErrorKind::InvalidInput);
        }
        TRY(auto child, BuildExpression(node.children[0]));
        return TryCatch([&]() -> unique_ptr<Expression> {
            return duckdb::BoundCastExpression::AddDefaultCastToType(
                std::move(child), ToLogicalType(node.return_type), node.try_cast);
        });
    }
    case ExprKind::CASE_EXPR: {
        // Always odd (n pairs + else) -> children: [when0, then0, ..., else]..
        if (node.children.empty() || node.children.size() % 2 == 0) {
            return Error("Case expects when/then pairs followed by an else result", ErrorKind::InvalidInput);
        }
        unique_ptr<Expression> result = duckdb::make_uniq<duckdb::BoundCaseExpression>(
            ToLogicalType(node.return_type));
        auto &case_expr = result->Cast<duckdb::BoundCaseExpression>();
        const size_t n = node.children.size();
        for (size_t i = 0; i + 1 < n; i += 2) {
            duckdb::BoundCaseCheck check;
            TRY(check.when_expr, BuildExpression(node.children[i]));
            TRY(check.then_expr, BuildExpression(node.children[i + 1]));
            case_expr.case_checks.push_back(std::move(check));
        }
        TRY(case_expr.else_expr, BuildExpression(node.children[n - 1]));
        return result;
    }
    case ExprKind::AGGREGATE:
    case ExprKind::BETWEEN:
        return Error("Expression kind " + std::to_string(static_cast<int>(node.kind)) +
                     " not yet supported in builder", ErrorKind::NotImplemented);
    default:
        return Error("Unknown ExprKind");
    }
}

} // namespace plume::expr
