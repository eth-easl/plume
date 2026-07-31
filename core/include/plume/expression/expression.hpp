#pragma once

#include "plume/common/types.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/value.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume {
namespace expr {

enum class ExprKind : uint8_t {
    REFERENCE = 0,
    CONSTANT = 1,    // literal value
    FUNCTION = 2,    // scalar function resolved via registry
    COMPARISON = 3,  // =, <>, <, >, <=, >=
    CONJUNCTION = 4, // AND / OR
    OPERATOR = 5,    // NOT, IS NULL, IS NOT NULL, ...
    CAST = 6,
    AGGREGATE = 7,
    BETWEEN = 8,
    CASE_EXPR = 9,
};

// One node of the expression tree. Fields used depend on `kind`.
// TODO: might want to clean this up in the future...
struct ExprNode {
    ExprKind kind = ExprKind::REFERENCE;
    ColumnType return_type;
    std::vector<ExprNode> children;

    // REFERENCE
    uint32_t ref_index = 0;

    // CONSTANT (held as a DuckDB Value in memory; serialized via Value::Serialize)
    duckdb::Value constant;

    // FUNCTION / AGGREGATE
    std::string func_name;

    // COMPARISON / CONJUNCTION / OPERATOR: raw duckdb::ExpressionType
    uint8_t expr_type = 0;

    // CAST
    bool try_cast = false;

    void Serialize(duckdb::Serializer &s) const;
    static ExprNode Deserialize(duckdb::Deserializer &d);

    // --- convenience constructors -----------------------------------------
    
    static ExprNode Reference(uint32_t index, ColumnType type) {
        ExprNode n;
        n.kind = ExprKind::REFERENCE;
        n.ref_index = index;
        n.return_type = type;
        return n;
    }
    static ExprNode Constant(duckdb::Value value, ColumnType type) {
        ExprNode n;
        n.kind = ExprKind::CONSTANT;
        n.constant = std::move(value);
        n.return_type = type;
        return n;
    }
    static ExprNode Function(std::string name, ColumnType return_type, std::vector<ExprNode> args) {
        ExprNode n;
        n.kind = ExprKind::FUNCTION;
        n.func_name = std::move(name);
        n.return_type = return_type;
        n.children = std::move(args);
        return n;
    }
    static ExprNode Comparison(duckdb::ExpressionType cmp, ExprNode left, ExprNode right) {
        ExprNode n;
        n.kind = ExprKind::COMPARISON;
        n.expr_type = static_cast<uint8_t>(cmp);
        n.return_type = {TypeId::BOOLEAN};
        n.children.push_back(std::move(left));
        n.children.push_back(std::move(right));
        return n;
    }
    static ExprNode Conjunction(duckdb::ExpressionType conj, std::vector<ExprNode> args) {
        ExprNode n;
        n.kind = ExprKind::CONJUNCTION;
        n.expr_type = static_cast<uint8_t>(conj);
        n.return_type = {TypeId::BOOLEAN};
        n.children = std::move(args);
        return n;
    }
    static ExprNode Operator(duckdb::ExpressionType op, ColumnType return_type, std::vector<ExprNode> args) {
        ExprNode n;
        n.kind = ExprKind::OPERATOR;
        n.expr_type = static_cast<uint8_t>(op);
        n.return_type = return_type;
        n.children = std::move(args);
        return n;
    }
    static ExprNode Cast(ExprNode child, ColumnType return_type, bool try_cast) {
        ExprNode n;
        n.kind = ExprKind::CAST;
        n.return_type = return_type;
        n.try_cast = try_cast;
        n.children.push_back(std::move(child));
        return n;
    }
    static ExprNode Case(std::vector<ExprNode> when_then_else, ColumnType return_type) {
        ExprNode n;
        n.kind = ExprKind::CASE_EXPR;
        n.return_type = return_type;
        n.children = std::move(when_then_else);
        return n;
    }
};

} // namespace expr

using Expression = expr::ExprNode;

// Convenience aliases so code in namespace plume can use ExprNode/ExprKind directly.
using ExprNode = expr::ExprNode;
using ExprKind = expr::ExprKind;

} // namespace plume
