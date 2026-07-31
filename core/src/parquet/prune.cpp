#include "plume/parquet/prune.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/value.hpp"

#include <cstring>
#include <optional>

namespace plume::parquet {

using duckdb::ExpressionType;
using duckdb::LogicalType;
using duckdb::Value;
using expr::ExprKind;
using expr::ExprNode;

namespace {

template <class T> T LoadLE(const uint8_t *p) {
    T v;
    std::memcpy(&v, p, sizeof(T));
    return v;
}

std::optional<Value> DecodeStat(const std::vector<uint8_t> &bytes, const ColumnType &ct) {
    const LogicalType lt = ToLogicalType(ct);
    auto norm = [&](Value v) -> std::optional<Value> {
        try {
            return v.DefaultCastAs(lt);
        } catch (...) {
            return std::nullopt;
        }
    };
    switch (ct.id) {
    case TypeId::INT8:
    case TypeId::INT16:
    case TypeId::INT32: // parquet's smallest physical int is INT32
        if (bytes.size() == 4) {
            return norm(Value::INTEGER(LoadLE<int32_t>(bytes.data())));
        }
        return std::nullopt;
    case TypeId::DATE:
        if (bytes.size() == 4) {
            return norm(Value::DATE(duckdb::date_t(LoadLE<int32_t>(bytes.data()))));
        }
        return std::nullopt;
    case TypeId::INT64:
        if (bytes.size() == 8) {
            return norm(Value::BIGINT(LoadLE<int64_t>(bytes.data())));
        }
        return std::nullopt;
    case TypeId::FLOAT:
        if (bytes.size() == 4) {
            return norm(Value::FLOAT(LoadLE<float>(bytes.data())));
        }
        return std::nullopt;
    case TypeId::DOUBLE:
        if (bytes.size() == 8) {
            return norm(Value::DOUBLE(LoadLE<double>(bytes.data())));
        }
        return std::nullopt;
    case TypeId::DECIMAL: // only INT16/INT32/INT64-backed decimals (little-endian)
        if (bytes.size() == 2) {
            return norm(Value::DECIMAL(LoadLE<int16_t>(bytes.data()), ct.decimal_width, ct.decimal_scale));
        }
        if (bytes.size() == 4) {
            return norm(Value::DECIMAL(LoadLE<int32_t>(bytes.data()), ct.decimal_width, ct.decimal_scale));
        }
        if (bytes.size() == 8) {
            return norm(Value::DECIMAL(LoadLE<int64_t>(bytes.data()), ct.decimal_width, ct.decimal_scale));
        }
        return std::nullopt;
    default:
        return std::nullopt;
    }
}

bool RangeSatisfies(ExpressionType op, const Value &lo, const Value &hi, const Value &rhs) {
    switch (op) {
    case ExpressionType::COMPARE_EQUAL:
        return lo <= rhs && rhs <= hi;
    case ExpressionType::COMPARE_NOTEQUAL:
        // Provably empty only when every value equals rhs (a single-point range).
        return !(lo == rhs && hi == rhs);
    case ExpressionType::COMPARE_LESSTHAN:
        return lo < rhs;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        return lo <= rhs;
    case ExpressionType::COMPARE_GREATERTHAN:
        return hi > rhs;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        return hi >= rhs;
    default:
        return true; // unmodeled comparison ⇒ keep
    }
}

ExpressionType Flip(ExpressionType op) {
    switch (op) {
    case ExpressionType::COMPARE_LESSTHAN: return ExpressionType::COMPARE_GREATERTHAN;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO: return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
    case ExpressionType::COMPARE_GREATERTHAN: return ExpressionType::COMPARE_LESSTHAN;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return ExpressionType::COMPARE_LESSTHANOREQUALTO;
    default: return op; // = and <> are symmetric
    }
}

bool ComparisonMayMatch(const ExprNode &cmp, const RowGroupMeta &rgm, const Schema &schema) {
    if (cmp.children.size() != 2) {
        return true;
    }
    const ExprNode *ref = nullptr;
    const ExprNode *con = nullptr;
    ExpressionType op = static_cast<ExpressionType>(cmp.expr_type);
    if (cmp.children[0].kind == ExprKind::REFERENCE && cmp.children[1].kind == ExprKind::CONSTANT) {
        ref = &cmp.children[0];
        con = &cmp.children[1];
    } else if (cmp.children[0].kind == ExprKind::CONSTANT && cmp.children[1].kind == ExprKind::REFERENCE) {
        ref = &cmp.children[1];
        con = &cmp.children[0];
        op = Flip(op);
    } else {
        return true; // not a bare column-vs-constant comparison
    }

    const uint32_t idx = ref->ref_index;
    if (idx >= rgm.columns.size() || idx >= schema.columns.size()) {
        return true;
    }
    const ColumnStats &stats = rgm.columns[idx].stats;
    if (!stats.has_min || !stats.has_max) {
        return true; // no stats ⇒ can't prune
    }
    const ColumnType &ct = schema.columns[idx].type;
    auto lo = DecodeStat(stats.min_value, ct);
    auto hi = DecodeStat(stats.max_value, ct);
    if (!lo || !hi) {
        return true;
    }
    Value rhs;
    try {
        rhs = con->constant.DefaultCastAs(ToLogicalType(ct));
        if (rhs.IsNull()) {
            return true; // NULL comparand ⇒ leave to the exact pipeline filter
        }
    } catch (...) {
        return true;
    }
    return RangeSatisfies(op, *lo, *hi, rhs);
}

} // namespace

bool RowGroupMayMatch(const ExprNode &pred, const RowGroupMeta &rgm, const Schema &file_schema) {
    switch (pred.kind) {
    case ExprKind::CONJUNCTION: {
        const auto conj = static_cast<ExpressionType>(pred.expr_type);
        if (conj == ExpressionType::CONJUNCTION_AND) {
            for (const auto &child : pred.children) {
                if (!RowGroupMayMatch(child, rgm, file_schema)) {
                    return false;
                }
            }
            return true;
        }
        for (const auto &child : pred.children) {
            if (RowGroupMayMatch(child, rgm, file_schema)) {
                return true;
            }
        }
        return false;
    }
    case ExprKind::COMPARISON:
        return ComparisonMayMatch(pred, rgm, file_schema);
    default:
        return true; // anything we don't model ⇒ keep
    }
}

} // namespace plume::parquet
