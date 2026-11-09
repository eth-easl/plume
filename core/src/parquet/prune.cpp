#include "plume/parquet/prune.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/value.hpp"

#include <algorithm>
#include <cstring>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

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
    case TypeId::BOOLEAN:
        if (bytes.size() == 1) {
            return Value::BOOLEAN(bytes[0] != 0);
        }
        return std::nullopt;
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
    case TypeId::TIME: // TIME_MICROS
        if (bytes.size() == 8) {
            return Value::TIME(duckdb::dtime_t(LoadLE<int64_t>(bytes.data())));
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
    case TypeId::VARCHAR:
        // Unsigned bytewise order, same as duckdb's string comparison. A truncated min (max) is
        // still a lower (upper) bound; one cut within a UTF-8 sequence fails to construct.
        try {
            return Value(std::string(reinterpret_cast<const char *>(bytes.data()), bytes.size()));
        } catch (...) {
            return std::nullopt;
        }
    default:
        return std::nullopt;
    }
}

// The column chunk's decoded [min, max], if its statistics have both.
std::optional<std::pair<Value, Value>> DecodeMinMax(const ColumnStats &stats, const ColumnType &ct) {
    if (!stats.has_min || !stats.has_max) {
        return std::nullopt;
    }
    if (ct.id == TypeId::VARCHAR && stats.deprecated_min_max) {
        return std::nullopt; // signed byte order
    }
    auto lo = DecodeStat(stats.min_value, ct);
    auto hi = DecodeStat(stats.max_value, ct);
    if (!lo || !hi) {
        return std::nullopt;
    }
    return std::make_pair(std::move(*lo), std::move(*hi));
}

// Strings are placed on the axis by the bytes following a common prefix; this many of them fit
// into a double's mantissa exactly.
constexpr size_t kStringPositionBytes = 6;

// The stat's position on an order-preserving (but not strictly) numeric axis. Strings skip their
// first string_prefix bytes.
std::optional<double> StatPosition(const Value &v, size_t string_prefix) {
    switch (v.type().id()) {
    case duckdb::LogicalTypeId::DATE:
        return static_cast<double>(v.GetValue<duckdb::date_t>().days);
    case duckdb::LogicalTypeId::TIME:
        return static_cast<double>(v.GetValue<duckdb::dtime_t>().micros);
    case duckdb::LogicalTypeId::VARCHAR: {
        const auto &str = duckdb::StringValue::Get(v);
        double pos = 0;
        for (size_t i = string_prefix; i < string_prefix + kStringPositionBytes; i++) {
            pos = pos * 256 + (i < str.size() ? static_cast<uint8_t>(str[i]) : 0);
        }
        return pos;
    }
    default:
        try {
            return v.GetValue<double>();
        } catch (...) {
            return std::nullopt;
        }
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

// The statistics of a file column, nullptr if unknown.
using StatsOf = std::function<const ColumnStats *(uint32_t column)>;

bool ComparisonMayMatch(const ExprNode &cmp, const StatsOf &stats_of, const Schema &schema) {
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
    const ColumnStats *stats = stats_of(idx);
    if (!stats || idx >= schema.columns.size()) {
        return true;
    }
    const ColumnType &ct = schema.columns[idx].type;
    auto range = DecodeMinMax(*stats, ct);
    if (!range) {
        return true; // no stats ⇒ can't prune
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
    return RangeSatisfies(op, range->first, range->second, rhs);
}

bool PredicateMayMatch(const ExprNode &pred, const StatsOf &stats_of, const Schema &schema) {
    switch (pred.kind) {
    case ExprKind::CONJUNCTION: {
        const auto conj = static_cast<ExpressionType>(pred.expr_type);
        if (conj == ExpressionType::CONJUNCTION_AND) {
            for (const auto &child : pred.children) {
                if (!PredicateMayMatch(child, stats_of, schema)) {
                    return false;
                }
            }
            return true;
        }
        for (const auto &child : pred.children) {
            if (PredicateMayMatch(child, stats_of, schema)) {
                return true;
            }
        }
        return false;
    }
    case ExprKind::COMPARISON:
        return ComparisonMayMatch(pred, stats_of, schema);
    default:
        return true; // anything we don't model ⇒ keep
    }
}

bool StatsMayContainAny(const ColumnStats &stats, const ColumnType &type, const std::vector<Value> &sorted_values) {
    auto range = DecodeMinMax(stats, type);
    if (!range) {
        return true;
    }
    // The first value >= min must also be <= max.
    auto it = std::lower_bound(sorted_values.begin(), sorted_values.end(), range->first,
                               [](const Value &a, const Value &b) { return a < b; });
    return it != sorted_values.end() && *it <= range->second;
}

} // namespace

bool RowGroupMayMatch(const ExprNode &pred, const RowGroupMeta &rgm, const Schema &file_schema) {
    return PredicateMayMatch(pred, [&](uint32_t c) { return c < rgm.columns.size() ? &rgm.columns[c].stats : nullptr; },
                             file_schema);
}

std::optional<std::vector<Value>> SortStatValues(const std::vector<Value> &values, const ColumnType &type) {
    const LogicalType lt = ToLogicalType(type);
    std::vector<Value> sorted;
    sorted.reserve(values.size());
    try {
        for (const auto &v : values) {
            sorted.push_back(v.DefaultCastAs(lt));
        }
    } catch (...) {
        return std::nullopt;
    }
    std::sort(sorted.begin(), sorted.end(), [](const Value &a, const Value &b) { return a < b; });
    return sorted;
}

bool RowGroupMayContainAny(const RowGroupMeta &rgm, const Schema &file_schema, uint32_t column,
                           const std::vector<Value> &sorted_values) {
    if (column >= rgm.columns.size() || column >= file_schema.columns.size()) {
        return true;
    }
    return StatsMayContainAny(rgm.columns[column].stats, file_schema.columns[column].type, sorted_values);
}

std::optional<std::vector<RowRange>> SelectPageRows(const ExprNode *pred, const std::vector<Value> *sorted_values,
                                                    uint32_t column, const Schema &file_schema,
                                                    const std::vector<PageLocation> &pages, const ColumnIndex &index,
                                                    uint64_t rg_rows) {
    if (column >= file_schema.columns.size() || pages.empty() || index.stats.size() != pages.size()) {
        return std::nullopt;
    }
    const ColumnType &type = file_schema.columns[column].type;
    std::vector<RowRange> rows;
    for (size_t p = 0; p < pages.size(); p++) {
        const uint64_t begin = static_cast<uint64_t>(pages[p].first_row);
        const uint64_t end = p + 1 < pages.size() ? static_cast<uint64_t>(pages[p + 1].first_row) : rg_rows;
        if (end < begin || end > rg_rows) {
            return std::nullopt; // doesn't fit the row group
        }
        if (index.null_pages[p]) {
            continue; // NULL keys never match
        }
        const ColumnStats &stats = index.stats[p];
        if (pred && !PredicateMayMatch(*pred, [&](uint32_t c) { return c == column ? &stats : nullptr; }, file_schema)) {
            continue;
        }
        if (sorted_values && !StatsMayContainAny(stats, type, *sorted_values)) {
            continue;
        }
        if (!rows.empty() && rows.back().end == begin) {
            rows.back().end = end;
        } else if (end > begin) {
            rows.push_back({begin, end});
        }
    }
    return rows;
}

std::optional<double> MeanRowGroupCoverage(const std::vector<FileMeta> &files, uint32_t column) {
    std::vector<std::pair<Value, Value>> stats;
    for (const auto &file : files) {
        if (column >= file.schema.columns.size()) {
            return std::nullopt;
        }
        const ColumnType &ct = file.schema.columns[column].type;
        for (const auto &rg : file.row_groups) {
            if (column >= rg.columns.size()) {
                return std::nullopt;
            }
            auto range = DecodeMinMax(rg.columns[column].stats, ct);
            if (!range) {
                return std::nullopt;
            }
            stats.push_back(std::move(*range));
        }
    }
    if (stats.empty()) {
        return std::nullopt;
    }

    Value global_min = stats[0].first;
    Value global_max = stats[0].second;
    for (const auto &[lo, hi] : stats) {
        if (lo < global_min) global_min = lo;
        if (hi > global_max) global_max = hi;
    }
    // Every string within [global_min, global_max] shares their common prefix.
    size_t string_prefix = 0;
    if (global_min.type().id() == duckdb::LogicalTypeId::VARCHAR) {
        const auto &a = duckdb::StringValue::Get(global_min);
        const auto &b = duckdb::StringValue::Get(global_max);
        while (string_prefix < a.size() && string_prefix < b.size() && a[string_prefix] == b[string_prefix]) {
            string_prefix++;
        }
    }

    std::vector<std::pair<double, double>> ranges;
    ranges.reserve(stats.size());
    for (const auto &[lo, hi] : stats) {
        auto lo_pos = StatPosition(lo, string_prefix);
        auto hi_pos = StatPosition(hi, string_prefix);
        if (!lo_pos || !hi_pos) {
            return std::nullopt;
        }
        ranges.emplace_back(*lo_pos, *hi_pos);
    }
    const double global_lo = *StatPosition(global_min, string_prefix);
    const double width = *StatPosition(global_max, string_prefix) - global_lo;
    if (!(width > 0)) {
        return 1.0; // a single value: every row group holds it
    }
    double coverage = 0;
    for (const auto &[lo, hi] : ranges) {
        coverage += (hi - lo) / width;
    }
    return coverage / static_cast<double>(ranges.size());
}

} // namespace plume::parquet
