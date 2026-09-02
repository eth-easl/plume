#include "plume/execution/operators/dynamic_filter.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/vector.hpp"

namespace plume::exec {

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::PhysicalType;
using duckdb::UnifiedVectorFormat;
using duckdb::Value;
using duckdb::Vector;

//===----------------------------------------------------------------------===//
// DynamicFilterBounds
//===----------------------------------------------------------------------===//

void DynamicFilterBounds::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "valid", valid);
    s.WriteProperty(101, "min", min);
    s.WriteProperty(102, "max", max);
}

DynamicFilterBounds DynamicFilterBounds::Deserialize(duckdb::Deserializer &d) {
    DynamicFilterBounds b;
    b.valid = d.ReadProperty<bool>(100, "valid");
    b.min = d.ReadProperty<Value>(101, "min");
    b.max = d.ReadProperty<Value>(102, "max");
    return b;
}

//===----------------------------------------------------------------------===//
// BuildRangeFilter
//===----------------------------------------------------------------------===//

std::optional<expr::ExprNode> BuildRangeFilter(uint32_t column, const ColumnType &type,
                                                const DynamicFilterBounds &bounds) {
    if (!bounds.valid) {
        return std::nullopt;
    }
    if (bounds.min == bounds.max) {
        return expr::ExprNode::Comparison(duckdb::ExpressionType::COMPARE_EQUAL,
                                          expr::ExprNode::Reference(column, type),
                                          expr::ExprNode::Constant(bounds.min, type));
    }
    auto ge = expr::ExprNode::Comparison(duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                                         expr::ExprNode::Reference(column, type),
                                         expr::ExprNode::Constant(bounds.min, type));
    auto le = expr::ExprNode::Comparison(duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO,
                                         expr::ExprNode::Reference(column, type),
                                         expr::ExprNode::Constant(bounds.max, type));
    return expr::ExprNode::Conjunction(duckdb::ExpressionType::CONJUNCTION_AND, {std::move(ge), std::move(le)});
}

//===----------------------------------------------------------------------===//
// DynamicFilterBuildTemplate
//===----------------------------------------------------------------------===//

bool DynamicFilterBuildTemplate::Equals(const OperatorTemplate &other) const {
    if (other.type != type) return false;
    auto &o = static_cast<const DynamicFilterBuildTemplate &>(other);
    return column == o.column;
}

void DynamicFilterBuildTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(101, "column", column);
}

//===----------------------------------------------------------------------===//
// DynamicFilterBuildOperator
//===----------------------------------------------------------------------===//

namespace {

template <class T>
inline void Accumulate(const T &v, bool &has_value, T &chunk_min, T &chunk_max) {
    if (!has_value) {
        chunk_min = v;
        chunk_max = v;
        has_value = true;
    } else {
        if (v < chunk_min) chunk_min = v;
        if (chunk_max < v) chunk_max = v;
    }
}

// No selection-vector or per-row validity check -> allow the compiler to autovectorize.
template <class T>
void ReduceFlatAllValid(const T *__restrict data, idx_t count, bool &has_value, T &chunk_min, T &chunk_max) {
    idx_t i = 0;
    if (!has_value) {
        if (count == 0) {
            return;
        }
        chunk_min = chunk_max = data[0];
        has_value = true;
        i = 1;
    }
    for (; i < count; i++) {
        const T &v = data[i];
        if (v < chunk_min) chunk_min = v;
        if (chunk_max < v) chunk_max = v;
    }
}

template <class T>
void UpdateBounds(Vector &vec, idx_t count, DynamicFilterBounds &bounds) {
    bool has_value = false;
    T chunk_min {};
    T chunk_max {};

    if (vec.GetVectorType() == duckdb::VectorType::FLAT_VECTOR) {
        auto data = duckdb::FlatVector::GetData<T>(vec);
        auto &validity = duckdb::FlatVector::Validity(vec);
        if (validity.AllValid()) {
            ReduceFlatAllValid<T>(data, count, has_value, chunk_min, chunk_max);
        } else {
            for (idx_t i = 0; i < count; i++) {
                if (validity.RowIsValid(i)) {
                    Accumulate(data[i], has_value, chunk_min, chunk_max);
                }
            }
        }
    } else {
        UnifiedVectorFormat udata;
        vec.ToUnifiedFormat(count, udata);
        auto data = UnifiedVectorFormat::GetData<T>(udata);
        for (idx_t i = 0; i < count; i++) {
            auto idx = udata.sel->get_index(i);
            if (udata.validity.RowIsValid(idx)) {
                Accumulate(data[idx], has_value, chunk_min, chunk_max);
            }
        }
    }
    if (!has_value) {
        return; // every row in this chunk was NULL
    }

    Value new_min = Value::CreateValue<T>(chunk_min);
    Value new_max = Value::CreateValue<T>(chunk_max);
    if (!bounds.valid) {
        bounds.min = std::move(new_min);
        bounds.max = std::move(new_max);
        bounds.valid = true;
    } else {
        if (new_min < bounds.min) bounds.min = std::move(new_min);
        if (new_max > bounds.max) bounds.max = std::move(new_max);
    }
}

} // namespace

DynamicFilterBuildOperator::DynamicFilterBuildOperator(uint32_t column, duckdb::vector<LogicalType> output_types)
    : Operator(std::move(output_types)), column_(column) {}

Result<void> DynamicFilterBuildOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    const idx_t count = chunk->size();
    if (count > 0) {
        auto &vec = chunk->data[column_];
        switch (vec.GetType().InternalType()) {
        case PhysicalType::BOOL: UpdateBounds<bool>(vec, count, bounds_); break;
        case PhysicalType::INT8: UpdateBounds<int8_t>(vec, count, bounds_); break;
        case PhysicalType::INT16: UpdateBounds<int16_t>(vec, count, bounds_); break;
        case PhysicalType::INT32: UpdateBounds<int32_t>(vec, count, bounds_); break;
        case PhysicalType::INT64: UpdateBounds<int64_t>(vec, count, bounds_); break;
        case PhysicalType::UINT8: UpdateBounds<uint8_t>(vec, count, bounds_); break;
        case PhysicalType::UINT16: UpdateBounds<uint16_t>(vec, count, bounds_); break;
        case PhysicalType::UINT32: UpdateBounds<uint32_t>(vec, count, bounds_); break;
        case PhysicalType::UINT64: UpdateBounds<uint64_t>(vec, count, bounds_); break;
        case PhysicalType::INT128: UpdateBounds<duckdb::hugeint_t>(vec, count, bounds_); break;
        case PhysicalType::UINT128: UpdateBounds<duckdb::uhugeint_t>(vec, count, bounds_); break;
        case PhysicalType::FLOAT: UpdateBounds<float>(vec, count, bounds_); break;
        case PhysicalType::DOUBLE: UpdateBounds<double>(vec, count, bounds_); break;
        case PhysicalType::VARCHAR: UpdateBounds<duckdb::string_t>(vec, count, bounds_); break;
        default:
            return Error("Dynamic filter build over an unsupported column type", ErrorKind::NotImplemented);
        }
    }
    return next_->Push(std::move(chunk));
}

Result<void> DynamicFilterBuildOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    return next_->Finish();
}

//===----------------------------------------------------------------------===//
// Build
//===----------------------------------------------------------------------===//

Result<std::unique_ptr<Operator>> BuildDynamicFilterBuildTemplate(std::shared_ptr<DynamicFilterBuildTemplate> templ,
        duckdb::vector<LogicalType> col_types) {
    if (templ->column >= col_types.size()) {
        return Error("Filter build column index out of range", ErrorKind::OutOfRange);
    }
    return std::unique_ptr<Operator>(
        std::make_unique<DynamicFilterBuildOperator>(templ->column, std::move(col_types)));
}

} // namespace plume::exec
