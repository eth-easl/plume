#include "plume/common/types.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace plume {

using duckdb::LogicalType;
using duckdb::LogicalTypeId;

//===----------------------------------------------------------------------===//
// LogicalType
//===----------------------------------------------------------------------===//

LogicalType ToLogicalType(const ColumnType &type) {
    switch (type.id) {
    case TypeId::BOOLEAN:
        return LogicalType::BOOLEAN;
    case TypeId::INT8:
        return LogicalType::TINYINT;
    case TypeId::INT16:
        return LogicalType::SMALLINT;
    case TypeId::INT32:
        return LogicalType::INTEGER;
    case TypeId::INT64:
        return LogicalType::BIGINT;
    case TypeId::HUGEINT:
        return LogicalType::HUGEINT;
    case TypeId::FLOAT:
        return LogicalType::FLOAT;
    case TypeId::DOUBLE:
        return LogicalType::DOUBLE;
    case TypeId::VARCHAR:
        return LogicalType::VARCHAR;
    case TypeId::DECIMAL:
        return LogicalType::DECIMAL(type.decimal_width, type.decimal_scale);
    case TypeId::DATE:
        return LogicalType::DATE;
    case TypeId::TIME:
        return LogicalType::TIME;
    default:
        throw duckdb::InternalException("Plume: unhandled TypeId in ToLogicalType");
    }
}

ColumnType FromLogicalType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
        return {TypeId::BOOLEAN, 0, 0};
    case LogicalTypeId::TINYINT:
        return {TypeId::INT8, 0, 0};
    case LogicalTypeId::SMALLINT:
        return {TypeId::INT16, 0, 0};
    case LogicalTypeId::INTEGER:
        return {TypeId::INT32, 0, 0};
    case LogicalTypeId::BIGINT:
        return {TypeId::INT64, 0, 0};
    case LogicalTypeId::HUGEINT:
        return {TypeId::HUGEINT, 0, 0};
    case LogicalTypeId::FLOAT:
        return {TypeId::FLOAT, 0, 0};
    case LogicalTypeId::DOUBLE:
        return {TypeId::DOUBLE, 0, 0};
    case LogicalTypeId::VARCHAR:
        return {TypeId::VARCHAR, 0, 0};
    case LogicalTypeId::DECIMAL:
        return {TypeId::DECIMAL, duckdb::DecimalType::GetWidth(type), duckdb::DecimalType::GetScale(type)};
    case LogicalTypeId::DATE:
        return {TypeId::DATE, 0, 0};
    case LogicalTypeId::TIME:
        return {TypeId::TIME, 0, 0};
    default:
        throw duckdb::NotImplementedException("Plume: unsupported LogicalType '%s'", type.ToString());
    }
}

uint32_t PhysicalWidth(const ColumnType &type) {
    return static_cast<uint32_t>(duckdb::GetTypeIdSize(ToLogicalType(type).InternalType()));
}

//===----------------------------------------------------------------------===//
// ColumnType
//===----------------------------------------------------------------------===//

void ColumnType::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "id", static_cast<uint8_t>(id));
    s.WriteProperty(101, "decimal_width", decimal_width);
    s.WriteProperty(102, "decimal_scale", decimal_scale);
}

ColumnType ColumnType::Deserialize(duckdb::Deserializer &d) {
    ColumnType t;
    t.id = static_cast<TypeId>(d.ReadProperty<uint8_t>(100, "id"));
    t.decimal_width = d.ReadProperty<uint8_t>(101, "decimal_width");
    t.decimal_scale = d.ReadProperty<uint8_t>(102, "decimal_scale");
    return t;
}

//===----------------------------------------------------------------------===//
// Column
//===----------------------------------------------------------------------===//

void Column::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "name", name);
    s.WriteProperty(101, "type", type);
    s.WriteProperty(102, "nullable", nullable);
}

Column Column::Deserialize(duckdb::Deserializer &d) {
    Column c;
    c.name = d.ReadProperty<std::string>(100, "name");
    c.type = d.ReadProperty<ColumnType>(101, "type");
    c.nullable = d.ReadProperty<bool>(102, "nullable");
    return c;
}

//===----------------------------------------------------------------------===//
// Schema
//===----------------------------------------------------------------------===//

void Schema::Serialize(duckdb::Serializer &s) const {
    s.WriteList(100, "columns", columns.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteElement(columns[i]);
    });
}

Schema Schema::Deserialize(duckdb::Deserializer &d) {
    Schema schema;
    d.ReadList(100, "columns", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
        schema.columns.push_back(list.ReadElement<Column>());
    });
    return schema;
}

} // namespace plume
