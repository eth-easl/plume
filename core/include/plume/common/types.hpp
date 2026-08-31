#pragma once

#include "duckdb/common/types.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume {

enum class TypeId : uint8_t {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    FLOAT = 3,
    DOUBLE = 4,
    VARCHAR = 5,
    DECIMAL = 6,
    DATE = 7,
    TIME = 8,
    HUGEINT = 9,  // INT128; enables sum(int) and other 128-bit results
    INT8 = 10,    // TINYINT (e.g. sign())
    INT16 = 11,   // SMALLINT
};

struct ColumnType {
    TypeId id = TypeId::INT32;
    uint8_t decimal_width = 0; // DECIMAL only
    uint8_t decimal_scale = 0; // DECIMAL only

    bool operator==(const ColumnType &o) const {
        return id == o.id && decimal_width == o.decimal_width && decimal_scale == o.decimal_scale;
    }

    void Serialize(duckdb::Serializer &s) const;
    static ColumnType Deserialize(duckdb::Deserializer &d);
};

struct Column {
    std::string name;
    ColumnType type;
    bool nullable = true;

    void Serialize(duckdb::Serializer &s) const;
    static Column Deserialize(duckdb::Deserializer &d);
};

struct Schema {
    std::vector<Column> columns;

    size_t size() const { return columns.size(); }

    bool Equals(const Schema &other) const;
    void Serialize(duckdb::Serializer &s) const;
    static Schema Deserialize(duckdb::Deserializer &d);
};

duckdb::LogicalType ToLogicalType(const ColumnType &type);
ColumnType FromLogicalType(const duckdb::LogicalType &type);

uint32_t PhysicalWidth(const ColumnType &type);

} // namespace plume
