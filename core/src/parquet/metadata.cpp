#include "plume/parquet/metadata.hpp"

#include "plume/common/result.hpp"
#include "plume/parquet/thrift.hpp"

#include <cstring>
#include <stdexcept>
#include <string>

namespace plume::parquet {

namespace {

// PhysicalType, ConvertedType, FieldRepetitionType constants
enum PhysType { P_BOOLEAN = 0, P_INT32 = 1, P_INT64 = 2, P_FLOAT = 4, P_DOUBLE = 5, P_BYTE_ARRAY = 6 };
enum ConvType { CONV_UTF8 = 0, CONV_DECIMAL = 5, CONV_DATE = 6, CONV_TIME_MICROS = 8, CONV_INT_8 = 15, CONV_INT_16 = 16 };
enum Repetition { REP_REQUIRED = 0 };

std::vector<uint8_t> ReadBinary(CompactReader &r) {
    uint64_t n = r.Varint();
    std::vector<uint8_t> out(n);
    for (uint64_t i = 0; i < n; i++) {
        out[i] = r.Byte();
    }
    return out;
}

ColumnType MapParquetType(int32_t physical, bool has_conv, int32_t conv, int32_t scale, int32_t precision) {
    ColumnType t;
    switch (physical) {
    case P_BOOLEAN:
        t.id = TypeId::BOOLEAN;
        return t;
    case P_FLOAT:
        t.id = TypeId::FLOAT;
        return t;
    case P_DOUBLE:
        t.id = TypeId::DOUBLE;
        return t;
    case P_INT32:
        if (has_conv && conv == CONV_INT_8) { t.id = TypeId::INT8; return t; }
        if (has_conv && conv == CONV_INT_16) { t.id = TypeId::INT16; return t; }
        if (has_conv && conv == CONV_DATE) { t.id = TypeId::DATE; return t; }
        if (has_conv && conv == CONV_DECIMAL) {
            t.id = TypeId::DECIMAL;
            t.decimal_width = static_cast<uint8_t>(precision);
            t.decimal_scale = static_cast<uint8_t>(scale);
            return t;
        }
        t.id = TypeId::INT32;
        return t;
    case P_INT64:
        if (has_conv && conv == CONV_TIME_MICROS) { t.id = TypeId::TIME; return t; }
        if (has_conv && conv == CONV_DECIMAL) {
            t.id = TypeId::DECIMAL;
            t.decimal_width = static_cast<uint8_t>(precision);
            t.decimal_scale = static_cast<uint8_t>(scale);
            return t;
        }
        t.id = TypeId::INT64;
        return t;
    case P_BYTE_ARRAY:
        t.id = TypeId::VARCHAR; // UTF8 (or untyped byte array treated as string)
        return t;
    default:
        throw std::runtime_error("parquet: unsupported physical column type " + std::to_string(physical));
    }
}

struct SchemaElem {
    bool is_leaf = false;
    Column column;
};

SchemaElem ParseSchemaElement(CompactReader &r) {
    int16_t last = 0, id;
    uint8_t type;
    bool has_type = false, has_conv = false;
    int32_t physical = 0, conv = 0, scale = 0, precision = 0, repetition = REP_REQUIRED;
    std::string name;
    while (r.Field(last, type, id)) {
        if (id == 1 && type == CT_I32) {
            physical = static_cast<int32_t>(r.ZigZag());
            has_type = true;
        } else if (id == 3 && type == CT_I32) {
            repetition = static_cast<int32_t>(r.ZigZag());
        } else if (id == 4 && type == CT_BINARY) {
            auto b = ReadBinary(r);
            name.assign(b.begin(), b.end());
        } else if (id == 6 && type == CT_I32) {
            conv = static_cast<int32_t>(r.ZigZag());
            has_conv = true;
        } else if (id == 7 && type == CT_I32) {
            scale = static_cast<int32_t>(r.ZigZag());
        } else if (id == 8 && type == CT_I32) {
            precision = static_cast<int32_t>(r.ZigZag());
        } else {
            r.Skip(type);
        }
    }
    SchemaElem out;
    if (!has_type) {
        return out; // group / root element — not a column
    }
    out.column.type = MapParquetType(physical, has_conv, conv, scale, precision);
    out.column.name = std::move(name);
    out.column.nullable = (repetition != REP_REQUIRED);
    out.is_leaf = true;
    return out;
}

void ParseStatistics(CompactReader &r, ColumnStats &st) {
    int16_t last = 0, id;
    uint8_t type;
    std::vector<uint8_t> dep_min, dep_max;
    bool has_dep_min = false, has_dep_max = false;
    while (r.Field(last, type, id)) {
        if (id == 1 && type == CT_BINARY) { // deprecated max
            dep_max = ReadBinary(r);
            has_dep_max = true;
        } else if (id == 2 && type == CT_BINARY) { // deprecated min
            dep_min = ReadBinary(r);
            has_dep_min = true;
        } else if (id == 3 && type == CT_I64) {
            st.null_count = r.ZigZag();
            st.has_null_count = true;
        } else if (id == 4 && type == CT_I64) {
            st.distinct_count = r.ZigZag();
            st.has_distinct_count = true;
        } else if (id == 5 && type == CT_BINARY) { // max_value (preferred)
            st.max_value = ReadBinary(r);
            st.has_max = true;
        } else if (id == 6 && type == CT_BINARY) { // min_value (preferred)
            st.min_value = ReadBinary(r);
            st.has_min = true;
        } else {
            r.Skip(type);
        }
    }
    // Fall back to the deprecated min/max only if the preferred *_value is absent.
    if (!st.has_min && has_dep_min) {
        st.min_value = std::move(dep_min);
        st.has_min = true;
    }
    if (!st.has_max && has_dep_max) {
        st.max_value = std::move(dep_max);
        st.has_max = true;
    }
}

void ParseColumnMetaData(CompactReader &r, ColumnChunkMeta &cc) {
    int16_t last = 0, id;
    uint8_t type;
    int64_t total_compressed = 0, data_page = 0, dict_page = -1;
    while (r.Field(last, type, id)) {
        if (id == 4 && type == CT_I32) {
            cc.codec = static_cast<int32_t>(r.ZigZag()); // CompressionCodec
        } else if (id == 7 && type == CT_I64) {
            total_compressed = r.ZigZag();
        } else if (id == 9 && type == CT_I64) {
            data_page = r.ZigZag();
        } else if (id == 11 && type == CT_I64) {
            dict_page = r.ZigZag();
        } else if (id == 12 && type == CT_STRUCT) {
            ParseStatistics(r, cc.stats);
        } else {
            r.Skip(type);
        }
    }
    cc.offset = (dict_page > 0 && dict_page < data_page) ? dict_page : data_page;
    cc.size = total_compressed;
}

ColumnChunkMeta ParseColumnChunk(CompactReader &r) {
    ColumnChunkMeta cc;
    int16_t last = 0, id;
    uint8_t type;
    while (r.Field(last, type, id)) {
        if (id == 3 && type == CT_STRUCT) {
            ParseColumnMetaData(r, cc);
        } else {
            r.Skip(type);
        }
    }
    return cc;
}

RowGroupMeta ParseRowGroup(CompactReader &r) {
    RowGroupMeta rg;
    int16_t last = 0, id;
    uint8_t type;
    while (r.Field(last, type, id)) {
        if (id == 1 && type == CT_LIST) {
            uint64_t n;
            uint8_t et;
            r.ListHeader(n, et);
            rg.columns.reserve(n);
            for (uint64_t i = 0; i < n; i++) {
                rg.columns.push_back(ParseColumnChunk(r));
            }
        } else {
            r.Skip(type);
        }
    }
    return rg;
}

FileMeta ParseFileMetaData(CompactReader &r) {
    FileMeta fm;
    int16_t last = 0, id;
    uint8_t type;
    while (r.Field(last, type, id)) {
        if (id == 2 && type == CT_LIST) { // schema (root group + one leaf per column)
            uint64_t n;
            uint8_t et;
            r.ListHeader(n, et);
            for (uint64_t i = 0; i < n; i++) {
                SchemaElem el = ParseSchemaElement(r);
                if (el.is_leaf) {
                    fm.schema.columns.push_back(std::move(el.column));
                }
            }
        } else if (id == 3 && type == CT_I64) { // num_rows
            fm.num_rows = r.ZigZag();
        } else if (id == 4 && type == CT_LIST) { // row_groups
            uint64_t n;
            uint8_t et;
            r.ListHeader(n, et);
            fm.row_groups.reserve(n);
            for (uint64_t i = 0; i < n; i++) {
                fm.row_groups.push_back(ParseRowGroup(r));
            }
        } else {
            r.Skip(type);
        }
    }
    return fm;
}

} // namespace

Result<FileMeta> ParseFooter(const uint8_t *tail, size_t size) {
    if (size < 8 || std::memcmp(tail + size - 4, "PAR1", 4) != 0) {
        return Error("Missing PAR1 footer magic", ErrorKind::InvalidInput);
    }
    uint32_t meta_len;
    std::memcpy(&meta_len, tail + size - 8, 4); // little-endian
    if (static_cast<uint64_t>(meta_len) + 8 > size) {
        return Error("Footer buffer does not contain the full metadata", ErrorKind::InvalidInput);
    }
    return TryCatch([&]() -> FileMeta {
        CompactReader reader(tail + size - 8 - meta_len, meta_len);
        return ParseFileMetaData(reader);
    });
}

} // namespace plume::parquet
