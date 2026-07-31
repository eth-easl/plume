#include "plume/parquet/encoder.hpp"

#include "plume/parquet/thrift.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/vector.hpp"

#include "snappy.h"

#include <cstring>
#include <string>

namespace plume::parquet {

using duckdb::DataChunk;
using duckdb::idx_t;

namespace {

// parquet Type (physical), ConvertedType, FieldRepetitionType, Encoding, PageType.
enum PhysType { P_BOOLEAN = 0, P_INT32 = 1, P_INT64 = 2, P_FLOAT = 4, P_DOUBLE = 5, P_BYTE_ARRAY = 6 };
enum ConvType { CONV_UTF8 = 0, CONV_DECIMAL = 5, CONV_DATE = 6, CONV_TIME_MICROS = 8, CONV_INT_8 = 15, CONV_INT_16 = 16 };
enum Repetition { REP_REQUIRED = 0, REP_OPTIONAL = 1 };
enum Encoding { ENC_PLAIN = 0, ENC_RLE = 3 };
enum PgType { PG_DATA = 0 };

// Snappy-compress `in` (the layout the V1 decoder decompresses as a whole page).
std::vector<uint8_t> SnappyCompress(const std::vector<uint8_t> &in) {
    std::vector<uint8_t> out(duckdb_snappy::MaxCompressedLength(in.size()));
    size_t outlen = 0;
    duckdb_snappy::RawCompress(reinterpret_cast<const char *>(in.data()), in.size(),
                               reinterpret_cast<char *>(out.data()), &outlen);
    out.resize(outlen);
    return out;
}

// How a Plume column maps onto parquet (mirror of decoder.cpp DecodePlain).
struct PqMapping {
    int32_t physical = P_INT32;
    bool has_converted = false;
    int32_t converted = 0;
    bool is_decimal = false;
};

Result<PqMapping> MapType(const ColumnType &t) {
    switch (t.id) {
    case TypeId::BOOLEAN:
        return PqMapping{P_BOOLEAN, false, 0, false};
    case TypeId::INT8:
        return PqMapping{P_INT32, true, CONV_INT_8, false};
    case TypeId::INT16:
        return PqMapping{P_INT32, true, CONV_INT_16, false};
    case TypeId::INT32:
        return PqMapping{P_INT32, false, 0, false};
    case TypeId::INT64:
        return PqMapping{P_INT64, false, 0, false};
    case TypeId::FLOAT:
        return PqMapping{P_FLOAT, false, 0, false};
    case TypeId::DOUBLE:
        return PqMapping{P_DOUBLE, false, 0, false};
    case TypeId::VARCHAR:
        return PqMapping{P_BYTE_ARRAY, true, CONV_UTF8, false};
    case TypeId::DATE:
        return PqMapping{P_INT32, true, CONV_DATE, false};
    case TypeId::TIME:
        return PqMapping{P_INT64, true, CONV_TIME_MICROS, false};
    case TypeId::DECIMAL:
        if (t.decimal_width <= 9) {
            return PqMapping{P_INT32, true, CONV_DECIMAL, true};
        }
        if (t.decimal_width <= 18) {
            return PqMapping{P_INT64, true, CONV_DECIMAL, true};
        }
        return Error("parquet write: DECIMAL width > 18 not supported", ErrorKind::NotImplemented);
    case TypeId::HUGEINT:
        return Error("parquet write: HUGEINT not supported", ErrorKind::NotImplemented);
    }
    return Error("parquet write: unsupported column type", ErrorKind::NotImplemented);
}

void PutI32(std::vector<uint8_t> &o, int32_t v) {
    uint32_t u = static_cast<uint32_t>(v);
    for (int i = 0; i < 4; i++) {
        o.push_back(uint8_t(u >> (8 * i)));
    }
}
void PutI64(std::vector<uint8_t> &o, int64_t v) {
    uint64_t u = static_cast<uint64_t>(v);
    for (int i = 0; i < 8; i++) {
        o.push_back(uint8_t(u >> (8 * i)));
    }
}

// One bit-packed RLE/hybrid run of 0/1 definition levels (bit width 1, LSB-first),
// the layout the V1 decoder reads back via RleDecode.
std::vector<uint8_t> EncodeDefLevels(const std::vector<uint8_t> &levels) {
    const size_t groups = (levels.size() + 7) / 8;
    std::vector<uint8_t> out;
    // run header: (groups << 1) | 1  -> bit-packed run
    uint64_t header = (uint64_t(groups) << 1) | 1;
    while (header > 0x7f) {
        out.push_back(uint8_t((header & 0x7f) | 0x80));
        header >>= 7;
    }
    out.push_back(uint8_t(header));
    std::vector<uint8_t> packed(groups, 0);
    for (size_t i = 0; i < levels.size(); i++) {
        if (levels[i]) {
            packed[i >> 3] |= uint8_t(1 << (i & 7));
        }
    }
    out.insert(out.end(), packed.begin(), packed.end());
    return out;
}

// PLAIN-encode the non-null values of column `col` across all chunks into `values`,
// recording definition levels (when nullable) and the total row count.
Result<void> EncodeColumnValues(const ColumnType &type, bool nullable,
                                const std::vector<DataChunk *> &chunks, idx_t col,
                                std::vector<uint8_t> &values, std::vector<uint8_t> &def_levels,
                                int64_t &total_rows) {
    std::vector<uint8_t> bool_vals; // 0/1 per non-null value (BOOLEAN only)
    for (auto *chunk : chunks) {
        if (chunk == nullptr || chunk->size() == 0) {
            continue;
        }
        const idx_t rows = chunk->size();
        auto &vec = chunk->data[col];
        vec.Flatten(rows);
        auto &validity = duckdb::FlatVector::Validity(vec);
        for (idx_t r = 0; r < rows; r++) {
            const bool valid = nullable ? validity.RowIsValid(r) : true;
            if (nullable) {
                def_levels.push_back(valid ? 1 : 0);
            }
            total_rows++;
            if (!valid) {
                continue; // null: no value bytes
            }
            switch (type.id) {
            case TypeId::BOOLEAN:
                bool_vals.push_back(duckdb::FlatVector::GetData<bool>(vec)[r] ? 1 : 0);
                break;
            case TypeId::INT8:
                PutI32(values, static_cast<int32_t>(duckdb::FlatVector::GetData<int8_t>(vec)[r]));
                break;
            case TypeId::INT16:
                PutI32(values, static_cast<int32_t>(duckdb::FlatVector::GetData<int16_t>(vec)[r]));
                break;
            case TypeId::INT32:
                PutI32(values, duckdb::FlatVector::GetData<int32_t>(vec)[r]);
                break;
            case TypeId::INT64:
                PutI64(values, duckdb::FlatVector::GetData<int64_t>(vec)[r]);
                break;
            case TypeId::DATE:
                PutI32(values, duckdb::FlatVector::GetData<duckdb::date_t>(vec)[r].days);
                break;
            case TypeId::TIME:
                PutI64(values, duckdb::FlatVector::GetData<duckdb::dtime_t>(vec)[r].micros);
                break;
            case TypeId::DECIMAL:
                // DuckDB's decimal physical storage is int16 (w<=4), int32 (<=9), int64
                // (<=18); parquet stores the same unscaled integer as INT32 (<=9) or
                // INT64 (<=18). Read the physical width, write the parquet width.
                if (type.decimal_width <= 4) {
                    PutI32(values, static_cast<int32_t>(duckdb::FlatVector::GetData<int16_t>(vec)[r]));
                } else if (type.decimal_width <= 9) {
                    PutI32(values, duckdb::FlatVector::GetData<int32_t>(vec)[r]);
                } else {
                    PutI64(values, duckdb::FlatVector::GetData<int64_t>(vec)[r]);
                }
                break;
            case TypeId::FLOAT: {
                float f = duckdb::FlatVector::GetData<float>(vec)[r];
                uint32_t u;
                std::memcpy(&u, &f, 4);
                PutI32(values, static_cast<int32_t>(u));
                break;
            }
            case TypeId::DOUBLE: {
                double d = duckdb::FlatVector::GetData<double>(vec)[r];
                uint64_t u;
                std::memcpy(&u, &d, 8);
                PutI64(values, static_cast<int64_t>(u));
                break;
            }
            case TypeId::VARCHAR: {
                const auto &s = duckdb::FlatVector::GetData<duckdb::string_t>(vec)[r];
                PutI32(values, static_cast<int32_t>(s.GetSize())); // BYTE_ARRAY: u32 LE length
                values.insert(values.end(), reinterpret_cast<const uint8_t *>(s.GetData()),
                              reinterpret_cast<const uint8_t *>(s.GetData()) + s.GetSize());
                break;
            }
            case TypeId::HUGEINT:
                return Error("parquet write: HUGEINT not supported", ErrorKind::NotImplemented);
            }
        }
    }
    if (type.id == TypeId::BOOLEAN) {
        // PLAIN boolean: 1 bit per non-null value, LSB-first.
        values.assign((bool_vals.size() + 7) / 8, 0);
        for (size_t i = 0; i < bool_vals.size(); i++) {
            if (bool_vals[i]) {
                values[i >> 3] |= uint8_t(1 << (i & 7));
            }
        }
    }
    return Ok();
}

} // namespace

Result<DataBuffer> WriteParquet(const Schema &schema, const std::vector<DataChunk *> &chunks, int32_t codec) {
    if (codec != WRITE_UNCOMPRESSED && codec != WRITE_SNAPPY) {
        return Error("parquet write: unsupported compression codec", ErrorKind::NotImplemented);
    }
    const size_t ncols = schema.size();

    // Validate type support up front.
    std::vector<PqMapping> maps(ncols);
    for (size_t c = 0; c < ncols; c++) {
        TRY(maps[c], MapType(schema.columns[c].type));
    }

    // Encode each column chunk (page header + PLAIN data page, optionally Snappy).
    std::vector<std::vector<uint8_t>> col_bytes(ncols);
    std::vector<int64_t> col_uncomp(ncols); // header + uncompressed page (for the footer)
    int64_t num_rows = 0;
    for (size_t c = 0; c < ncols; c++) {
        const auto &col = schema.columns[c];
        std::vector<uint8_t> values, def_levels;
        int64_t rows = 0;
        TRYV(EncodeColumnValues(col.type, col.nullable, chunks, c, values, def_levels, rows));
        num_rows = rows; // identical across columns

        std::vector<uint8_t> page;
        if (col.nullable) {
            auto dl = EncodeDefLevels(def_levels);
            PutI32(page, static_cast<int32_t>(dl.size())); // V1 prefixes def levels with a u32 LE length
            page.insert(page.end(), dl.begin(), dl.end());
        }
        page.insert(page.end(), values.begin(), values.end());

        // V1 compresses the whole page (levels + values) as one unit.
        const size_t uncomp_size = page.size();
        std::vector<uint8_t> stored = codec == WRITE_SNAPPY ? SnappyCompress(page) : std::move(page);

        CompactWriter ph; // DataPageHeader V1
        ph.I32(1, PG_DATA);
        ph.I32(2, static_cast<int32_t>(uncomp_size));   // uncompressed_page_size
        ph.I32(3, static_cast<int32_t>(stored.size())); // compressed_page_size
        ph.StructField(5);                              // data_page_header
        ph.I32(1, static_cast<int32_t>(rows));          // num_values (incl. nulls)
        ph.I32(2, ENC_PLAIN);                           // encoding
        ph.I32(3, ENC_RLE);                             // definition_level_encoding
        ph.I32(4, ENC_RLE);                             // repetition_level_encoding
        ph.StructEnd();
        ph.Stop();

        auto &cb = col_bytes[c];
        cb.insert(cb.end(), ph.bytes().begin(), ph.bytes().end());
        cb.insert(cb.end(), stored.begin(), stored.end());
        col_uncomp[c] = static_cast<int64_t>(ph.bytes().size() + uncomp_size);
    }

    // Assemble: PAR1 | column data | footer | [u32 footer_len] | PAR1.
    std::vector<uint8_t> file = {'P', 'A', 'R', '1'};
    std::vector<int64_t> col_offset(ncols), col_size(ncols);
    int64_t total_byte_size = 0;
    for (size_t c = 0; c < ncols; c++) {
        col_offset[c] = static_cast<int64_t>(file.size());
        col_size[c] = static_cast<int64_t>(col_bytes[c].size());
        total_byte_size += col_size[c];
        file.insert(file.end(), col_bytes[c].begin(), col_bytes[c].end());
    }

    CompactWriter f; // FileMetaData
    f.I32(1, 1);     // version
    f.List(2, CT_STRUCT, ncols + 1); // schema: root group + one leaf per column
    f.ElementBegin();                // root
    f.String(4, "schema");
    f.I32(5, static_cast<int32_t>(ncols)); // num_children
    f.StructEnd();
    for (size_t c = 0; c < ncols; c++) {
        const auto &col = schema.columns[c];
        f.ElementBegin();
        f.I32(1, maps[c].physical);                                       // type
        f.I32(3, col.nullable ? REP_OPTIONAL : REP_REQUIRED);             // repetition_type
        f.String(4, col.name);                                           // name
        if (maps[c].has_converted) {
            f.I32(6, maps[c].converted); // converted_type
        }
        if (maps[c].is_decimal) {
            f.I32(7, col.type.decimal_scale); // scale
            f.I32(8, col.type.decimal_width); // precision
        }
        f.StructEnd();
    }
    f.I64(3, num_rows);
    f.List(4, CT_STRUCT, 1); // row_groups
    f.ElementBegin();        // RowGroup
    f.List(1, CT_STRUCT, ncols); // columns
    for (size_t c = 0; c < ncols; c++) {
        f.ElementBegin();         // ColumnChunk
        f.I64(2, col_offset[c]);  // file_offset
        f.StructField(3);         // meta_data (ColumnMetaData)
        f.I32(1, maps[c].physical);
        f.List(2, CT_I32, 2); // encodings
        f.I32Elem(ENC_PLAIN);
        f.I32Elem(ENC_RLE);
        f.List(3, CT_BINARY, 1); // path_in_schema
        f.StringElem(schema.columns[c].name);
        f.I32(4, codec); // CompressionCodec (UNCOMPRESSED / SNAPPY)
        f.I64(5, num_rows);
        f.I64(6, col_uncomp[c]); // total_uncompressed_size
        f.I64(7, col_size[c]);   // total_compressed_size (bytes stored)
        f.I64(9, col_offset[c]); // data_page_offset
        f.StructEnd();           // ColumnMetaData
        f.StructEnd();           // ColumnChunk
    }
    f.I64(2, total_byte_size);
    f.I64(3, num_rows);
    f.StructEnd(); // RowGroup
    f.String(6, "plume");
    f.Stop(); // FileMetaData

    file.insert(file.end(), f.bytes().begin(), f.bytes().end());
    PutI32(file, static_cast<int32_t>(f.bytes().size()));
    file.push_back('P');
    file.push_back('A');
    file.push_back('R');
    file.push_back('1');

    DataBuffer out(file.size());
    std::memcpy(out.mutable_data(), file.data(), file.size());
    return out;
}

} // namespace plume::parquet
