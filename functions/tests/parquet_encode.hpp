// Test-only helpers to hand-encode parquet pages, column chunks, and whole files
// (Thrift compact protocol). Used to drive the decoder / pq_prepare / pq_stage with
// fully known values.
#pragma once

#include "snappy.h"

#include <cstdint>
#include <utility>
#include <vector>

namespace pqtest {

constexpr uint8_t T_I32 = 5, T_I64 = 6, T_STRUCT = 12;
constexpr int ENC_PLAIN = 0, ENC_RLE_DICTIONARY = 8;
constexpr int PAGE_DATA = 0, PAGE_DICT = 2, PAGE_DATA_V2 = 3;
constexpr int CODEC_UNCOMPRESSED = 0, CODEC_SNAPPY = 1;

struct W {
    std::vector<uint8_t> b;
    void Byte(uint8_t v) { b.push_back(v); }
    void Varint(uint64_t v) {
        while (v > 0x7f) {
            b.push_back(uint8_t((v & 0x7f) | 0x80));
            v >>= 7;
        }
        b.push_back(uint8_t(v));
    }
    void ZigZag(int64_t n) { Varint((uint64_t(n) << 1) ^ uint64_t(n >> 63)); }
    void Field(uint8_t delta, uint8_t type) { Byte(uint8_t((delta << 4) | type)); }
    void Stop() { Byte(0); }
    void Raw(const std::vector<uint8_t> &x) { b.insert(b.end(), x.begin(), x.end()); }
};

inline std::vector<uint8_t> U32LE(uint32_t v) {
    return {uint8_t(v), uint8_t(v >> 8), uint8_t(v >> 16), uint8_t(v >> 24)};
}

inline std::vector<uint8_t> Concat(std::vector<uint8_t> a, const std::vector<uint8_t> &b) {
    a.insert(a.end(), b.begin(), b.end());
    return a;
}

inline std::vector<uint8_t> PlainInt32(const std::vector<int32_t> &vals) {
    std::vector<uint8_t> out;
    for (int32_t v : vals) {
        auto le = U32LE(static_cast<uint32_t>(v));
        out.insert(out.end(), le.begin(), le.end());
    }
    return out;
}

inline std::vector<uint8_t> PlainInt64(const std::vector<int64_t> &vals) {
    std::vector<uint8_t> out;
    for (int64_t v : vals) {
        auto u = static_cast<uint64_t>(v);
        for (int i = 0; i < 8; i++) {
            out.push_back(uint8_t(u >> (8 * i)));
        }
    }
    return out;
}

// One bit-packed RLE/hybrid run of `vals` at `bit_width` bits (LSB-first).
inline std::vector<uint8_t> RleBitPack(const std::vector<uint32_t> &vals, int bit_width) {
    const size_t num = vals.size();
    const size_t groups = (num + 7) / 8;
    const size_t total = groups * 8;
    W w;
    w.Varint((uint64_t(groups) << 1) | 1);
    std::vector<uint8_t> packed(groups * bit_width, 0);
    size_t bit_pos = 0;
    for (size_t v = 0; v < total; v++) {
        uint32_t val = (v < num) ? vals[v] : 0;
        for (int b = 0; b < bit_width; b++) {
            if ((val >> b) & 1) {
                size_t bi = bit_pos + b;
                packed[bi >> 3] |= uint8_t(1 << (bi & 7));
            }
        }
        bit_pos += bit_width;
    }
    w.Raw(packed);
    return w.b;
}

inline std::vector<uint8_t> SnappyCompress(const std::vector<uint8_t> &in) {
    std::vector<uint8_t> out(duckdb_snappy::MaxCompressedLength(in.size()));
    size_t outlen = 0;
    duckdb_snappy::RawCompress(reinterpret_cast<const char *>(in.data()), in.size(),
                               reinterpret_cast<char *>(out.data()), &outlen);
    out.resize(outlen);
    return out;
}

inline std::vector<uint8_t> DataPageV1Header(int32_t num_values, int32_t encoding, int32_t uncomp, int32_t comp) {
    W w;
    w.Field(1, T_I32);
    w.ZigZag(PAGE_DATA);
    w.Field(1, T_I32);
    w.ZigZag(uncomp);
    w.Field(1, T_I32);
    w.ZigZag(comp);
    w.Field(2, T_STRUCT); // data_page_header (field 5)
    w.Field(1, T_I32);
    w.ZigZag(num_values);
    w.Field(1, T_I32);
    w.ZigZag(encoding);
    w.Stop();
    w.Stop();
    return w.b;
}

inline std::vector<uint8_t> DataPageV2Header(int32_t num_values, int32_t encoding, int32_t uncomp, int32_t comp,
                                             int32_t def_len, int32_t rep_len, bool is_compressed) {
    W w;
    w.Field(1, T_I32);
    w.ZigZag(PAGE_DATA_V2);
    w.Field(1, T_I32);
    w.ZigZag(uncomp);
    w.Field(1, T_I32);
    w.ZigZag(comp);
    w.Field(5, T_STRUCT); // data_page_header_v2 (field 8)
    w.Field(1, T_I32);
    w.ZigZag(num_values);
    w.Field(3, T_I32); // delta 3 -> field 4 encoding
    w.ZigZag(encoding);
    w.Field(1, T_I32); // field 5 def_levels_byte_length
    w.ZigZag(def_len);
    w.Field(1, T_I32); // field 6 rep_levels_byte_length
    w.ZigZag(rep_len);
    w.Field(1, is_compressed ? 1 : 2); // field 7 is_compressed (bool in type)
    w.Stop();
    w.Stop();
    return w.b;
}

inline std::vector<uint8_t> DictPageHeader(int32_t num_values, int32_t uncomp, int32_t comp) {
    W w;
    w.Field(1, T_I32);
    w.ZigZag(PAGE_DICT);
    w.Field(1, T_I32);
    w.ZigZag(uncomp);
    w.Field(1, T_I32);
    w.ZigZag(comp);
    w.Field(4, T_STRUCT); // dictionary_page_header (field 7)
    w.Field(1, T_I32);
    w.ZigZag(num_values);
    w.Field(1, T_I32);
    w.ZigZag(ENC_PLAIN);
    w.Stop();
    w.Stop();
    return w.b;
}

// Required INT32, PLAIN V1 data page, optionally Snappy-compressed.
inline std::vector<uint8_t> PlainInt32Chunk(const std::vector<int32_t> &vals, int codec = CODEC_UNCOMPRESSED) {
    auto raw = PlainInt32(vals);
    auto page = codec == CODEC_SNAPPY ? SnappyCompress(raw) : raw;
    return Concat(DataPageV1Header(int32_t(vals.size()), ENC_PLAIN, int32_t(raw.size()), int32_t(page.size())), page);
}

// Required column whose parquet physical is INT64 (time / larger decimal).
inline std::vector<uint8_t> PlainInt64Chunk(const std::vector<int64_t> &vals) {
    auto raw = PlainInt64(vals);
    return Concat(DataPageV1Header(int32_t(vals.size()), ENC_PLAIN, int32_t(raw.size()), int32_t(raw.size())), raw);
}

// Required INT32, dictionary page + RLE_DICTIONARY V1 data page (uncompressed).
inline std::vector<uint8_t> DictInt32Chunk(const std::vector<int32_t> &dict, const std::vector<uint32_t> &indices,
                                           int bit_width) {
    auto dict_data = PlainInt32(dict);
    auto chunk = Concat(DictPageHeader(int32_t(dict.size()), int32_t(dict_data.size()), int32_t(dict_data.size())),
                        dict_data);
    std::vector<uint8_t> page_data;
    page_data.push_back(uint8_t(bit_width));
    auto rle = RleBitPack(indices, bit_width);
    page_data.insert(page_data.end(), rle.begin(), rle.end());
    auto dp = Concat(DataPageV1Header(int32_t(indices.size()), ENC_RLE_DICTIONARY, int32_t(page_data.size()),
                                      int32_t(page_data.size())),
                     page_data);
    return Concat(chunk, dp);
}

// Nullable INT32, PLAIN values with RLE definition levels (bit width 1), V1.
inline std::vector<uint8_t> NullableInt32Chunk(const std::vector<uint32_t> &def_levels,
                                               const std::vector<int32_t> &non_null_vals) {
    auto def = RleBitPack(def_levels, 1);
    std::vector<uint8_t> page_data = U32LE(uint32_t(def.size()));
    page_data.insert(page_data.end(), def.begin(), def.end());
    auto vals = PlainInt32(non_null_vals);
    page_data.insert(page_data.end(), vals.begin(), vals.end());
    return Concat(DataPageV1Header(int32_t(def_levels.size()), ENC_PLAIN, int32_t(page_data.size()),
                                   int32_t(page_data.size())),
                  page_data);
}

// Nullable INT32, V2 data page: uncompressed levels in front, values compressed.
inline std::vector<uint8_t> NullableInt32ChunkV2(const std::vector<uint32_t> &def_levels,
                                                 const std::vector<int32_t> &non_null_vals, int codec) {
    auto def = RleBitPack(def_levels, 1);
    auto vals_raw = PlainInt32(non_null_vals);
    auto vals = codec == CODEC_SNAPPY ? SnappyCompress(vals_raw) : vals_raw;
    std::vector<uint8_t> page_data = def;
    page_data.insert(page_data.end(), vals.begin(), vals.end());
    const int32_t uncomp = int32_t(def.size() + vals_raw.size());
    const int32_t comp = int32_t(def.size() + vals.size());
    return Concat(DataPageV2Header(int32_t(def_levels.size()), ENC_PLAIN, uncomp, comp, int32_t(def.size()),
                                   /*rep_len=*/0, /*is_compressed=*/codec == CODEC_SNAPPY),
                  page_data);
}

// Build a full parquet file from one row group of pre-encoded column chunks
// (each with its codec). Footer carries only what pq_prepare needs: per-column
// offset, total_compressed_size, codec.
inline std::vector<uint8_t> BuildParquetFile(const std::vector<std::pair<std::vector<uint8_t>, int>> &columns) {
    std::vector<uint8_t> file = {'P', 'A', 'R', '1'};
    std::vector<int64_t> offsets, sizes;
    std::vector<int> codecs;
    for (auto &col : columns) {
        offsets.push_back(int64_t(file.size()));
        sizes.push_back(int64_t(col.first.size()));
        codecs.push_back(col.second);
        file.insert(file.end(), col.first.begin(), col.first.end());
    }

    W w;
    w.Field(4, 9);                  // FileMetaData.row_groups (list)
    w.Byte(uint8_t((1 << 4) | 12)); // list header: 1 row group, element STRUCT
    // RowGroup
    w.Field(1, 9); // RowGroup.columns (list)
    w.Byte(uint8_t((uint8_t(columns.size()) << 4) | 12));
    for (size_t i = 0; i < columns.size(); i++) {
        w.Field(3, T_STRUCT); // ColumnChunk.meta_data
        w.Field(4, T_I32);    // codec (i32)
        w.ZigZag(codecs[i]);
        w.Field(3, T_I64); // delta 3 -> field 7 total_compressed_size (i64)
        w.ZigZag(sizes[i]);
        w.Field(2, T_I64); // delta 2 -> field 9 data_page_offset (i64)
        w.ZigZag(offsets[i]);
        w.Stop(); // ColumnMetaData
        w.Stop(); // ColumnChunk
    }
    w.Stop(); // RowGroup
    w.Stop(); // FileMetaData

    file.insert(file.end(), w.b.begin(), w.b.end());
    auto len = U32LE(uint32_t(w.b.size()));
    file.insert(file.end(), len.begin(), len.end());
    file.push_back('P');
    file.push_back('A');
    file.push_back('R');
    file.push_back('1');
    return file;
}

} // namespace pqtest
