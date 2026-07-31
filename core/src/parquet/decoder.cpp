#include "plume/parquet/decoder.hpp"

#include "plume/parquet/thrift.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"

#include "snappy.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace plume::parquet {

namespace {

enum PageType { PAGE_DATA = 0, PAGE_DICTIONARY = 2, PAGE_DATA_V2 = 3 };
enum Encoding { ENC_PLAIN = 0, ENC_PLAIN_DICTIONARY = 2, ENC_RLE = 3, ENC_RLE_DICTIONARY = 8 };
enum Codec { CODEC_UNCOMPRESSED = 0, CODEC_SNAPPY = 1 };

struct PageHeader {
    int32_t type = -1;
    int32_t uncompressed_size = 0;
    int32_t compressed_size = 0;
    int32_t num_values = 0;
    int32_t encoding = ENC_PLAIN;
    bool is_v2 = false;
    int32_t def_levels_len = 0;
    int32_t rep_levels_len = 0;
    bool is_compressed = true;
};

void ParseDataOrDictHeader(CompactReader &r, PageHeader &ph) {
    int16_t last = 0, id;
    uint8_t type;
    while (r.Field(last, type, id)) {
        if (id == 1 && type == CT_I32) {
            ph.num_values = static_cast<int32_t>(r.ZigZag());
        } else if (id == 2 && type == CT_I32) {
            ph.encoding = static_cast<int32_t>(r.ZigZag());
        } else {
            r.Skip(type);
        }
    }
}

void ParseV2Header(CompactReader &r, PageHeader &ph) {
    ph.is_v2 = true;
    int16_t last = 0, id;
    uint8_t type;
    while (r.Field(last, type, id)) {
        if (id == 1 && type == CT_I32) {
            ph.num_values = static_cast<int32_t>(r.ZigZag());
        } else if (id == 4 && type == CT_I32) {
            ph.encoding = static_cast<int32_t>(r.ZigZag());
        } else if (id == 5 && type == CT_I32) {
            ph.def_levels_len = static_cast<int32_t>(r.ZigZag());
        } else if (id == 6 && type == CT_I32) {
            ph.rep_levels_len = static_cast<int32_t>(r.ZigZag());
        } else if (id == 7 && (type == CT_BOOL_TRUE || type == CT_BOOL_FALSE)) {
            ph.is_compressed = (type == CT_BOOL_TRUE);
        } else {
            r.Skip(type);
        }
    }
}

PageHeader ParsePageHeader(CompactReader &r) {
    PageHeader ph;
    int16_t last = 0, id;
    uint8_t type;
    while (r.Field(last, type, id)) {
        if (id == 1 && type == CT_I32) {
            ph.type = static_cast<int32_t>(r.ZigZag());
        } else if (id == 2 && type == CT_I32) {
            ph.uncompressed_size = static_cast<int32_t>(r.ZigZag());
        } else if (id == 3 && type == CT_I32) {
            ph.compressed_size = static_cast<int32_t>(r.ZigZag());
        } else if (id == 5 && type == CT_STRUCT) {
            ParseDataOrDictHeader(r, ph);
        } else if (id == 7 && type == CT_STRUCT) {
            ParseDataOrDictHeader(r, ph);
        } else if (id == 8 && type == CT_STRUCT) {
            ParseV2Header(r, ph);
        } else {
            r.Skip(type);
        }
    }
    return ph;
}

uint32_t ReadU32LE(const uint8_t *p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}

ByteVec Decompress(int codec, const uint8_t *src, size_t comp_size, size_t uncomp_size) {
    if (codec == CODEC_SNAPPY) {
        ByteVec out;
        out.resize(uncomp_size); // uninitialized; snappy fills every byte
        if (!duckdb_snappy::RawUncompress(reinterpret_cast<const char *>(src), comp_size,
                                          reinterpret_cast<char *>(out.data()))) {
            throw std::runtime_error("parquet: snappy decompression failed");
        }
        return out;
    }
    throw std::runtime_error("parquet: unsupported compression codec");
}

// Walk an RLE/bit-pack hybrid stream, delivering the `count` decoded values via
// two callbacks: `emit_run(value, n)` for a repeated-value run and `emit_one(value)`
// for a single bit-packed value. This is the shared decode primitive — RleDecode
// materializes the values into a vector, while the dictionary scatter applies the
// dictionary in-place with no intermediate index array. Uses 64-bit word reads to
// avoid the O(bit_width) inner loop in the common bit-packed case.
template <class EmitRun, class EmitOne>
void RleWalk(const uint8_t *data, size_t size, size_t count, int bit_width, EmitRun emit_run, EmitOne emit_one) {
    if (count == 0) {
        return;
    }
    if (bit_width == 0) {
        emit_run(uint64_t(0), count);
        return;
    }
    const size_t byte_width = (bit_width + 7) / 8;
    const uint64_t mask = (bit_width == 64) ? ~uint64_t(0) : ((uint64_t(1) << bit_width) - 1);
    size_t pos = 0;
    size_t done = 0;

    auto read_varint = [&]() {
        uint64_t r = 0;
        int shift = 0;
        while (pos < size) {
            uint8_t b = data[pos++];
            r |= uint64_t(b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return r;
    };

    while (done < count && pos < size) {
        uint64_t header = read_varint();
        if ((header & 1) == 0) {
            // RLE run: repeated value
            uint64_t run_len = header >> 1;
            uint64_t val = 0;
            for (size_t i = 0; i < byte_width && pos < size; i++) {
                val |= uint64_t(data[pos++]) << (8 * i);
            }
            size_t n = static_cast<size_t>(run_len);
            if (n > count - done) {
                n = count - done;
            }
            if (n) {
                emit_run(val, n);
                done += n;
            }
        } else {
            // Bit-packed section: groups * 8 values packed at bit_width bits each.
            uint64_t groups = header >> 1;
            const size_t num = static_cast<size_t>(groups) * 8;
            const uint8_t *base = data + pos;
            const size_t buf_remaining = size - pos; // bytes from base to end

            // Safe 8-byte reads: possible when (i * bit_width / 8) + 8 ≤ buf_remaining,
            // conservatively: i * bit_width < (buf_remaining - 8) * 8.
            const size_t fast_end = (buf_remaining > 8)
                                        ? std::min(num, (buf_remaining - 8) * 8 / static_cast<size_t>(bit_width))
                                        : 0;

            // Fast path: unaligned 64-bit word read extracts the value in one shot.
            size_t i = 0;
            for (; i < fast_end && done < count; i++, done++) {
                const size_t bit_pos = i * static_cast<size_t>(bit_width);
                uint64_t word;
                std::memcpy(&word, base + (bit_pos >> 3), sizeof(word));
                emit_one((word >> (bit_pos & 7)) & mask);
            }
            // Slow path: near end of buffer, fall back to bit-by-bit extraction.
            const size_t avail_bits = buf_remaining * 8;
            for (; i < num && done < count; i++, done++) {
                const size_t bit_pos = i * static_cast<size_t>(bit_width);
                uint64_t v = 0;
                for (int b = 0; b < bit_width; b++) {
                    const size_t bit_index = bit_pos + b;
                    if (bit_index >= avail_bits) {
                        break;
                    }
                    if ((base[bit_index >> 3] >> (bit_index & 7)) & 1) {
                        v |= (uint64_t(1) << b);
                    }
                }
                emit_one(v);
            }
            pos += static_cast<size_t>(groups) * static_cast<size_t>(bit_width);
        }
    }
    // Truncated stream: pad the shortfall with zeros (matches prior behavior).
    if (done < count) {
        emit_run(uint64_t(0), count - done);
    }
}

// RLE/bit-pack hybrid decoder materializing all values (used for definition levels).
std::vector<uint64_t> RleDecode(const uint8_t *data, size_t size, size_t count, int bit_width) {
    std::vector<uint64_t> out;
    out.reserve(count);
    RleWalk(
        data, size, count, bit_width, [&](uint64_t v, size_t n) { out.insert(out.end(), n, v); },
        [&](uint64_t v) { out.push_back(v); });
    return out;
}

// Decode `count` dictionary indices (leading byte of `vals` is the RLE bit width)
// into a flat index array — used by the streaming reader, which gathers the
// dictionary directly into the target vector afterwards.
std::vector<uint32_t> DecodeDictIndices(const uint8_t *vals, size_t vals_size, size_t count) {
    if (vals_size == 0) {
        throw std::runtime_error("parquet: truncated dictionary data page");
    }
    const int bit_width = vals[0];
    const uint8_t *data = vals + 1;
    const size_t size = vals_size - 1;
    std::vector<uint32_t> idx;
    idx.reserve(count);
    RleWalk(
        data, size, count, bit_width,
        [&](uint64_t v, size_t n) { idx.insert(idx.end(), n, static_cast<uint32_t>(v)); },
        [&](uint64_t v) { idx.push_back(static_cast<uint32_t>(v)); });
    return idx;
}

// Returns true if parquet's PLAIN encoding uses the same byte layout as
// DuckDB's FlatVector physical storage for this type, enabling a bulk memcpy.
bool CanBulkCopy(const ColumnType &type) {
    switch (type.id) {
    case TypeId::INT32:
    case TypeId::INT64:
    case TypeId::FLOAT:
    case TypeId::DOUBLE:
    case TypeId::DATE:  // parquet INT32 == DuckDB date_t (both 4-byte LE)
    case TypeId::TIME:  // parquet INT64 == DuckDB dtime_t (both 8-byte LE)
        return true;
    case TypeId::DECIMAL:
        // width 5-9: parquet INT32 == DuckDB int32_t; width 10-18: INT64 == int64_t.
        // width 1-4 maps to DuckDB int16_t (not same as parquet INT32), so not bulk.
        return type.decimal_width > 4;
    default:
        return false;
    }
}

// Decode `count` PLAIN-encoded non-null values into out_raw (fixed-width) or
// out_views (VARCHAR). `stride` = ColumnPhysicalWidth(type). VARCHAR views borrow
// `data` (no copy); the caller keeps that buffer alive.
void DecodePlainRaw(const uint8_t *data, size_t size, size_t &pos, const ColumnType &type, size_t stride,
                    size_t count, ByteVec &out_raw, std::vector<StrView> &out_views) {
    auto need = [&](size_t n) {
        if (pos + n > size) {
            throw std::runtime_error("parquet: truncated PLAIN page");
        }
    };

    if (type.id == TypeId::VARCHAR) {
        out_views.reserve(out_views.size() + count);
        for (size_t i = 0; i < count; i++) {
            need(4);
            uint32_t len = ReadU32LE(data + pos);
            pos += 4;
            need(len);
            out_views.push_back({reinterpret_cast<const char *>(data + pos), len});
            pos += len;
        }
        return;
    }

    if (type.id == TypeId::BOOLEAN) {
        const size_t off = out_raw.size();
        out_raw.resize(off + count);
        uint8_t *dst = out_raw.data() + off;
        for (size_t i = 0; i < count; i++) {
            const size_t byte_idx = pos + (i >> 3);
            if (byte_idx >= size) {
                throw std::runtime_error("parquet: truncated PLAIN boolean page");
            }
            dst[i] = (data[byte_idx] >> (i & 7)) & 1;
        }
        pos += (count + 7) / 8;
        return;
    }

    // For types where the parquet PLAIN layout == DuckDB physical layout,
    // a single memcpy replaces the entire per-value loop.
    if (CanBulkCopy(type)) {
        const size_t bytes = count * stride;
        need(bytes);
        const size_t off = out_raw.size();
        out_raw.resize(off + bytes);
        std::memcpy(out_raw.data() + off, data + pos, bytes);
        pos += bytes;
        return;
    }

    // Types that require per-value conversion (narrowing or type change).
    const size_t off = out_raw.size();
    out_raw.resize(off + count * stride);
    uint8_t *dst = out_raw.data() + off;

    for (size_t i = 0; i < count; i++) {
        uint8_t *d = dst + i * stride;
        switch (type.id) {
        case TypeId::INT8: {
            need(4);
            int32_t v;
            std::memcpy(&v, data + pos, 4);
            pos += 4;
            int8_t v8 = static_cast<int8_t>(v);
            std::memcpy(d, &v8, 1);
            break;
        }
        case TypeId::INT16: {
            need(4);
            int32_t v;
            std::memcpy(&v, data + pos, 4);
            pos += 4;
            int16_t v16 = static_cast<int16_t>(v);
            std::memcpy(d, &v16, 2);
            break;
        }
        case TypeId::DECIMAL:
            // Only width 1-4 reaches here (decimal_width > 4 is handled by CanBulkCopy).
            {
                need(4);
                int32_t v;
                std::memcpy(&v, data + pos, 4);
                pos += 4;
                int16_t v16 = static_cast<int16_t>(v);
                std::memcpy(d, &v16, 2);
            }
            break;
        default:
            throw std::runtime_error("parquet: unsupported column type in PLAIN decode");
        }
    }
}

// Fused RLE-decode + dictionary scatter for fixed-width values: decode `count`
// dictionary indices straight from the RLE stream and write the referenced
// dictionary entries into `dst`, with no intermediate index array. `W` is the
// compile-time byte stride (2/4/8) so the copy lowers to a single store; W==0 is
// the generic fallback using the runtime `stride`.
template <int W>
void ScatterDictFixed(const uint8_t *data, size_t size, size_t count, int bit_width, const uint8_t *dict,
                      size_t dict_size, size_t stride, uint8_t *dst) {
    const size_t w = (W > 0) ? static_cast<size_t>(W) : stride;
    uint8_t *out = dst;
    auto check = [&](uint64_t v) {
        if (v >= dict_size) {
            throw std::runtime_error("parquet: dictionary index out of range");
        }
    };
    RleWalk(
        data, size, count, bit_width,
        [&](uint64_t val, size_t n) {
            check(val);
            const uint8_t *s = dict + val * w;
            for (size_t k = 0; k < n; k++) {
                std::memcpy(out, s, w);
                out += w;
            }
        },
        [&](uint64_t val) {
            check(val);
            std::memcpy(out, dict + val * w, w);
            out += w;
        });
}

// Fused RLE-decode + dictionary scatter for VARCHAR: copies the borrowed (ptr,len)
// view from the dictionary — no string bytes are touched.
void ScatterDictVarchar(const uint8_t *data, size_t size, size_t count, int bit_width,
                        const std::vector<StrView> &dict_views, size_t dict_size, StrView *dst) {
    StrView *out = dst;
    auto check = [&](uint64_t v) {
        if (v >= dict_size) {
            throw std::runtime_error("parquet: dictionary index out of range");
        }
    };
    RleWalk(
        data, size, count, bit_width,
        [&](uint64_t val, size_t n) {
            check(val);
            const StrView v = dict_views[val];
            for (size_t k = 0; k < n; k++) {
                *out++ = v;
            }
        },
        [&](uint64_t val) {
            check(val);
            *out++ = dict_views[val];
        });
}

// Decode `count` dictionary-encoded values (the leading byte of `vals` is the RLE
// bit width) into `raw_dst`/`view_dst`, dispatching fixed-width strides to the
// compile-time-specialized scatter.
void ScatterDictPage(const uint8_t *vals, size_t vals_size, size_t count, const ByteVec &dict_raw,
                     const std::vector<StrView> &dict_views, size_t dict_size, size_t stride, bool is_varchar,
                     uint8_t *raw_dst, StrView *view_dst) {
    if (vals_size == 0) {
        throw std::runtime_error("parquet: truncated dictionary data page");
    }
    const int bit_width = vals[0];
    const uint8_t *data = vals + 1;
    const size_t size = vals_size - 1;
    if (is_varchar) {
        ScatterDictVarchar(data, size, count, bit_width, dict_views, dict_size, view_dst);
        return;
    }
    switch (stride) {
    case 2: ScatterDictFixed<2>(data, size, count, bit_width, dict_raw.data(), dict_size, stride, raw_dst); break;
    case 4: ScatterDictFixed<4>(data, size, count, bit_width, dict_raw.data(), dict_size, stride, raw_dst); break;
    case 8: ScatterDictFixed<8>(data, size, count, bit_width, dict_raw.data(), dict_size, stride, raw_dst); break;
    default: ScatterDictFixed<0>(data, size, count, bit_width, dict_raw.data(), dict_size, stride, raw_dst); break;
    }
}

} // namespace

size_t ColumnPhysicalWidth(const ColumnType &type) {
    switch (type.id) {
    case TypeId::BOOLEAN: return 1;
    case TypeId::INT8:    return 1;
    case TypeId::INT16:   return 2;
    case TypeId::INT32:   return 4;
    case TypeId::INT64:   return 8;
    case TypeId::FLOAT:   return 4;
    case TypeId::DOUBLE:  return 8;
    case TypeId::DATE:    return 4;
    case TypeId::TIME:    return 8;
    case TypeId::DECIMAL:
        if (type.decimal_width <= 4) return 2;
        if (type.decimal_width <= 9) return 4;
        return 8;
    default: return 0;
    }
}

static DecodedColumn DecodeColumnChunkImpl(const uint8_t *data, size_t size, const ColumnType &type, bool nullable,
                                           int codec) {
    DecodedColumn result;
    const size_t stride = ColumnPhysicalWidth(type);
    const bool is_varchar = (type.id == TypeId::VARCHAR);

    ByteVec dict_raw;
    std::vector<StrView> dict_views;
    size_t dict_size = 0;

    size_t pos = 0;
    while (pos < size) {
        CompactReader reader(data + pos, size - pos);
        PageHeader ph = ParsePageHeader(reader);
        const size_t header_len = reader.Consumed();
        const uint8_t *page = data + pos + header_len;
        const size_t comp_size = static_cast<size_t>(ph.compressed_size);
        const size_t uncomp_size = static_cast<size_t>(ph.uncompressed_size);
        if (pos + header_len + comp_size > size) {
            throw std::runtime_error("parquet: page extends past chunk");
        }
        pos += header_len + comp_size;

        if (ph.type == PAGE_DICTIONARY) {
            dict_raw.clear();
            dict_views.clear();
            if (codec == CODEC_UNCOMPRESSED) {
                // Uncompressed dict values stay in the source buffer; views borrow it.
                size_t dpos = 0;
                DecodePlainRaw(page, comp_size, dpos, type, stride,
                               static_cast<size_t>(ph.num_values), dict_raw, dict_views);
            } else if (is_varchar) {
                // VARCHAR dict views borrow the decompressed bytes, so keep them alive
                // for the whole column decode (data pages reference them by index).
                result.str_backing.push_back(Decompress(codec, page, comp_size, uncomp_size));
                const auto &buf = result.str_backing.back();
                size_t dpos = 0;
                DecodePlainRaw(buf.data(), buf.size(), dpos, type, stride,
                               static_cast<size_t>(ph.num_values), dict_raw, dict_views);
            } else {
                auto buf = Decompress(codec, page, comp_size, uncomp_size);
                size_t dpos = 0;
                DecodePlainRaw(buf.data(), buf.size(), dpos, type, stride,
                               static_cast<size_t>(ph.num_values), dict_raw, dict_views);
            }
            dict_size = static_cast<size_t>(ph.num_values);
            continue;
        }
        if (ph.type != PAGE_DATA && ph.type != PAGE_DATA_V2) {
            continue;
        }

        const size_t num_values = static_cast<size_t>(ph.num_values);
        std::vector<uint64_t> def_levels;
        ByteVec owned;
        const uint8_t *vals = nullptr;
        size_t vals_size = 0;

        if (!ph.is_v2) {
            if (codec == CODEC_UNCOMPRESSED) {
                size_t ppos = 0;
                if (nullable) {
                    if (ppos + 4 > comp_size) {
                        throw std::runtime_error("parquet: truncated definition levels");
                    }
                    uint32_t len = ReadU32LE(page + ppos);
                    ppos += 4;
                    if (ppos + len > comp_size) {
                        throw std::runtime_error("parquet: truncated definition levels");
                    }
                    def_levels = RleDecode(page + ppos, len, num_values, 1);
                    ppos += len;
                }
                vals = page + ppos;
                vals_size = comp_size - ppos;
            } else {
                owned = Decompress(codec, page, comp_size, uncomp_size);
                size_t ppos = 0;
                if (nullable) {
                    if (ppos + 4 > owned.size()) {
                        throw std::runtime_error("parquet: truncated definition levels");
                    }
                    uint32_t len = ReadU32LE(owned.data() + ppos);
                    ppos += 4;
                    if (ppos + len > owned.size()) {
                        throw std::runtime_error("parquet: truncated definition levels");
                    }
                    def_levels = RleDecode(owned.data() + ppos, len, num_values, 1);
                    ppos += len;
                }
                vals = owned.data() + ppos;
                vals_size = owned.size() - ppos;
            }
        } else {
            const size_t lvl_len = static_cast<size_t>(ph.rep_levels_len) + static_cast<size_t>(ph.def_levels_len);
            if (lvl_len > comp_size) {
                throw std::runtime_error("parquet: V2 level lengths exceed page");
            }
            if (nullable && ph.def_levels_len > 0) {
                def_levels = RleDecode(page + ph.rep_levels_len, ph.def_levels_len, num_values, 1);
            }
            const uint8_t *vsrc = page + lvl_len;
            const size_t vcomp = comp_size - lvl_len;
            if (ph.is_compressed && codec != CODEC_UNCOMPRESSED) {
                owned = Decompress(codec, vsrc, vcomp, uncomp_size - lvl_len);
                vals = owned.data();
                vals_size = owned.size();
            } else {
                vals = vsrc;
                vals_size = vcomp;
            }
        }

        size_t num_non_null = num_values;
        if (nullable && !def_levels.empty()) {
            num_non_null = 0;
            for (auto d : def_levels) {
                num_non_null += (d != 0);
            }
        }

        const bool has_nulls = nullable && !def_levels.empty();

        // VARCHAR views borrow `vals`; if that is a decompressed buffer, keep it alive
        // for the DecodedColumn's lifetime (uncompressed pages borrow the stable source).
        // Preserve vals' offset into the buffer (it points past the definition levels).
        if (is_varchar && !owned.empty()) {
            const size_t vals_off = static_cast<size_t>(vals - owned.data());
            result.str_backing.push_back(std::move(owned));
            vals = result.str_backing.back().data() + vals_off;
        }

        // === Non-nullable fast paths: decode directly into result (no temp buffer). ===

        if (!has_nulls && ph.encoding == ENC_PLAIN) {
            size_t vpos = 0;
            DecodePlainRaw(vals, vals_size, vpos, type, stride, num_non_null, result.raw, result.str_views);
            result.count += num_values;
            continue;
        }

        if (!has_nulls && (ph.encoding == ENC_PLAIN_DICTIONARY || ph.encoding == ENC_RLE_DICTIONARY)) {
            if (!is_varchar) {
                const size_t raw_start = result.raw.size();
                result.raw.resize(raw_start + num_non_null * stride);
                ScatterDictPage(vals, vals_size, num_non_null, dict_raw, dict_views, dict_size, stride, false,
                                result.raw.data() + raw_start, nullptr);
            } else {
                const size_t view_start = result.str_views.size();
                result.str_views.resize(view_start + num_non_null);
                ScatterDictPage(vals, vals_size, num_non_null, dict_raw, dict_views, dict_size, stride, true, nullptr,
                                result.str_views.data() + view_start);
            }
            result.count += num_values;
            continue;
        }

        // === Nullable path: need temp buffer for scatter with def_levels. ===

        ByteVec temp_raw;
        std::vector<StrView> temp_views;

        if (ph.encoding == ENC_PLAIN) {
            size_t vpos = 0;
            DecodePlainRaw(vals, vals_size, vpos, type, stride, num_non_null, temp_raw, temp_views);
        } else if (ph.encoding == ENC_PLAIN_DICTIONARY || ph.encoding == ENC_RLE_DICTIONARY) {
            if (!is_varchar) {
                temp_raw.resize(num_non_null * stride);
                ScatterDictPage(vals, vals_size, num_non_null, dict_raw, dict_views, dict_size, stride, false,
                                temp_raw.data(), nullptr);
            } else {
                temp_views.resize(num_non_null);
                ScatterDictPage(vals, vals_size, num_non_null, dict_raw, dict_views, dict_size, stride, true, nullptr,
                                temp_views.data());
            }
        } else {
            throw std::runtime_error("parquet: unsupported data page encoding");
        }

        // Scatter temp values into result with null slots from def_levels.
        const size_t page_row_start = result.count;

        if (result.valid.empty() && page_row_start > 0) {
            result.valid.assign(page_row_start, 1);
        }
        if (!is_varchar) {
            result.raw.resize((page_row_start + num_values) * stride, 0);
            uint8_t *dst = result.raw.data() + page_row_start * stride;
            size_t vi = 0;
            for (size_t i = 0; i < num_values; i++) {
                if (def_levels[i] != 0) {
                    std::memcpy(dst + i * stride, temp_raw.data() + vi * stride, stride);
                    vi++;
                    result.valid.push_back(1);
                } else {
                    result.valid.push_back(0);
                }
            }
        } else {
            result.str_views.resize(page_row_start + num_values);
            size_t vi = 0;
            for (size_t i = 0; i < num_values; i++) {
                if (def_levels[i] != 0) {
                    result.str_views[page_row_start + i] = temp_views[vi++];
                    result.valid.push_back(1);
                } else {
                    result.valid.push_back(0);
                }
            }
        }
        result.count += num_values;
    }
    return result;
}

Result<DecodedColumn> DecodeColumnChunk(const uint8_t *data, size_t size, const ColumnType &type, bool nullable,
                                        int codec) {
    return TryCatch([&] { return DecodeColumnChunkImpl(data, size, type, nullable, codec); });
}

//===----------------------------------------------------------------------===//
// ColumnChunkReader — streaming, decode-into-vector
//===----------------------------------------------------------------------===//

ColumnChunkReader::ColumnChunkReader(const uint8_t *data, size_t size, const ColumnType &type, bool nullable,
                                     int codec)
    : data_(data), size_(size), type_(type), nullable_(nullable), codec_(codec),
      stride_(ColumnPhysicalWidth(type)), is_varchar_(type.id == TypeId::VARCHAR), bulk_(CanBulkCopy(type)) {
    // Header-only prescan: sum every data page's num_values so callers know the
    // row count up front without decoding page bodies.
    size_t p = 0;
    while (p < size_) {
        CompactReader reader(data_ + p, size_ - p);
        PageHeader ph = ParsePageHeader(reader);
        const size_t header_len = reader.Consumed();
        const size_t comp = static_cast<size_t>(ph.compressed_size);
        if (p + header_len + comp > size_) {
            break; // truncated tail; stop counting
        }
        p += header_len + comp;
        if (ph.type == PAGE_DATA || ph.type == PAGE_DATA_V2) {
            total_rows_ += static_cast<size_t>(ph.num_values);
        }
    }
}

void ColumnChunkReader::SetupDictionaryPage(const uint8_t *page, size_t comp_size, size_t uncomp_size,
                                            size_t num_values) {
    dict_raw_.clear();
    dict_views_.clear();
    dict_backing_.clear();
    if (codec_ == CODEC_UNCOMPRESSED) {
        size_t dpos = 0;
        DecodePlainRaw(page, comp_size, dpos, type_, stride_, num_values, dict_raw_, dict_views_);
    } else if (is_varchar_) {
        // VARCHAR dict views borrow the decompressed bytes for the whole chunk.
        dict_backing_.push_back(Decompress(codec_, page, comp_size, uncomp_size));
        const auto &buf = dict_backing_.back();
        size_t dpos = 0;
        DecodePlainRaw(buf.data(), buf.size(), dpos, type_, stride_, num_values, dict_raw_, dict_views_);
    } else {
        auto buf = Decompress(codec_, page, comp_size, uncomp_size);
        size_t dpos = 0;
        DecodePlainRaw(buf.data(), buf.size(), dpos, type_, stride_, num_values, dict_raw_, dict_views_);
    }
    dict_size_ = num_values;
    has_dict_ = true;
    dict_vec_built_ = false; // rebuilt lazily for this dictionary
}

void ColumnChunkReader::SetupDataPage(const uint8_t *page, size_t comp_size, size_t uncomp_size, size_t num_values,
                                      bool is_v2, size_t def_levels_len, size_t rep_levels_len, bool page_compressed,
                                      int encoding) {
    page_def_.clear();
    page_owned_.clear();
    page_indices_.clear();

    const uint8_t *vals = nullptr;
    size_t vals_size = 0;
    size_t vals_off_in_owned = 0; // if set, vals lives in page_owned_ at this offset
    bool vals_in_owned = false;

    if (!is_v2) {
        const uint8_t *base = page;
        size_t base_size = comp_size;
        if (codec_ != CODEC_UNCOMPRESSED) {
            page_owned_ = Decompress(codec_, page, comp_size, uncomp_size);
            base = page_owned_.data();
            base_size = page_owned_.size();
            vals_in_owned = true;
        }
        size_t ppos = 0;
        if (nullable_) {
            if (ppos + 4 > base_size) {
                throw std::runtime_error("parquet: truncated definition levels");
            }
            uint32_t len = ReadU32LE(base + ppos);
            ppos += 4;
            if (ppos + len > base_size) {
                throw std::runtime_error("parquet: truncated definition levels");
            }
            page_def_ = RleDecode(base + ppos, len, num_values, 1);
            ppos += len;
        }
        vals_off_in_owned = ppos;
        vals = base + ppos;
        vals_size = base_size - ppos;
    } else {
        const size_t lvl_len = rep_levels_len + def_levels_len;
        if (lvl_len > comp_size) {
            throw std::runtime_error("parquet: V2 level lengths exceed page");
        }
        if (nullable_ && def_levels_len > 0) {
            page_def_ = RleDecode(page + rep_levels_len, def_levels_len, num_values, 1);
        }
        const uint8_t *vsrc = page + lvl_len;
        const size_t vcomp = comp_size - lvl_len;
        if (page_compressed && codec_ != CODEC_UNCOMPRESSED) {
            page_owned_ = Decompress(codec_, vsrc, vcomp, uncomp_size - lvl_len);
            vals = page_owned_.data();
            vals_size = page_owned_.size();
            vals_in_owned = true;
            vals_off_in_owned = 0;
        } else {
            vals = vsrc;
            vals_size = vcomp;
        }
    }
    // Re-point into page_owned_ after any moves are done (the vector is stable now).
    if (vals_in_owned) {
        vals = page_owned_.data() + vals_off_in_owned;
    }

    page_has_nulls_ = nullable_ && !page_def_.empty();
    size_t non_null = num_values;
    if (page_has_nulls_) {
        non_null = 0;
        for (auto d : page_def_) {
            non_null += (d != 0);
        }
    }

    page_encoding_ = encoding;
    page_vals_ = vals;
    page_vals_size_ = vals_size;
    page_rows_ = num_values;
    page_non_null_ = non_null;
    page_cursor_ = 0;
    page_val_cursor_ = 0;
    page_plain_pos_ = 0;

    if (encoding == ENC_PLAIN_DICTIONARY || encoding == ENC_RLE_DICTIONARY) {
        page_indices_ = DecodeDictIndices(vals, vals_size, non_null);
    } else if (encoding == ENC_PLAIN) {
        if (!is_varchar_) {
            // Validate the fixed-width value region so EmitFixedNonNull can read it
            // without per-value bounds checks.
            size_t need_bytes;
            if (type_.id == TypeId::BOOLEAN) {
                need_bytes = (non_null + 7) / 8;
            } else {
                need_bytes = non_null * (bulk_ ? stride_ : 4);
            }
            if (page_vals_size_ < need_bytes) {
                throw std::runtime_error("parquet: truncated PLAIN page");
            }
        }
    } else {
        throw std::runtime_error("parquet: unsupported data page encoding");
    }
}

ColumnChunkReader::~ColumnChunkReader() = default;
ColumnChunkReader::ColumnChunkReader(ColumnChunkReader &&) noexcept = default;
ColumnChunkReader &ColumnChunkReader::operator=(ColumnChunkReader &&) noexcept = default;

// Materialize the shared dictionary vector once per dictionary page. Fixed-width
// values are copied in bulk (dict_raw_ is already native layout); VARCHAR values
// are copied into the vector's string heap exactly dict_size_ times (vs once per
// row in the gather path). Index dict_size_ is an invalid null sentinel.
void ColumnChunkReader::EnsureDictVector(const duckdb::Vector &like) {
    using duckdb::FlatVector;
    using duckdb::string_t;
    using duckdb::StringVector;
    if (dict_vec_built_) {
        return;
    }
    dict_vec_ = std::make_unique<duckdb::Vector>(like.GetType(), dict_size_ + 1);
    if (is_varchar_) {
        auto *d = FlatVector::GetData<string_t>(*dict_vec_);
        for (size_t i = 0; i < dict_size_; i++) {
            d[i] = StringVector::AddString(*dict_vec_, dict_views_[i].ptr, dict_views_[i].len);
        }
    } else {
        std::memcpy(FlatVector::GetData(*dict_vec_), dict_raw_.data(), dict_size_ * stride_);
    }
    FlatVector::Validity(*dict_vec_).SetInvalid(dict_size_); // null sentinel
    dict_vec_built_ = true;
}

// Emit `m` rows of the current dictionary-encoded page as a DICTIONARY_VECTOR that
// references the shared dictionary via a selection vector — no per-row value copy.
void ColumnChunkReader::EmitDictVector(duckdb::Vector &vec, size_t m) {
    EnsureDictVector(vec);
    duckdb::SelectionVector sel(m); // owned SelectionData; shared into the dict buffer
    if (!page_has_nulls_) {
        for (size_t i = 0; i < m; i++) {
            const uint32_t idx = page_indices_[page_val_cursor_ + i];
            if (idx >= dict_size_) {
                throw std::runtime_error("parquet: dictionary index out of range");
            }
            sel.set_index(i, idx);
        }
        page_val_cursor_ += m;
    } else {
        for (size_t i = 0; i < m; i++) {
            if (page_def_[page_cursor_ + i] == 0) {
                sel.set_index(i, dict_size_); // null sentinel
                continue;
            }
            const uint32_t idx = page_indices_[page_val_cursor_++];
            if (idx >= dict_size_) {
                throw std::runtime_error("parquet: dictionary index out of range");
            }
            sel.set_index(i, idx);
        }
    }
    vec.Dictionary(*dict_vec_, dict_size_ + 1, sel, m);
}

bool ColumnChunkReader::EnterNextDataPage() {
    while (pos_ < size_) {
        CompactReader reader(data_ + pos_, size_ - pos_);
        PageHeader ph = ParsePageHeader(reader);
        const size_t header_len = reader.Consumed();
        const uint8_t *page = data_ + pos_ + header_len;
        const size_t comp_size = static_cast<size_t>(ph.compressed_size);
        const size_t uncomp_size = static_cast<size_t>(ph.uncompressed_size);
        if (pos_ + header_len + comp_size > size_) {
            throw std::runtime_error("parquet: page extends past chunk");
        }
        pos_ += header_len + comp_size;

        if (ph.type == PAGE_DICTIONARY) {
            SetupDictionaryPage(page, comp_size, uncomp_size, static_cast<size_t>(ph.num_values));
            continue;
        }
        if (ph.type != PAGE_DATA && ph.type != PAGE_DATA_V2) {
            continue;
        }
        SetupDataPage(page, comp_size, uncomp_size, static_cast<size_t>(ph.num_values), ph.is_v2,
                      static_cast<size_t>(ph.def_levels_len), static_cast<size_t>(ph.rep_levels_len),
                      ph.is_compressed, ph.encoding);
        return true;
    }
    return false;
}

// Decode `k` consecutive non-null values (native layout) into `dst`, advancing
// the page's non-null value cursor.
void ColumnChunkReader::EmitFixedNonNull(uint8_t *dst, size_t k) {
    if (page_encoding_ == ENC_PLAIN) {
        if (type_.id == TypeId::BOOLEAN) {
            for (size_t i = 0; i < k; i++) {
                const size_t bit = page_val_cursor_ + i;
                dst[i] = (page_vals_[bit >> 3] >> (bit & 7)) & 1;
            }
        } else if (bulk_) {
            std::memcpy(dst, page_vals_ + page_val_cursor_ * stride_, k * stride_);
        } else {
            // int8 / int16 / small decimal: parquet stores these as INT32 (4 bytes).
            for (size_t i = 0; i < k; i++) {
                int32_t v;
                std::memcpy(&v, page_vals_ + (page_val_cursor_ + i) * 4, 4);
                if (stride_ == 1) {
                    int8_t v8 = static_cast<int8_t>(v);
                    std::memcpy(dst + i, &v8, 1);
                } else {
                    int16_t v16 = static_cast<int16_t>(v);
                    std::memcpy(dst + i * 2, &v16, 2);
                }
            }
        }
    } else {
        // Dictionary: gather referenced entries straight into `dst`.
        for (size_t i = 0; i < k; i++) {
            const uint32_t idx = page_indices_[page_val_cursor_ + i];
            if (idx >= dict_size_) {
                throw std::runtime_error("parquet: dictionary index out of range");
            }
            std::memcpy(dst + i * stride_, dict_raw_.data() + static_cast<size_t>(idx) * stride_, stride_);
        }
    }
    page_val_cursor_ += k;
}

// Return the next non-null VARCHAR value's borrowed view, advancing the cursor.
StrView ColumnChunkReader::NextVarcharView() {
    if (page_encoding_ == ENC_PLAIN) {
        if (page_plain_pos_ + 4 > page_vals_size_) {
            throw std::runtime_error("parquet: truncated PLAIN page");
        }
        uint32_t len = ReadU32LE(page_vals_ + page_plain_pos_);
        page_plain_pos_ += 4;
        if (page_plain_pos_ + len > page_vals_size_) {
            throw std::runtime_error("parquet: truncated PLAIN page");
        }
        StrView v{reinterpret_cast<const char *>(page_vals_ + page_plain_pos_), len};
        page_plain_pos_ += len;
        return v;
    }
    const uint32_t idx = page_indices_[page_val_cursor_++];
    if (idx >= dict_size_) {
        throw std::runtime_error("parquet: dictionary index out of range");
    }
    return dict_views_[idx];
}

void ColumnChunkReader::EmitFromPage(duckdb::Vector &vec, size_t dst_off, size_t m, const uint8_t *keep) {
    using duckdb::FlatVector;
    using duckdb::string_t;
    using duckdb::StringVector;

    if (is_varchar_) {
        auto *dst = FlatVector::GetData<string_t>(vec);
        auto &validity = FlatVector::Validity(vec);
        for (size_t i = 0; i < m; i++) {
            if (page_has_nulls_ && page_def_[page_cursor_ + i] == 0) {
                validity.SetInvalid(dst_off + i);
                continue;
            }
            StrView v = NextVarcharView(); // always advance the source cursor
            if (keep && !keep[dst_off + i]) {
                continue; // filtered out: skip the string copy; slot dropped by the caller's Slice
            }
            dst[dst_off + i] = StringVector::AddString(vec, v.ptr, v.len);
        }
        return;
    }
    // Fixed-width materialization is cheap (bulk memcpy / small conversion); decode
    // fully regardless of `keep` and let the caller's Slice compact.
    (void)keep;

    auto *base = FlatVector::GetData(vec);
    if (!page_has_nulls_) {
        EmitFixedNonNull(base + dst_off * stride_, m);
        return;
    }

    // Nullable: decode the run's non-null values contiguously, then scatter them
    // into the vector's row slots, marking the gaps invalid.
    auto &validity = FlatVector::Validity(vec);
    size_t nn = 0;
    for (size_t i = 0; i < m; i++) {
        nn += (page_def_[page_cursor_ + i] != 0);
    }
    scatter_tmp_.resize(nn * stride_);
    EmitFixedNonNull(scatter_tmp_.data(), nn);
    size_t vi = 0;
    for (size_t i = 0; i < m; i++) {
        if (page_def_[page_cursor_ + i] != 0) {
            std::memcpy(base + (dst_off + i) * stride_, scatter_tmp_.data() + vi * stride_, stride_);
            vi++;
        } else {
            validity.SetInvalid(dst_off + i);
        }
    }
}

size_t ColumnChunkReader::ReadInto(duckdb::Vector &vec, size_t n, const uint8_t *keep) {
    if (n == 0) {
        return 0;
    }
    // Ensure a current page is loaded before probing the fast path.
    if (page_cursor_ == page_rows_ && !EnterNextDataPage()) {
        return 0;
    }
    // Fast path: the whole request fits in the current dictionary-encoded page —
    // emit it as a DICTIONARY_VECTOR (no gather / no per-row string copy). A window
    // straddling a page boundary (the page's tail) falls through to the gather loop.
    // Dictionary decode is already cheap, so `keep` is ignored here (Slice compacts).
    const bool dict_page = has_dict_ && (page_encoding_ == ENC_PLAIN_DICTIONARY || page_encoding_ == ENC_RLE_DICTIONARY);
    if (dict_page && (page_rows_ - page_cursor_) >= n) {
        EmitDictVector(vec, n);
        page_cursor_ += n;
        emitted_ += n;
        return n;
    }

    size_t produced = 0;
    while (produced < n) {
        if (page_cursor_ == page_rows_) {
            if (!EnterNextDataPage()) {
                break;
            }
        }
        const size_t m = std::min(n - produced, page_rows_ - page_cursor_);
        EmitFromPage(vec, produced, m, keep);
        produced += m;
        page_cursor_ += m;
    }
    emitted_ += produced;
    return produced;
}

// Advance the current page's value cursors over `m` rows without materializing.
void ColumnChunkReader::SkipInPage(size_t m) {
    size_t non_null = m;
    if (page_has_nulls_) {
        non_null = 0;
        for (size_t i = 0; i < m; i++) {
            non_null += (page_def_[page_cursor_ + i] != 0);
        }
    }
    if (is_varchar_ && page_encoding_ == ENC_PLAIN) {
        // PLAIN VARCHAR is variable-length: walk the length prefixes to advance.
        for (size_t k = 0; k < non_null; k++) {
            (void)NextVarcharView();
        }
    } else {
        // Dictionary indices and fixed-width PLAIN are addressed by value index, so
        // advancing the cursor is enough (values are read at page_val_cursor_).
        page_val_cursor_ += non_null;
    }
    page_cursor_ += m;
}

void ColumnChunkReader::Skip(size_t n) {
    size_t remaining = n;

    // Drain any partially-consumed current page (a previous ReadInto may have stopped
    // mid-page); this one is already decompressed, so just advance the value cursors.
    if (page_cursor_ < page_rows_) {
        const size_t m = std::min(remaining, page_rows_ - page_cursor_);
        SkipInPage(m);
        remaining -= m;
    }

    // Then skip whole pages by header only — no decompression, no level/index decode —
    // whenever the entire page falls within the skip. This is where filter pushdown
    // pays off: a rejected window's pages are dropped without touching their bodies.
    while (remaining > 0 && pos_ < size_) {
        CompactReader reader(data_ + pos_, size_ - pos_);
        PageHeader ph = ParsePageHeader(reader);
        const size_t header_len = reader.Consumed();
        const size_t comp_size = static_cast<size_t>(ph.compressed_size);
        if (pos_ + header_len + comp_size > size_) {
            break; // truncated tail
        }
        const uint8_t *page = data_ + pos_ + header_len;

        if (ph.type == PAGE_DICTIONARY) {
            // The dictionary must be materialized even while skipping: a later
            // (non-skipped) window of this column will reference it. It appears once,
            // at the head of the chunk, so this is paid at most once.
            SetupDictionaryPage(page, comp_size, static_cast<size_t>(ph.uncompressed_size),
                                static_cast<size_t>(ph.num_values));
            pos_ += header_len + comp_size;
            continue;
        }
        if (ph.type != PAGE_DATA && ph.type != PAGE_DATA_V2) {
            pos_ += header_len + comp_size;
            continue;
        }

        const size_t page_vals = static_cast<size_t>(ph.num_values);
        if (page_vals <= remaining) {
            // Whole page skipped: advance past its header + compressed body only.
            pos_ += header_len + comp_size;
            remaining -= page_vals;
        } else {
            // Partial: decode this page's structure (decompress + levels) and skip
            // within it. EnterNextDataPage re-parses from the unchanged pos_.
            if (!EnterNextDataPage()) {
                break;
            }
            const size_t m = std::min(remaining, page_rows_ - page_cursor_);
            SkipInPage(m);
            remaining -= m;
        }
    }
    emitted_ += (n - remaining);
}

} // namespace plume::parquet
