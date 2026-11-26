#include "plume/memory/adapter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <new>
#include <utility>
#include <type_traits>

namespace plume::memory {

using duckdb::data_ptr_t;
using duckdb::const_data_ptr_t;
using duckdb::DataChunk;
using duckdb::FlatVector;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::string_t;
using duckdb::ValidityMask;
using duckdb::validity_t;
using duckdb::sel_t;
using duckdb::Vector;

namespace {

inline uint64_t ValidityWords(uint64_t rows) { return (rows + 63) / 64; }
inline uint64_t ValidityBytes(uint64_t rows) { return ValidityWords(rows) * sizeof(validity_t); }

void SwizzleVarchar(data_ptr_t base, const ColumnDescriptor &desc, idx_t rows) {
    auto *strs = reinterpret_cast<string_t *>(base + desc.data_offset);
    for (idx_t r = 0; r < rows; r++) {
        if (!strs[r].IsInlined()) {
            auto off = reinterpret_cast<uint64_t>(strs[r].GetPointer());
            strs[r].SetPointer(reinterpret_cast<char *>(base + off));
        }
    }
}

Result<const BlockHeader &> Header(const_data_ptr_t base, size_t size) {
    if (size < sizeof(BlockHeader)) {
        return Error("Block smaller than header", ErrorKind::InvalidInput);
    }
    auto &h = *reinterpret_cast<const BlockHeader *>(base);
    if (h.magic != kBlockMagic) {
        return Error("Bad block magic", ErrorKind::InvalidInput);
    }
    if (h.format_version != kFormatVersion) {
        return Error("Unsupported block format version " + std::to_string(h.format_version),
                     ErrorKind::InvalidInput);
    }
    if (h.total_size > size) {
        return Error("Block total_size exceeds buffer", ErrorKind::InvalidInput);
    }
    return h;
}

} // namespace

//===----------------------------------------------------------------------===//
// Import
//===----------------------------------------------------------------------===//

Result<Schema> ParseSchema(const_data_ptr_t base, size_t size) {
    TRY(const BlockHeader &h, Header(base, size));
    auto *descs = reinterpret_cast<const ColumnDescriptor *>(base + h.schema_offset);
    const char *names = reinterpret_cast<const char *>(base + h.names_offset);
    Schema schema;
    schema.columns.reserve(h.column_count);
    for (uint32_t c = 0; c < h.column_count; c++) {
        const auto &d = descs[c];
        Column col;
        col.name.assign(names + d.name_offset, d.name_length);
        col.type = {static_cast<TypeId>(d.type_id), d.decimal_width, d.decimal_scale};
        col.nullable = (d.flags & kColNullable) != 0;
        schema.columns.push_back(std::move(col));
    }
    return schema;
}

static void ReferenceBlockData(Vector &vec, data_ptr_t data, idx_t count) {
    vec.Reference(Vector(vec.GetType(), data, count));
}

Result<void> ImportBlock(data_ptr_t base, size_t size, DataChunk &out_chunk, Schema &out_schema) {
    TRY(const BlockHeader &h, Header(base, size));
    TRY(out_schema, ParseSchema(base, size));
    auto *descs = reinterpret_cast<const ColumnDescriptor *>(base + h.schema_offset);
    const idx_t rows = h.row_count;

    duckdb::vector<LogicalType> types;
    types.reserve(out_schema.size());
    for (auto &col : out_schema.columns) {
        types.push_back(ToLogicalType(col.type));
    }

    out_chunk.Destroy();
    out_chunk.InitializeEmpty(types);
    for (uint32_t c = 0; c < h.column_count; c++) {
        const auto &d = descs[c];
        Vector &vec = out_chunk.data[c];
        // Fixed-width / string_t array: reference the in-block region (zero-copy).
        ReferenceBlockData(vec, base + d.data_offset, rows);
        // Validity: point at the in-block bitmask (validity_offset 0 => all valid).
        if (d.validity_offset != 0) {
            ValidityMask mask(reinterpret_cast<validity_t *>(base + d.validity_offset), rows);
            FlatVector::SetValidity(vec, mask);
        }
        // VARCHAR: swizzle non-inlined string_t offsets to absolute pointers.
        if (out_schema.columns[c].type.id == TypeId::VARCHAR) {
            SwizzleVarchar(base, d, rows);
        }
    }
    out_chunk.SetChildCardinality(rows);
    return Ok();
}

Result<std::vector<std::unique_ptr<DataChunk>>> ImportBlockChunks(data_ptr_t base, size_t size, Schema &out_schema) {
    TRY(const BlockHeader &h, Header(base, size));
    TRY(out_schema, ParseSchema(base, size));
    auto *descs = reinterpret_cast<const ColumnDescriptor *>(base + h.schema_offset);
    const idx_t rows = h.row_count;

    duckdb::vector<LogicalType> types;
    std::vector<uint32_t> width(h.column_count);
    types.reserve(out_schema.size());
    for (uint32_t c = 0; c < h.column_count; c++) {
        types.push_back(ToLogicalType(out_schema.columns[c].type));
        width[c] = PhysicalWidth(out_schema.columns[c].type);
        // Swizzle each VARCHAR column once over the whole block. Slices then reference the 
        // already-swizzled sub-arrays.
        if (out_schema.columns[c].type.id == TypeId::VARCHAR) {
            SwizzleVarchar(base, descs[c], rows);
        }
    }

    std::vector<std::unique_ptr<DataChunk>> chunks;
    for (idx_t start = 0; start < rows; start += STANDARD_VECTOR_SIZE) {
        const idx_t n = std::min<idx_t>(STANDARD_VECTOR_SIZE, rows - start);
        auto chunk = std::make_unique<DataChunk>();
        chunk->InitializeEmpty(types);
        for (uint32_t c = 0; c < h.column_count; c++) {
            const auto &d = descs[c];
            Vector &vec = chunk->data[c];
            // Fixed-width / string_t slice: reference the in-block sub-array.
            ReferenceBlockData(vec, base + d.data_offset + uint64_t(start) * width[c], n);
            if (d.validity_offset != 0) {
                // NOTE: `start` is a multiple of STANDARD_VECTOR_SIZE (a multiple of 64), so the 
                //       slice begins on a validity-word boundary.
                auto *mask_words = reinterpret_cast<validity_t *>(
                    base + d.validity_offset + (uint64_t(start) / 64) * sizeof(validity_t));
                ValidityMask mask(mask_words, n);
                FlatVector::SetValidity(vec, mask);
            }
        }
        chunk->SetChildCardinality(n);
        chunks.push_back(std::move(chunk));
    }
    return chunks;
}

//===----------------------------------------------------------------------===//
// Export
//===----------------------------------------------------------------------===//

namespace {

using duckdb::UnifiedVectorFormat;

// Block layout: absolute offsets of every region, computed from the row/heap counts. 
struct BlockLayout {
    std::vector<uint32_t> width;      // physical width per column
    std::vector<uint64_t> data_off;   // absolute data-region offset per column
    std::vector<uint64_t> valid_off;  // absolute validity offset (0 == not nullable)
    std::vector<uint32_t> name_rel;   // name offset relative to names_off
    uint64_t schema_off = 0, names_off = 0, heap_off = 0, total = 0;
};

BlockLayout ComputeLayout(const Schema &schema, uint64_t rows, uint64_t heap_size) {
    const uint32_t ncol = static_cast<uint32_t>(schema.size());
    BlockLayout L;
    L.width.resize(ncol);
    L.data_off.resize(ncol);
    L.valid_off.assign(ncol, 0);
    L.name_rel.resize(ncol);

    uint64_t cur = sizeof(BlockHeader);
    L.schema_off = cur;
    cur += uint64_t(ncol) * sizeof(ColumnDescriptor);
    L.names_off = cur;
    uint32_t names_cur = 0;
    for (uint32_t c = 0; c < ncol; c++) {
        L.width[c] = PhysicalWidth(schema.columns[c].type);
        L.name_rel[c] = names_cur;
        names_cur += static_cast<uint32_t>(schema.columns[c].name.size());
    }
    cur += names_cur;
    for (uint32_t c = 0; c < ncol; c++) {
        cur = AlignUp(cur, kDataAlign);
        L.data_off[c] = cur;
        cur += rows * L.width[c];
        if (schema.columns[c].nullable) {
            cur = AlignUp(cur, kValidityAlign);
            L.valid_off[c] = cur;
            cur += ValidityBytes(rows);
        }
    }
    L.heap_off = AlignUp(cur, kDataAlign);
    L.total = L.heap_off + heap_size;
    return L;
}

void WriteBlockHeader(data_ptr_t base, const Schema &schema, const BlockLayout &L, uint64_t rows, uint64_t heap_size) {
    const uint32_t ncol = static_cast<uint32_t>(schema.size());
    auto &h = *reinterpret_cast<BlockHeader *>(base);
    h = {};
    h.magic = kBlockMagic;
    h.format_version = kFormatVersion;
    h.column_count = ncol;
    h.row_count = rows;
    h.total_size = L.total;
    h.schema_offset = L.schema_off;
    h.names_offset = L.names_off;
    h.heap_offset = L.heap_off;
    h.heap_size = heap_size;

    auto *descs = reinterpret_cast<ColumnDescriptor *>(base + L.schema_off);
    char *names = reinterpret_cast<char *>(base + L.names_off);
    for (uint32_t c = 0; c < ncol; c++) {
        const auto &col = schema.columns[c];
        auto &d = descs[c];
        d = {};
        d.type_id = static_cast<uint8_t>(col.type.id);
        d.decimal_width = col.type.decimal_width;
        d.decimal_scale = col.type.decimal_scale;
        d.flags = col.nullable ? kColNullable : 0;
        d.name_offset = L.name_rel[c];
        d.name_length = static_cast<uint32_t>(col.name.size());
        d.data_offset = L.data_off[c];
        d.validity_offset = L.valid_off[c];
        std::memcpy(names + L.name_rel[c], col.name.data(), col.name.size());
    }
}

// Calls f(PSEL, USEL) with std::true_type/std::false_type for whether a part selection and a
// vector selection are present, so the row loops are instantiated without per-row branches.
template <class F>
inline void DispatchSel(const sel_t *psel, const sel_t *usel, F &&f) {
    using Y = std::true_type;
    using N = std::false_type;
    if (psel) {
        usel ? f(Y {}, Y {}) : f(Y {}, N {});
    } else {
        usel ? f(N {}, Y {}) : f(N {}, N {});
    }
}

// Source row of the part's i-th row: the part selection (else row start + i), then the vector's
// own selection.
template <bool PSEL, bool USEL>
inline idx_t SourceRow(const sel_t *psel, const sel_t *usel, idx_t start, idx_t i) {
    const idx_t r = PSEL ? psel[i] : start + i;
    return USEL ? usel[r] : r;
}

inline const sel_t *VectorSel(const UnifiedVectorFormat &u) { return u.sel->IsSet() ? u.sel->data() : nullptr; }

struct Fixed16 {
    uint64_t lo, hi;
};

template <class T>
void CopyFixed(data_ptr_t dst, const_data_ptr_t src, const sel_t *psel, const sel_t *usel, idx_t start,
               idx_t count) {
    auto *d = reinterpret_cast<T *>(dst);
    auto *s = reinterpret_cast<const T *>(src);
    DispatchSel(psel, usel, [&](auto p, auto u) {
        constexpr bool P = decltype(p)::value, U = decltype(u)::value;
        if constexpr (!P && !U) {
            std::memcpy(d, s + start, count * sizeof(T));
        } else {
            for (idx_t i = 0; i < count; i++) {
                d[i] = s[SourceRow<P, U>(psel, usel, start, i)];
            }
        }
    });
}

using CopyFn = void (*)(data_ptr_t, const_data_ptr_t, const sel_t *, const sel_t *, idx_t, idx_t);

CopyFn GetCopyFunction(uint32_t width) {
    switch (width) {
    case 1: return CopyFixed<uint8_t>;
    case 2: return CopyFixed<uint16_t>;
    case 4: return CopyFixed<uint32_t>;
    case 8: return CopyFixed<uint64_t>;
    case 16: return CopyFixed<Fixed16>;
    default: return nullptr;
    }
}

// Clears the validity bits of the part's NULL rows, which land at output rows [out, out + count).
void CopyNulls(validity_t *dst, const UnifiedVectorFormat &u, const sel_t *psel, idx_t start, idx_t out,
               idx_t count) {
    if (u.validity.CannotHaveNull()) {
        return;
    }
    DispatchSel(psel, VectorSel(u), [&](auto p, auto v) {
        constexpr bool P = decltype(p)::value, U = decltype(v)::value;
        const sel_t *usel = VectorSel(u);
        for (idx_t i = 0; i < count; i++) {
            if (!u.validity.RowIsValidUnsafe(SourceRow<P, U>(psel, usel, start, i))) {
                const idx_t o = out + i;
                dst[o / 64] &= ~(validity_t(1) << (o % 64));
            }
        }
    });
}

// Copies the part's strings to `dst`, long ones into the heap at `heap_cur` (stored as offsets).
// Two passes: the string_t gather issues independent loads (the source rows of a split part are
// scattered over a chunk that is usually no longer cached), and only then are the bodies copied.
void CopyStrings(data_ptr_t base, string_t *dst, const UnifiedVectorFormat &u, const sel_t *psel, idx_t start,
                 idx_t count, uint64_t &heap_cur) {
    auto *src = UnifiedVectorFormat::GetData<string_t>(u);
    const bool has_null = !u.validity.CannotHaveNull();
    DispatchSel(psel, VectorSel(u), [&](auto p, auto v) {
        constexpr bool P = decltype(p)::value, U = decltype(v)::value;
        const sel_t *usel = VectorSel(u);
        for (idx_t i = 0; i < count; i++) {
            const idx_t r = SourceRow<P, U>(psel, usel, start, i);
            dst[i] = has_null && !u.validity.RowIsValidUnsafe(r) ? string_t() : src[r]; // NULL -> inlined empty
        }
    });
    for (idx_t i = 0; i < count; i++) {
        string_t &s = dst[i];
        if (!s.IsInlined()) {
            std::memcpy(base + heap_cur, s.GetData(), s.GetSize());
            s.SetPointer(reinterpret_cast<char *>(static_cast<uintptr_t>(heap_cur)));
            heap_cur += s.GetSize();
        }
    }
}

uint64_t HeapBytes(const UnifiedVectorFormat &u, const sel_t *psel, idx_t start, idx_t count) {
    auto *strs = UnifiedVectorFormat::GetData<string_t>(u);
    const bool has_null = !u.validity.CannotHaveNull();
    uint64_t heap = 0;
    DispatchSel(psel, VectorSel(u), [&](auto p, auto v) {
        constexpr bool P = decltype(p)::value, U = decltype(v)::value;
        const sel_t *usel = VectorSel(u);
        for (idx_t i = 0; i < count; i++) {
            const idx_t r = SourceRow<P, U>(psel, usel, start, i);
            const string_t &s = strs[r];
            const bool counted = !s.IsInlined() && (!has_null || u.validity.RowIsValidUnsafe(r));
            heap += counted ? s.GetSize() : 0;
        }
    });
    return heap;
}

// Adds each row's heap bytes (see HeapBytes) to row_heap[i].
void AddRowHeapBytes(const UnifiedVectorFormat &u, const sel_t *psel, idx_t start, idx_t count, uint64_t *row_heap) {
    auto *strs = UnifiedVectorFormat::GetData<string_t>(u);
    const bool has_null = !u.validity.CannotHaveNull();
    DispatchSel(psel, VectorSel(u), [&](auto p, auto v) {
        constexpr bool P = decltype(p)::value, U = decltype(v)::value;
        const sel_t *usel = VectorSel(u);
        for (idx_t i = 0; i < count; i++) {
            const idx_t r = SourceRow<P, U>(psel, usel, start, i);
            const string_t &s = strs[r];
            const bool counted = !s.IsInlined() && (!has_null || u.validity.RowIsValidUnsafe(r));
            row_heap[i] += counted ? s.GetSize() : 0;
        }
    });
}

// Clears the validity bits past `rows` in the mask's last word.
inline void TrimValidity(validity_t *mask, uint64_t rows) {
    if (rows % 64 != 0) {
        mask[ValidityWords(rows) - 1] &= (validity_t(1) << (rows % 64)) - 1;
    }
}

// Zeroes every byte of the block outside the column regions and the heap (header, alignment gaps).
void ZeroGaps(data_ptr_t base, const BlockLayout &L, uint64_t rows) {
    uint64_t cur = 0;
    auto zero_to = [&](uint64_t off) {
        if (off > cur) {
            std::memset(base + cur, 0, off - cur);
        }
    };
    for (size_t c = 0; c < L.width.size(); c++) {
        zero_to(L.data_off[c]);
        cur = L.data_off[c] + rows * L.width[c];
        if (L.valid_off[c] != 0) {
            zero_to(L.valid_off[c]);
            cur = L.valid_off[c] + ValidityBytes(rows);
        }
    }
    zero_to(L.heap_off);
}

void WriteBlockData(data_ptr_t base, const Schema &schema, const std::vector<ChunkSelection> &parts,
                    const std::vector<const UnifiedVectorFormat *> &uf, const BlockLayout &L, uint64_t rows) {
    const uint32_t ncol = static_cast<uint32_t>(schema.size());
    uint64_t heap_cur = L.heap_off;
    for (uint32_t c = 0; c < ncol; c++) {
        const bool is_varchar = schema.columns[c].type.id == TypeId::VARCHAR;
        const uint32_t w = L.width[c];
        const CopyFn copy = is_varchar ? nullptr : GetCopyFunction(w);
        validity_t *mask = nullptr;
        if (L.valid_off[c] != 0) {
            mask = reinterpret_cast<validity_t *>(base + L.valid_off[c]);
            const uint64_t words = ValidityWords(rows);
            std::fill(mask, mask + words, ~validity_t(0));
            if (rows % 64 != 0) {
                mask[words - 1] = (validity_t(1) << (rows % 64)) - 1;
            }
        }
        data_ptr_t dst = base + L.data_off[c];
        idx_t out_row = 0;
        for (size_t p = 0; p < parts.size(); p++) {
            const auto &u = uf[p][c];
            const sel_t *psel = parts[p].sel;
            const idx_t count = parts[p].count;
            if (is_varchar) {
                CopyStrings(base, reinterpret_cast<string_t *>(dst) + out_row, u, psel, 0, count, heap_cur);
            } else if (copy) {
                copy(dst + size_t(out_row) * w, u.data, psel, VectorSel(u), 0, count);
            } else {
                const sel_t *usel = VectorSel(u);
                for (idx_t i = 0; i < count; i++) {
                    const idx_t r = psel ? psel[i] : i;
                    std::memcpy(dst + size_t(out_row + i) * w, u.data + size_t(usel ? usel[r] : r) * w, w);
                }
            }
            if (mask) {
                CopyNulls(mask, u, psel, 0, out_row, count);
            }
            out_row += count;
        }
    }
}

template <typename Alloc>
auto BuildBlock(const Schema &schema, const std::vector<ChunkSelection> &parts, Alloc allocate) {
    const uint32_t ncol = static_cast<uint32_t>(schema.size());
    std::vector<const UnifiedVectorFormat *> uf(parts.size());
    std::vector<std::unique_ptr<UnifiedVectorFormat[]>> formats(parts.size());
    uint64_t rows = 0;
    uint64_t heap_size = 0;
    for (size_t p = 0; p < parts.size(); p++) {
        rows += parts[p].count;
        formats[p].reset(new UnifiedVectorFormat[ncol]);
        for (uint32_t c = 0; c < ncol; c++) {
            parts[p].chunk->data[c].ToUnifiedFormat(formats[p][c]);
            if (schema.columns[c].type.id == TypeId::VARCHAR) {
                heap_size += HeapBytes(formats[p][c], parts[p].sel, 0, parts[p].count);
            }
        }
        uf[p] = formats[p].get();
    }

    const BlockLayout L = ComputeLayout(schema, rows, heap_size);
    data_ptr_t base = allocate(L.total);
    ZeroGaps(base, L, rows);
    WriteBlockHeader(base, schema, L, rows, heap_size);
    WriteBlockData(base, schema, parts, uf, L, rows);
    return base;
}

Result<void> ValidateParts(const Schema &schema, const std::vector<ChunkSelection> &parts) {
    const uint32_t ncol = static_cast<uint32_t>(schema.size());
    for (const auto &part : parts) {
        if (part.chunk == nullptr) {
            return Error("Plume: export got a null chunk", ErrorKind::InvalidInput);
        }
        if (part.chunk->ColumnCount() != ncol) {
            return Error("Plume: schema/chunk column count mismatch", ErrorKind::InvalidInput);
        }
        if (part.count > part.chunk->size()) {
            return Error("Plume: selection count exceeds chunk size", ErrorKind::InvalidInput);
        }
    }
    return Ok();
}

} // namespace

Result<OwnedBlock> ExportChunks(const Schema &schema, const std::vector<DataChunk *> &chunks, Allocator &alloc) {
    if (chunks.empty()) {
        return Error("Plume: ExportChunks requires at least one chunk", ErrorKind::InvalidInput);
    }
    std::vector<ChunkSelection> parts;
    parts.reserve(chunks.size());
    for (auto *chunk : chunks) {
        parts.push_back({chunk, nullptr, chunk ? chunk->size() : 0});
    }
    TRYV(ValidateParts(schema, parts));

    OwnedBlock block;
    block.data = BuildBlock(schema, parts, [&](uint64_t total) {
        block.size = total;
        return alloc.Allocate(total);
    });
    return block;
}

Result<OwnedBlock> ExportChunk(const Schema &schema, DataChunk &chunk, Allocator &alloc) {
    return ExportChunks(schema, {&chunk}, alloc);
}

Result<DataBuffer> ExportSelection(const Schema &schema, const std::vector<ChunkSelection> &parts) {
    TRYV(ValidateParts(schema, parts));
    DataBuffer buf;
    BuildBlock(schema, parts, [&](uint64_t total) {
        buf = DataBuffer(total); // malloc-backed; owned by the caller / AddOutput
        return reinterpret_cast<data_ptr_t>(buf.mutable_data());
    });
    return buf;
}

//===----------------------------------------------------------------------===//
// BlockWriter
//===----------------------------------------------------------------------===//

namespace {

// A writer's first block starts with this byte budget and grows by kGrowth up to max_bytes, so a
// partition that receives little data only takes memory in proportion to it.
constexpr uint64_t kInitialBlockBytes = 64u << 10;
constexpr uint64_t kGrowth = 4;

// Copies `rows` rows and `heap_size` heap bytes of a block from layout Ls (in src) to layout Ld (in
// dst), rebasing the long strings' heap offsets. Validity is copied in whole words.
void MoveRows(data_ptr_t dst, const BlockLayout &Ld, const_data_ptr_t src, const BlockLayout &Ls, idx_t rows,
              uint64_t heap_size, const std::vector<uint32_t> &varchar_cols) {
    for (size_t c = 0; c < Ld.width.size(); c++) {
        std::memcpy(dst + Ld.data_off[c], src + Ls.data_off[c], size_t(rows) * Ld.width[c]);
        if (Ld.valid_off[c] != 0) {
            std::memcpy(dst + Ld.valid_off[c], src + Ls.valid_off[c], ValidityBytes(rows));
        }
    }
    std::memcpy(dst + Ld.heap_off, src + Ls.heap_off, heap_size);
    if (Ld.heap_off == Ls.heap_off) {
        return;
    }
    for (uint32_t c : varchar_cols) {
        auto *strs = reinterpret_cast<string_t *>(dst + Ld.data_off[c]);
        for (idx_t r = 0; r < rows; r++) {
            if (!strs[r].IsInlined()) {
                const auto off = reinterpret_cast<uintptr_t>(strs[r].GetPointer()) - Ls.heap_off + Ld.heap_off;
                strs[r].SetPointer(reinterpret_cast<char *>(off));
            }
        }
    }
}

data_ptr_t AllocateBlock(uint64_t size) {
    auto *p = static_cast<data_ptr_t>(std::malloc(size));
    if (!p) {
        throw std::bad_alloc();
    }
    return p;
}

} // namespace

struct BlockWriter::Impl {
    Schema schema;
    size_t max_bytes;
    std::vector<uint32_t> varchar_cols;
    std::vector<CopyFn> copy; // per fixed-width column (nullptr: generic per-row copy)
    double fixed_per_row = 0; // column bytes + validity bits per row
    uint64_t overhead = 0;    // header, descriptors, names and worst-case alignment padding
    double heap_per_row = -1; // VARCHAR heap bytes per row seen last (< 0: none yet)
    uint64_t budget = 0;      // byte budget of the current block (grows up to max_bytes)

    data_ptr_t buf = nullptr; // the block being built, laid out for `capacity` rows
    uint64_t buf_size = 0;
    BlockLayout L;
    idx_t capacity = 0;
    idx_t rows = 0;
    uint64_t heap_end = 0; // absolute offset of the next heap byte
    std::vector<uint64_t> row_heap;

    idx_t CapacityFor(uint64_t bytes) const;
    void InitLayout(data_ptr_t block) const;
    void Start(const UnifiedVectorFormat *formats, const sel_t *psel, idx_t start, idx_t count);
    void Grow();
    idx_t FitRows(const UnifiedVectorFormat *formats, const sel_t *psel, idx_t start, idx_t count, uint64_t space);
};

BlockWriter::BlockWriter(Schema schema, size_t max_bytes) : impl_(std::make_unique<Impl>()) {
    auto &w = *impl_;
    w.max_bytes = max_bytes;
    w.budget = std::min<uint64_t>(max_bytes, kInitialBlockBytes);
    const uint32_t ncol = static_cast<uint32_t>(schema.size());
    w.copy.resize(ncol, nullptr);
    uint64_t names = 0;
    for (uint32_t c = 0; c < ncol; c++) {
        const auto &col = schema.columns[c];
        const uint32_t width = PhysicalWidth(col.type);
        names += col.name.size();
        w.fixed_per_row += width + (col.nullable ? 0.125 : 0);
        if (col.type.id == TypeId::VARCHAR) {
            w.varchar_cols.push_back(c);
        } else {
            w.copy[c] = GetCopyFunction(width);
        }
    }
    w.overhead = sizeof(BlockHeader) + uint64_t(ncol) * (sizeof(ColumnDescriptor) + kDataAlign + kValidityAlign + 8) +
                 names + kDataAlign;
    w.schema = std::move(schema);
}

BlockWriter::~BlockWriter() { std::free(impl_->buf); }

// Rows that fit `bytes`, given the heap bytes per row seen so far plus a margin, so a block
// normally fills up on rows rather than heap.
idx_t BlockWriter::Impl::CapacityFor(uint64_t bytes) const {
    const double per_row = fixed_per_row + (varchar_cols.empty() ? 0.0 : std::max(heap_per_row, 0.0) * 1.05);
    const double avail = bytes > overhead ? double(bytes - overhead) : 0.0;
    return static_cast<idx_t>(std::max(1.0, std::floor(avail / std::max(per_row, 1.0))));
}

// Zeroes the gaps of layout L in `block` and sets the validity of all `capacity` rows.
void BlockWriter::Impl::InitLayout(data_ptr_t block) const {
    ZeroGaps(block, L, capacity);
    for (size_t c = 0; c < L.width.size(); c++) {
        if (L.valid_off[c] != 0) {
            auto *mask = reinterpret_cast<validity_t *>(block + L.valid_off[c]);
            std::fill(mask, mask + ValidityWords(capacity), ~validity_t(0));
        }
    }
}

void BlockWriter::Impl::Start(const UnifiedVectorFormat *formats, const sel_t *psel, idx_t start, idx_t count) {
    if (!varchar_cols.empty() && heap_per_row < 0) {
        uint64_t heap = 0;
        for (uint32_t c : varchar_cols) {
            heap += HeapBytes(formats[c], psel, start, count);
        }
        heap_per_row = double(heap) / double(count);
    }
    capacity = CapacityFor(budget);
    L = ComputeLayout(schema, capacity, 0);
    const uint64_t need = std::max<uint64_t>(budget, L.heap_off);
    if (buf_size < need) {
        std::free(buf);
        buf = AllocateBlock(need);
        buf_size = need;
    }
    InitLayout(buf);
    rows = 0;
    heap_end = L.heap_off;
}

// Moves the block into a buffer with a kGrowth times larger budget.
void BlockWriter::Impl::Grow() {
    const uint64_t heap_size = heap_end - L.heap_off;
    if (!varchar_cols.empty() && rows > 0) {
        heap_per_row = double(heap_size) / double(rows);
    }
    budget = std::min<uint64_t>(max_bytes, budget * kGrowth);
    const BlockLayout old = L;
    capacity = std::max(CapacityFor(budget), capacity);
    L = ComputeLayout(schema, capacity, 0);
    const uint64_t need = std::max<uint64_t>(budget, L.heap_off + heap_size);
    data_ptr_t grown = AllocateBlock(need);
    InitLayout(grown);
    MoveRows(grown, L, buf, old, rows, heap_size, varchar_cols);
    std::free(buf);
    buf = grown;
    buf_size = need;
    heap_end = L.heap_off + heap_size;
}

// The longest prefix of the rows whose strings fit `space` heap bytes.
idx_t BlockWriter::Impl::FitRows(const UnifiedVectorFormat *formats, const sel_t *psel, idx_t start, idx_t count,
                                 uint64_t space) {
    row_heap.assign(count, 0);
    for (uint32_t c : varchar_cols) {
        AddRowHeapBytes(formats[c], psel, start, count, row_heap.data());
    }
    uint64_t used = 0;
    for (idx_t i = 0; i < count; i++) {
        used += row_heap[i];
        if (used > space) {
            return i;
        }
    }
    return count;
}

idx_t BlockWriter::Append(const UnifiedVectorFormat *formats, const sel_t *sel, idx_t start, idx_t count) {
    auto &w = *impl_;
    if (count == 0) {
        return 0;
    }
    const sel_t *psel = sel ? sel + start : nullptr;
    const idx_t base = sel ? 0 : start;
    if (w.rows == 0) {
        w.Start(formats, psel, base, count);
    }
    idx_t n;
    uint64_t heap, space;
    for (;;) {
        n = std::min<idx_t>(count, w.capacity - w.rows);
        heap = 0;
        for (uint32_t c : w.varchar_cols) {
            heap += HeapBytes(formats[c], psel, base, n);
        }
        space = w.buf_size - w.heap_end;
        if ((n < count || heap > space) && w.budget < w.max_bytes) {
            w.Grow();
            continue;
        }
        break;
    }
    if (n == 0) {
        return 0; // full
    }
    if (heap > space) {
        n = w.FitRows(formats, psel, base, n, space);
        if (n == 0 && w.rows > 0) {
            return 0; // full
        }
        if (n == 0) { // a single row larger than the block: grow the buffer to hold it
            n = 1;
            const uint64_t need = w.heap_end + w.row_heap[0];
            auto *grown = static_cast<data_ptr_t>(std::realloc(w.buf, need));
            if (!grown) {
                throw std::bad_alloc();
            }
            w.buf = grown;
            w.buf_size = need;
        }
    }
    const auto &L = w.L;
    for (uint32_t c = 0; c < L.width.size(); c++) {
        const auto &u = formats[c];
        const uint32_t width = L.width[c];
        data_ptr_t dst = w.buf + L.data_off[c] + size_t(w.rows) * width;
        if (w.schema.columns[c].type.id == TypeId::VARCHAR) {
            CopyStrings(w.buf, reinterpret_cast<string_t *>(dst), u, psel, base, n, w.heap_end);
        } else if (w.copy[c]) {
            w.copy[c](dst, u.data, psel, VectorSel(u), base, n);
        } else {
            const sel_t *usel = VectorSel(u);
            for (idx_t i = 0; i < n; i++) {
                const idx_t r = psel ? psel[i] : base + i;
                std::memcpy(dst + size_t(i) * width, u.data + size_t(usel ? usel[r] : r) * width, width);
            }
        }
        if (L.valid_off[c] != 0) {
            CopyNulls(reinterpret_cast<validity_t *>(w.buf + L.valid_off[c]), u, psel, base, w.rows, n);
        }
    }
    w.rows += n;
    return n;
}

idx_t BlockWriter::Rows() const { return impl_->rows; }

DataBuffer BlockWriter::Finish() {
    auto &w = *impl_;
    const uint64_t heap_size = w.heap_end - w.L.heap_off;
    if (!w.varchar_cols.empty() && w.rows > 0) {
        w.heap_per_row = double(heap_size) / double(w.rows);
    }
    const uint32_t ncol = static_cast<uint32_t>(w.L.width.size());
    DataBuffer out;
    if (w.rows == w.capacity) {
        // Full: the block is already in its final layout.
        for (uint32_t c = 0; c < ncol; c++) {
            if (w.L.valid_off[c] != 0) {
                TrimValidity(reinterpret_cast<validity_t *>(w.buf + w.L.valid_off[c]), w.rows);
            }
        }
        BlockLayout L = w.L;
        L.total = w.heap_end;
        WriteBlockHeader(w.buf, w.schema, L, w.rows, heap_size);
        out = DataBuffer(std::exchange(w.buf, nullptr), w.heap_end, true);
        w.buf_size = 0;
    } else {
        // Short of its capacity: compact into an exact-size block (the buffer is reused).
        const BlockLayout C = ComputeLayout(w.schema, w.rows, heap_size);
        data_ptr_t dst = AllocateBlock(C.total);
        ZeroGaps(dst, C, w.rows);
        WriteBlockHeader(dst, w.schema, C, w.rows, heap_size);
        MoveRows(dst, C, w.buf, w.L, w.rows, heap_size, w.varchar_cols);
        for (uint32_t c = 0; c < ncol; c++) {
            if (C.valid_off[c] != 0) {
                TrimValidity(reinterpret_cast<validity_t *>(dst + C.valid_off[c]), w.rows);
            }
        }
        out = DataBuffer(dst, C.total, true);
    }
    w.rows = 0;
    return out;
}

uint64_t VarcharHeapBytes(const Vector &vec, const sel_t *sel, idx_t count) {
    UnifiedVectorFormat u;
    vec.ToUnifiedFormat(u);
    return HeapBytes(u, sel, 0, count);
}

uint64_t VarcharHeapBytes(const UnifiedVectorFormat &vec, const sel_t *sel, idx_t count) {
    return HeapBytes(vec, sel, 0, count);
}

} // namespace plume::memory
