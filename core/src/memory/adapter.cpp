#include "plume/memory/adapter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"

#include <cstring>

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
        FlatVector::SetData(vec, base + d.data_offset);
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
    out_chunk.SetCardinality(rows);
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
            FlatVector::SetData(vec, base + d.data_offset + uint64_t(start) * width[c]);
            if (d.validity_offset != 0) {
                // NOTE: `start` is a multiple of STANDARD_VECTOR_SIZE (a multiple of 64), so the 
                //       slice begins on a validity-word boundary.
                auto *mask_words = reinterpret_cast<validity_t *>(
                    base + d.validity_offset + (uint64_t(start) / 64) * sizeof(validity_t));
                ValidityMask mask(mask_words, n);
                FlatVector::SetValidity(vec, mask);
            }
        }
        chunk->SetCardinality(n);
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

void WriteBlockData(data_ptr_t base, const Schema &schema, const std::vector<ChunkSelection> &parts,
                    const std::vector<std::vector<UnifiedVectorFormat>> &uf, const BlockLayout &L, uint64_t rows) {
    const uint32_t ncol = static_cast<uint32_t>(schema.size());
    uint64_t heap_cur = L.heap_off;
    for (uint32_t c = 0; c < ncol; c++) {
        const auto &col = schema.columns[c];
        const bool is_varchar = col.type.id == TypeId::VARCHAR;
        const uint32_t w = L.width[c];

        std::unique_ptr<ValidityMask> dmask;
        if (col.nullable) {
            dmask = std::make_unique<ValidityMask>(reinterpret_cast<validity_t *>(base + L.valid_off[c]), rows);
            dmask->SetAllValid(rows);
        }
        auto *str_dst = reinterpret_cast<string_t *>(base + L.data_off[c]);
        data_ptr_t fix_dst = base + L.data_off[c];

        idx_t out_row = 0;
        for (size_t p = 0; p < parts.size(); p++) {
            const auto &u = uf[p][c];
            const auto *psel = parts[p].sel;
            const idx_t count = parts[p].count;
            if (is_varchar) {
                auto *src = UnifiedVectorFormat::GetData<string_t>(u);
                for (idx_t i = 0; i < count; i++, out_row++) {
                    const idx_t r = psel ? psel->get_index(i) : i;
                    const idx_t phys = u.sel->get_index(r);
                    if (!u.validity.RowIsValid(phys)) {
                        str_dst[out_row] = string_t(); // null -> zeroed (inlined empty)
                        if (dmask) {
                            dmask->SetInvalid(out_row);
                        }
                        continue;
                    }
                    string_t s = src[phys];
                    if (s.IsInlined()) {
                        str_dst[out_row] = s; // self-contained
                    } else {
                        std::memcpy(base + heap_cur, s.GetData(), s.GetSize());
                        s.SetPointer(reinterpret_cast<char *>(static_cast<uintptr_t>(heap_cur))); // store offset
                        str_dst[out_row] = s;
                        heap_cur += s.GetSize();
                    }
                }
            } else {
                auto *src = u.data;
                for (idx_t i = 0; i < count; i++, out_row++) {
                    const idx_t r = psel ? psel->get_index(i) : i;
                    const idx_t phys = u.sel->get_index(r);
                    std::memcpy(fix_dst + size_t(out_row) * w, src + size_t(phys) * w, w);
                    if (dmask && !u.validity.RowIsValid(phys)) {
                        dmask->SetInvalid(out_row);
                    }
                }
            }
        }
    }
}

template <typename Alloc>
auto BuildBlock(const Schema &schema, const std::vector<ChunkSelection> &parts, Alloc allocate) {
    const uint32_t ncol = static_cast<uint32_t>(schema.size());

    std::vector<std::vector<UnifiedVectorFormat>> uf(parts.size());
    uint64_t rows = 0;
    uint64_t heap_size = 0;
    for (size_t p = 0; p < parts.size(); p++) {
        auto *chunk = parts[p].chunk;
        uf[p].resize(ncol);
        const idx_t chunk_rows = chunk->size();
        const auto *psel = parts[p].sel;
        const idx_t count = parts[p].count;
        rows += count;
        for (uint32_t c = 0; c < ncol; c++) {
            chunk->data[c].ToUnifiedFormat(chunk_rows, uf[p][c]);
            if (schema.columns[c].type.id != TypeId::VARCHAR) {
                continue;
            }
            auto *strs = UnifiedVectorFormat::GetData<string_t>(uf[p][c]);
            for (idx_t i = 0; i < count; i++) {
                const idx_t r = psel ? psel->get_index(i) : i;
                const idx_t phys = uf[p][c].sel->get_index(r);
                if (uf[p][c].validity.RowIsValid(phys) && !strs[phys].IsInlined()) {
                    heap_size += strs[phys].GetSize();
                }
            }
        }
    }

    const BlockLayout L = ComputeLayout(schema, rows, heap_size);
    data_ptr_t base = allocate(L.total);
    std::memset(base, 0, L.total);
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

} // namespace plume::memory
