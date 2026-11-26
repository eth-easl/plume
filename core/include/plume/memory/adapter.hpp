#pragma once

#include "plume/common/buffer.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/memory/block.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace plume::memory {

Result<Schema> ParseSchema(duckdb::const_data_ptr_t base, size_t size);

Result<void> ImportBlock(duckdb::data_ptr_t base, size_t size, duckdb::DataChunk &out_chunk, Schema &out_schema);

Result<std::vector<std::unique_ptr<duckdb::DataChunk>>> ImportBlockChunks(duckdb::data_ptr_t base, size_t size,
                                                                          Schema &out_schema);

Result<OwnedBlock> ExportChunk(const Schema &schema, duckdb::DataChunk &chunk, Allocator &alloc);

Result<OwnedBlock> ExportChunks(const Schema &schema, const std::vector<duckdb::DataChunk *> &chunks,
                                Allocator &alloc);

struct ChunkSelection {
    duckdb::DataChunk *chunk = nullptr;
    const duckdb::sel_t *sel = nullptr; // rows sel[0..count) of chunk; nullptr => rows [0, count)
    duckdb::idx_t count = 0;
};

// Bytes the selected strings of a VARCHAR vector take in a block's varchar heap (the valid ones
// too long to inline). `sel` as in ChunkSelection.
uint64_t VarcharHeapBytes(const duckdb::Vector &vec, const duckdb::sel_t *sel, duckdb::idx_t count);
uint64_t VarcharHeapBytes(const duckdb::UnifiedVectorFormat &vec, const duckdb::sel_t *sel, duckdb::idx_t count);

// Exports the parts' rows, in order, as one block.
Result<DataBuffer> ExportSelection(const Schema &schema, const std::vector<ChunkSelection> &parts);

// Builds blocks by appending rows straight into their final layout: the columns are laid out for a
// row capacity sized so the block fits `max_bytes` (the VARCHAR heap estimated from the bytes per
// row seen so far), long strings go to the heap behind them. A block that reaches its capacity is
// handed out as is; one that ends short of it (heap full, or the last one) is compacted.
class BlockWriter {
public:
    BlockWriter(Schema schema, size_t max_bytes);
    ~BlockWriter();
    BlockWriter(const BlockWriter &) = delete;
    BlockWriter &operator=(const BlockWriter &) = delete;

    // Appends rows sel[start, start + count) (sel == nullptr: rows [start, start + count)) of a
    // chunk given as its columns' unified formats. Returns how many rows fit; fewer than `count`
    // means the block is full and must be finished before appending the rest.
    duckdb::idx_t Append(const duckdb::UnifiedVectorFormat *formats, const duckdb::sel_t *sel, duckdb::idx_t start,
                         duckdb::idx_t count);
    duckdb::idx_t Rows() const;
    // Returns the block built so far (requires Rows() > 0) and starts a new one.
    DataBuffer Finish();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace plume::memory
