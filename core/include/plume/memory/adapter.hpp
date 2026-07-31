#pragma once

#include "plume/common/buffer.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/memory/block.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"

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
    const duckdb::SelectionVector *sel = nullptr; // nullptr => rows [0, count)
    duckdb::idx_t count = 0;
};

Result<DataBuffer> ExportSelection(const Schema &schema, const std::vector<ChunkSelection> &parts);

} // namespace plume::memory
