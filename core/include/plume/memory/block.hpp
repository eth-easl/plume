#pragma once

#include "duckdb/common/typedefs.hpp"

#include <cstddef>
#include <cstdint>

namespace plume::memory {

// A non-owning view of a host-provided input block.
struct InputBlock {
    duckdb::data_ptr_t data;
    size_t size;
};

// A contiguous block owned by Plume (allocated via the Plume Allocator).
struct OwnedBlock {
    duckdb::data_ptr_t data = nullptr;
    size_t size = 0;
};

//===----------------------------------------------------------------------===//
// On-block binary structures (POD, little-endian, packed to fixed sizes).
//===----------------------------------------------------------------------===//

static constexpr uint32_t kBlockMagic = 0x424D4C50; // "PLMB"
static constexpr uint16_t kFormatVersion = 1;
static constexpr uint64_t kDataAlign = 16;    // data-region alignment
static constexpr uint64_t kValidityAlign = 8; // validity_t word alignment

struct BlockHeader {
    uint32_t magic;          // kBlockMagic
    uint16_t format_version; // kFormatVersion
    uint16_t flags;          // reserved (0)
    uint32_t column_count;
    uint32_t reserved0;
    uint64_t row_count;
    uint64_t total_size;     // whole block size in bytes
    uint64_t schema_offset;  // ColumnDescriptor array
    uint64_t names_offset;   // names blob
    uint64_t heap_offset;    // varchar heap start (== total_size if empty)
    uint64_t heap_size;
};
static_assert(sizeof(BlockHeader) == 64, "BlockHeader layout pinned");

// flags bit meanings for ColumnDescriptor::flags
static constexpr uint8_t kColNullable = 0x01;

struct ColumnDescriptor {
    uint8_t type_id;
    uint8_t decimal_width;
    uint8_t decimal_scale;
    uint8_t flags;
    uint32_t name_offset;
    uint32_t name_length;
    uint32_t reserved0;
    uint64_t data_offset;     // absolute; fixed-width array (string_t[] for VARCHAR)
    uint64_t validity_offset; // absolute; 0 == column is all-valid (no mask stored)
};
static_assert(sizeof(ColumnDescriptor) == 32, "ColumnDescriptor layout pinned");

inline uint64_t AlignUp(uint64_t v, uint64_t a) { return (v + a - 1) & ~(a - 1); }

} // namespace plume::memory
