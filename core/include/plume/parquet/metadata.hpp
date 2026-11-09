#pragma once

#include "plume/common/result.hpp"
#include "plume/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace plume::parquet {

// A column chunk's Parquet statistics. 
// min/max are kept as their raw PLAIN-encoded bytes (caller decodes them).
struct ColumnStats {
    bool has_min = false;
    bool has_max = false;
    std::vector<uint8_t> min_value; // prefers Statistics.min_value, else the deprecated min
    std::vector<uint8_t> max_value;
    // min/max come from the deprecated fields, which order BYTE_ARRAY values by signed bytes.
    bool deprecated_min_max = false;
    bool has_null_count = false;
    int64_t null_count = 0;
    bool has_distinct_count = false;
    int64_t distinct_count = 0;
};

// A column chunk's compressed byte range in the file.
struct ColumnChunkMeta {
    int64_t offset = 0;
    int64_t size = 0;
    int32_t codec = 0; // parquet CompressionCodec
    ColumnStats stats;
    // Where the chunk's page index lives in the file (-1 -> none).
    int64_t column_index_offset = -1;
    int32_t column_index_length = 0;
    int64_t offset_index_offset = -1;
    int32_t offset_index_length = 0;
};

struct RowGroupMeta {
    std::vector<ColumnChunkMeta> columns;
    int64_t num_rows = 0;
};

// A data page of a column chunk (parquet OffsetIndex PageLocation).
struct PageLocation {
    int64_t offset = 0;    // in the file
    int32_t size = 0;      // compressed page size, header included
    int64_t first_row = 0; // within the row group
};

// Parses a column chunk's OffsetIndex: its data pages in order.
Result<std::vector<PageLocation>> ParseOffsetIndex(const uint8_t *data, size_t size);

// A column chunk's ColumnIndex: per data page min/max statistics.
struct ColumnIndex {
    std::vector<uint8_t> null_pages; // 1 -> the page holds only NULLs (and no min/max)
    std::vector<ColumnStats> stats;
};

Result<ColumnIndex> ParseColumnIndex(const uint8_t *data, size_t size);

struct FileMeta {
    std::vector<RowGroupMeta> row_groups;
    Schema schema; // full physical (leaf) schema, in file column order
    int64_t num_rows = 0;
};

Result<FileMeta> ParseFooter(const uint8_t *tail, size_t size);

} // namespace plume::parquet
