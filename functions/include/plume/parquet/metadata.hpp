//===----------------------------------------------------------------------===//
// plume — minimal Parquet footer (FileMetaData) parser
//
// Just enough of the Thrift compact protocol to walk FileMetaData -> row_groups
// -> columns -> column_meta_data and extract each column chunk's byte range in
// the file, plus the file's schema, row count, and per-column statistics. No
// column decoding (that is plume_pq_stage). Custom + instance-free (no DuckDB
// parquet extension).
//===----------------------------------------------------------------------===//

#pragma once

#include "plume/common/result.hpp"
#include "plume/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace plume::parquet {

// A column chunk's parquet Statistics (best-effort; a file may carry none). min/max
// are kept as their raw PLAIN-encoded bytes (per the column's physical type) — the
// caller decodes them with the resolved column type.
struct ColumnStats {
	bool has_min = false;
	bool has_max = false;
	std::vector<uint8_t> min_value; // prefers Statistics.min_value, else the deprecated min
	std::vector<uint8_t> max_value;
	bool has_null_count = false;
	int64_t null_count = 0;
	bool has_distinct_count = false;
	int64_t distinct_count = 0;
};

// A column chunk's compressed byte range in the file (covers the dictionary page,
// if any, and all data pages).
struct ColumnChunkMeta {
	int64_t offset = 0;
	int64_t size = 0;
	int32_t codec = 0; // parquet CompressionCodec (0=UNCOMPRESSED, 1=SNAPPY, ...)
	ColumnStats stats;
};

struct RowGroupMeta {
	std::vector<ColumnChunkMeta> columns;
};

struct FileMeta {
	std::vector<RowGroupMeta> row_groups;
	Schema schema;       // full physical (leaf) schema, in file column order
	int64_t num_rows = 0;
};

// Parse the FileMetaData out of a parquet footer `tail` — which must contain the
// full metadata plus the 8-byte trailer ([i32 metadata_len]["PAR1"]). Returns an
// Error on malformed input.
Result<FileMeta> ParseFooter(const uint8_t *tail, size_t size);

} // namespace plume::parquet
