//===----------------------------------------------------------------------===//
// plume — assign parquet row groups to regions and coalesce their column chunks
// into byte-range fetch requests (TASK_v2 plume_pq_prepare).
//===----------------------------------------------------------------------===//

#pragma once

#include "plume/parquet/metadata.hpp"
#include "plume/parquet/parquet.hpp"

#include <cstdint>
#include <vector>

namespace plume::parquet {

// Split the file's row groups across ~`num_splits` regions (row groups distributed
// evenly). Within each region, the column chunks of every assigned row group become
// DataChunks; chunks are sorted by offset and coalesced into fetch requests (merging
// gaps <= `coalesce_distance`), with each DataChunk pointed at its request via
// req_idx/req_offset.
//
// `projection` (projection pushdown) selects the file column indices to read, in
// output order; empty ⇒ all columns in file order. Each emitted DataChunk's
// `column_idx` is its OUTPUT position (0..N-1), matching the projected schema the
// stage reads, so the columns actually fetched/decoded are only the projected ones.
//
// `keep_row_groups` (filter pushdown) lists the file row-group indices to include,
// ascending; empty ⇒ all row groups. Callers pass the survivors of statistics-based
// pruning here (see prune.hpp), so pruned row groups are never fetched.
//
// `max_region_size` (bytes, 0 ⇒ unlimited) caps the compressed data each region
// processes (summing only the projected columns). It takes precedence over
// `num_splits`: a file may be split into more than `num_splits` regions to honor it,
// though a single row group larger than the cap still forms its own region.
std::vector<Region> DefineRegions(const FileMeta &meta, uint32_t num_splits, uint64_t coalesce_distance,
                                  const std::vector<uint32_t> &projection = {},
                                  const std::vector<uint32_t> &keep_row_groups = {},
                                  uint64_t max_region_size = 0);

} // namespace plume::parquet
