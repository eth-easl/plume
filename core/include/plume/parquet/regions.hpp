#pragma once

#include "plume/parquet/metadata.hpp"
#include "plume/parquet/parquet.hpp"

#include <cstdint>
#include <vector>

namespace plume::parquet {

std::vector<Region> DefineRegions(const FileMeta &meta, uint32_t num_splits, uint64_t coalesce_distance,
                                  const std::vector<uint32_t> &projection = {},
                                  const std::vector<uint32_t> &keep_row_groups = {},
                                  uint64_t max_region_size = 0);

uint32_t FileSplitCount(uint32_t num_splits, uint32_t num_files, uint32_t file_idx,
                        size_t kept_row_groups, size_t total_row_groups);

} // namespace plume::parquet
