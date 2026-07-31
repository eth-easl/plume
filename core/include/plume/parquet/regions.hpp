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

} // namespace plume::parquet
