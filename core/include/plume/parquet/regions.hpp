#pragma once

#include "plume/parquet/metadata.hpp"
#include "plume/parquet/parquet.hpp"

#include <cstdint>
#include <map>
#include <vector>

namespace plume::parquet {

// The data pages to read of each read column of a row group (in DataChunk order) if not all.
struct RowGroupPages {
    std::vector<int64_t> dict_sizes;          // bytes of the chunk's leading dictionary page (0 -> none)
    std::vector<std::vector<PageRun>> runs;   // req_idx/req_offset are set by DefineRegions
};

// `page_selection` maps a row group to the pages read of it.
std::vector<Region> DefineRegions(const FileMeta &meta, uint32_t num_splits, uint64_t coalesce_distance,
                                  const std::vector<uint32_t> &projection = {},
                                  const std::vector<uint32_t> &keep_row_groups = {},
                                  uint64_t max_region_size = 0,
                                  const std::map<uint32_t, RowGroupPages> &page_selection = {});

uint32_t FileSplitCount(uint32_t num_splits, uint32_t num_files, uint32_t file_idx,
                        size_t kept_row_groups, size_t total_row_groups);

} // namespace plume::parquet
