#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/expression/expression.hpp"
#include "plume/parquet/metadata.hpp"

#include <memory>
#include <vector>

namespace plume::catalog {

struct RemoteParquetDataSource : public DataSource {
    std::vector<parquet::FileMeta> metadata = {};
    std::vector<std::vector<uint8_t>> footer_bytes = {};
    // Per file: bytes of the file from page_index_offsets[i] on holding page indexes (see
    // FetchPageIndexes), empty if none were fetched.
    std::vector<int64_t> page_index_offsets = {};
    std::vector<std::vector<uint8_t>> page_index_bytes = {};

    RemoteParquetDataSource() : DataSource(DataSourceType::REMOTE_PARQUET) {}
    Result<void> Resolve(const RemoteResolver &resolver) override;
};

// Queues fetching each file's page indexes pq_prepare needs to select pages by a dynamic filter on
// file column `key_col`: the key column's column and offset indexes and the read columns' offset
// indexes (all columns for an empty projection). Files without them are left out.
void FetchPageIndexes(RemoteParquetDataSource &src, const RemoteResolver &resolver, uint32_t key_col,
                      const std::vector<uint32_t> &projection);

std::shared_ptr<DataSource> CreateRemoteParquetSource(std::string name, std::vector<std::string> urls);

struct ParquetStageInputs {
    dandelion::DataItemVec region_info;
    dandelion::DataItemVec chunk_reqs;
};

Result<ParquetStageInputs> BuildParquetStageInputs(const RemoteParquetDataSource &src,
    const std::vector<uint32_t> &projection, const ExprNode *pushed_filter,
    uint32_t num_splits = 1, uint64_t coalesce_distance = 1 << 10, uint64_t max_region_bytes = 0,
    int32_t dyn_filter_col = -1);

struct ParquetPrepareInputs {
    dandelion::DataItemVec cfg;
    dandelion::DataItemVec footers;
    dandelion::DataItemVec urls;
};

ParquetPrepareInputs BuildParquetPrepareInputs(const RemoteParquetDataSource &src,
    std::vector<uint32_t> projection, const ExprNode *pushed_filter, int32_t dyn_filter_col = -1,
    uint32_t num_splits = 1, uint64_t coalesce_distance = 1 << 10, uint64_t max_region_bytes = 0);

} // namespace plume::catalog
