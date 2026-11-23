#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/expression/expression.hpp"
#include "plume/parquet/metadata.hpp"
#include "plume/parquet/parquet.hpp"

#include <memory>
#include <vector>

namespace plume::catalog {

struct RemoteParquetDataSource : public DataSource {
    std::vector<parquet::FileMeta> metadata = {};
    std::vector<std::vector<uint8_t>> footer_bytes = {};
    std::vector<int64_t> page_index_offsets = {};
    std::vector<std::vector<uint8_t>> page_index_bytes = {};

    RemoteParquetDataSource() : DataSource(DataSourceType::REMOTE_PARQUET) {}
    Result<void> Resolve(const RemoteResolver &resolver) override;
    void ResolvePageIndexes(const RemoteResolver &resolver, const std::vector<uint32_t> &key_cols, 
                            const std::vector<uint32_t> &read_cols);
};


std::shared_ptr<DataSource> CreateRemoteParquetSource(std::string name, std::vector<std::string> urls);

struct ParquetPrepareInputs {
    dandelion::DataItemVec cfg;
    dandelion::DataItemVec footers;
    dandelion::DataItemVec urls;
};

ParquetPrepareInputs BuildParquetPrepareInputs(const RemoteParquetDataSource &src,
    std::vector<uint32_t> projection, const ExprNode *pushed_filter,
    std::vector<exec::DynamicFilterProbeSpec> dyn_filters = {}, uint32_t num_splits = 1, uint64_t coalesce_distance = 1 << 10, uint64_t max_region_bytes = 0);

struct ParquetStageInputs {
    dandelion::DataItemVec region_info;
    dandelion::DataItemVec chunk_reqs;
};

Result<ParquetStageInputs> BuildParquetStageInputs(const RemoteParquetDataSource &src,
    const std::vector<uint32_t> &projection, const ExprNode *pushed_filter,
    uint32_t num_splits = 1, uint64_t coalesce_distance = 1 << 10, uint64_t max_region_bytes = 0);

} // namespace plume::catalog
