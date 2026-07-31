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

    RemoteParquetDataSource() : DataSource(DataSourceType::REMOTE_PARQUET) {}
    Result<void> Resolve(const RemoteResolver &resolver) override;
};

std::shared_ptr<DataSource> CreateRemoteParquetSource(std::string name, std::vector<std::string> urls);

struct ParquetStageInputs {
    dandelion::DataItemVec region_info;
    dandelion::DataItemVec chunk_reqs;
};

Result<ParquetStageInputs> BuildParquetStageInputs(const RemoteParquetDataSource &src,
    const std::vector<uint32_t> &projection, const ExprNode *pushed_filter,
    uint32_t num_splits = 1, uint64_t coalesce_distance = 1 << 10, uint64_t max_region_bytes = 0);

} // namespace plume::catalog
