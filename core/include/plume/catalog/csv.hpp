#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"
#include "plume/csv/csv.hpp"
#include "plume/dandelion/api.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace plume::catalog {

struct RemoteCSVDataSource : public DataSource {
    csv::CSVOptions csv_options;
    uint64_t total_size = 0;
    std::vector<uint64_t> file_sizes;

    RemoteCSVDataSource() : DataSource(DataSourceType::REMOTE_CSV) {}
    Result<void> Resolve(const RemoteResolver &resolver) override;
};

std::shared_ptr<DataSource> CreateRemoteCSVSource(std::string name, std::vector<std::string> urls,
    csv::CSVOptions opt = {});

struct CSVStageInputs {
    dandelion::DataItemVec chunk_info;
    dandelion::DataItemVec chunk_reqs;
};

Result<CSVStageInputs> BuildCSVStageInputs(const RemoteCSVDataSource &src,
    const std::vector<uint32_t> &projection, uint32_t num_splits = 1, uint64_t max_region_bytes = 0);

} // namespace plume::catalog
