#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/remote_resolver.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/expression/expression.hpp"

#include "duckdb/main/connection.hpp"

#include <cstdint>
#include <vector>

namespace plume::catalog {

struct LocalTableDataSource : public DataSource {
    LocalTableDataSource() : DataSource(DataSourceType::LOCAL_TABLE) {}
    Result<void> Resolve(const RemoteResolver &resolver) override { return Ok(); }
};

std::shared_ptr<DataSource> CreateTableSource(std::string name, Schema schema);

Result<dandelion::DataItemVec> MaterializeTable(duckdb::Connection &con, const LocalTableDataSource &src,
    const std::vector<uint32_t> *projection, const ExprNode *pushed_filter);

Result<void> LoadDataFile(duckdb::Connection &con, const std::string &table_name,
    const std::vector<std::string> &paths);

} // namespace plume::catalog
