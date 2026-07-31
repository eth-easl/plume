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
    const std::vector<uint32_t> &projection, const ExprNode *pushed_filter);

// Loads a local CSV/TSV/parquet file (or several, concatenated) into `con` as a
// real table named `table_name` (CREATE TABLE ... AS SELECT * FROM read_csv_auto/
// read_parquet(...)), so the rest of the compile pipeline sees a normal base
// table. Reading .parquet requires the caller to have registered DuckDB's parquet
// extension on `con` (see client/src/duckdb/duckdb_glue.cpp).
Result<void> LoadDataFile(duckdb::Connection &con, const std::string &table_name,
    const std::vector<std::string> &paths);

} // namespace plume::catalog
