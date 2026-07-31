#pragma once

#include "plume/catalog/catalog.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"

#include <memory>

namespace plume::catalog {

inline constexpr const char *kPlumeRemoteName = "plume_remote";

struct PlumeRemoteInfo : duckdb::TableFunctionInfo {
    std::shared_ptr<SourceCatalog> catalog;
};

struct PlumeRemoteBindData : duckdb::FunctionData {
    std::shared_ptr<DataSource> info;

    duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
    bool Equals(const duckdb::FunctionData &other) const override;
};

Result<std::string> ResolveAndRewriteSources(const std::string &sql, SourceCatalog &catalog, RemoteResolver &resolver);

void RegisterPlumeRemote(duckdb::Connection &con, duckdb::shared_ptr<PlumeRemoteInfo> registry);

} // namespace plume::catalog
