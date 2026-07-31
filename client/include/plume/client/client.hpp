#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/dandelion/composition.hpp"
#include "plume/parser/compile.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <memory>
#include <string>

namespace plume::client {

struct QueryResponse {
    dandelion::BinaryData data;
    Schema schema;

    std::string ToString();
};
    
struct CompiledQuery {
    dandelion::DandelionComposition composition;
    dandelion::DataSetVec table_blocks;
    dandelion::DataSetVec remote_info;
    dandelion::DataSetVec remote_requests;
    Schema output_schema;

    Result<dandelion::BinaryData> Request();
    std::string ResultToString(const dandelion::BinaryData& data);
};

struct ExecutionConfig {
    std::string dandelion_url;
    uint32_t timeout_secs;
};

class Client {
public:
    explicit Client(parser::ConverterConfig converter_cfg);

    Result<CompiledQuery> Resolve(const std::string &sql, const std::string &query_name = "Query");

    Result<QueryResponse> Execute(const std::string &sql, const ExecutionConfig &exec_cfg, const std::string &query_name = "Query");

private:
    duckdb::DuckDB db_;
    duckdb::Connection con_;
    // Shared (not owned solely by Client) because the plume_remote table function
    // registered on con_ holds its own reference, resolved at bind time (i.e. while
    // DuckDB is parsing/binding a query, after ResolveAndRewriteSources has already
    // populated it for that query).
    std::shared_ptr<catalog::SourceCatalog> catalog_;

    parser::ConverterConfig converter_cfg_;
};
    
} // namespace plume::client
