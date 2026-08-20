#include "plume/parser/compile.hpp"

#include "plume/catalog/csv.hpp"
#include "plume/catalog/data_fetcher.hpp"
#include "plume/catalog/local.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/catalog/plume_remote.hpp"
#include "plume/catalog/remote_resolver.hpp"
#include "plume/catalog/source_scan.hpp"
#include "plume/common/result.hpp"

#include "duckdb/main/connection.hpp"

#include <memory>

namespace plume::parser {

using catalog::AutoFetcher;
using catalog::BuildCSVStageInputs;
using catalog::BuildParquetStageInputs;
using catalog::DataSourceType;
using catalog::DetectFileSources;
using catalog::LocalTableDataSource;
using catalog::MaterializeTable;
using catalog::RemoteCSVDataSource;
using catalog::RemoteParquetDataSource;
using catalog::RemoteResolver;
using catalog::ResolveAndRewriteSources;
using catalog::SourceCatalog;

Result<CompiledComposition> CompileQuery(duckdb::Connection &con, SourceCatalog &catalog, const std::string &sql,
        const std::string &name, const ConverterConfig &config, size_t fetcher_threads) {
    TRY(std::string detected_sql, DetectFileSources(con, sql));

    RemoteResolver resolver(AutoFetcher(), fetcher_threads);
    TRY(std::string resolved_sql, ResolveAndRewriteSources(detected_sql, catalog, resolver));

    CompiledComposition out;
    TRY(out.plan, BuildPhysicalPlan(con, resolved_sql, catalog, config));
    TRY(out.composition, dandelion::BuildDandelionComposition(*out.plan, name));

    out.table_blocks.reserve(out.composition.table_inputs.size());
    for (const auto &ti : out.composition.table_inputs) {
        auto table_source = std::static_pointer_cast<LocalTableDataSource>(ti.source);
        TRY(auto materialized, MaterializeTable(con, *table_source, ti.projection.get(), ti.pushed_filter.get()));
        out.table_blocks.push_back(std::move(materialized));
    }

    out.remote_info.reserve(out.composition.remote_inputs.size());
    out.remote_requests.reserve(out.composition.remote_inputs.size());
    for (const auto &ri : out.composition.remote_inputs) {
        const std::vector<uint32_t> projection = ri.projection ? *ri.projection : std::vector<uint32_t>{};
        if (ri.source->type == DataSourceType::REMOTE_PARQUET) {
            auto pq_src = std::static_pointer_cast<RemoteParquetDataSource>(ri.source);
            TRY(auto pq_stage_inputs, BuildParquetStageInputs(*pq_src, projection, ri.pushed_filter.get(),
                                                              ri.target_splits, config.coalesce_distance,
                                                              config.max_region_bytes));
            out.remote_info.push_back(std::move(pq_stage_inputs.region_info));
            out.remote_requests.push_back(std::move(pq_stage_inputs.chunk_reqs));
        } else {
            auto csv_src = std::static_pointer_cast<RemoteCSVDataSource>(ri.source);
            TRY(auto csv_stage_inputs,
                BuildCSVStageInputs(*csv_src, projection, ri.target_splits, config.max_region_bytes));
            out.remote_info.push_back(std::move(csv_stage_inputs.chunk_info));
            out.remote_requests.push_back(std::move(csv_stage_inputs.chunk_reqs));
        }
    }

    return out;
}

} // namespace plume::parser
