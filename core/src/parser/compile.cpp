#include "plume/parser/compile.hpp"

#include "plume/catalog/data_fetcher.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/catalog/plume_remote.hpp"
#include "plume/catalog/remote_resolver.hpp"
#include "plume/catalog/source_scan.hpp"
#include "plume/common/result.hpp"

#include "duckdb/main/connection.hpp"

namespace plume::parser {

using catalog::AutoFetcher;
using catalog::DetectFileSources;
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

    // pq_prepare selects pages by the dynamic filter if it has the page indexes.
    for (auto &stage : out.plan->stages) {
        if (!stage->IsLeaf()) {
            continue;
        }
        auto &leaf = static_cast<LeafStage &>(*stage);
        if (leaf.HasDynFilter() && leaf.dynamic_filter_prune &&
            leaf.data_source->type == catalog::DataSourceType::REMOTE_PARQUET) {
            catalog::FetchPageIndexes(static_cast<catalog::RemoteParquetDataSource &>(*leaf.data_source), resolver,
                                      static_cast<uint32_t>(leaf.dynamic_filter_column),
                                      leaf.projection ? *leaf.projection : std::vector<uint32_t>{});
        }
    }
    TRYV(resolver.Execute());
    TRY(out.composition, dandelion::BuildDandelionComposition(con, *out.plan, name, config));

    return out;
}

} // namespace plume::parser
