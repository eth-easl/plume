#include "plume/parser/compile.hpp"

#include "plume/catalog/data_fetcher.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/catalog/plume_remote.hpp"
#include "plume/catalog/remote_resolver.hpp"
#include "plume/catalog/source_scan.hpp"
#include "plume/common/result.hpp"

#include "duckdb/main/connection.hpp"

#include <map>
#include <set>
#include <vector>

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

    struct PageIndexRequest {
        std::set<uint32_t> key_cols;
        std::set<uint32_t> read_cols;
        bool all_cols = false;
    };
    std::map<catalog::RemoteParquetDataSource *, PageIndexRequest> page_index_requests;
    for (auto &stage : out.plan->stages) {
        if (!stage->IsLeaf() || !stage->PrunesByDynFilter()) {
            continue;
        }
        auto &leaf = static_cast<LeafStage &>(*stage);
        if (leaf.data_source->type != catalog::DataSourceType::REMOTE_PARQUET) {
            continue;
        }

        auto &req = page_index_requests[static_cast<catalog::RemoteParquetDataSource *>(leaf.data_source.get())];
        for (const auto &in : leaf.dynamic_filter_inputs) {
            if (in.prune) {
                req.key_cols.insert(in.file_column);
            }
        }
        if (!leaf.projection || leaf.projection->empty()) {
            req.all_cols = true;
        } else {
            req.read_cols.insert(leaf.projection->begin(), leaf.projection->end());
        }
    }

    for (auto &[src, req] : page_index_requests) {
        std::vector<uint32_t> key_cols(req.key_cols.begin(), req.key_cols.end());
        std::vector<uint32_t> read_cols;
        if (!req.all_cols) {
            read_cols.assign(req.read_cols.begin(), req.read_cols.end());
        }
        src->ResolvePageIndexes(resolver, key_cols, read_cols);
    }
    TRYV(resolver.Execute());
    
    TRY(out.composition, dandelion::BuildDandelionComposition(con, *out.plan, name, config));
    return out;
}

} // namespace plume::parser
