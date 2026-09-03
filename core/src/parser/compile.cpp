#include "plume/parser/compile.hpp"

#include "plume/catalog/data_fetcher.hpp"
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
    TRY(out.composition, dandelion::BuildDandelionComposition(con, *out.plan, name, config));

    return out;
}

} // namespace plume::parser
