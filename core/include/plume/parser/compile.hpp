#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/dandelion/composition.hpp"
#include "plume/parser/converter.hpp"

#include <string>

namespace duckdb {
class Connection;
} // namespace duckdb

namespace plume::parser {

// Everything needed to invoke a compiled query as a dandelion composition: the
// physical plan, its composition DSL + per-stage templates, and every base-
// table/remote source's materialized input data, in composition order (parallel
// to composition.table_inputs / composition.remote_inputs — see BuildDandelionComposition,
// which builds both by the same walk over plan.stages).
struct CompiledComposition {
    PhysicalPlan plan;
    dandelion::DandelionComposition composition;
    dandelion::DataSetVec table_blocks;
    dandelion::DataSetVec remote_info;
    dandelion::DataSetVec remote_requests;
};

// Compiles `sql` against `con` end to end:
//  1. rewrites bare file/URL table references (DetectFileSources) and any
//     plume_remote(...) calls already in the text (ResolveAndRewriteSources)
//     against `catalog`;
//  2. binds + converts the result into a PhysicalPlan (BuildPhysicalPlan);
//  3. builds its dandelion composition (dandelion::BuildDandelionComposition);
//  4. materializes every base table (read back out of `con`) and remote source's
//     (region/chunk-info + fetch-request) input data.
// `catalog` persists resolved remote sources across calls sharing the same `con`,
// so a source referenced by more than one query is only resolved/loaded once.
// `con` must have plume_remote registered (see RegisterPlumeRemote) if any query
// compiled against it might reference a file/URL source.
Result<CompiledComposition> CompileQuery(duckdb::Connection &con, catalog::SourceCatalog &catalog, const std::string &sql,
    const std::string &name, const ConverterConfig &config = {});

} // namespace plume::parser
