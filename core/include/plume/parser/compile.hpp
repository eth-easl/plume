#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"
#include "plume/dandelion/composition.hpp"
#include "plume/parser/converter.hpp"

#include <string>

namespace duckdb {
class Connection;
} // namespace duckdb

namespace plume::parser {

struct CompiledComposition {
    std::shared_ptr<PhysicalPlan> plan;
    dandelion::DandelionComposition composition;
};

Result<CompiledComposition> CompileQuery(duckdb::Connection &con, catalog::SourceCatalog &catalog, const std::string &sql,
    const std::string &name, const ConverterConfig &config = {}, size_t fetcher_threads = 10);

} // namespace plume::parser
