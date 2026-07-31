#include "duckdb.hpp"

#include "duckdb/main/extension_helper.hpp"

#include "core_functions_extension.hpp"

namespace duckdb {

void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
    db.LoadStaticExtension<CoreFunctionsExtension>();
}

} // namespace duckdb
