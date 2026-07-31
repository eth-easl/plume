#pragma once

#include "plume/common/result.hpp"

#include <string>

namespace duckdb {
class Connection;
} // namespace duckdb

namespace plume::catalog {

// A query may reference its data as a file path / URL sitting where a table name
// would go (`FROM orders.csv`, `JOIN https://host/orders`, or a bracket-list of
// either) — DuckDB cannot bind those tokens directly. This scans `sql`'s FROM/JOIN
// clauses (recursing into derived-table subqueries) for such references and
// rewrites each one:
//  - a local file (.csv/.tsv/.parquet, or a bracket-list of them) is loaded into
//    `con` as a real table (see LoadDataFile) and the reference is rewritten to
//    that table's (generated) name;
//  - a remote URL (or bracket-list of URLs) is left unresolved here but wrapped in
//    a plume_remote(...) call, which a later ResolveAndRewriteSources pass
//    resolves (schema + statistics) and rewrites to its catalog alias.
// The same local path referenced more than once maps to one generated table.
// Quoted-string literals outside a table-ref position are left untouched.
Result<std::string> DetectFileSources(duckdb::Connection &con, const std::string &sql);

} // namespace plume::catalog
