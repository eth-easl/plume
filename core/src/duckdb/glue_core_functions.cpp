// Registers the statically linked core_functions extension (SUM/AVG/...) for
// targets that open a DuckDB instance but do not need parquet.

#include "duckdb_static_extension.h"

extern "C" int32_t duckdb_extension_core_functions_describe(duckdb_extension_descriptor *descriptor);

namespace {
struct PlumeCoreFunctionsRegistration {
    PlumeCoreFunctionsRegistration() {
        duckdb_register_static_extension(duckdb_extension_core_functions_describe);
    }
};
const PlumeCoreFunctionsRegistration plume_core_functions_registration;
} // namespace
