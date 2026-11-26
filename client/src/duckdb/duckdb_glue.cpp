// Registers the statically linked DuckDB extensions the client needs (core functions + parquet)

#include "duckdb_static_extension.h"

extern "C" {
int32_t duckdb_extension_core_functions_describe(duckdb_extension_descriptor *descriptor);
int32_t duckdb_extension_parquet_describe(duckdb_extension_descriptor *descriptor);
}

namespace {
struct PlumeStaticExtensionRegistration {
    PlumeStaticExtensionRegistration() {
        // Failures are recorded by the registry and surface as an exception
        // when the first database opens.
        duckdb_register_static_extension(duckdb_extension_core_functions_describe);
        duckdb_register_static_extension(duckdb_extension_parquet_describe);
    }
};
const PlumeStaticExtensionRegistration plume_static_extension_registration;
} // namespace
