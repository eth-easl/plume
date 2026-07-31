// Link/runtime shims for the Dandelion target. These satisfy symbols that the
// host toolchain provides on linux but the Dandelion runtime/libc do not. They
// are compiled only for the Dandelion build (see functions/CMakeLists.txt).

#include "duckdb/main/extension_helper.hpp"

// ---------------------------------------------------------------------------
// __cxa_thread_atexit
//   DuckDB has thread_local state with non-trivial destructors (e.g. the
//   BlockAllocator thread-local cache). The C++ runtime registers those dtors
//   via __cxa_thread_atexit, which the Dandelion runtime does not provide.
//   Functions are single-shot and the sandbox reclaims all memory on teardown,
//   so skipping thread-local destruction is safe: a no-op shim is enough.
// ---------------------------------------------------------------------------
extern "C" int __cxa_thread_atexit(void (*func)(void *), void *obj, void *dso_symbol) {
    (void)func;
    (void)obj;
    (void)dso_symbol;
    return 0;
}

// ---------------------------------------------------------------------------
// DuckDB static extension loader
//   We build DuckDB with DISABLE_EXTENSION_LOAD and link no static extensions,
//   so DuckDB::DuckDB() still references ExtensionHelper::LoadAllExtensions.
//   This mirrors DuckDB's own dummy_static_extension_loader.cpp ("link this to
//   libduckdb_static.a to get a working system"); Plume never actually
//   constructs a DuckDB instance, so these are link-satisfying no-ops.
// ---------------------------------------------------------------------------
namespace duckdb {

void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
    // nop
}

vector<string> ExtensionHelper::LoadedExtensionTestPaths() {
    return {};
}

} // namespace duckdb
