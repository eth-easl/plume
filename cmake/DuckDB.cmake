# ---------------------------------------------------------------------------
#  DuckDB v1.5.4 (pinned)
#   We link against the full duckdb_static archive; the linker selects only
#   the translation units the kernels actually need. Exposed to the rest of the
#   build as the INTERFACE target `plume_duckdb` (include dirs + archives).
# ---------------------------------------------------------------------------
set(DUCKDB_DIR "${CMAKE_SOURCE_DIR}/external/duckdb" CACHE PATH "DuckDB checkout")

# Directory holding the prebuilt DuckDB archives. Defaults to the native
# build/duckdb tree; cross builds (e.g. Dandelion) override this to point at
# their own archive dir (build_dandelion/duckdb).
set(DUCKDB_BUILD_DIR "${CMAKE_SOURCE_DIR}/build/duckdb"
    CACHE PATH "DuckDB build dir holding the prebuilt archives")

set(DUCKDB_STATIC_LIB "${DUCKDB_BUILD_DIR}/src/libduckdb_static.a"
    CACHE FILEPATH "Prebuilt duckdb_static archive")
# core_functions holds SUM/AVG (and other) aggregate/scalar functions.
set(DUCKDB_CORE_FUNCTIONS_LIB
    "${DUCKDB_BUILD_DIR}/extension/core_functions/libcore_functions_extension.a"
    CACHE FILEPATH "Prebuilt core_functions extension archive")
# duckdb_static declares ExtensionHelper::LoadAllExtensions (called from every
# DuckDB::DuckDB(...) construction) but does not define it -- DuckDB expects
# consumers to link either its own real extension-loading glue or a stub.
# This is the no-op stub (see external/duckdb/extension/loader/dummy_static_extension_loader.cpp),
# for consumers that only need the statically-linked archives above and never
# touch DuckDB's dynamic extension/catalog registration. The client links its
# own real implementation instead (client/src/duckdb/duckdb_glue.cpp) because
# it needs a live read_parquet, so this is NOT part of the shared
# plume_duckdb interface below -- link it explicitly where needed.
set(DUCKDB_DUMMY_EXTENSION_LOADER_LIB
    "${DUCKDB_BUILD_DIR}/extension/libdummy_static_extension_loader.a"
    CACHE FILEPATH "Prebuilt no-op ExtensionHelper::LoadAllExtensions stub")

set(DUCKDB_INCLUDE_DIRS
  ${DUCKDB_DIR}/src/include
  ${DUCKDB_DIR}/extension/core_functions/include
  ${DUCKDB_DIR}/third_party/fsst
  ${DUCKDB_DIR}/third_party/fmt/include
  ${DUCKDB_DIR}/third_party/hyperloglog
  ${DUCKDB_DIR}/third_party/fastpforlib
  ${DUCKDB_DIR}/third_party/skiplist
  ${DUCKDB_DIR}/third_party/ska_sort
  ${DUCKDB_DIR}/third_party/fast_float
  ${DUCKDB_DIR}/third_party/re2
  ${DUCKDB_DIR}/third_party/miniz
  ${DUCKDB_DIR}/third_party/utf8proc/include
  ${DUCKDB_DIR}/third_party/concurrentqueue
  ${DUCKDB_DIR}/third_party/pcg
  ${DUCKDB_DIR}/third_party/pdqsort
  ${DUCKDB_DIR}/third_party/tdigest
  ${DUCKDB_DIR}/third_party/mbedtls/include
  ${DUCKDB_DIR}/third_party/httplib
  ${DUCKDB_DIR}/third_party/jaro_winkler
  ${DUCKDB_DIR}/third_party/vergesort
  ${DUCKDB_DIR}/third_party/yyjson/include
  ${DUCKDB_DIR}/third_party/zstd/include
)

find_package(Threads REQUIRED)

add_library(plume_duckdb INTERFACE)
target_include_directories(plume_duckdb SYSTEM INTERFACE ${DUCKDB_INCLUDE_DIRS})
# --start-group handles the interdependency between core_functions and the main
# archive (each references symbols in the other).
target_link_libraries(plume_duckdb INTERFACE
  -Wl,--start-group ${DUCKDB_CORE_FUNCTIONS_LIB} ${DUCKDB_STATIC_LIB} -Wl,--end-group
  Threads::Threads ${CMAKE_DL_LIBS})
