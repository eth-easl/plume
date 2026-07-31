# ---------------------------------------------------------------------------
#  PLUME UBENCH — micro-benchmark tool
#
#  Two independently-linked pieces, neither linking plume_client:
#
#   - ubench_export : links core's SQL-compile layer (plume::client::CompileQuery,
#                     a real DuckDB instance). Compiles SQL into a self-contained
#                     `.ub` plan. This is the only ubench piece that depends on a
#                     DuckDB instance — and, since that compile layer (+ the cpr
#                     it needs) is only compiled into `plume` when
#                     PLUME_WITH_COMPILE_LAYER is on, the only piece gated on it.
#   - ubench_run    : standalone runner. Replays a `.ub` plan through the real
#                     function engine on a pinned thread pool, timing it and
#                     emitting a per-operator trace. Links a *traced* recompile of
#                     just the core v1 engine + function-stage sources
#                     (ubench_engine) and its own thread-local host-ABI backend —
#                     no SQL compilation, no DuckDB instance, no cpr — so it
#                     builds regardless of PLUME_WITH_COMPILE_LAYER.
#
#  ubench_engine recompiles the core engine sources (PLUME_CORE_ENGINE_SOURCES —
#  memory/expression/execution/csv/parquet-codec/common; deliberately not the
#  catalog/dandelion/parser sources, which compile SQL and need a DuckDB
#  Connection + cpr + nlohmann_json that a replay-only engine has no use for) plus
#  the function-stage sources, with -DPLUME_UBENCH_TRACE, so the timing
#  instrumentation is present only here. The definition is PUBLIC so the runner's
#  own TUs (which call the trace control API) are built with it too.
# ---------------------------------------------------------------------------

# --- Traced engine (core engine + function stages, recompiled with tracing) --

# The function-stage sources the runner needs to run every stage kind (the union
# of the plume_stage / plume_csv_stage / plume_pq_stage source lists). Parquet
# (de)serialization, decode, and encode all live in core (PLUME_CORE_ENGINE_SOURCES
# already carries them); only the function-stage glue is functions-only.
set(UBENCH_FUNCTION_SOURCES
  ${CMAKE_SOURCE_DIR}/functions/src/functions/stage.cpp
  ${CMAKE_SOURCE_DIR}/functions/src/functions/csv_stage.cpp
  ${CMAKE_SOURCE_DIR}/functions/src/functions/pq_stage.cpp
  ${CMAKE_SOURCE_DIR}/functions/src/functions/output_split.cpp)

# An OBJECT library so every object is linked unconditionally (the fn <-> abi
# back-references never hit archive-ordering issues).
add_library(ubench_engine OBJECT ${PLUME_CORE_ENGINE_SOURCES} ${UBENCH_FUNCTION_SOURCES})
target_compile_definitions(ubench_engine PUBLIC PLUME_UBENCH_TRACE)
target_compile_definitions(ubench_engine PRIVATE PLUME_VERBOSE=0) # silence per-stage allocator prints
if(PLUME_RECYCLING_ALLOCATOR)
  target_compile_definitions(ubench_engine PRIVATE PLUME_RECYCLING_ALLOCATOR)
endif()
target_include_directories(ubench_engine PUBLIC
  ${PLUME_CORE_INCLUDE}
  ${CMAKE_SOURCE_DIR}/functions/include)
target_link_libraries(ubench_engine PUBLIC plume_duckdb plume_snappy)

# --- Runner (standalone) -----------------------------------------------------

add_executable(ubench_run
  src/run_main.cpp
  src/runner/stage_runner.cpp
  src/runner/thread_pool.cpp
  src/runner/fetch.cpp
  src/runner/trace_writer.cpp
  src/runner/abi_ubench.cpp
  src/runner/config.cpp
  src/format/plan.cpp)
target_include_directories(ubench_run PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(ubench_run PRIVATE ubench_engine nlohmann_json::nlohmann_json Threads::Threads)

# --- Exporter (DuckDB-instance-linked; needs core's SQL-compile layer) -------

# plume::client::CompileQuery spins up a DuckDB instance and DetectFileSources'
# LoadDataFile can read a local .parquet via the parquet extension, so — like the
# client's own binaries — link the parquet extension + DuckDB archives directly
# and compile the extension-loader glue TU in. `plume` only carries the
# SQL-compile layer this needs (and the cpr it in turn needs) when
# PLUME_WITH_COMPILE_LAYER is on (see core/CMakeLists.txt), so skip this target
# entirely otherwise rather than fail to link cpr::cpr/catalog symbols.
if(PLUME_WITH_COMPILE_LAYER)
  set(UBENCH_PARQUET_EXT_LIB "${CMAKE_SOURCE_DIR}/build/duckdb/extension/parquet/libparquet_extension.a")
  if(NOT EXISTS "${UBENCH_PARQUET_EXT_LIB}")
    message(FATAL_ERROR "Missing parquet extension archive:\n  ${UBENCH_PARQUET_EXT_LIB}\n"
                        "Build it with: ninja -C build/duckdb parquet_extension")
  endif()

  add_executable(ubench_export
    src/export_main.cpp
    src/format/plan.cpp
    ${CMAKE_SOURCE_DIR}/client/src/duckdb/duckdb_glue.cpp)
  target_include_directories(ubench_export PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/functions/include
    ${CMAKE_SOURCE_DIR}/external/duckdb/extension/parquet/include
    ${CMAKE_SOURCE_DIR}/external/duckdb/third_party/parquet
    ${CMAKE_SOURCE_DIR}/external/duckdb/third_party/thrift)
  target_link_libraries(ubench_export PRIVATE
    plume
    nlohmann_json::nlohmann_json
    -Wl,--start-group
    ${UBENCH_PARQUET_EXT_LIB} ${DUCKDB_CORE_FUNCTIONS_LIB} ${DUCKDB_STATIC_LIB}
    -Wl,--end-group)
else()
  message(STATUS "Skipping ubench_export: needs PLUME_WITH_COMPILE_LAYER=ON (or PLUME_BUILD_CLIENT=ON) for core's SQL-compile layer")
endif()
