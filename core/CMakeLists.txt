# ---------------------------------------------------------------------------
#  PLUME CORE
#
#  The Plume query engine (common, memory, expression, execution, csv, parquet).
#  Depended on by both the client and the functions.
# ---------------------------------------------------------------------------

# The instance-free execution engine.
set(PLUME_CORE_ENGINE_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/common/trace.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/common/types.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/csv/csv.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/dandelion/s3.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/operators/aggregate.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/operators/filter.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/operators/join.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/operators/limit.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/operators/output.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/operators/projection.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/operators/sort.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/operators/top_n.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/executor.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/operator.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/pipeline.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/execution/sink.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/expression/expression_builder.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/expression/expression.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/expression/function_registry.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/memory/adapter.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/memory/allocator.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/decoder.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/encoder.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/metadata.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/parquet.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/prune.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/region.cpp
)
set(PLUME_CORE_ENGINE_SOURCES ${PLUME_CORE_ENGINE_SOURCES} PARENT_SCOPE)

# The SQL-compile layer.
set(PLUME_CORE_COMPILE_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/catalog/catalog.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/catalog/csv.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/catalog/data_fetcher.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/catalog/local.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/catalog/parquet.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/catalog/plume_remote.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/catalog/remote_resolver.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/catalog/source_scan.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/dandelion/composition.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parser/compile.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parser/converter.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parser/expression_translator.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parser/physical_plan.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parser/plan_builder.cpp
)

set(PLUME_CORE_SOURCES ${PLUME_CORE_ENGINE_SOURCES})
if(PLUME_WITH_COMPILE_LAYER)
  list(APPEND PLUME_CORE_SOURCES ${PLUME_CORE_COMPILE_SOURCES})
endif()
set(PLUME_CORE_SOURCES ${PLUME_CORE_SOURCES} PARENT_SCOPE)
set(PLUME_CORE_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/include PARENT_SCOPE)

# Vendored Snappy (from the pinned DuckDB tree), for parquet page compression
# (decoder.cpp/encoder.cpp only). Built into the DuckDB parquet extension, which Plume does not 
# link, so compile the few sources directly instead.
add_library(plume_snappy STATIC
  ${DUCKDB_DIR}/third_party/snappy/snappy.cc
  ${DUCKDB_DIR}/third_party/snappy/snappy-sinksource.cc
  ${DUCKDB_DIR}/third_party/snappy/snappy-stubs-internal.cc
)
target_include_directories(plume_snappy SYSTEM PUBLIC ${DUCKDB_DIR}/third_party/snappy)

add_library(plume STATIC ${PLUME_CORE_SOURCES})
target_include_directories(plume PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(plume PUBLIC plume_duckdb)
target_link_libraries(plume PRIVATE plume_snappy)
if(PLUME_WITH_COMPILE_LAYER)
  target_link_libraries(plume PRIVATE cpr::cpr nlohmann_json::nlohmann_json)
endif()
if(PLUME_RECYCLING_ALLOCATOR)
  target_compile_definitions(plume PRIVATE PLUME_RECYCLING_ALLOCATOR)
endif()

# Header-only test scaffolding (test_util.hpp), shared by every project's tests.
add_library(plume_test_support INTERFACE)
target_include_directories(plume_test_support INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/tests)

# ---------------------------------------------------------------------------
#  Tests
# ---------------------------------------------------------------------------

if(PLUME_WITH_TESTS)
  add_subdirectory(tests)
endif()
