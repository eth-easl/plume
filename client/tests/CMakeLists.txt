# ---------------------------------------------------------------------------
# PLUME CLIENT - TESTS
#
# plume_link_parquet() (from ../CMakeLists.txt) adds duckdb_glue.cpp as a
# direct source and links the parquet + duckdb archives. Call it on every
# test target that creates a DuckDB instance (all of them here).
# ---------------------------------------------------------------------------

# v3 end-to-end: SQL -> multi-stage plan -> in-process mock dandelion runtime
# (invokes the real v2 function logic per stage) -> compare to DuckDB. Needs the
# function sources + the host-ABI mock (plume_mock), so it is only built when the
# functions project + its test mock are available.
set(FN_SRC ${CMAKE_SOURCE_DIR}/functions/src)
set(MOCK_RUNTIME_FN_SRC
  mock_runtime.cpp
  # plume_stage (BLOCKS / join stages)
  ${FN_SRC}/functions/stage.cpp
  ${FN_SRC}/functions/output_split.cpp
  # csv source stages
  ${FN_SRC}/functions/csv_prepare.cpp
  ${FN_SRC}/functions/csv_stage.cpp
  # parquet source stages
  ${FN_SRC}/functions/pq_prepare.cpp
  ${FN_SRC}/functions/pq_stage.cpp)

if(TARGET plume_mock)
  add_executable(test_plan_e2e test_plan_e2e.cpp ${MOCK_RUNTIME_FN_SRC})
  target_link_libraries(test_plan_e2e PRIVATE plume_client plume_mock plume_test_support)
  plume_link_parquet(test_plan_e2e) # duckdb_glue now registers the parquet extension
  add_test(NAME test_plan_e2e COMMAND test_plan_e2e)
endif()

# External-readability check for the parquet writer: reads Plume's output with a
# real DuckDB instance + the parquet extension. Only built when that extension
# archive has been built (setup.sh builds only core_functions by default):
#   ninja -C build/duckdb parquet_extension
set(PLUME_PARQUET_EXT_LIB "${CMAKE_SOURCE_DIR}/build/duckdb/extension/parquet/libparquet_extension.a")
if(EXISTS "${PLUME_PARQUET_EXT_LIB}")
  add_executable(test_parquet_external test_parquet_external.cpp)
  target_link_libraries(test_parquet_external PRIVATE plume_client plume_test_support)
  plume_link_parquet(test_parquet_external)
  add_test(NAME test_parquet_external COMMAND test_parquet_external)
endif()
