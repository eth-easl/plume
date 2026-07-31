# ---------------------------------------------------------------------------
# PLUME FUNCTIONS - TESTS
#
# Unlike the executables (which link the linux/dandelion ABI backend), tests
# link the in-process mock so they can set inputs and inspect outputs in 
# memory. Each test compiles the same per-function sources the corresponding 
# executable does.
# ---------------------------------------------------------------------------

set(FN_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../src)

# ---------------------------------------------------------------------------
#  Mocking
# ---------------------------------------------------------------------------
# Mock host ABI: provides plume::abi::* + a plume::mock control surface.
add_library(plume_mock STATIC mock/abi_mock.cpp)
target_include_directories(plume_mock PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/mock
  ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(plume_mock PUBLIC plume)

# ---------------------------------------------------------------------------
#  Tests
# ---------------------------------------------------------------------------
# A function test: per-function sources + the mock backend (instead of linux).
function(plume_add_function_test name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE plume plume_mock plume_test_support)
  target_include_directories(${name} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${CMAKE_CURRENT_SOURCE_DIR})
  add_test(NAME ${name} COMMAND ${name})
endfunction()

plume_add_function_test(test_stage
  test_stage.cpp
  ${FN_SRC}/functions/stage.cpp
  ${FN_SRC}/functions/output_split.cpp)

plume_add_function_test(test_join
  test_join.cpp
  ${FN_SRC}/functions/stage.cpp
  ${FN_SRC}/functions/output_split.cpp)

plume_add_function_test(test_csv
  test_csv.cpp
  ${FN_SRC}/functions/csv_prepare.cpp
  ${FN_SRC}/functions/csv_stage.cpp
  ${FN_SRC}/functions/output_split.cpp)

plume_add_function_test(test_pq_prepare
  test_pq_prepare.cpp
  ${FN_SRC}/functions/pq_prepare.cpp)
target_compile_definitions(test_pq_prepare PRIVATE
  PLUME_TEST_PARQUET="${CMAKE_SOURCE_DIR}/external/duckdb/data/parquet-testing/bug687_nulls.parquet")

plume_add_function_test(test_pq_stage
  test_pq_stage.cpp
  ${FN_SRC}/functions/pq_stage.cpp
  ${FN_SRC}/functions/output_split.cpp)
target_link_libraries(test_pq_stage PRIVATE plume_snappy) # parquet_encode.hpp hand-encodes Snappy fixtures

# Decode microbenchmark (not a ctest; run manually against a parquet file).
add_executable(bench_pq_decode bench_pq_decode.cpp)
target_link_libraries(bench_pq_decode PRIVATE plume)
target_include_directories(bench_pq_decode PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# Stage output-split microbenchmark (old vs new split over decoded parquet).
add_executable(bench_pq_stage
  bench_pq_stage.cpp
  ${FN_SRC}/functions/output_split.cpp)
target_link_libraries(bench_pq_stage PRIVATE plume plume_mock)
target_include_directories(bench_pq_stage PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
  ${CMAKE_CURRENT_SOURCE_DIR})

# Single-core DuckDB scan baseline. duckdb_glue.cpp defines LoadAllExtensions to
# register core_functions + parquet (so read_parquet works); linked as a direct
# object, then the extension archives in a group.
add_executable(bench_duckdb_scan
  bench_duckdb_scan.cpp
  ${CMAKE_SOURCE_DIR}/client/src/duckdb/duckdb_glue.cpp)
target_include_directories(bench_duckdb_scan PRIVATE
  ${CMAKE_SOURCE_DIR}/external/duckdb/extension/parquet/include
  ${CMAKE_SOURCE_DIR}/external/duckdb/extension/core_functions/include)
target_link_libraries(bench_duckdb_scan PRIVATE plume
  -Wl,--start-group
  ${CMAKE_SOURCE_DIR}/build/duckdb/extension/parquet/libparquet_extension.a
  ${CMAKE_SOURCE_DIR}/build/duckdb/extension/core_functions/libcore_functions_extension.a
  ${CMAKE_SOURCE_DIR}/build/duckdb/src/libduckdb_static.a
  -Wl,--end-group)

plume_add_function_test(test_pq_e2e
  test_pq_e2e.cpp
  ${FN_SRC}/functions/pq_prepare.cpp
  ${FN_SRC}/functions/pq_stage.cpp
  ${FN_SRC}/functions/output_split.cpp)
target_link_libraries(test_pq_e2e PRIVATE plume_snappy) # parquet_encode.hpp hand-encodes Snappy fixtures

plume_add_function_test(test_file_output
  test_file_output.cpp
  ${FN_SRC}/functions/stage.cpp
  ${FN_SRC}/functions/output_split.cpp)
