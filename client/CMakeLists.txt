# ---------------------------------------------------------------------------
#  PLUME CLIENT
#
#  Binds real SQL with a DuckDB instance and emits a Plume pipeline. 
#  Links the full DuckDB archive (an instance is allowed here).
# ---------------------------------------------------------------------------

# The DuckDB parquet extension — linked + registered ONLY by the client.
set(PLUME_PARQUET_EXT_LIB "${CMAKE_SOURCE_DIR}/build/duckdb/extension/parquet/libparquet_extension.a")
if(NOT EXISTS "${PLUME_PARQUET_EXT_LIB}")
  message(FATAL_ERROR "Missing parquet extension archive:\n  ${PLUME_PARQUET_EXT_LIB}\n"
                      "Build it with: ninja -C build/duckdb parquet_extension")
endif()
set(PLUME_PARQUET_INCLUDES
  ${CMAKE_SOURCE_DIR}/external/duckdb/extension/parquet/include
  ${CMAKE_SOURCE_DIR}/external/duckdb/third_party/parquet
  ${CMAKE_SOURCE_DIR}/external/duckdb/third_party/thrift)

# Path to the duckdb_glue.cpp that provides ExtensionHelper::LoadAllExtensions.
set(PLUME_DUCKDB_GLUE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/duckdb/duckdb_glue.cpp")

# Apply to a client target that needs a DuckDB instance: adds duckdb_glue.cpp
# as a direct source, the parquet_extension.hpp include path, and the parquet +
# duckdb archives in one group (they reference each other's symbols).
function(plume_link_parquet target)
  target_sources(${target} PRIVATE ${PLUME_DUCKDB_GLUE_SRC})
  target_include_directories(${target} PRIVATE ${PLUME_PARQUET_INCLUDES})
  target_link_libraries(${target} PRIVATE
    -Wl,--start-group
    ${PLUME_PARQUET_EXT_LIB} ${DUCKDB_CORE_FUNCTIONS_LIB} ${DUCKDB_STATIC_LIB}
    -Wl,--end-group)
endfunction()

add_library(plume_client STATIC
  src/client.cpp
)
target_include_directories(plume_client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(plume_client PUBLIC plume cpr::cpr)
target_link_libraries(plume_client PRIVATE nlohmann_json::nlohmann_json)


# ---------------------------------------------------------------------------
#  Executables
# ---------------------------------------------------------------------------

# plume_export — compile a query and dump the composition + config/data buffers.
add_executable(plume_export src/export_main.cpp)
target_link_libraries(plume_export PRIVATE plume_client nlohmann_json::nlohmann_json)
plume_link_parquet(plume_export)

# Plume — compile a query, invoke it on a dandelion instance, show the results.
# (The CMake target is `plume_run` to avoid clashing with the `plume` library
# target; the emitted binary is still named `plume`.)
add_executable(plume_run src/run_main.cpp)
set_target_properties(plume_run PROPERTIES OUTPUT_NAME plume)
target_link_libraries(plume_run PRIVATE plume_client) # cpr comes transitively via plume_client
plume_link_parquet(plume_run)


# ---------------------------------------------------------------------------
#  Tests
# ---------------------------------------------------------------------------

if(PLUME_WITH_TESTS)
  add_subdirectory(tests)
endif()
