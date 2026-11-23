add_executable(plume_bench
  src/main.cpp
  src/config.cpp
  src/trace.cpp
  src/dandelion_trace.cpp
  src/checksum.cpp
  src/runner.cpp
  ${CMAKE_SOURCE_DIR}/core/src/duckdb/glue_core_functions.cpp)

# zlib gzip-compresses the exported traces (.plmt.gz). Reuses the zlib that curl (via cpr) links.
find_package(ZLIB REQUIRED)

target_include_directories(plume_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(plume_bench PRIVATE plume cpr::cpr nlohmann_json::nlohmann_json ZLIB::ZLIB)
target_link_options(plume_bench PRIVATE -static-libgcc -static-libstdc++)
set_target_properties(plume_bench PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/..)
