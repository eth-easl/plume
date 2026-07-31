add_executable(plume_bench
  src/main.cpp
  src/config.cpp
  src/trace.cpp
  src/checksum.cpp
  src/runner.cpp
  ${CMAKE_SOURCE_DIR}/core/src/duckdb/glue_core_functions.cpp)

target_include_directories(plume_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(plume_bench PRIVATE plume cpr::cpr nlohmann_json::nlohmann_json)
target_link_options(plume_bench PRIVATE -static-libgcc -static-libstdc++)
