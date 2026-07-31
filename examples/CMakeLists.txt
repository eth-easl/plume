# End-to-end example. Uses only the core engine (no DuckDB instance).
add_executable(plume_example end_to_end.cpp)
target_link_libraries(plume_example PRIVATE plume)
