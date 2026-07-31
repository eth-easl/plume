# ---------------------------------------------------------------------------
#  PLUME FUNCTIONS
#   
#  There is no bundled functions library: each executable is built directly from
#  only the sources it needs, plus the host ABI backend it links against. The
#  executables ship with the linux backend (src/abi/linux.cpp); the tests swap in
#  the in-process mock instead (see tests/CMakeLists.txt).
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#  Functions
# ---------------------------------------------------------------------------

if(PLUME_PLATFORM_DANDELION)
  # dandelion_runtime_shims.cpp adds link/runtime shims for symbols the Dandelion runtime and libc
  # do not provide.
  set(PLUME_ABI_BACKEND
    ${CMAKE_CURRENT_SOURCE_DIR}/src/abi/dandelion.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/abi/dandelion_runtime_shims.cpp)
else()
  set(PLUME_ABI_BACKEND ${CMAKE_CURRENT_SOURCE_DIR}/src/abi/linux.cpp)
endif()

function(plume_function name)
  add_executable(${name} ${ARGN} ${PLUME_ABI_BACKEND})
  target_link_libraries(${name} PRIVATE plume ${DUCKDB_DUMMY_EXTENSION_LOADER_LIB})
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
endfunction()

plume_function(plume_stage
  src/bin/plume_stage.cpp
  src/functions/stage.cpp
  src/functions/output_split.cpp)

plume_function(plume_csv_prepare
  src/bin/plume_csv_prepare.cpp
  src/functions/csv_prepare.cpp)

plume_function(plume_csv_stage
  src/bin/plume_csv_stage.cpp
  src/functions/csv_stage.cpp
  src/functions/output_split.cpp)

plume_function(plume_pq_prepare
  src/bin/plume_pq_prepare.cpp
  src/functions/pq_prepare.cpp)

plume_function(plume_pq_stage
  src/bin/plume_pq_stage.cpp
  src/functions/pq_stage.cpp
  src/functions/output_split.cpp)

# ---------------------------------------------------------------------------
#  Tests
# ---------------------------------------------------------------------------

if(PLUME_WITH_TESTS)
  add_subdirectory(tests)
endif()
