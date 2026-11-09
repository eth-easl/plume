cmake_minimum_required(VERSION 3.16)
project(plume CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# detect dandelion from platform triple
execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} -dumpmachine
    OUTPUT_VARIABLE COMPILER_TARGET_TRIPLE
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
string(REPLACE "-" ";" TRIPLE_LIST ${COMPILER_TARGET_TRIPLE})
list(GET TRIPLE_LIST -1 COMPILER_TARGET_PLATFORM)
list(GET TRIPLE_LIST 0 COMPILER_TARGET_ARCHITECTURE)
string(COMPARE EQUAL ${COMPILER_TARGET_PLATFORM} "dandelion" PLUME_PLATFORM_DANDELION)
message(STATUS "Detected architecture: ${COMPILER_TARGET_ARCHITECTURE}")

if(PLUME_PLATFORM_DANDELION)
  # The Dandelion libc headers use the C99 keyword 'restrict' as a parameter
  # name, which is invalid in C++ and breaks any TU that transitively includes
  # them (e.g. DuckDB threading pulling in <semaphore.h>). Map the token to
  # Clang's __restrict qualifier for the whole plume build. The prebuilt DuckDB
  # archives apply the same flag via scripts/setup_duckdb.sh.
  add_compile_definitions(restrict=__restrict)
endif()


# ---------------------------------------------------------------------------
#  Build options.
#   - core is always built (both client and functions depend on it)
#   - client/functions are opt-out
# ---------------------------------------------------------------------------

option(PLUME_BUILD_FUNCTIONS  "Build the plume function executables"           ON)
option(PLUME_BUILD_CLIENT     "Build the plume client (client)"                ON)
option(PLUME_BUILD_BENCHMARKS "Build the benchmark harness"                    OFF)
option(PLUME_BUILD_UBENCH     "Build the ubench micro-benchmark tool"          OFF)
option(PLUME_WITH_TESTS       "Build the test suites"                          OFF)

# internals
option(PLUME_RECYCLING_ALLOCATOR "Recycle small/short-lived allocations through a per-thread free-list pool instead of malloc/free" ON)
option(PLUME_DSL_STDIO "Register the stdio output set on generated Dandelion function compositions (debugging)" ON)
if(PLUME_DSL_STDIO)
  add_compile_definitions(PLUME_DSL_STDIO)
endif()
set(PLUME_VERBOSE "0" CACHE STRING "Verbosity level (0 = off, higher = more verbose)")
if(PLUME_VERBOSE GREATER 0)
  message(STATUS "Using Plume verbosity level: ${PLUME_VERBOSE}")
endif()
add_compile_definitions(PLUME_VERBOSE=${PLUME_VERBOSE})
option(PLUME_WITH_COMPILE_LAYER "Build core's SQL-compile layer (catalog/dandelion-composition/parser) + cpr into the plume library" OFF)
if(PLUME_BUILD_CLIENT)
  set(PLUME_WITH_COMPILE_LAYER ON)
endif()

# Emit compile_commands.json (gitignored) so editors / clangd resolve the
# per-project include roots (core/include, functions/include, ...) correctly.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Transparent object-file cache: identical output, just skips recompiling
# unchanged TUs (e.g. reconfiguring build/plume, or switching PLUME_* options
# on and off). No-op if ccache isn't installed.
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()

# --gc-sections drops dead code pulled in only by conservative archive symbol
# resolution. The archive was compiled with -ffunction-sections/-fdata-sections.
add_link_options("-Wl,--gc-sections")

include(${CMAKE_SOURCE_DIR}/cmake/DuckDB.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/Json.cmake)
if(PLUME_WITH_COMPILE_LAYER)
  include(${CMAKE_SOURCE_DIR}/cmake/Cpr.cmake)
endif()

if(PLUME_WITH_TESTS)
  enable_testing()
endif()

# core is the shared query-engine library -> build it first
add_subdirectory(core)

if(PLUME_BUILD_FUNCTIONS)
  add_subdirectory(functions)
endif()

if(PLUME_BUILD_CLIENT)
  add_subdirectory(client)
endif()

if(PLUME_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
