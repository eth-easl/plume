# ---------------------------------------------------------------------------
# PLUME CORE - TESTS
#
# stdlib-based, no external test dependency.
# ---------------------------------------------------------------------------

function(plume_add_test name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE plume plume_test_support)
  add_test(NAME ${name} COMMAND ${name})
endfunction()

plume_add_test(test_block_roundtrip   test_block_roundtrip.cpp)
plume_add_test(test_pipeline_ir       test_pipeline_ir.cpp)
plume_add_test(test_pipeline_execution test_pipeline_execution.cpp)
plume_add_test(test_aggregate         test_aggregate.cpp)
plume_add_test(test_sort              test_sort.cpp)
plume_add_test(test_top_n             test_top_n.cpp)
plume_add_test(test_runner            test_runner.cpp)
plume_add_test(test_large_input       test_large_input.cpp)
plume_add_test(test_streaming         test_streaming.cpp)

# Runner (runner.hpp/.cpp): the noexcept host-boundary entry point. No
# production caller drives the host boundary this way (that lives in
# functions/), so it lives here rather than in the Plume library. A small
# static lib (rather than a plain test source) so client/tests can link it
# too (test_client exercises the same noexcept boundary against real SQL).
add_library(plume_test_runner STATIC runner.cpp)
target_include_directories(plume_test_runner PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(plume_test_runner PUBLIC plume)
target_link_libraries(test_runner PRIVATE plume_test_runner)
