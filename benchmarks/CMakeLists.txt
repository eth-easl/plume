# ---------------------------------------------------------------------------
#  PLUME BENCHMARKS
#
#  Two tools live here, each in its own subdirectory:
#
#   - tpch/    : plume_bench — the config-driven dandelion benchmark harness,
#                plus the TPC-H queries/configs/traces it's run against.
#   - ubench/  : a vibe-coded micro-benchmark tool (traced per-operator compute 
#                analysis), built only when PLUME_BUILD_UBENCH (declared at 
#                the top level) is ON and the function executables are being
#                built.
# ---------------------------------------------------------------------------

if(PLUME_WITH_COMPILE_LAYER)
  add_subdirectory(tpch)
else()
  message(STATUS "Skipping plume_bench (tpch): needs PLUME_WITH_COMPILE_LAYER=ON (or PLUME_BUILD_CLIENT=ON) for core's SQL-compile layer")
endif()

# ubench needs the function sources + plume_snappy (PLUME_BUILD_FUNCTIONS) for its traced engine.
if(PLUME_BUILD_UBENCH AND PLUME_BUILD_FUNCTIONS)
  add_subdirectory(ubench)
endif()
