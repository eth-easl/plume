#!/bin/bash
# Sets up external/duckdb: clones the pinned DuckDB, applies extraction patches,
# and builds the static archives needed by the plume build.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR"/setup_common.sh

REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# configuration
readonly duckdb_tag="v1.5.4"
duckdb_dir="external/duckdb" # relative to repository root
build_dir="build/duckdb"     # relative to repository root
while getopts 'd:b:' flag; do
  case "${flag}" in
    d) duckdb_dir="${OPTARG}" ;;
    b) build_dir="${OPTARG}" ;;
    *) printf "Usage: %s [-d duckdb_dir] [-b build_dir]\n" $0
       exit 1 ;;
  esac
done
printf "Build configuration:\n duckdb_dir=$duckdb_dir\n build_dir=$build_dir\n"

if [ ! -d "${duckdb_dir}" ]; then
  msg "Cloning DuckDB (${duckdb_tag}) into '${duckdb_dir}'..."
  git clone --depth 1 --branch "${duckdb_tag}" https://github.com/duckdb/duckdb.git "${duckdb_dir}"
fi

msg "Applying patches..."
for p in cmake/duckdb_patches/*.patch; do
  if ! git -C "${duckdb_dir}" apply --reverse --check "../../$p" 2>/dev/null; then
    git -C "${duckdb_dir}" apply "../../$p"
    echo "applied $p"
  else
    echo "already applied $p"
  fi
done

CMAKE_COMPILER_ARGS=()
if [ -n "${CC:-}" ]; then
  CMAKE_COMPILER_ARGS+=(-DCMAKE_C_COMPILER="${CC}")
fi
if [ -n "${CXX:-}" ]; then
  CMAKE_COMPILER_ARGS+=(-DCMAKE_CXX_COMPILER="${CXX}")
fi

# Reuse cached object files across identical builds.
if has_command "ccache"; then
  CMAKE_COMPILER_ARGS+=(-DCMAKE_C_COMPILER_LAUNCHER=ccache)
  CMAKE_COMPILER_ARGS+=(-DCMAKE_CXX_COMPILER_LAUNCHER=ccache)
fi

# DuckDB only turns on -ffunction-sections/-fdata-sections for GCC so we pass the flags ourselves.
EXTRA_SECTION_FLAGS="-ffunction-sections -fdata-sections"

# DuckDB's built-time platform probe compiles and runs duckdb_platform_binary to autodetect the 
# platform string which segfaults for the Dandelion SDK. Pinning the platform explicitely skips this step.
if [[ "${CXX:-}" == *dandelion* || "${CC:-}" == *dandelion* ]]; then
  case "$(uname -m)" in
    x86_64)  duckdb_arch="amd64" ;;
    aarch64) duckdb_arch="arm64" ;;
    *)       duckdb_arch="$(uname -m)" ;;
  esac
  CMAKE_COMPILER_ARGS+=(-DDUCKDB_EXPLICIT_PLATFORM="dandelion_${duckdb_arch}")
  CMAKE_COMPILER_ARGS+=(-DDISABLE_EXTENSION_LOAD=TRUE)
  # Dandelion libc headers use the C99 keyword 'restrict' as a parameter name, which is invalid in 
  # C++ and breaks every header in this class. Mapping it to __restrict neutralises it.
  CMAKE_COMPILER_ARGS+=(-DCMAKE_CXX_FLAGS="-Drestrict=__restrict ${EXTRA_SECTION_FLAGS}")
  CMAKE_COMPILER_ARGS+=(-DCMAKE_C_FLAGS="-Drestrict=__restrict ${EXTRA_SECTION_FLAGS}")
else
  CMAKE_COMPILER_ARGS+=(-DCMAKE_CXX_FLAGS="${EXTRA_SECTION_FLAGS}")
  CMAKE_COMPILER_ARGS+=(-DCMAKE_C_FLAGS="${EXTRA_SECTION_FLAGS}")
fi

msg "Building DuckDB archives in '${build_dir}'..."
cmake -S "${duckdb_dir}" -B "${build_dir}" -G Ninja -DCMAKE_BUILD_TYPE=Release \
      "${CMAKE_COMPILER_ARGS[@]}" \
      -DBUILD_SHELL=0 -DBUILD_UNITTESTS=0 -DBUILD_BENCHMARKS=0 \
      -DENABLE_SANITIZER=0 -DDISABLE_UNITY=0 -DENABLE_JEMALLOC=0
ninja -C "${build_dir}" -j"$(nproc)" duckdb_static core_functions_extension parquet_extension dummy_static_extension_loader
