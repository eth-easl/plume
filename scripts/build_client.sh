#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR"/setup_common.sh

# configuration
build_dir="build"
duckdb_dir="external/duckdb"
while getopts 'b:d:' flag; do
  case "${flag}" in
    b) build_dir="${OPTARG}" ;;
    d) duckdb_dir="${OPTARG}" ;;
    *) printf "Usage: %s [-b build_dir] [-d duckdb_dir]\n" $0
       exit 1 ;;
  esac
done
printf "Build configuration:\n build_dir=$build_dir\n duckdb_dir=$duckdb_dir\n"

# create build folder
if [ -d "$build_dir" ]; then
  msg "Build folder '$build_dir' already exists!" $color_red
  exit 1
fi
mkdir $build_dir -p
pushd $build_dir

# build the external duckdb dependency using the dsdk compiler if not built already
msg "Building (Dandelion) DuckDB archives from '${duckdb_dir}'..."
"$DIR/setup_duckdb.sh" -d "${duckdb_dir}" -b "${build_dir}/duckdb"
duckdb_build_dir="$(pwd)/duckdb"

mkdir plume -p
pushd plume

# configure and build project (only the functions for the dandelion platform).
if has_command "ninja"; then
  cmake ../.. -GNinja -DPLUME_BUILD_FUNCTIONS=OFF -DPLUME_BUILD_BENCHMARKS=ON -DDUCKDB_BUILD_DIR="${duckdb_build_dir}"
  ninja
else
  cmake ../.. -DPLUME_BUILD_FUNCTIONS=OFF -DPLUME_BUILD_BENCHMARKS=ON -DDUCKDB_BUILD_DIR="${duckdb_build_dir}"
  make -j$cores
fi

popd # plume
popd # $build_dir
