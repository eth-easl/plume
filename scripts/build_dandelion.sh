#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR"/setup_common.sh

# configuration
build_dir="build_dandelion"
duckdb_dir="external/duckdb"
dsdk_compiler_path=""
platform="kvm"
tune_arch=""
while getopts 'b:i:c:p:d:t:' flag; do
  case "${flag}" in
    b) build_dir="${OPTARG}" ;;
    c) dsdk_compiler_path="${OPTARG}" ;;
    p) platform="${OPTARG}" ;;
    d) duckdb_dir="${OPTARG}" ;;
    t) tune_arch="${OPTARG}" ;;
    *) printf "Usage: %s [-b build_dir] [-i install_dir] [-c dsdk_compiler_path] [-p platform] [-d duckdb_dir] [-t cpu_arch]\n" $0
       exit 1 ;;
  esac
done
printf "Build configuration:\n build_dir=$build_dir\n dsdk_compiler_path=$dsdk_compiler_path\n platform=$platform\n duckdb_dir=$duckdb_dir\n tune_arch=$tune_arch\n"

# create build folder
if [ -d "$build_dir" ]; then
  msg "Build folder '$build_dir' already exists!" $color_red
  exit 1
fi
mkdir $build_dir -p
pushd $build_dir

# configure dandelion sdk compiler
if [ -z "$dsdk_compiler_path" ]; then
  "$DIR"/setup_dsdk.sh -i _dsdk -p "${platform}" -v experimental
  dsdk_compiler_path=$(pwd)/_dsdk
else
  msg "Using manually specified compiler under '${dsdk_compiler_path}'"
fi
arch=$(uname -m)
export CC=${dsdk_compiler_path}/${arch}-unknown-dandelion-clang
export CXX=${dsdk_compiler_path}/${arch}-unknown-dandelion-clang++

# build the external duckdb dependency using the dsdk compiler if not built already
msg "Building (Dandelion) DuckDB archives from '${duckdb_dir}'..."
"$DIR/setup_duckdb.sh" -d "${duckdb_dir}" -b "${build_dir}/duckdb"

# (optionally) set tuning compiler flags
if [ ! -z "$tune_arch" ]; then
  sed -i "s/^#-march=.*/-march=$tune_arch/" ${dsdk_compiler_path}/${arch}-unknown-dandelion-clang.cfg
  sed -i "s/^#-mtune=.*/-mtune=$tune_arch/" ${dsdk_compiler_path}/${arch}-unknown-dandelion-clang.cfg
fi

# configure and build project (only the functions for the dandelion platform).
# Point DUCKDB_BUILD_DIR at the Dandelion archives built above ($build_dir/duckdb,
# i.e. ./duckdb relative to the current build dir) so we don't link the host build.
duckdb_build_dir="$(pwd)/duckdb"
if has_command "ninja"; then
  cmake .. -GNinja -DPLUME_BUILD_CLIENT=OFF -DDUCKDB_BUILD_DIR="${duckdb_build_dir}"
  ninja
else
  cmake .. -DPLUME_BUILD_CLIENT=OFF -DDUCKDB_BUILD_DIR="${duckdb_build_dir}"
  make -j$cores
fi

popd # $build_dir
