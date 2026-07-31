#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR"/setup_common.sh

install_dir="$(pwd)/dsdk_install"
clang=""
d_arch=$(uname -m)
d_build="Release"
d_platform="kvm"
d_version="latest"
while getopts 'a:b:c:i:p:v:' flag; do
  case "${flag}" in
    a) d_arch="${OPTARG}" ;;
    b) d_build="${OPTARG}" ;;
    c) clang="${OPTARG}" ;;
    i) install_dir="${OPTARG}" ;;
    p) d_platform="${OPTARG}" ;;
    v) d_version="${OPTARG}" ;;
    *) printf "Usage: %s [-a x86_64|aarch64] [-b Debug|Release] [-c <clang>] [-i <install_dir>] [-p debug|kvm|mmu_linux] [-v latest|experimental]\n" $0
       exit 1 ;;
  esac
done
if [[ $d_arch != x86_64 && $d_arch != aarch64 ]]; then
  printf "Got unsupported architecture '%s'. Valid options: x86_64, aarch64.\n" $0
  exit 1
fi
if [[ $d_build != Debug && $d_build != Release ]]; then
  printf "Got unsupported build type '%s'. Valid options: Debug, Release.\n" $0
  exit 1
fi
if [[ $d_platform != debug && $d_platform != kvm && $d_platform != mmu_linux ]]; then
  printf "Got unsupported platform '%s'. Valid options: debug, kvm, mmu_linux.\n" $0
  exit 1
fi
if [[ $d_version != latest && $d_version != experimental ]]; then
  printf "Got unsupported dsdk version '%s'. Valid options: latest, experimental.\n" $0
  exit 1
fi
if [[ $clang == "" ]]; then
  if ! type clang > /dev/null; then
    echo "Did not find clang installation!"
    exit -1
  fi
  clang="clang"
fi

msg "Installing dandelion sdk compiler (arch=${d_arch}, build=${d_build}, platform=${d_platform} and version=${d_version}) under ${install_dir}."

if [ ! -d "$install_dir" ]; then
  mkdir $install_dir -p
fi
wget --continue --quiet https://github.com/eth-easl/dandelionSDK/releases/download/${d_version}/dandelion_sdk_${d_build}_${d_platform}_${d_arch}.tar.gz -O dandelionSDK.tar.gz
tar -xzf dandelionSDK.tar.gz -C "$install_dir" --strip-components=1
"$install_dir/create-compiler.sh" -c $clang
rm dandelionSDK.tar.gz

echo "DandelionSDK compilers installed:"
echo -e "  CC compiler: ${color_cyan}$(ls $install_dir/*clang)${color_reset}"
echo -e "  CXX compiler: ${color_cyan}$(ls $install_dir/*clang++)${color_reset}"
