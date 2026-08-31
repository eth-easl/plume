#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR"/setup_common.sh

install_dir=$HOME/.local

# versions
readonly llvm_version="21.1.0"
readonly cmake_version="3.31.9"
readonly ninja_version="1.13.2"

# setup .local installation folder
if [ ! -d "$install_dir" ]; then
    mkdir $install_dir
fi
if [ ! -d "$install_dir/bin" ]; then
    mkdir "$install_dir/bin"
    echo "export PATH=\"$install_dir/bin:\$PATH\"" >> ~/.bashrc
    source ~/.bashrc
fi
create_not_exists $install_dir/share
create_not_exists $install_dir/lib
create_not_exists $install_dir/include

source ~/.bashrc

# install llvm
if has_command "clang"; then
    msg "LLVM already installed" $color_blue
else
    msg "Installing LLVM v${llvm_version}"
    wget --continue --quiet "https://github.com/llvm/llvm-project/releases/download/llvmorg-${llvm_version}/LLVM-${llvm_version}-Linux-X64.tar.xz"
    tar -xvf LLVM-$llvm_version-Linux-X64.tar.xz
    if [ ! -d "$install_dir/llvm" ]; then
        mkdir $install_dir/llvm
    fi
    mv LLVM-$llvm_version-Linux-X64/* $install_dir/llvm
    echo "export PATH=\"$install_dir/llvm/bin:\$PATH\"" >> ~/.bashrc
    echo "export CC=\$(which clang)" >> ~/.bashrc
    echo "export CXX=\$(which clang++)" >> ~/.bashrc
    source ~/.bashrc
    cleanup LLVM
fi

# install CMake
if has_command "cmake"; then
    msg "CMake already installed" $color_blue
else
    msg "Installing CMake v${cmake_version}"
    wget_and_untar "https://github.com/Kitware/CMake/releases/download/v${cmake_version}/cmake-${cmake_version}-linux-x86_64.tar.gz" cmake_bin
    mv cmake_bin/bin/* $install_dir/bin
    mv cmake_bin/share/* $install_dir/share
    cleanup cmake_bin
fi

# install libssl-dev
# TODO: possible without sudo?
sudo apt update && sudo apt install -y libssl-dev

# (optional) install ninja
if has_command "ninja"; then
    msg "Ninja already installed" $color_blue
else
    msg "Installing ninja v${ninja_version}"
    wget --continue --quiet "https://github.com/ninja-build/ninja/releases/download/v${ninja_version}/ninja-linux.zip" -O "ninja.zip"
    unzip ninja.zip
    mv ninja $install_dir/bin/
    chmod +x $install_dir/bin/ninja
    cleanup ninja
fi
