#!/bin/bash

readonly color_cyan="\033[96m"
readonly color_blue="\033[94m"
readonly color_red="\033[91m"
readonly color_bold="\033[1m"
readonly color_reset="\033[0m"

cores=$(nproc)

function msg() {
    local msg=$1
    local col=${2:-$color_cyan}
    echo -e "${col}${color_bold}${1}${color_reset}"
}

function create_not_exists() {
    local dir=$1
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
    fi
}

function has_command() {
    local cmd=$1
    type "$cmd" &> /dev/null
}

function has_library() {
    local lib=$1
    shift
    local paths=$@
    if ldconfig -p | grep "lib$lib\." &> /dev/null; then
        return 0
    else
        for dir in "${paths[@]}"; do
            if [ -d $dir/lib ]; then
                if ls $dir/lib | grep "lib$lib\." &> /dev/null; then
                    return 0
                fi
            fi
        done
    fi
    return 1
}

function wget_and_untar() {
    local url=$1
    local out_dir=$2
    wget --continue --quiet "$url" -O "$out_dir.tar.gz"
    if [ ! -d "$out_dir" ]; then
        mkdir "$out_dir"
    fi
    tar -xzf "$out_dir.tar.gz" -C "$out_dir" --strip-components=1
}

function cleanup() {
    local dir=$1
    rm -rf $dir*
}

function cmake_build_and_install() {
    local src=$1
    local install_dir=$2
    shift
    shift
    pushd "$src"
    mkdir _build
    cd _build
    if has_command "ninja"; then
        cmake .. -DCMAKE_INSTALL_PREFIX=$install_dir -GNinja "$@"
        ninja install
    else
        cmake .. -DCMAKE_INSTALL_PREFIX=$install_dir "$@"
        make -j$cores install
    fi
    popd
}
