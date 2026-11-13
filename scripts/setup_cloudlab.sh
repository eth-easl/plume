#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR"/setup_common.sh

install_dir=$HOME/.local

# versions
readonly llvm_version="21.1.0"
readonly cmake_version="3.31.9"
readonly ninja_version="1.13.2"
readonly protoc_version="28.0"
readonly protobuf_version="28.0"
readonly abseil_version="20240116.0" # Abseil LTS pinned by protobuf v28.0
readonly openssl_version="3.5.9"

# setup .local installation folder
if [ ! -d "$install_dir" ]; then
    mkdir $install_dir
fi
mkdir -p "$install_dir/bin" "$install_dir/include" "$install_dir/lib"
add_to_bashrc "export PATH=\"$install_dir/bin:\$PATH\""
add_to_bashrc "export LD_LIBRARY_PATH=\"$install_dir/lib:\${LD_LIBRARY_PATH:-}\""
add_to_bashrc "export PKG_CONFIG_PATH=\"$install_dir/lib/pkgconfig:\${PKG_CONFIG_PATH:-}\""
add_to_bashrc "export CMAKE_PREFIX_PATH=\"$install_dir:\${CMAKE_PREFIX_PATH:-}\""
source $HOME/.bashrc

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
    add_to_bashrc "export PATH=\"$install_dir/llvm/bin:\$PATH\""
    add_to_bashrc "export CC=\$(which clang)"
    add_to_bashrc "export CXX=\$(which clang++)"
    source $HOME/.bashrc
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

# make sure clang finds libstdc++ headers + archives
gcc_root=/usr/lib/gcc/x86_64-linux-gnu
function gcc_is_complete() {
    [ -d "/usr/include/c++/$1" ] && [ -f "$gcc_root/$1/libstdc++.a" ]
}
clang_dir=$(dirname "$(readlink -f "$(command -v clang)")") # clang reads <driver>.cfg from its resolved dir
selected_gcc=$(clang -v 2>&1 | sed -n 's|^Selected GCC installation: .*/\([0-9][0-9]*\)$|\1|p')
if [ -n "$selected_gcc" ] && gcc_is_complete "$selected_gcc"; then
    msg "libstdc++ dev files found (GCC $selected_gcc)" $color_blue
else
    usable_gcc=""
    for dir in $(ls -d "$gcc_root"/[0-9]* 2>/dev/null | sort -V -r); do
        if gcc_is_complete "$(basename "$dir")"; then
            usable_gcc=$(basename "$dir")
            break
        fi
    done
    if [ -z "$usable_gcc" ]; then
        msg "No GCC installation with libstdc++ headers + libstdc++.a found under $gcc_root." $color_red
        msg "Ask an administrator to install libstdc++-${selected_gcc:-<N>}-dev." $color_red
        exit 1
    fi
    msg "GCC ${selected_gcc:-?} lacks libstdc++ dev files; pinning clang to GCC $usable_gcc"
    for driver in clang clang++; do
        echo "--gcc-install-dir=$gcc_root/$usable_gcc" > "$clang_dir/$driver.cfg"
    done
fi

# install OpenSSL for HTTPS support in cpr/curl. 
# The OpenSSL runtime (libssl.so) is usually installed system-wide without headers, so check for headers.
if [ -f "$install_dir/include/openssl/ssl.h" ] || [ -f /usr/include/openssl/ssl.h ]; then
    msg "OpenSSL already installed" $color_blue
else
    msg "Installing OpenSSL v${openssl_version}"
    wget_and_untar "https://github.com/openssl/openssl/releases/download/openssl-${openssl_version}/openssl-${openssl_version}.tar.gz" openssl
    pushd openssl
    ./Configure --prefix="$install_dir" --openssldir="$install_dir/ssl" --libdir=lib \
        no-shared no-tests no-docs -fPIC
    make -j$cores
    make install_sw
    popd
    cleanup openssl
fi

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

source $HOME/.bashrc

# install protoc + protobuf lib
if has_command "protoc"; then
    msg "protoc already installed" $color_blue
else
    msg "Installing protoc v${protoc_version}"
    protoc_zip="protoc-${protoc_version}-linux-x86_64.zip"
    curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v${protoc_version}/${protoc_zip}" -o "$protoc_zip"
    unzip -o "$protoc_zip" -d "$install_dir"
    rm -f "$protoc_zip"
fi
if has_library protobuf "$install_dir"; then
    msg "libprotobuf-dev already installed" $color_blue
else
    msg "Installing libprotobuf-dev v${protobuf_version}"
    wget_and_untar "https://github.com/protocolbuffers/protobuf/releases/download/v${protobuf_version}/protobuf-${protobuf_version}.tar.gz" libprotobuf
    # release tarballs omit git submodules, so fetch abseil for protobuf_ABSL_PROVIDER=module
    wget_and_untar "https://github.com/abseil/abseil-cpp/archive/refs/tags/${abseil_version}.tar.gz" libprotobuf/third_party/abseil-cpp
    cmake_build_and_install libprotobuf "$install_dir" -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 -DCMAKE_POSITION_INDEPENDENT_CODE=ON -Dprotobuf_BUILD_TESTS=OFF -Dprotobuf_ABSL_PROVIDER=module
    cleanup libprotobuf
fi

msg "Setup complete."
msg "Run 'source ~/.bashrc' or open a new shell to pick up the environment changes."
