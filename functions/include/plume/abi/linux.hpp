#pragma once

#include "plume/abi/abi.hpp"

#include <cstddef>
#include <string>

namespace plume::linux {

struct IoItem {
    std::string path;
    size_t key = 0;
};

void Init(int argc, char* argv[]);
void Close();

} // namespace plume::linux
