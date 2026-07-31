#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace plume::dandelion {

using BinaryData = std::vector<uint8_t>;

struct DataItem {
    uint64_t key = 0;
    std::string identifier;
    BinaryData data;
};
using DataItemVec = std::vector<DataItem>;
using DataSetVec = std::vector<DataItemVec>;

} // namespace plume::dandelion
