#pragma once

#include "plume/common/buffer.hpp"
#include "plume/common/result.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

namespace plume::abi {

// An input buffer with an identifier and key.
struct InputItem {
    DataBuffer buffer;
    std::string ident;
    size_t key = 0;
};

Result<std::vector<InputItem>> GetInputSet(size_t set_idx);
Result<std::vector<InputItem>> GetInputSetWithSize(size_t set_idx, size_t expected_size);
Result<InputItem> GetInputSingleton(size_t set_idx);
Result<InputItem> GetInputItem(size_t set_idx, size_t item_idx);

std::optional<std::vector<InputItem>> GetOptionalInputSet(size_t set_idx);
Result<std::optional<InputItem>> GetOptionalInputSingleton(size_t set_idx);

void AddOutput(const std::string &ident, size_t set_idx, DataBuffer buffer, size_t key = 0);

} // namespace plume::abi
