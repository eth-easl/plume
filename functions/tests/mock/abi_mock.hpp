#pragma once

#include "plume/abi/abi.hpp"
#include "plume/common/buffer.hpp"

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

namespace plume::mock {

// One captured abi::AddOutput call.
struct Output {
    std::string ident;
    size_t set_idx = 0;
    size_t key = 0;
    DataBuffer buffer;
};

// Clear all input/output state (call at the start of each test case).
void Reset();

// Populate the input set the function reads via abi::GetInputSet(set_idx).
// The buffer-only overload tags items with ident="" and key=0.
void SetInput(size_t set_idx, std::vector<DataBuffer> buffers);
void SetInput(size_t set_idx, std::vector<abi::InputItem> items);
void AddInput(size_t set_idx, DataBuffer buffer, std::string ident = std::string(), size_t key = 0);

// Inspect what the function wrote via abi::AddOutput.
const std::vector<Output> &Outputs();
std::vector<const Output *> OutputsForSet(size_t set_idx);

// Convenience: make an owning DataBuffer holding a copy of `size` bytes at `data`.
inline DataBuffer MakeBuffer(const void *data, size_t size) {
    DataBuffer buf(size);
    if (size > 0) {
        std::memcpy(buf.mutable_data(), data, size);
    }
    return buf;
}

} // namespace plume::mock
