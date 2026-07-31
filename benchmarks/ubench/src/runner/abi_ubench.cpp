#include "abi_ubench.hpp"

#include <map>

namespace {

// Per-thread ABI state. Inputs are non-owning views over caller-owned memory
// (the runner keeps the block/request bytes alive across the invocation);
// outputs own their buffers (AddOutput hands ownership to the host).
struct ThreadIo {
    std::map<size_t, std::vector<plume::abi::InputItem>> inputs;
    std::vector<plume::ubench::rt::CapturedOutput> outputs;
};

thread_local ThreadIo t_io;

} // namespace

//===----------------------------------------------------------------------===//
// Host ABI implementation (linked into the ubench runner in place of the linux
// backend / test mock).
//===----------------------------------------------------------------------===//

namespace plume::abi {

Result<std::vector<InputItem>> GetInputSet(size_t set_idx) {
    auto it = t_io.inputs.find(set_idx);
    if (it == t_io.inputs.end()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    std::vector<InputItem> out;
    out.reserve(it->second.size());
    for (auto &item : it->second) {
        out.push_back(InputItem{DataBuffer(const_cast<uint8_t *>(item.buffer.data()), item.buffer.size(),
                                           /*owns_data=*/false),
                                item.ident, item.key});
    }
    return out;
}

Result<std::vector<InputItem>> GetInputSetWithSize(size_t set_idx, size_t expected_size) {
    auto it = t_io.inputs.find(set_idx);
    if (it == t_io.inputs.end()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (it->second.size() != expected_size) {
        return Error("Size mismatch for set index " + std::to_string(set_idx) + ": expected " +
                         std::to_string(expected_size) + ", got " + std::to_string(it->second.size()) + ".",
                     ErrorKind::InvalidInput);
    }
    return GetInputSet(set_idx);
}

Result<InputItem> GetInputSingleton(size_t set_idx) {
    auto it = t_io.inputs.find(set_idx);
    if (it == t_io.inputs.end()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (it->second.size() != 1) {
        return Error("Set with index " + std::to_string(set_idx) + " is not a singleton.", ErrorKind::InvalidInput);
    }
    auto &item = it->second[0];
    return InputItem{DataBuffer(const_cast<uint8_t *>(item.buffer.data()), item.buffer.size(), false), item.ident,
                     item.key};
}

Result<InputItem> GetInputItem(size_t set_idx, size_t item_idx) {
    auto it = t_io.inputs.find(set_idx);
    if (it == t_io.inputs.end()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (item_idx >= it->second.size()) {
        return Error("Item index " + std::to_string(item_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    auto &item = it->second[item_idx];
    return InputItem{DataBuffer(const_cast<uint8_t *>(item.buffer.data()), item.buffer.size(), false), item.ident,
                     item.key};
}

void AddOutput(const std::string &ident, size_t set_idx, DataBuffer buffer, size_t key) {
    t_io.outputs.push_back(plume::ubench::rt::CapturedOutput{ident, set_idx, key, std::move(buffer)});
}

} // namespace plume::abi

//===----------------------------------------------------------------------===//
// Control surface
//===----------------------------------------------------------------------===//

namespace plume::ubench::rt {

void Reset() {
    t_io.inputs.clear();
    t_io.outputs.clear();
}

void AddInput(size_t set_idx, const uint8_t *data, size_t size, std::string ident, size_t key) {
    t_io.inputs[set_idx].push_back(
        abi::InputItem{DataBuffer(const_cast<uint8_t *>(data), size, /*owns_data=*/false), std::move(ident), key});
}

void EnsureInputSet(size_t set_idx) { t_io.inputs[set_idx]; }

std::vector<CapturedOutput> &Outputs() { return t_io.outputs; }

} // namespace plume::ubench::rt
