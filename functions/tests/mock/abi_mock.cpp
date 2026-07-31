#include "abi_mock.hpp"

#include "plume/abi/abi.hpp"

#include <map>

namespace {

// Owning storage of input sets, keyed by set index. The function-under-test gets
// non-owning views over these (with the ident/key preserved).
std::map<size_t, std::vector<plume::abi::InputItem>> g_inputs;
std::vector<plume::mock::Output> g_outputs;

plume::abi::InputItem View(const plume::abi::InputItem &it) {
    return plume::abi::InputItem {
        plume::DataBuffer(const_cast<uint8_t *>(it.buffer.data()), it.buffer.size(), /*owns_data=*/false),
        it.ident, it.key
    };
}

} // namespace

//===----------------------------------------------------------------------===//
// Host ABI implementation (what the function binary links against the runtime).
//===----------------------------------------------------------------------===//

namespace plume::abi {

Result<std::vector<InputItem>> GetInputSet(size_t set_idx) {
    std::vector<InputItem> out;
    auto it = g_inputs.find(set_idx);
    if (it == g_inputs.end()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    out.reserve(it->second.size());
    for (auto &item : it->second) {
        out.push_back(View(item));
    }
    return out;
}

Result<std::vector<InputItem>> GetInputSetWithSize(size_t set_idx, size_t expected_size) {
    std::vector<InputItem> out;
    auto it = g_inputs.find(set_idx);
    if (it == g_inputs.end()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (it->second.size() != expected_size) {
        return Error("Size mismatch for set index " + std::to_string(set_idx) + ": Expected size " +
                     std::to_string(expected_size) + ", got size " + std::to_string(it->second.size()) + ".");
    }
    out.reserve(it->second.size());
    for (auto &item : it->second) {
        out.push_back(View(item));
    }
    return out;
}

Result<InputItem> GetInputSingleton(size_t set_idx) {
    auto it = g_inputs.find(set_idx);
    if (it == g_inputs.end()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (it->second.size() != 1) {
        return Error("Set with index " + std::to_string(set_idx) + " is not a singleton.");
    }
    return View(it->second[0]);
}

Result<InputItem> GetInputItem(size_t set_idx, size_t item_idx) {
    auto it = g_inputs.find(set_idx);
    if (it == g_inputs.end()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (item_idx >= it->second.size()) {
        return Error("Item index " + std::to_string(item_idx) + " out of range for set index "
                     + std::to_string(item_idx) + ".", ErrorKind::OutOfRange);
    }
    return View(it->second[item_idx]);
}

void AddOutput(const std::string &ident, size_t set_idx, DataBuffer buffer, size_t key) {
    g_outputs.push_back(plume::mock::Output {ident, set_idx, key, std::move(buffer)});
}

} // namespace plume::abi

//===----------------------------------------------------------------------===//
// Mock control surface (test-only).
//===----------------------------------------------------------------------===//

namespace plume::mock {

void Reset() {
    g_inputs.clear();
    g_outputs.clear();
}

void SetInput(size_t set_idx, std::vector<DataBuffer> buffers) {
    std::vector<abi::InputItem> items;
    items.reserve(buffers.size());
    for (auto &b : buffers) {
        items.push_back(abi::InputItem {std::move(b), std::string(), 0});
    }
    g_inputs[set_idx] = std::move(items);
}

void SetInput(size_t set_idx, std::vector<abi::InputItem> items) { g_inputs[set_idx] = std::move(items); }

void AddInput(size_t set_idx, DataBuffer buffer, std::string ident, size_t key) {
    g_inputs[set_idx].push_back(abi::InputItem {std::move(buffer), std::move(ident), key});
}

const std::vector<Output> &Outputs() { return g_outputs; }

std::vector<const Output *> OutputsForSet(size_t set_idx) {
    std::vector<const Output *> r;
    for (auto &o : g_outputs) {
        if (o.set_idx == set_idx) {
            r.push_back(&o);
        }
    }
    return r;
}

} // namespace plume::mock
