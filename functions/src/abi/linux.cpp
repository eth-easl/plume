#include "plume/abi/linux.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

namespace {

// Tracking input and output sets. g_inputs needs to be initialized on startup using the `Init(...)` function
std::vector<std::vector<plume::linux::IoItem>> g_inputs;
std::vector<std::vector<plume::linux::IoItem>> g_outputs;

std::vector<plume::linux::IoItem> ParseArg(char* argv, char delimiter = ',') {
    // TODO: parse key
    std::vector<plume::linux::IoItem> out;
    size_t start = 0;
    size_t end = 0;
    while (argv[end] != '\0') {
        if (argv[end] == delimiter) {
            size_t key = 0;
            out.push_back(plume::linux::IoItem{
                std::string(argv + start, argv + end), 
                key
            });
            start = end + 1;
        }
        end++;
    }
    return out;
}

void PrintInfo(const std::vector<std::vector<plume::linux::IoItem>>& sets) {
    for (size_t set_idx = 0; set_idx < sets.size(); set_idx++) {
        std::cout << "set " << set_idx << " (size = " << sets[set_idx].size() << ")" << std::endl;
        for (size_t itm_idx = 0; itm_idx < sets[set_idx].size(); itm_idx++) {
            const auto& itm = sets[set_idx][itm_idx];
            std::cout << " " << itm_idx << ": { path: '" << itm.path << "', key: "
                      << itm.key << " }" << std::endl;
        }
    }
}

} // namespace

//===----------------------------------------------------------------------===//
// Host ABI implementation (what the function binary links against the runtime).
//===----------------------------------------------------------------------===//

namespace plume::abi {

Result<std::vector<InputItem>> GetInputSet(size_t set_idx) {
    if (set_idx >= g_inputs.size()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }

    std::vector<InputItem> out;
    out.reserve(g_inputs[set_idx].size());
    for (auto& itm : g_inputs[set_idx]) {
        std::ifstream in_file(itm.path);
        if (!in_file.good()) {
            return Error("Failed to open file: " + itm.path);
        }

        in_file.seekg(0, std::ios::end);
        size_t buf_size = in_file.tellg();
        in_file.seekg(0, std::ios::beg);

        DataBuffer buffer(buf_size);
        in_file.read((char*)buffer.mutable_data(), buf_size);
        
        out.push_back(InputItem{std::move(buffer), itm.path, itm.key});
    }

    return out;
}

Result<std::vector<InputItem>> GetInputSetWithSize(size_t set_idx, size_t expected_size) {
    if (set_idx >= g_inputs.size()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (g_inputs[set_idx].size() != expected_size) {
        return Error("Size mismatch for set index " + std::to_string(set_idx) + ": Expected size " +
                     std::to_string(expected_size) + ", got size " + std::to_string(g_inputs[set_idx].size()) + ".");
    }

    std::vector<InputItem> out;
    out.reserve(g_inputs[set_idx].size());
    for (auto& itm : g_inputs[set_idx]) {
        std::ifstream in_file(itm.path);
        if (!in_file.good()) {
            return Error("Failed to open file: " + itm.path);
        }
        
        in_file.seekg(0, std::ios::end);
        size_t buf_size = in_file.tellg();
        in_file.seekg(0, std::ios::beg);

        DataBuffer buffer(buf_size);
        in_file.read((char*)buffer.mutable_data(), buf_size);
        
        out.push_back(InputItem{std::move(buffer), itm.path, itm.key});
    }

    return out;
}

Result<InputItem> GetInputSingleton(size_t set_idx) {
    if (set_idx >= g_inputs.size()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (g_inputs[set_idx].size() != 1) {
        return Error("Set with index " + std::to_string(set_idx) + " is not a singleton.");
    }

    auto& itm = g_inputs[set_idx][0];
    std::ifstream in_file(itm.path);
    if (!in_file.good()) {
        return Error("Failed to open file: " + itm.path);
    }
    
    in_file.seekg(0, std::ios::end);
    size_t buf_size = in_file.tellg();
    in_file.seekg(0, std::ios::beg);

    DataBuffer buffer(buf_size);
    in_file.read((char*)buffer.mutable_data(), buf_size);
    
    return InputItem{std::move(buffer), itm.path, itm.key};
}

Result<InputItem> GetInputItem(size_t set_idx, size_t item_idx) {
    if (set_idx >= g_inputs.size()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (item_idx >= g_inputs[set_idx].size()) {
        return Error("Item index " + std::to_string(item_idx) + " out of range for set index "
                     + std::to_string(item_idx) + ".", ErrorKind::OutOfRange);
    }

    auto& itm = g_inputs[set_idx][item_idx];
    std::ifstream in_file(itm.path);
    if (!in_file.good()) {
        return Error("Failed to open file: " + itm.path);
    }
    
    in_file.seekg(0, std::ios::end);
    size_t buf_size = in_file.tellg();
    in_file.seekg(0, std::ios::beg);

    DataBuffer buffer(buf_size);
    in_file.read((char*)buffer.mutable_data(), buf_size);
    
    return InputItem{std::move(buffer), itm.path, itm.key};
}

std::optional<std::vector<InputItem>> GetOptionalInputSet(size_t set_idx) {
    if (set_idx >= g_inputs.size()) {
        return std::nullopt;
    }

    std::vector<InputItem> out;
    out.reserve(g_inputs[set_idx].size());
    for (auto& itm : g_inputs[set_idx]) {
        std::ifstream in_file(itm.path);
        if (!in_file.good()) {
            return std::nullopt;
        }

        in_file.seekg(0, std::ios::end);
        size_t buf_size = in_file.tellg();
        in_file.seekg(0, std::ios::beg);

        DataBuffer buffer(buf_size);
        in_file.read((char*)buffer.mutable_data(), buf_size);
        
        out.push_back(InputItem{std::move(buffer), itm.path, itm.key});
    }

    return std::make_optional(std::move(out));
}

Result<std::optional<InputItem>> GetOptionalInputSingleton(size_t set_idx) {
    if (set_idx >= g_inputs.size()) {
        return std::nullopt;
    }
    if (g_inputs[set_idx].size() == 0) {
        return std::nullopt;
    } else if (g_inputs[set_idx].size() > 1) {
        return Error("Set with index " + std::to_string(set_idx) + " contains more than a single item.");
    }

    auto& itm = g_inputs[set_idx][0];
    std::ifstream in_file(itm.path);
    if (!in_file.good()) {
        return Error("Failed to open file: " + itm.path);
    }
    
    in_file.seekg(0, std::ios::end);
    size_t buf_size = in_file.tellg();
    in_file.seekg(0, std::ios::beg);

    DataBuffer buffer(buf_size);
    in_file.read((char*)buffer.mutable_data(), buf_size);
    
    return std::make_optional(InputItem{std::move(buffer), itm.path, itm.key});
}

void AddOutput(const std::string &ident, size_t set_idx, DataBuffer buffer, size_t key) {
    if (set_idx >= g_outputs.size()) {
        g_outputs.resize(set_idx+1, {});
    }
    size_t itm_idx = g_outputs[set_idx].size();
    g_outputs[set_idx].push_back(plume::linux::IoItem{ident, key});
    
    // TODO: allow for some base directory + add key information
    std::filesystem::path set_path = std::to_string(set_idx);
    std::filesystem::path itm_path = std::to_string(set_idx) + "/" + std::to_string(itm_idx) + "_" + ident;
    
    try {
        std::filesystem::create_directories(set_path);
        std::ofstream out_file(itm_path, std::ios::out | std::ios::binary | std::ios::trunc);
        int64_t out_size = buffer.size();
        out_file.write((char *)buffer.data(), out_size);
        out_file.close();
    } catch (const std::exception& e) {
        std::cerr << "Error adding output: " << e.what() << std::endl;
    }
}

} // namespace plume::abi

//===----------------------------------------------------------------------===//
// Linux control surface.
//===----------------------------------------------------------------------===//

namespace plume::linux {

void Init(int argc, char* argv[]) {
    g_inputs.clear();
    g_outputs.clear();
    g_inputs.reserve(argc);
    for (int i = 0; i < argc; i++) {
        g_inputs.push_back(ParseArg(argv[i]));
    }

    std::cout << "Initialized with inputs:" << std::endl;
    PrintInfo(g_inputs);
}

void Close() {
    std::cout << "Function outputs:" << std::endl;
    PrintInfo(g_outputs);
}

} // namespace plume::linux
