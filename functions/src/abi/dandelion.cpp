#include "plume/abi/abi.hpp"

#include "dandelion/runtime.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

// TODO: continue here
namespace {

inline void RemoveHTTPResponse(uint8_t** data, size_t* data_len) {
    char *chars = reinterpret_cast<char*>(*data);
    if (strncmp(chars, "HTTP/", 5) != 0) { // -> does not have a HTTP response part in the data
        return;
    }
    // search for first double new line (i.e. empty line) -> remove everything before
    size_t last_newline = 0;
    for (size_t i=5; i<*data_len; i++) {
        if (chars[i] == '\n') {
            if (last_newline == i-1 && *data_len > i) {
                *data = reinterpret_cast<uint8_t*>(chars + i + 1);
                *data_len = *data_len - i - 1;
                return;
            }
            last_newline = i;
        }
    }
}

} // namespace

//===----------------------------------------------------------------------===//
// Host ABI implementation (what the function binary links against the runtime).
//===----------------------------------------------------------------------===//

namespace plume::abi {

Result<std::vector<InputItem>> GetInputSet(size_t set_idx) {
    if (set_idx >= dandelion_input_set_count()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    size_t num_itms = dandelion_input_buffer_count(set_idx);

    std::vector<InputItem> out;
    out.reserve(num_itms);
    for (size_t itm_idx = 0; itm_idx < num_itms; itm_idx++) {
        IoBuffer* itm_buf = dandelion_get_input(set_idx, itm_idx);
        uint8_t* data_ptr = static_cast<uint8_t*>(itm_buf->data);
        size_t data_len = itm_buf->data_len;
        RemoveHTTPResponse(&data_ptr, &data_len);
        DataBuffer buffer(data_ptr, data_len, /*owns_data=*/false);
        std::string ident(itm_buf->ident, itm_buf->ident_len);
        out.push_back(InputItem{std::move(buffer), std::move(ident), itm_buf->key});
    }

    return out;
}

Result<std::vector<InputItem>> GetInputSetWithSize(size_t set_idx, size_t expected_size) {
    if (set_idx >= dandelion_input_set_count()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    size_t num_itms = dandelion_input_buffer_count(set_idx);
    if (num_itms != expected_size) {
        return Error("Size mismatch for set index " + std::to_string(set_idx) + ": Expected size " +
                     std::to_string(expected_size) + ", got size " + std::to_string(num_itms) + ".");
    }

    std::vector<InputItem> out;
    out.reserve(num_itms);
    for (size_t itm_idx = 0; itm_idx < num_itms; itm_idx++) {
        IoBuffer* itm_buf = dandelion_get_input(set_idx, itm_idx);
        uint8_t* data_ptr = static_cast<uint8_t*>(itm_buf->data);
        size_t data_len = itm_buf->data_len;
        RemoveHTTPResponse(&data_ptr, &data_len);
        DataBuffer buffer(data_ptr, data_len, /*owns_data=*/false);
        std::string ident(itm_buf->ident, itm_buf->ident_len);
        out.push_back(InputItem{std::move(buffer), std::move(ident), itm_buf->key});
    }

    return out;
}

Result<InputItem> GetInputSingleton(size_t set_idx) {
    if (set_idx >= dandelion_input_set_count()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (dandelion_input_buffer_count(set_idx) != 1) {
        return Error("Set with index " + std::to_string(set_idx) + " is not a singleton.");
    }

    IoBuffer* itm_buf = dandelion_get_input(set_idx, 0);
    uint8_t* data_ptr = static_cast<uint8_t*>(itm_buf->data);
    size_t data_len = itm_buf->data_len;
    RemoveHTTPResponse(&data_ptr, &data_len);
    DataBuffer buffer(data_ptr, data_len, /*owns_data=*/false);
    std::string ident(itm_buf->ident, itm_buf->ident_len);
    return InputItem{std::move(buffer), std::move(ident), itm_buf->key};
}

Result<InputItem> GetInputItem(size_t set_idx, size_t item_idx) {
    if (set_idx >= dandelion_input_set_count()) {
        return Error("Set index " + std::to_string(set_idx) + " out of range.", ErrorKind::OutOfRange);
    }
    if (item_idx >= dandelion_input_buffer_count(set_idx)) {
        return Error("Item index " + std::to_string(item_idx) + " out of range for set index "
                     + std::to_string(item_idx) + ".", ErrorKind::OutOfRange);
    }

    IoBuffer* itm_buf = dandelion_get_input(set_idx, item_idx);
    uint8_t* data_ptr = static_cast<uint8_t*>(itm_buf->data);
    size_t data_len = itm_buf->data_len;
    RemoveHTTPResponse(&data_ptr, &data_len);
    DataBuffer buffer(data_ptr, data_len, /*owns_data=*/false);
    std::string ident(itm_buf->ident, itm_buf->ident_len);
    return InputItem{std::move(buffer), std::move(ident), itm_buf->key};
}

void AddOutput(const std::string &ident, size_t set_idx, DataBuffer buffer, size_t key) {
    IoBuffer out_buf;
    out_buf.data = (void*)buffer.data();
    out_buf.data_len = buffer.size();
    out_buf.key = key;

    char *ident_buf = (char*)malloc(ident.size());
    memcpy((void*)ident_buf, ident.data(), ident.size());
    out_buf.ident = ident_buf;
    out_buf.ident_len = ident.size();

    if (!buffer.TakeBufferOwnership()) {
        std::cerr << "WARNING: Failed to take ownership of output buffer. This could lead to the buffer being overwritten before the program exits." << std::endl;
    }

    dandelion_add_output(set_idx, out_buf);
}

} // namespace plume::abi
