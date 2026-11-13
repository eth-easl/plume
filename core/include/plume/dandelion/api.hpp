#pragma once

#include "plume/common/result.hpp"
#include "plume/common/types.hpp"

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

constexpr uint64_t kWireVersion = 1;

struct DandelionComposition;

// Body to register the composition (RegisterComposition).
Result<BinaryData> RegistrationBody(const DandelionComposition& comp);

// Body to invoke the composition (DandelionRequest). Sends the composition dsl
// inline unless `is_registered`, in which case it is referenced by name.
Result<BinaryData> InvocationBody(const DandelionComposition& comp, bool is_registered = false);

// Parses a DandelionResponse body into its output sets. If `traces` is given, it
// receives the response's trace (empty if none).
Result<DataSetVec> ParseResponseBody(const BinaryData& body, std::string* traces = nullptr);

// Parses a response and renders its first output set as tab-separated rows.
Result<std::string> ParseAndRenderResponseBody(const BinaryData& data,
    const Schema &schema, std::string* traces = nullptr);

} // namespace plume::dandelion
