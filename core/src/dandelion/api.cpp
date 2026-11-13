#include "plume/dandelion/api.hpp"

#include "plume/common/result.hpp"
#include "plume/dandelion/composition.hpp"
#include "plume/memory/adapter.hpp"

#include "dandelion.pb.h"

#include <cstring>
#include <limits>
#include <memory>
#include <sstream>

namespace plume::dandelion {

namespace pb = ::dandelion_frontend;

namespace {

//===----------------------------------------------------------------------===//
// Framing: <varint version><varint metadata length><protobuf metadata>[payload]
//===----------------------------------------------------------------------===//

void AppendVarint(BinaryData &out, uint64_t value) {
    while (value >= 0x80) {
        out.push_back(static_cast<uint8_t>(value | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<uint8_t>(value));
}

Result<uint64_t> ReadVarint(const BinaryData &in, size_t &pos) {
    uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 7) {
        if (pos >= in.size()) {
            return Error("Truncated varint in dandelion body.", ErrorKind::InvalidInput);
        }
        uint8_t byte = in[pos++];
        value |= static_cast<uint64_t>(byte & 0x7f) << shift;
        if (!(byte & 0x80)) {
            return value;
        }
    }
    return Error("Malformed varint in dandelion body.", ErrorKind::InvalidInput);
}

Result<BinaryData> FrameHeader(const google::protobuf::MessageLite &meta, size_t payload_size) {
    const size_t meta_size = meta.ByteSizeLong();
    BinaryData out;
    out.reserve(2 * 10 + meta_size + payload_size);
    AppendVarint(out, kWireVersion);
    AppendVarint(out, meta_size);
    const size_t meta_pos = out.size();
    out.resize(meta_pos + meta_size);
    if (meta_size > static_cast<size_t>(std::numeric_limits<int>::max()) ||
            !meta.SerializeToArray(out.data() + meta_pos, static_cast<int>(meta_size))) {
        return Error("Could not serialize dandelion metadata.");
    }
    return out;
}

struct Frame {
    size_t meta_pos = 0;
    size_t meta_size = 0;
    size_t payload_pos = 0;
};

Result<Frame> ReadFrame(const BinaryData &body) {
    size_t pos = 0;
    TRY(auto version, ReadVarint(body, pos));
    if (version != kWireVersion) {
        return Error("Unsupported dandelion wire version " + std::to_string(version) + " (expected " +
                     std::to_string(kWireVersion) + ").", ErrorKind::InvalidInput);
    }
    TRY(auto meta_size, ReadVarint(body, pos));
    if (meta_size > body.size() - pos) {
        return Error("Dandelion metadata length exceeds body size.", ErrorKind::InvalidInput);
    }
    return Frame{pos, static_cast<size_t>(meta_size), pos + static_cast<size_t>(meta_size)};
}

//===----------------------------------------------------------------------===//
// Rendering
//===----------------------------------------------------------------------===//

Result<std::string> RenderChunks(const std::vector<std::unique_ptr<duckdb::DataChunk>> &chunks,
                                 const Schema &schema, bool write_header = true) {
    std::ostringstream out;
    if (write_header) {
        for (size_t c = 0; c < schema.columns.size(); c++) {
            if (c) out << "\t";
            out << schema.columns[c].name;
        }
        out << "\n";
    }

    for (const auto &chunk : chunks) {
        for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
            for (duckdb::idx_t c = 0; c < chunk->ColumnCount(); c++) {
                if (c) out << "\t";
                out << chunk->GetValue(c, r).ToString();
            }
            out << "\n";
        }
    }
    return out.str();
}

} // namespace

//===----------------------------------------------------------------------===//
// Requests
//===----------------------------------------------------------------------===//

Result<BinaryData> RegistrationBody(const DandelionComposition& comp) {
    pb::RegisterComposition meta;
    meta.set_composition(comp.dsl);
    return FrameHeader(meta, 0);
}

Result<BinaryData> InvocationBody(const DandelionComposition& comp, bool is_registered) {
    pb::DandelionRequest meta;
    if (is_registered) {
        meta.set_name(comp.name);
    } else {
        meta.set_composition(comp.dsl);
    }

    size_t payload_size = 0;
    for (const auto &set : comp.in_sets) {
        auto *pb_set = meta.add_sets();
        for (const auto &item : set) {
            if (item.key > std::numeric_limits<uint32_t>::max()) {
                return Error("Item key " + std::to_string(item.key) + " of '" + item.identifier +
                             "' does not fit the 32-bit dandelion key.", ErrorKind::InvalidInput);
            }
            auto *pb_item = pb_set->add_items();
            pb_item->set_identifier(item.identifier);
            pb_item->set_key(static_cast<uint32_t>(item.key));
            pb_item->set_payload_length(item.data.size());
            payload_size += item.data.size();
        }
    }

    TRY(auto out, FrameHeader(meta, payload_size));
    for (const auto &set : comp.in_sets) {
        for (const auto &item : set) {
            out.insert(out.end(), item.data.begin(), item.data.end());
        }
    }
    return out;
}

//===----------------------------------------------------------------------===//
// Responses
//===----------------------------------------------------------------------===//

Result<DataSetVec> ParseResponseBody(const BinaryData& body, std::string* traces) {
    TRY(auto frame, ReadFrame(body));
    pb::DandelionResponse meta;
    if (frame.meta_size > static_cast<size_t>(std::numeric_limits<int>::max()) ||
            !meta.ParseFromArray(body.data() + frame.meta_pos, static_cast<int>(frame.meta_size))) {
        return Error("Could not parse dandelion response metadata.", ErrorKind::InvalidInput);
    }

    size_t pos = frame.payload_pos;
    DataSetVec out_sets;
    out_sets.reserve(meta.sets_size());
    for (const auto &pb_set : meta.sets()) {
        DataItemVec out_items;
        out_items.reserve(pb_set.items_size());
        for (const auto &pb_item : pb_set.items()) {
            const uint64_t len = pb_item.payload_length();
            if (len > body.size() - pos) {
                return Error("Payload of item '" + pb_item.identifier() + "' exceeds response body size.",
                             ErrorKind::InvalidInput);
            }
            auto begin = body.begin() + static_cast<std::ptrdiff_t>(pos);
            out_items.push_back({pb_item.key(), pb_item.identifier(),
                                 BinaryData(begin, begin + static_cast<std::ptrdiff_t>(len))});
            pos += len;
        }
        out_sets.push_back(std::move(out_items));
    }
    if (pos != body.size()) {
        return Error(std::to_string(body.size() - pos) + " trailing bytes after dandelion response payload.",
                     ErrorKind::InvalidInput);
    }

    if (traces) {
        *traces = meta.has_trace() ? meta.trace() : "";
    }
    return out_sets;
}

Result<std::string> ParseAndRenderResponseBody(const BinaryData& data, const Schema &schema,
        std::string* traces) {
    TRY(auto sets, ParseResponseBody(data, traces));
    if (sets.empty()) {
        return "";
    }

    std::string out;
    bool write_header = true;
    for (auto &block : sets[0]) {
        Schema block_schema;
        TRY(auto chunks, memory::ImportBlockChunks(block.data.data(), block.data.size(), block_schema));
        if (block_schema.columns.size() != schema.columns.size()) {
            return Error("Block schema does not match given schema.");
        }
        TRY(auto chunk_str, RenderChunks(chunks, schema, write_header));
        out += chunk_str;
        write_header = false;
    }
    return out;
}

} // namespace plume::dandelion
