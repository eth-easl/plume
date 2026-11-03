#pragma once

// Test/bench helper: drives the production block output path (output operator + BlockSink)
// over in-memory chunks and collects the emitted blocks.

#include "plume/common/buffer.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/operator.hpp"
#include "plume/execution/operators/output.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/execution/sink.hpp"

#include "duckdb/common/types/data_chunk.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace plume_test {

// One serialized output block destined for a partition.
struct SplitBlock {
    uint32_t key = 0; // partition index
    plume::DataBuffer buffer;
};

// Routes the rows of `chunks` (laid out per `schema`) into Plume blocks per `split`, returning
// the emitted blocks tagged with their partition. Borrows `chunks` (does not consume them).
inline plume::Result<std::vector<SplitBlock>> SplitOutput(const plume::exec::OutputSplit &split,
                                                          const plume::Schema &schema,
                                                          const plume::exec::ChunkList &chunks,
                                                          size_t max_bytes = plume::exec::kOutputMaxBytes) {
    using namespace plume;
    std::vector<SplitBlock> out;
    exec::OutputEmit emit = [&](const std::string &, size_t, DataBuffer buffer, size_t key) {
        out.push_back({static_cast<uint32_t>(key), std::move(buffer)});
    };
    const uint32_t n = split.partitions == 0 ? 1 : split.partitions;
    auto op = exec::MakeOutputOperator(schema, split.key_columns, n,
                                       std::make_unique<exec::BlockSink>(emit, max_bytes));

    duckdb::vector<duckdb::LogicalType> types;
    types.reserve(schema.size());
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }
    // Push a zero-copy reference of each borrowed chunk so `chunks` is not consumed
    // (the operator retains the reference until the partition flushes).
    for (auto &c : chunks) {
        if (!c || c->size() == 0) {
            continue;
        }
        auto ref = std::make_unique<duckdb::DataChunk>();
        ref->InitializeEmpty(types); // allocate the vector slots Reference() writes into
        ref->Reference(*c);
        TRYV(op->Push(std::move(ref)));
    }
    TRYV(op->Finish());
    return out;
}

} // namespace plume_test
