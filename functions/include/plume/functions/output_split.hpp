#pragma once

#include "plume/common/buffer.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/operator.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/execution/sink.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace plume::fn {

// Maximum size (bytes) of a single coalesced output block/CSV file.
static constexpr size_t kSplitMaxBytes = exec::kOutputMaxBytes;

// Target row count per emitted parquet file (== one row group). A partition's rows
// are packed into files of up to this many rows; each file is a complete,
// single-row-group parquet file.
static constexpr size_t kParquetRowGroupRows = 122880;

// Build the output sink for `sink` emitting via `emit` (footprint of abi::AddOutput):
// PLUME_BLOCKS -> BlockSink; CSV/PARQUET -> EncodedSink. Hand it to Executor::Build,
// which binds the output schema/split and wraps it in the routing operator.
std::unique_ptr<exec::PartitionSink> MakeSink(const exec::OutputSink &sink, exec::OutputEmit emit);

// One serialized output block destined for a partition (test/bench helper output).
struct SplitBlock {
    uint32_t key = 0; // partition index
    DataBuffer buffer;
};

// Test/bench helper: route the rows of `chunks` (laid out per `schema`) into Plume
// blocks per `split`, returning the emitted blocks tagged with their partition.
// Borrows `chunks` (does not consume them). Mirrors the production block path.
Result<std::vector<SplitBlock>> SplitOutput(const exec::OutputSplit &split, const Schema &schema,
                                            const exec::ChunkList &chunks, size_t max_bytes = kSplitMaxBytes);

} // namespace plume::fn
