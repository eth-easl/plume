#pragma once

#include "plume/execution/pipeline.hpp"
#include "plume/execution/sink.hpp"

#include <memory>

namespace plume::util {

// Maximum size (bytes) of a single coalesced output block/CSV file.
static constexpr size_t kSplitMaxBytes = exec::kOutputMaxBytes;

// Target row count per emitted parquet file (== one row group).
static constexpr size_t kParquetRowGroupRows = 122880;

// Builds the output sink for `sink` emitting via `emit`.
std::unique_ptr<exec::PartitionSink> MakeSink(const exec::OutputSink &sink, exec::OutputEmit emit);

} // namespace plume::util
