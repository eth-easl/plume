#pragma once

#include "plume/common/buffer.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/memory/adapter.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace plume::exec {

using OutputEmit = std::function<void(const std::string &ident, size_t set_idx, DataBuffer buffer, size_t key)>;

// Default byte cap for a coalesced output block (1 MiB).
static constexpr size_t kOutputMaxBytes = 1u << 20;

// Constant + per-row byte factors used to estimate a coalesced block's size.
// The varchar heap term is approximate (see PartitionSink::RunHeap).
struct SizeFactors {
    size_t base_overhead = 0;           // header + descriptors + names
    size_t per_row_fixed = 0;           // sum of physical column widths
    size_t num_nullable = 0;            // columns carrying a validity mask
    std::vector<uint32_t> varchar_cols; // indices of VARCHAR columns
};

SizeFactors ComputeSizeFactors(const Schema &schema);
size_t EstimateBlockBytes(const SizeFactors &f, size_t rows, size_t heap);

struct PartitionBatch {
    uint32_t key = 0;
    std::vector<std::shared_ptr<duckdb::DataChunk>> chunks;    // keep sources alive until flush
    std::vector<memory::ChunkSelection> parts;                 // (chunk*, sel*, count) views
    std::vector<std::unique_ptr<duckdb::SelectionVector>> sels; // the per-run selection (null = identity)
    std::vector<std::shared_ptr<duckdb::SelectionVector>> sel_owners;
    size_t rows = 0;
    size_t heap = 0;  // approximate accumulated varchar bytes
    uint64_t seq = 0; // per-partition emitted-block counter (ident source)
};

// Per-partition streaming accumulator.
class PartitionSink {
public:
    explicit PartitionSink(OutputEmit emit) : emit_(std::move(emit)) {}
    virtual ~PartitionSink() = default;

    // Binds the output schema + partition count, allocating the per-partition batches.
    void Bind(Schema schema, uint32_t partitions);

    // Accept rows sel[0..count) of `chunk` (sel == nullptr => rows [0, count)) as one
    // run for partition p, retaining `chunk` and taking ownership of `sel`. `sel_owner`
    // (optional) keeps a shared selection buffer that `sel` views into alive until flush.
    Result<void> Accept(uint32_t p, std::shared_ptr<duckdb::DataChunk> chunk,
                        std::unique_ptr<duckdb::SelectionVector> sel, duckdb::idx_t count,
                        std::shared_ptr<duckdb::SelectionVector> sel_owner = nullptr);

    // Flush every partition's remainder, then run the finalize hook.
    Result<void> FlushAll();

protected:
    virtual Result<void> FlushBatch(PartitionBatch &b) = 0;
    virtual bool WouldExceed(const PartitionBatch &b, size_t add_rows, size_t add_heap) const = 0;
    virtual Result<void> Finalize() { return Ok(); }

    Result<void> Flush(PartitionBatch &b);
    size_t RunHeap(duckdb::DataChunk &chunk, const duckdb::SelectionVector *sel, duckdb::idx_t count) const;

    Schema schema_;
    uint32_t partitions_ = 1;
    SizeFactors factors_;
    OutputEmit emit_;
    std::vector<PartitionBatch> batches_;
};

class BlockSink : public PartitionSink {
public:
    explicit BlockSink(OutputEmit emit, size_t max_bytes = kOutputMaxBytes)
        : PartitionSink(std::move(emit)), max_bytes_(max_bytes) {}

protected:
    Result<void> FlushBatch(PartitionBatch &b) override;
    bool WouldExceed(const PartitionBatch &b, size_t add_rows, size_t add_heap) const override;

private:
    size_t max_bytes_;
};

} // namespace plume::exec
