#pragma once

#include "plume/common/buffer.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/memory/adapter.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace plume::exec {

using OutputEmit = std::function<void(const std::string &ident, size_t set_idx, DataBuffer buffer, size_t key)>;

// Default byte cap for an output block (1 MiB).
static constexpr size_t kOutputMaxBytes = 1u << 20;

struct OutputRows {
    const duckdb::DataChunk &chunk;
    const duckdb::UnifiedVectorFormat *formats; // one per column
    const duckdb::SelectionVector *sel;
    duckdb::idx_t count;
};

// A sink receives the output rows, routed to partitions, and emits them.
class Sink {
public:
    virtual ~Sink() = default;

    // Binds the output schema + partition count.
    virtual void Bind(const Schema &schema, uint32_t partitions) = 0;
    // Appends rows destined for `partition`.
    virtual Result<void> Append(uint32_t partition, const OutputRows &rows) = 0;
    // Emits everything still buffered.
    virtual Result<void> Finish() = 0;
};

// Writes each partition's rows straight into its current output block emitting the block once it 
// holds max_bytes.
class BlockSink final : public Sink {
public:
    explicit BlockSink(OutputEmit emit, size_t max_bytes = kOutputMaxBytes);
    ~BlockSink() override;

    void Bind(const Schema &schema, uint32_t partitions) override;
    Result<void> Append(uint32_t partition, const OutputRows &rows) override;
    Result<void> Finish() override;

private:
    void Emit(uint32_t partition);

    OutputEmit emit_;
    size_t max_bytes_;
    Schema schema_;

    // per partition
    std::vector<std::unique_ptr<memory::BlockWriter>> writers_; // created on first use
    std::vector<uint64_t> seq_;                                 // # of emitted blocks (ident)
};

} // namespace plume::exec
