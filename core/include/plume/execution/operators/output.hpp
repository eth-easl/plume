#pragma once

#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/operator.hpp"
#include "plume/execution/sink.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace plume::exec {

inline uint32_t PartitionOf(uint64_t hash, uint32_t partitions) {
    return static_cast<uint32_t>((((hash >> 16) & 0xFFFFFFFFu) * partitions) >> 32);
}

// Final operator that hands every chunk to the sink.
class OutputOperator : public Operator {
public:
    OutputOperator(const Schema &schema, std::unique_ptr<Sink> sink, 
                   std::vector<uint32_t> key_columns, uint32_t partitions);

    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;
    Result<void> Finish() override;

private:
    Result<void> Route(const duckdb::DataChunk &chunk);

    std::unique_ptr<Sink> sink_;
    std::vector<uint32_t> key_columns_;
    uint32_t partitions_;
    
    // Per-chunk scratch.
    std::vector<duckdb::UnifiedVectorFormat> formats_; // per column
    std::vector<uint32_t> partition_ids_;              // per row
    std::vector<duckdb::idx_t> offsets_, fill_;        // per partition
    std::vector<duckdb::sel_t> sel_;                   // the rows, grouped by partition
};

std::unique_ptr<Operator> MakeOutputOperator(const Schema &schema, std::vector<uint32_t> key_columns,
                                             uint32_t partitions, std::unique_ptr<Sink> sink);

} // namespace plume::exec
