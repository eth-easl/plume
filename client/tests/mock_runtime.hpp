#pragma once

#include "plume/dandelion/api.hpp"
#include "plume/parser/physical_plan.hpp"

#include "duckdb.hpp"

#include <cstdint>
#include <map>
#include <unordered_map>
#include <vector>

namespace plume::client {

// Raw serialized Plume blocks (one entry per emitted block).
using BlockSet = std::vector<std::vector<uint8_t>>;

// A stage's output grouped by the partition index its OutputSplit tagged each
// block with (the union of every invocation's output). At a single partition this
// is just {0 -> all blocks}.
using PartitionedBlocks = std::map<uint32_t, BlockSet>;

class MockRuntime {
public:
    explicit MockRuntime(duckdb::Connection &con) : con_(con) {}

    // Execute the whole plan; returns the root stage's output blocks (all partitions
    // concatenated — the root gathers into a single result). `provided` supplies
    // pre-materialized blocks for LOCAL_TABLE source stages by stage index (e.g. a
    // caller that already materialized them in its own DuckDB instance); stages
    // absent from the map are materialized from `con_`.
    BlockSet Run(const parser::PhysicalPlan &plan, const std::unordered_map<size_t, BlockSet> &provided = {});

private:
    duckdb::Connection &con_;
    const parser::PhysicalPlan *plan_ = nullptr;
    std::unordered_map<size_t, PartitionedBlocks> stage_outputs_;
    std::unordered_map<size_t, BlockSet> provided_table_blocks_;

    // The number of output partitions a producer stage emits (>= 1).
    uint32_t ProducerPartitions(size_t stage_idx) const;

    // Materialize a base table (the stage's input_schema columns, in order) into
    // serialized Plume blocks.
    BlockSet MaterializeTable(const parser::LeafStage &stage);

    // Run one stage and return its output keyed by partition. Dispatches on the
    // leaf stage's data source: plume_stage for LOCAL_TABLE/non-leaf, or the
    // prepare+fetch+stage handshake for a CSV/parquet file source.
    PartitionedBlocks RunStage(const parser::Stage &stage);
    PartitionedBlocks RunBlockStage(const parser::Stage &stage);
    PartitionedBlocks RunCsvStage(const parser::LeafStage &stage);
    PartitionedBlocks RunParquetStage(const parser::LeafStage &stage);

    // One plume_stage invocation over a single input partition (left = primary,
    // right = join build side), returning its output keyed by partition.
    PartitionedBlocks InvokeBlockStage(const parser::Stage &stage, const BlockSet &left, const BlockSet &right);

    // Resolve a client-built byte-range fetch request against the local file it
    // targets (stands in for the runtime's HTTP fetch), returning the fetched bytes.
    std::vector<uint8_t> ResolveRequest(const dandelion::DataItem &req);
};

} // namespace plume::client
