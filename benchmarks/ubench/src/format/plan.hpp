#pragma once

#include "plume/common/result.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace plume::ubench {

// Mirrors plume::catalog::DataSourceType (kept independent so the format has no
// dependency on core's SQL-compile layer).
enum class UbSourceKind : uint8_t {
    STAGE_OUTPUT = 0, // one/two upstream stages' block outputs
    TABLE_BLOCKS = 1, // a base table, materialized to Plume blocks at export time
    CSV = 2,          // a CSV file source (resolved to local reads by the runner)
    PARQUET = 3,      // a parquet file source
};

// A keyed + identified data buffer, matching the (key, ident, bytes) an ABI
// InputItem / client SourceItem carries. The runner replays these into the
// per-thread ABI input sets.
struct UbItem {
    uint64_t key = 0;
    std::string ident;
    std::vector<uint8_t> data;
};

// One stage: a single function-invocation pipeline. `partitions` is the stage's
// output fan-out (pipeline.output_split.partitions); the runner runs one
// invocation per producer partition of a consuming stage.
struct UbStage {
    int32_t id = 0;
    UbSourceKind source = UbSourceKind::STAGE_OUTPUT;
    uint32_t partitions = 1;
    bool leads_with_join = false;

    // Upstream stage ids in pipeline-input order. For a join: {probe/left, build/right}.
    std::vector<int32_t> input_stages;

    // The serialized pipeline template (core SerializePipeline bytes).
    std::vector<uint8_t> pipeline_blob;

    // TABLE_BLOCKS only: the materialized input blocks (one item per block).
    std::vector<UbItem> table_blocks;

    // CSV/PARQUET only: the precomputed chunk-info / region-info items (fed to the
    // stage as input set 1) and the byte-range fetch requests (input set 2, after
    // the runner resolves each against the local filesystem).
    std::vector<UbItem> source_info;
    std::vector<UbItem> source_reqs;
};

// A whole compiled query: a topologically-ordered DAG of stages (a stage's inputs
// always precede it) plus the id of the stage producing the result.
struct UbPlan {
    int32_t root_stage = -1;
    std::vector<UbStage> stages;

    std::string name; // informational (the query name / label)
};

// Encode a plan to the .ub binary format.
std::vector<uint8_t> WriteUbPlan(const UbPlan &plan);

// Decode a .ub binary. Fails (Error) on a bad magic/version or truncated input.
Result<UbPlan> ReadUbPlan(const uint8_t *data, size_t size);

inline Result<UbPlan> ReadUbPlan(const std::vector<uint8_t> &bytes) {
    return ReadUbPlan(bytes.data(), bytes.size());
}

} // namespace plume::ubench
