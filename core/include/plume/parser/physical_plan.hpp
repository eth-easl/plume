#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/pipeline.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace plume::parser {

enum class StageType {
    COMMON,
    JOIN,
    LEAF,
};

// A dynamic filter a stage consumes.
struct DynamicFilterInput {
    // Id of the filter (the build template's filter_id).
    uint32_t filter_id;
    // Index of the stage producing the filter.
    uint32_t source_stage;
    // The key column in the scan schema (pq_stage probe).
    uint32_t scan_column;
    // The key column in the file schema (pq_prepare pruning / page selection).
    uint32_t file_column;
    // Whether pq_prepare waits for this filter to prune row groups and pages before fetching.
    bool prune = false;
};

struct Stage {
    Stage(StageType type, size_t idx) : type(type), idx(idx) {}
    virtual ~Stage() = default;

    // The type of the stage.
    StageType type;
    // Index in the physical plan.
    size_t idx;

    // Upstream stage indices (empty for leaf stages).
    std::vector<size_t> input_stages;

    // The pipeline this stage runs.
    exec::PipelineTemplate pipeline;
    // The output schema of this stage.
    Schema output_schema;

    // Ids of the dynamic filters this stage emits (one per DynamicFilterBuildTemplate).
    std::vector<uint32_t> dynamic_filter_outputs;
    // The dynamic filters this stage consumes.
    std::vector<DynamicFilterInput> dynamic_filter_inputs;

    std::string ToString() const;
    const bool LeadsWithJoin() const { return type == StageType::JOIN; }
    const bool IsLeaf() const { return type == StageType::LEAF; }
    bool ProducesDynFilter() const { return !dynamic_filter_outputs.empty(); }
    bool ConsumesDynFilter() const { return !dynamic_filter_inputs.empty(); }
    bool PrunesByDynFilter() const;
    // The distinct stages producing the consumed dynamic filters in ascending order.
    std::vector<uint32_t> DynFilterSourceStages(bool prune_only = false) const;
};

struct LeafStage : Stage {
    LeafStage(size_t idx) : Stage(StageType::LEAF, idx) {}

    // Data source from the catalog.
    std::shared_ptr<catalog::DataSource> data_source;

    // How many partitions to parallelize the source parsing over.
    uint32_t source_splits = 1;

    // Projection pushdown (empty => no pushed projection).
    std::shared_ptr<std::vector<uint32_t>> projection;
    // Filter pushdown.
    std::shared_ptr<expr::ExprNode> pushed_filter;
};

struct PhysicalPlan {
    std::vector<std::unique_ptr<Stage>> stages; // root has idx 0
    Schema output_schema;
    size_t root_idx;

    const Stage &Root() const { return *stages[0]; }

    std::string ToString() const;
};

} // namespace plume::parser
