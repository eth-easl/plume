#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/pipeline.hpp"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace plume::parser {

enum class StageType {
    COMMON,
    JOIN,
    LEAF,
};

inline constexpr uint32_t kNoDynamicFilter = std::numeric_limits<uint32_t>::max();

struct Stage {
    Stage(StageType type, size_t idx) : type(type), idx(idx) {}

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

    // Index of the stage that consumes the dynamic filter produced by this stage.
    uint32_t dynamic_filter_consumer_stage = kNoDynamicFilter;

    std::string ToString() const;
    const bool LeadsWithJoin() const { return type == StageType::JOIN; }
    const bool IsLeaf() const { return type == StageType::LEAF; }
    bool ProducesDynFilter() const { return dynamic_filter_consumer_stage != kNoDynamicFilter; }
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

    // Runtime dynamic filter.
    int32_t dynamic_filter_column = -1;
    uint32_t dynamic_filter_source_stage = kNoDynamicFilter;

    bool HasDynFilter() const { return dynamic_filter_column != -1; }
};

struct PhysicalPlan {
    std::vector<std::unique_ptr<Stage>> stages; // root has idx 0
    Schema output_schema;
    size_t root_idx;

    const Stage &Root() const { return *stages[0]; }

    std::string ToString() const;
};

} // namespace plume::parser
