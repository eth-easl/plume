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

    std::string ToString() const;
    const bool leads_with_join() const { return type == StageType::JOIN; }
    const bool is_leaf() const { return type == StageType::LEAF; }
};

struct LeafStage : Stage {
    LeafStage(size_t idx) : Stage(StageType::LEAF, idx) {}

    // Data source from the catalog.
    std::shared_ptr<catalog::DataSource> data_source;

    // How many partitions to parallelize the source parsing over.
    uint32_t source_splits = 1;

    // Projection pushdown (emtpyt => no pushed projection).
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
