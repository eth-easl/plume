#pragma once

// The predicate transfer dynamic filter planner (see predicate-transfer.md): relations -> join
// graph -> forward-pass schedule -> dynamic filter edges.

#include "plume/parser/dynamic_filter_planning.hpp"
#include "plume/parser/plan_builder.hpp"

namespace plume::parser::transfer {

class PredicateTransferPlanner {
public:
    PredicateTransferPlanner(PlanBuilder &builder, const DynamicFilterContext &ctx, const ConverterConfig &cfg)
        : builder_(builder), ctx_(ctx), cfg_(cfg) {}

    // Plans the transfers of the graph under `root` and attaches them as dynamic filters.
    void Plan(const PlanBuilder::SharedOpNode &root);

private:
    PlanBuilder &builder_;
    const DynamicFilterContext &ctx_;
    const ConverterConfig &cfg_;
};

} // namespace plume::parser::transfer
