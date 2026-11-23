#pragma once

#include "plume/execution/operators/join.hpp"
#include "plume/parser/plan_builder.hpp"

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace plume::parser {

struct ConverterConfig;

// An equi join of the node graph.
struct EquiJoin {
    PlanBuilder::SharedOpNode left;  // probe input
    PlanBuilder::SharedOpNode right; // build input
    exec::JoinKind kind;
    std::vector<uint32_t> left_keys;
    std::vector<uint32_t> right_keys;
    std::vector<bool> keys_match_null; // IS NOT DISTINCT FROM: a NULL key matches
};

// What the converter records while building the node graph, for planning once it is complete.
struct DynamicFilterContext {
    std::vector<EquiJoin> joins;
    // Index for every join node into `joins`.
    std::unordered_map<const PlanBuilder::OperatorNode *, size_t> join_index;
    // DuckDB's estimated cardinality of every operator node.
    std::unordered_map<const PlanBuilder::OperatorNode *, uint64_t> node_card;
};

// Whether a join drops the rows of its `side` (0: probe/left, 1: build/right) without a match.
bool DropsUnmatched(exec::JoinKind kind, size_t side);

// The distinct values of a leaf column (if unknown its source's row count or 0 if neither is known).
uint64_t LeafColumnDomain(const PlanBuilder::LeafColumn &leaf);

// Determines the plan's dynamic filters with predicate transfer and attaches them to `builder`.
void PlanDynamicFilters(PlanBuilder &builder, const PlanBuilder::SharedOpNode &root, 
    const DynamicFilterContext &ctx, const ConverterConfig &cfg);

} // namespace plume::parser
