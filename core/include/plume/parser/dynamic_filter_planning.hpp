#pragma once

// Dynamic filter planning: what the converter records while building the node graph, the
// helpers the planner uses, and its entry point. Filters are chosen by a forward predicate
// transfer pass over a join graph of relations (transfer/planner.hpp, see predicate-transfer.md).

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
    // Join node -> index into `joins`.
    std::unordered_map<const PlanBuilder::OperatorNode *, size_t> join_index;
    // DuckDB's estimated cardinality of every node built from a logical operator.
    std::unordered_map<const PlanBuilder::OperatorNode *, uint64_t> node_card;
};

// Whether a join drops the rows of its `side` (0: probe/left, 1: build/right) without a match:
// only then may that side be thinned to the rows that can match.
bool DropsUnmatched(exec::JoinKind kind, size_t side);

// The distinct values of a leaf column (its source's row count if unknown, 0 if neither is known).
uint64_t LeafColumnDomain(const PlanBuilder::LeafColumn &leaf);

// Whether the target column's row groups are clustered enough for a filter to prune them before
// the fetch (pq_prepare), judged by their mean key range coverage.
bool RowGroupsPrunable(const PlanBuilder::LeafColumn &target, const ConverterConfig &cfg);

// A dynamic filter edge from `build`'s column into the leaf column `target`, sized for an
// estimated `build_distinct` keys. Exact keys and hashes are only comparable to the target if
// both key columns share a type; otherwise the filter carries the min/max range alone.
PlanBuilder::DynamicFilterEdge MakeDynamicFilterEdge(const ConverterConfig &cfg, PlanBuilder::SharedOpNode build,
                                                     uint32_t build_column, const PlanBuilder::LeafColumn &target,
                                                     double build_distinct, bool prune);

// Chooses the plan's dynamic filters and attaches them to `builder`.
void PlanDynamicFilters(PlanBuilder &builder, const PlanBuilder::SharedOpNode &root, const DynamicFilterContext &ctx,
                        const ConverterConfig &cfg);

} // namespace plume::parser
