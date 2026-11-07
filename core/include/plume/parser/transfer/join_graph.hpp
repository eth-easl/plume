#pragma once

// The predicate transfer join graph (predicate-transfer.md §4.2): directed transfer edges between
// relation columns, derived from the equi joins and closed over equal keys.

#include "plume/parser/dynamic_filter_planning.hpp"
#include "plume/parser/plan_builder.hpp"
#include "plume/parser/transfer/key_classes.hpp"
#include "plume/parser/transfer/relation.hpp"

#include <vector>

namespace plume::parser::transfer {

// `source` may filter `target`: every target row whose key is not among the source's keys at
// its source point is dropped by the plan anyway.
struct TransferEdge {
    RelationColumn source;
    RelationColumn target;

    bool operator==(const TransferEdge &o) const { return source == o.source && target == o.target; }
};

class JoinGraph {
public:
    // A join key pair whose sides trace to columns `a` (left) and `b` (right) of two relations
    // gives `a -> b` if the join drops unmatched right rows and `b`'s path from its relation's
    // source point to the join is exclusive (likewise `b -> a`). The edges are then closed
    // transitively: they only connect columns of equal values, and a chain of sound edges is a
    // sound edge.
    static JoinGraph Build(const RelationSet &relations, const std::vector<EquiJoin> &joins,
                           const PlanBuilder::SharedOpNode &root);

    // The edges the joins give directly.
    const std::vector<TransferEdge> &JoinEdges() const { return join_edges_; }
    // The closed edges out of `relation`'s columns.
    const std::vector<TransferEdge> &EdgesFrom(uint32_t relation) const { return closed_from_[relation]; }
    // The key equivalence classes of every traced join key pair, of any join kind.
    const KeyClasses &Classes() const { return classes_; }

private:
    void AddJoinEdge(TransferEdge edge);
    void Close(size_t num_relations);

    std::vector<TransferEdge> join_edges_;
    std::vector<std::vector<TransferEdge>> closed_from_;
    KeyClasses classes_;
};

} // namespace plume::parser::transfer
