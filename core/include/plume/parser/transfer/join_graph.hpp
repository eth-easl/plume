#pragma once

#include "plume/parser/dynamic_filter_planning.hpp"
#include "plume/parser/plan_builder.hpp"
#include "plume/parser/transfer/key_classes.hpp"
#include "plume/parser/transfer/relation.hpp"

#include <vector>

namespace plume::parser::transfer {

struct TransferEdge {
    RelationColumn source;
    RelationColumn target;

    bool operator==(const TransferEdge &o) const { return source == o.source && target == o.target; }
};

class JoinGraph {
public:
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
