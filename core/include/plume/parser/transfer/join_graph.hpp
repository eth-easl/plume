#pragma once

#include "plume/parser/dynamic_filter_planning.hpp"
#include "plume/parser/plan_builder.hpp"
#include "plume/parser/transfer/relation.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <vector>

namespace plume::parser::transfer {

struct TransferEdge {
    RelationColumn source;
    RelationColumn target;

    bool operator==(const TransferEdge &o) const { return source == o.source && target == o.target; }
};

// A transfer the forward pass keeps: `edge.source` builds a filter `edge.target` applies.
struct Transfer {
    TransferEdge edge;
    double fraction;        // estimated share of the target's rows the filter keeps
    double source_distinct; // estimated distinct keys the filter carries
    double source_max_distinct; // upper bound on source's distinct keys before any transfer
    uint32_t depth;         // transfers on the longest chain ending with this one
};

struct ForwardPassOptions {
    // The max share of the keys a source keeps to pass.
    double max_match_fraction;
    // The max distinct keys of the source relative to the target's rows to pass.
    double max_card_fraction;
    // Upper limit on a chain of transfers.
    uint32_t max_depth;
};

class JoinGraph {
public:
    static JoinGraph Build(const RelationSet &relations, const std::vector<EquiJoin> &joins,
                           const PlanBuilder::SharedOpNode &root);

    // The edges the joins give directly.
    const std::vector<TransferEdge> &JoinEdges() const { return join_edges_; }
    // The closed edges out of `relation`'s columns.
    const std::vector<TransferEdge> &EdgesFrom(uint32_t relation) const { return closed_from_[relation]; }

    // The distinct values of a join key.
    double KeyDomain(RelationColumn col, std::optional<RelationColumn> exclude = std::nullopt) const;

    // Schedules the predicate transfers in one forward pass (relations smallest first).
    std::vector<Transfer> ForwardPass(const RelationSet &relations, const ForwardPassOptions &options) const;

private:
    struct KeyMember {
        RelationColumn column;
        double domain; // the column's own (0: unknown)
    };

    void AddJoinEdge(TransferEdge edge);
    void Close(size_t num_relations);
    void BuildKeyClasses(const RelationSet &relations, const std::vector<std::pair<RelationColumn, RelationColumn>> &keys);

    std::vector<TransferEdge> join_edges_;
    std::vector<std::vector<TransferEdge>> closed_from_;

    // Key equivalence classes: the columns joined (transitively) to each other.
    std::map<RelationColumn, uint32_t> key_class_;
    std::vector<std::vector<KeyMember>> key_classes_;
};

} // namespace plume::parser::transfer
