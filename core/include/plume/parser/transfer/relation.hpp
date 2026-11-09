#pragma once

#include "plume/parser/physical_plan.hpp"
#include "plume/parser/plan_builder.hpp"

#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

namespace plume::parser::transfer {

struct Relation {
    uint32_t id;
    // The target to which the filters are applied to at the scan.
    PlanBuilder::SharedOpNode leaf;
    LeafStage *stage;
    // The source from where the filters are produced from.
    PlanBuilder::SharedOpNode top;
    // Estimated rows at `top` before any transfer (infinity: unknown).
    double card;
    // Whether the target is filterable.
    bool filterable;
};

struct RelationColumn {
    uint32_t relation;
    uint32_t column;

    bool operator==(const RelationColumn &o) const { return relation == o.relation && column == o.column; }
    bool operator<(const RelationColumn &o) const {
        return relation != o.relation ? relation < o.relation : column < o.column;
    }
};

class RelationSet {
public:
    // Discovers one relation per leaf scan reachable from `root`.
    static RelationSet Discover(PlanBuilder &builder, const PlanBuilder::SharedOpNode &root,
                                const std::unordered_map<const PlanBuilder::OperatorNode *, uint64_t> &node_card);

    size_t size() const { return relations_.size(); }
    const Relation &operator[](uint32_t id) const { return relations_[id]; }
    const std::vector<Relation> &All() const { return relations_; }

    std::optional<uint32_t> FindByTop(const PlanBuilder::OperatorNode *top) const;
    std::optional<PlanBuilder::LeafColumn> LeafColumnOf(RelationColumn col) const;
    RelationColumn Canonical(RelationColumn col) const;

private:
    std::vector<Relation> relations_;
    std::unordered_map<const PlanBuilder::OperatorNode *, uint32_t> by_top_;
    // Per relation and column of `top`: the leaf column it is read from.
    std::vector<std::vector<std::optional<PlanBuilder::LeafColumn>>> leaf_columns_;
};

} // namespace plume::parser::transfer
