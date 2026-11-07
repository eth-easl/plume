#pragma once

// Relations: the vertices of the predicate transfer join graph (predicate-transfer.md §4.1).

#include "plume/parser/physical_plan.hpp"
#include "plume/parser/plan_builder.hpp"

#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

namespace plume::parser::transfer {

// A leaf scan together with its chain: the single-input, single-consumer operator nodes above
// it up to (not including) the first join. A scan shared by several consumers (remote fetch
// deduplication, CTEs) also gives one forked relation per branch: those may send filters (from
// their branch's own filters) but not receive them.
struct Relation {
    uint32_t id;
    // Target point: filters into the relation are applied at the scan (probe / prune).
    PlanBuilder::SharedOpNode leaf;
    LeafStage *stage;
    // Source point: filters out of the relation are tapped here, after the scan filter, the
    // incoming filters and the chain's own operators (filters, aggregates, HAVING, ...).
    PlanBuilder::SharedOpNode top;
    // Estimated rows at `top` before any transfer (infinity: unknown).
    double card;
    // Unforked remote parquet scans may receive filters; every relation may send them.
    bool filterable;
};

// A column of a relation, identified by its column at the relation's source point (`top`).
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
    // Discovers one relation per leaf scan reachable from `root`. `node_card` holds DuckDB's
    // estimated cardinalities of nodes built from a logical operator.
    static RelationSet Discover(PlanBuilder &builder, const PlanBuilder::SharedOpNode &root,
                                const std::unordered_map<const PlanBuilder::OperatorNode *, uint64_t> &node_card);

    size_t size() const { return relations_.size(); }
    const Relation &operator[](uint32_t id) const { return relations_[id]; }
    const std::vector<Relation> &All() const { return relations_; }

    // The relation whose source point is `top`, if any.
    std::optional<uint32_t> FindByTop(const PlanBuilder::OperatorNode *top) const;
    // The scan / file column a relation column is read from, if it passes the chain unchanged
    // (else it is computed or aggregated in the chain: it may send filters but not receive them).
    std::optional<PlanBuilder::LeafColumn> LeafColumnOf(RelationColumn col) const;
    // The representative of the relation columns holding the same scan column (the lowest).
    RelationColumn Canonical(RelationColumn col) const;

private:
    std::vector<Relation> relations_;
    std::unordered_map<const PlanBuilder::OperatorNode *, uint32_t> by_top_;
    // Per relation and column of `top`: the leaf column it is read from.
    std::vector<std::vector<std::optional<PlanBuilder::LeafColumn>>> leaf_columns_;
};

} // namespace plume::parser::transfer
