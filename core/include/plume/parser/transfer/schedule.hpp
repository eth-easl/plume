#pragma once

#include "plume/parser/transfer/estimates.hpp"
#include "plume/parser/transfer/join_graph.hpp"
#include "plume/parser/transfer/relation.hpp"

#include <cstdint>
#include <vector>

namespace plume::parser::transfer {

// A transfer the schedule keeps: `edge.source` builds a filter `edge.target` applies.
struct Transfer {
    TransferEdge edge;
    double fraction;        // estimated share of the target's rows the filter keeps
    double source_distinct; // estimated distinct keys the filter carries
    // Upper bound on them: the source's distinct keys before any transfer. Estimates compound
    // along a chain of transfers, so the filter is sized by this bound.
    double source_max_distinct;
    uint32_t depth;         // transfers on the longest chain ending with this one
};

class TransferSchedule {
public:
    struct Options {
        SelectivityThresholds thresholds;
        uint32_t max_depth; // upper limit on a chain of transfers
    };

    static TransferSchedule ForwardPass(const RelationSet &relations, const JoinGraph &graph, const Options &options);

    // Kept transfers, by target relation in scheduling order, then target column.
    const std::vector<Transfer> &Transfers() const { return transfers_; }
    // Relations in scheduling order.
    const std::vector<uint32_t> &Order() const { return order_; }
    // A relation's estimated rows at its source point after the filters it receives.
    double FilteredCard(uint32_t relation) const { return filtered_card_[relation]; }

private:
    std::vector<Transfer> transfers_;
    std::vector<uint32_t> order_;
    std::vector<double> filtered_card_;
};

} // namespace plume::parser::transfer
