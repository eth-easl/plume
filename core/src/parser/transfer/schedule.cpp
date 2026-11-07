#include "plume/parser/transfer/schedule.hpp"

#include <algorithm>
#include <map>
#include <optional>

namespace plume::parser::transfer {

namespace {

// Whether offer `a` beats `b` for the same target column: more selective, then a shorter chain.
bool Better(const Transfer &a, const Transfer &b) {
    if (a.fraction != b.fraction) {
        return a.fraction < b.fraction;
    }
    return a.depth < b.depth;
}

} // namespace

TransferSchedule TransferSchedule::ForwardPass(const RelationSet &relations, const JoinGraph &graph,
                                               const Options &options) {
    const size_t n = relations.size();
    TransferSchedule schedule;
    schedule.filtered_card_.resize(n);
    for (uint32_t r = 0; r < n; r++) {
        schedule.filtered_card_[r] = relations[r].card;
    }
    std::vector<uint32_t> depth(n, 0);
    std::vector<bool> scheduled(n, false);
    // Per target relation: its kept offer per column.
    std::vector<std::map<uint32_t, Transfer>> offers(n);

    for (size_t step = 0; step < n; step++) {
        // The smallest unscheduled relation by its size before any transfer: a relation's filters
        // must all be in before it sends its own. Ties go to the one the filters it received
        // shrank most, then to the lower id.
        std::optional<uint32_t> next;
        for (uint32_t r = 0; r < n; r++) {
            if (scheduled[r]) {
                continue;
            }
            if (!next || relations[r].card < relations[*next].card ||
                (relations[r].card == relations[*next].card &&
                 schedule.filtered_card_[r] < schedule.filtered_card_[*next])) {
                next = r;
            }
        }
        const uint32_t src = *next;
        scheduled[src] = true;
        schedule.order_.push_back(src);
        if (depth[src] >= options.max_depth) {
            continue; // any filter of it would make the chain too long
        }

        std::vector<uint32_t> touched;
        for (const TransferEdge &edge : graph.EdgesFrom(src)) {
            const uint32_t dst = edge.target.relation;
            if (scheduled[dst] || !relations[dst].filterable || !relations.LeafColumnOf(edge.target)) {
                continue;
            }
            // Source and target share a class, hence a domain. It is only trusted if known apart
            // from the target: the target's alone (often just its row count) says nothing about
            // which share of it the source's keys match.
            double domain = graph.Classes().Domain(relations, edge.source, edge.target);
            if (domain > 0) {
                domain = graph.Classes().Domain(relations, edge.source);
            }
            const double distinct = DistinctKeys(schedule.filtered_card_[src], domain);
            const Selectivity sel =
                EstimateSelectivity(distinct, domain, relations[dst].card, options.thresholds);
            if (!sel.passes) {
                continue;
            }
            const double max_distinct = DistinctKeys(relations[src].card, domain);
            const Transfer offer{edge, sel.fraction, distinct, max_distinct, depth[src] + 1};
            auto [it, inserted] = offers[dst].try_emplace(edge.target.column, offer);
            if (!inserted && Better(offer, it->second)) {
                it->second = offer;
            }
            touched.push_back(dst);
        }

        // Re-estimate the targets from their kept offers (independent filters).
        for (uint32_t dst : touched) {
            double card = relations[dst].card;
            uint32_t d = 0;
            for (const auto &[_, offer] : offers[dst]) {
                card *= offer.fraction;
                d = std::max(d, offer.depth);
            }
            schedule.filtered_card_[dst] = card;
            depth[dst] = d;
        }
    }

    for (uint32_t r : schedule.order_) {
        for (const auto &[_, offer] : offers[r]) {
            schedule.transfers_.push_back(offer);
        }
    }
    return schedule;
}

} // namespace plume::parser::transfer
