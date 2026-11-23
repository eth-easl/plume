#include "plume/parser/transfer/join_graph.hpp"

#include <algorithm>
#include <cmath>
#include <set>

namespace plume::parser::transfer {

namespace {

struct TracedKey {
    RelationColumn column;
    bool exclusive;
};

std::optional<TracedKey> TraceKey(const RelationSet &relations, const PlanBuilder::SharedOpNode &input, uint32_t key,
                                  const PlanBuilder::SharedOpNode &root) {
    PlanBuilder::SharedOpNode cur = input;
    uint32_t col = key;
    bool exclusive = true;
    while (true) {
        exclusive = exclusive && cur->consumers.size() == 1 && cur != root;
        if (auto relation = relations.FindByTop(cur.get())) {
            return TracedKey{relations.Canonical({*relation, col}), exclusive};
        }
        auto step = PlanBuilder::TraceColumnStep(*cur, col);
        if (!step) {
            return std::nullopt;
        }
        col = step->column;
        cur = cur->sources[step->src_idx];
    }
}

struct Selectivity {
    double fraction; // estimated share of the target rows the filter keeps
    bool passes;     // below the applicable threshold
};

Selectivity EstimateSelectivity(double source_distinct, double key_domain, double target_card,
                                const ForwardPassOptions &options) {
    if (!std::isfinite(source_distinct)) {
        return {1.0, false};
    }
    if (key_domain > 0) {
        const double fraction = std::min(1.0, source_distinct / key_domain);
        return {fraction, fraction <= options.max_match_fraction};
    }
    if (target_card <= 0 || !std::isfinite(target_card)) {
        return {1.0, false};
    }
    const double fraction = std::min(1.0, source_distinct / target_card);
    return {fraction, fraction <= options.max_card_fraction};
}

inline double DistinctKeys(double card, double domain) {
    return domain > 0 ? std::min(card, domain) : card;
}

inline bool Better(const Transfer &a, const Transfer &b) {
    if (a.fraction != b.fraction) {
        return a.fraction < b.fraction;
    }
    return a.depth < b.depth;
}

} // namespace

JoinGraph JoinGraph::Build(const RelationSet &relations, const std::vector<EquiJoin> &joins,
                           const PlanBuilder::SharedOpNode &root) {
    JoinGraph graph;
    std::vector<std::pair<RelationColumn, RelationColumn>> keys;
    for (const EquiJoin &join : joins) {
        for (size_t i = 0; i < join.left_keys.size(); i++) {
            auto left = TraceKey(relations, join.left, join.left_keys[i], root);
            auto right = TraceKey(relations, join.right, join.right_keys[i], root);
            if (!left || !right) {
                continue;
            }
            keys.emplace_back(left->column, right->column);
            if (join.keys_match_null[i] || left->column.relation == right->column.relation) {
                continue; // NULL keys match: a filter would wrongly reject them
            }
            if (DropsUnmatched(join.kind, 1) && right->exclusive) {
                graph.AddJoinEdge({left->column, right->column});
            }
            if (DropsUnmatched(join.kind, 0) && left->exclusive) {
                graph.AddJoinEdge({right->column, left->column});
            }
        }
    }
    graph.Close(relations.size());
    graph.BuildKeyClasses(relations, keys);
    return graph;
}

void JoinGraph::AddJoinEdge(TransferEdge edge) {
    if (std::find(join_edges_.begin(), join_edges_.end(), edge) == join_edges_.end()) {
        join_edges_.push_back(edge);
    }
}

void JoinGraph::Close(size_t num_relations) {
    std::map<RelationColumn, std::vector<RelationColumn>> adjacent;
    for (const auto &e : join_edges_) {
        adjacent[e.source].push_back(e.target);
    }

    closed_from_.assign(num_relations, {});
    for (const auto &[source, _] : adjacent) {
        // Every column reachable from `source` (all hold values equal to it).
        std::set<RelationColumn> reached;
        std::vector<RelationColumn> stack{source};
        while (!stack.empty()) {
            const RelationColumn cur = stack.back();
            stack.pop_back();
            auto it = adjacent.find(cur);
            if (it == adjacent.end()) {
                continue;
            }
            for (const auto &next : it->second) {
                if (reached.insert(next).second) {
                    stack.push_back(next);
                }
            }
        }
        for (const auto &target : reached) {
            if (target.relation != source.relation) {
                closed_from_[source.relation].push_back({source, target});
            }
        }
    }
}

void JoinGraph::BuildKeyClasses(const RelationSet &relations,
                                const std::vector<std::pair<RelationColumn, RelationColumn>> &keys) {
    // Unlike the edges, every joined key pair counts: any join of two columns says they share a
    // domain, whether or not a filter may cross it.
    std::map<RelationColumn, std::vector<RelationColumn>> adjacent;
    for (const auto &[a, b] : keys) {
        adjacent[a].push_back(b);
        adjacent[b].push_back(a);
    }
    for (const auto &[start, _] : adjacent) {
        if (key_class_.count(start)) {
            continue;
        }
        const auto id = static_cast<uint32_t>(key_classes_.size());
        auto &members = key_classes_.emplace_back();
        std::vector<RelationColumn> stack{start};
        key_class_.emplace(start, id);
        while (!stack.empty()) {
            const RelationColumn cur = stack.back();
            stack.pop_back();
            const auto leaf = relations.LeafColumnOf(cur);
            members.push_back({cur, leaf ? static_cast<double>(LeafColumnDomain(*leaf)) : 0.0});
            for (const auto &next : adjacent[cur]) {
                if (key_class_.emplace(next, id).second) {
                    stack.push_back(next);
                }
            }
        }
    }
}

double JoinGraph::KeyDomain(RelationColumn col, std::optional<RelationColumn> exclude) const {
    auto it = key_class_.find(col);
    if (it == key_class_.end()) {
        return 0.0;
    }
    double domain = 0;
    for (const KeyMember &member : key_classes_[it->second]) {
        if (exclude && *exclude == member.column) {
            continue;
        }
        if (member.domain > 0 && (domain == 0 || member.domain < domain)) {
            domain = member.domain;
        }
    }
    return domain;
}

std::vector<Transfer> JoinGraph::ForwardPass(const RelationSet &relations, const ForwardPassOptions &options) const {
    const size_t n = relations.size();
    std::vector<double> filtered_card(n);
    for (uint32_t r = 0; r < n; r++) {
        filtered_card[r] = relations[r].card;
    }
    std::vector<uint32_t> order;
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
                (relations[r].card == relations[*next].card && filtered_card[r] < filtered_card[*next])) {
                next = r;
            }
        }
        const uint32_t src = *next;
        scheduled[src] = true;
        order.push_back(src);
        if (depth[src] >= options.max_depth) {
            continue; // any filter of it would make the chain too long
        }

        std::vector<uint32_t> touched;
        for (const TransferEdge &edge : EdgesFrom(src)) {
            const uint32_t dst = edge.target.relation;
            if (scheduled[dst] || !relations[dst].filterable || !relations.LeafColumnOf(edge.target)) {
                continue;
            }
            // Source and target share a class, hence a domain. It is only trusted if known apart
            // from the target: the target's alone (often just its row count) says nothing about
            // which share of it the source's keys match.
            double domain = KeyDomain(edge.source, edge.target);
            if (domain > 0) {
                domain = KeyDomain(edge.source);
            }
            const double distinct = DistinctKeys(filtered_card[src], domain);
            const Selectivity sel = EstimateSelectivity(distinct, domain, relations[dst].card, options);
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
            filtered_card[dst] = card;
            depth[dst] = d;
        }
    }

    std::vector<Transfer> transfers;
    for (uint32_t r : order) {
        for (const auto &[_, offer] : offers[r]) {
            transfers.push_back(offer);
        }
    }
    return transfers;
}

} // namespace plume::parser::transfer
