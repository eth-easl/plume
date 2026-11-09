#include "plume/parser/transfer/join_graph.hpp"

#include <algorithm>
#include <map>
#include <optional>
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

} // namespace

JoinGraph JoinGraph::Build(const RelationSet &relations, const std::vector<EquiJoin> &joins,
                           const PlanBuilder::SharedOpNode &root) {
    JoinGraph graph;
    for (const EquiJoin &join : joins) {
        for (size_t i = 0; i < join.left_keys.size(); i++) {
            auto left = TraceKey(relations, join.left, join.left_keys[i], root);
            auto right = TraceKey(relations, join.right, join.right_keys[i], root);
            if (!left || !right) {
                continue;
            }
            graph.classes_.Union(left->column, right->column);
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

} // namespace plume::parser::transfer
