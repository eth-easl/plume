#include "plume/parser/transfer/relation.hpp"

#include "plume/catalog/catalog.hpp"

#include <limits>
#include <unordered_set>

namespace plume::parser::transfer {

namespace {

using SharedOpNode = PlanBuilder::SharedOpNode;

bool IsLeaf(const PlanBuilder::OperatorNode &node) {
    return !node.templ && !node.sources[0];
}

// Every leaf reachable from `root`, in a deterministic (depth-first, left to right) order,
// and every node reachable from it in `visited`.
void CollectLeaves(const SharedOpNode &node, std::unordered_set<const PlanBuilder::OperatorNode *> &visited,
                   std::vector<SharedOpNode> &leaves) {
    if (!node || !visited.insert(node.get()).second) {
        return;
    }
    if (IsLeaf(*node)) {
        leaves.push_back(node);
        return;
    }
    for (const auto &src : node->sources) {
        CollectLeaves(src, visited, leaves);
    }
}

} // namespace

RelationSet RelationSet::Discover(PlanBuilder &builder, const SharedOpNode &root,
                                  const std::unordered_map<const PlanBuilder::OperatorNode *, uint64_t> &node_card) {
    std::unordered_set<const PlanBuilder::OperatorNode *> reachable;
    std::vector<SharedOpNode> leaves;
    CollectLeaves(root, reachable, leaves);

    RelationSet set;
    auto add = [&](const SharedOpNode &leaf, LeafStage *stage, const std::vector<SharedOpNode> &chain, bool forked) {
        // The topmost estimate on the chain, else the source's row count.
        double card = std::numeric_limits<double>::infinity();
        for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
            if (auto c = node_card.find(it->get()); c != node_card.end()) {
                card = static_cast<double>(c->second);
                break;
            }
        }
        if (card == std::numeric_limits<double>::infinity() && stage->data_source->cardinality_total > 0) {
            card = static_cast<double>(stage->data_source->cardinality_total);
        }

        Relation rel;
        rel.id = static_cast<uint32_t>(set.relations_.size());
        rel.leaf = leaf;
        rel.stage = stage;
        rel.top = chain.back();
        rel.card = card;
        // A filter at a forked chain's scan would thin the other branches too.
        rel.filterable = !forked && stage->data_source->type == catalog::DataSourceType::REMOTE_PARQUET;

        // Where each of the top's columns is read from.
        std::vector<std::optional<PlanBuilder::LeafColumn>> columns;
        const auto ncol = static_cast<uint32_t>(rel.top->output_schema.columns.size());
        for (uint32_t c = 0; c < ncol; c++) {
            auto traced = builder.TraceLeafColumn(rel.top, c);
            columns.push_back(traced && traced->node == leaf ? traced : std::nullopt);
        }

        set.by_top_.emplace(rel.top.get(), rel.id);
        set.leaf_columns_.push_back(std::move(columns));
        set.relations_.push_back(std::move(rel));
    };

    // Walks up the chain from its last node: through a node whose only consumer reads only it.
    // Where the chain forks into several single-input consumers, it ends (a relation of its own,
    // e.g. for a join reading the shared node) and every branch continues as a forked relation.
    auto walk = [&](auto &self, const SharedOpNode &leaf, LeafStage *stage, std::vector<SharedOpNode> chain,
                    bool forked) -> void {
        const SharedOpNode &cur = chain.back();
        std::vector<SharedOpNode> consumers;
        if (cur != root) {
            for (const auto &c : cur->consumers) {
                if (reachable.count(c.get())) {
                    consumers.push_back(c);
                }
            }
        }
        if (consumers.size() == 1 && !consumers[0]->sources[1]) {
            chain.push_back(consumers[0]);
            self(self, leaf, stage, std::move(chain), forked);
            return;
        }
        add(leaf, stage, chain, forked);
        if (consumers.size() > 1) {
            for (const auto &c : consumers) {
                if (!c->sources[1]) {
                    auto branch = chain;
                    branch.push_back(c);
                    self(self, leaf, stage, std::move(branch), /*forked=*/true);
                }
            }
        }
    };

    for (const auto &leaf : leaves) {
        LeafStage *stage = builder.LeafStageAt(leaf->stage_idx);
        if (stage && stage->data_source) {
            walk(walk, leaf, stage, {leaf}, /*forked=*/false);
        }
    }
    return set;
}

std::optional<uint32_t> RelationSet::FindByTop(const PlanBuilder::OperatorNode *top) const {
    auto it = by_top_.find(top);
    if (it == by_top_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<PlanBuilder::LeafColumn> RelationSet::LeafColumnOf(RelationColumn col) const {
    const auto &columns = leaf_columns_[col.relation];
    if (col.column >= columns.size()) {
        return std::nullopt;
    }
    return columns[col.column];
}

RelationColumn RelationSet::Canonical(RelationColumn col) const {
    const auto leaf = LeafColumnOf(col);
    if (!leaf) {
        return col;
    }
    const auto &columns = leaf_columns_[col.relation];
    for (uint32_t c = 0; c < col.column; c++) {
        if (columns[c] && columns[c]->scan_column == leaf->scan_column) {
            return {col.relation, c};
        }
    }
    return col;
}

} // namespace plume::parser::transfer
