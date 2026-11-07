#include "plume/parser/transfer/planner.hpp"

#include "plume/parser/converter.hpp"
#include "plume/parser/transfer/join_graph.hpp"
#include "plume/parser/transfer/relation.hpp"
#include "plume/parser/transfer/schedule.hpp"

namespace plume::parser::transfer {

void PredicateTransferPlanner::Plan(const PlanBuilder::SharedOpNode &root) {
    const RelationSet relations = RelationSet::Discover(builder_, root, ctx_.node_card);
    const JoinGraph graph = JoinGraph::Build(relations, ctx_.joins, root);

    TransferSchedule::Options options;
    options.thresholds.max_match_fraction = cfg_.dynamic_filter_max_match_fraction;
    options.thresholds.max_card_fraction = cfg_.dynamic_filter_selectivity_threshold;
    options.max_depth = cfg_.dynamic_filter_max_chain_depth;
    const TransferSchedule schedule = TransferSchedule::ForwardPass(relations, graph, options);

    for (const Transfer &transfer : schedule.Transfers()) {
        const Relation &source = relations[transfer.edge.source.relation];
        const PlanBuilder::LeafColumn target = *relations.LeafColumnOf(transfer.edge.target);

        auto edge = MakeDynamicFilterEdge(cfg_, source.top, transfer.edge.source.column, target,
                                          transfer.source_max_distinct, /*prune=*/false);
        // Waiting for the filter before fetching only pays off if it can prune row groups: the
        // row groups must be clustered on the key, and the filter must carry its exact keys
        // (a Bloom filter only prunes by its min/max range, which spans most of the file).
        edge.prune = RowGroupsPrunable(target, cfg_) && edge.max_values > 0 &&
                     transfer.source_max_distinct <= static_cast<double>(edge.max_values);
        builder_.AddDynamicFilter(std::move(edge));
    }
    builder_.AttachDynamicFilters();
}

} // namespace plume::parser::transfer
