#include "plume/parser/dynamic_filter_planning.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/parquet/prune.hpp"
#include "plume/parser/converter.hpp"
#include "plume/parser/transfer/planner.hpp"

#include <algorithm>

namespace plume::parser {

bool DropsUnmatched(exec::JoinKind kind, size_t side) {
    using K = exec::JoinKind;
    switch (kind) {
    case K::INNER:
    case K::SEMI:
    case K::RIGHT_SEMI:
        return true;
    case K::RIGHT:
        return side == 0;
    case K::LEFT:
        return side == 1;
    default:
        // OUTER preserves both sides, ANTI/RIGHT_ANTI/MARK/SINGLE output depends on the misses.
        return false;
    }
}

uint64_t LeafColumnDomain(const PlanBuilder::LeafColumn &leaf) {
    const auto &src = *leaf.leaf->data_source;
    if (leaf.file_column < src.col_stats.size() && src.col_stats[leaf.file_column].has_distinct) {
        return src.col_stats[leaf.file_column].distinct_count;
    }
    return src.cardinality_total;
}

bool RowGroupsPrunable(const PlanBuilder::LeafColumn &target, const ConverterConfig &cfg) {
    if (target.leaf->data_source->type != catalog::DataSourceType::REMOTE_PARQUET) {
        return false;
    }
    const auto &src = static_cast<const catalog::RemoteParquetDataSource &>(*target.leaf->data_source);
    const auto coverage = parquet::MeanRowGroupCoverage(src.metadata, target.file_column);
    return coverage && *coverage <= cfg.dynamic_filter_max_rowgroup_coverage;
}

PlanBuilder::DynamicFilterEdge MakeDynamicFilterEdge(const ConverterConfig &cfg, PlanBuilder::SharedOpNode build,
                                                     uint32_t build_column, const PlanBuilder::LeafColumn &target,
                                                     double build_distinct, bool prune) {
    const ColumnType key_type = build->output_schema.columns[build_column].type;
    const bool same_type = key_type == target.node->output_schema.columns[target.scan_column].type;
    uint64_t bloom_bits = 0;
    if (same_type && cfg.dynamic_filter_bloom_bits_per_key > 0) {
        const auto keys = static_cast<uint64_t>(std::max(build_distinct, 1.0));
        bloom_bits = std::min<uint64_t>(keys * cfg.dynamic_filter_bloom_bits_per_key,
                                        cfg.dynamic_filter_max_bloom_bytes * 8);
    }

    PlanBuilder::DynamicFilterEdge edge;
    edge.build = std::move(build);
    edge.build_column = build_column;
    edge.target = target.node;
    edge.target_column = target.scan_column;
    edge.target_file_column = target.file_column;
    edge.key_type = key_type;
    edge.max_values = same_type ? cfg.dynamic_filter_max_values : 0;
    edge.bloom_bits = bloom_bits;
    edge.prune = prune;
    return edge;
}

void PlanDynamicFilters(PlanBuilder &builder, const PlanBuilder::SharedOpNode &root, const DynamicFilterContext &ctx,
                        const ConverterConfig &cfg) {
    transfer::PredicateTransferPlanner(builder, ctx, cfg).Plan(root);
}

} // namespace plume::parser
