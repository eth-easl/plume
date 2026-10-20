#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/parser/physical_plan.hpp"
#include "plume/parser/plan_builder.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
class Connection;
class LogicalOperator;
class LogicalGet;
class LogicalAggregate;
class LogicalMaterializedCTE;
class LogicalComparisonJoin;
class LogicalColumnDataGet;
class LogicalCrossProduct;
} // namespace duckdb

namespace plume::parser {

struct ConverterConfig {
    // Enables projection pushdown into the reader, skipping columns not needed for execution.
    bool projection_pushdown = true;
    // Enables filter pushdown into the reader, allowing the prepare function to prune row groups
    // based on statistics.
    bool filter_pushdown = true;
    // Enables two-phase aggregation: a partial aggregate in the producing stage + a final aggregate.
    bool early_aggregation = true;
    // Enables plan builder to optimize remote data fetching.
    bool optimize_remote_fetching = true;
    // Enables attaching a runtime dynamic filter.
    bool dynamic_filter = true;

    // Upper limit on the number of output splits produced.
    uint32_t max_splits = 1;
    // How many rows each split should get ideally.
    uint64_t target_rows_per_split = 1u << 20;
    // Upper limit on the input bytes processed by a source region (0 => unlimited).
    // Overturns max_splits.
    uint64_t max_region_bytes = 0;

    // Maximum gap to coalesce requests over.
    uint64_t coalesce_distance = 1 << 10;
    // A dynamic filter is only attached when it is estimated to keep at most this fraction of
    // the probe rows: the share of the build key's distinct values the build side keeps, if the
    // build key traces back to a remote scan.
    double dynamic_filter_max_match_fraction = 0.5;
    // Otherwise, a dynamic filter is only attached when the build side's estimated cardinality
    // is at most this fraction of the probe side's.
    double dynamic_filter_selectivity_threshold = 0.2;
    // The probe scan waits for the filter to prune row groups before fetching (pq_prepare) only
    // if a row group's key range spans at most this fraction of the key column's overall range on
    // average. Otherwise the fetch starts right away and only pq_stage applies the filter.
    double dynamic_filter_max_rowgroup_coverage = 0.5;
    // Upper limit on the exact distinct build keys a dynamic filter carries (0 => no exact set).
    uint32_t dynamic_filter_max_values = 1024;
    // Bloom filter bits per estimated build-side row (0 => no Bloom filter, 10 bits => ~1% false positives).
    uint32_t dynamic_filter_bloom_bits_per_key = 10;
    // Upper limit on a dynamic filter's Bloom filter size.
    uint64_t dynamic_filter_max_bloom_bytes = 4 << 20;
};

// Converts a bound + optimized DuckDB logical plan into Plume PhysicalPlan.
class Converter {
public:
    Converter(duckdb::Connection &con, const catalog::SourceCatalog &sources, ConverterConfig cfg)
        : con_(con), catalog_(sources), cfg_(cfg), builder_(PlanBuilder(cfg.optimize_remote_fetching)) {}

    Result<std::unique_ptr<PhysicalPlan>> Convert(duckdb::LogicalOperator &root);

private:
    uint32_t SplitCount(uint64_t estimated_rows) const;

    // A dynamic filter from a join's build side to a probe-side scan.
    struct PendingDynamicFilter {
        PlanBuilder::SharedOpNode build; // the DynamicFilterBuildTemplate node
        PlanBuilder::SharedOpNode join;  // the join whose probe side it filters
        uint32_t probe_key;              // the key column in the join's probe input
        LeafStage *leaf;                 // the filtered scan
    };

    std::optional<PendingDynamicFilter> AttachDynamicFilter(PlanBuilder::SharedOpNode &left,
        PlanBuilder::SharedOpNode &right, exec::JoinKind kind, const std::vector<uint32_t> &left_keys,
        const std::vector<uint32_t> &right_keys, const std::vector<bool> &keys_match_null,
        uint64_t left_card, uint64_t right_card);
    void DropSharedDynamicFilters(const PlanBuilder::SharedOpNode &root);
    
    Result<PlanBuilder::SharedOpNode> BuildGet(duckdb::LogicalGet &get);
    Result<PlanBuilder::SharedOpNode> BuildChunkGet(duckdb::LogicalColumnDataGet &get);
    Result<PlanBuilder::SharedOpNode> BuildAggregate(duckdb::LogicalAggregate &agg, PlanBuilder::SharedOpNode child);
    Result<PlanBuilder::SharedOpNode> BuildDistinct(PlanBuilder::SharedOpNode child, const std::vector<uint32_t> &cols, uint64_t card);
    Result<PlanBuilder::SharedOpNode> BuildMaterializedCTE(duckdb::LogicalMaterializedCTE &cte);
    Result<PlanBuilder::SharedOpNode> BuildJoin(PlanBuilder::SharedOpNode left, PlanBuilder::SharedOpNode right, 
        duckdb::LogicalComparisonJoin &join, exec::JoinKind kind, uint64_t left_card, uint64_t right_card);
    Result<PlanBuilder::SharedOpNode> BuildComparisonJoin(duckdb::LogicalComparisonJoin &join);
    Result<PlanBuilder::SharedOpNode> BuildDelimJoin(duckdb::LogicalComparisonJoin &join);
    Result<PlanBuilder::SharedOpNode> BuildCrossProduct(duckdb::LogicalCrossProduct &cp);

    Result<PlanBuilder::SharedOpNode> Build(duckdb::LogicalOperator &op);

    duckdb::Connection &con_;
    catalog::SourceCatalog catalog_;
    ConverterConfig cfg_;
    PlanBuilder builder_;

    std::map<uint64_t, PlanBuilder::SharedOpNode> cte_subs_;
    std::vector<PlanBuilder::SharedOpNode> delim_stack_;
    std::vector<PendingDynamicFilter> pending_dynamic_filters_;
};

Result<std::unique_ptr<PhysicalPlan>> BuildPhysicalPlan(duckdb::Connection &con, 
    const std::string &sql, const catalog::SourceCatalog &sources = {}, const ConverterConfig &config = {});

} // namespace plume::parser
