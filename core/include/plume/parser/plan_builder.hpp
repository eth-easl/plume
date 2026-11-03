#pragma once

#include "plume/parser/physical_plan.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace plume::parser {

constexpr size_t kUnmaterialized = std::numeric_limits<std::size_t>::max();

class PlanBuilder {
public:
    PlanBuilder(bool optimize_remote_fetching) : optimize_remote_fetching_(optimize_remote_fetching) {}

    // Intermediate struct representing an operator in the query plan.
    struct OperatorNode {
        // The output schema of this node.
        Schema output_schema;
        // The operator template of this node.
        std::shared_ptr<exec::OperatorTemplate> templ = nullptr;
        
        // The source nodes.
        std::array<std::shared_ptr<OperatorNode>, 2> sources = { nullptr, nullptr };
        
        // The key columns to split the stage output over.
        std::vector<uint32_t> key_columns = {};
        // 0 if node may be fused with previous node, > 0 if shuffle is required
        uint32_t num_splits = 0;
        // The nodes consuming this node's output. Used to check if output can be split
        // directly or not.
        std::vector<std::shared_ptr<OperatorNode>> consumers = {};
        
        // Stage index of the stage this node is materialized in or -1 if not yet materialized.
        size_t stage_idx = kUnmaterialized;
        // Whether Export() has visited the node (emitted its operator and dynamic filter taps).
        bool materialized = false;

        // Indices into `dynamic_filter_taps_` built over this node's output.
        std::vector<uint32_t> dynamic_filter_builds = {};

        bool CanSplit() { return consumers.size() == 0 && num_splits == 0; }
        bool IsSameSplit(const std::vector<uint32_t> &other_key_cols, uint32_t other_num_splits) {
            if (num_splits != other_num_splits) return false;
            if (key_columns.size() != other_key_cols.size()) return false;
            for (size_t i = 0; i < key_columns.size(); i++) {
                auto it = std::find(other_key_cols.begin(), other_key_cols.end(), key_columns[i]);
                if (it == other_key_cols.end()) {
                    return false;
                }
            }
            return true;
        }
    };
    using SharedOpNode = std::shared_ptr<OperatorNode>;

    // A dynamic filter where the `build_column` keys filter the `target_column`.
    struct DynamicFilterEdge {
        // Build of the dynamic filter.
        SharedOpNode build;
        uint32_t build_column;

        // Target of the dynamic filter (a leaf node).
        SharedOpNode target;
        uint32_t target_column;      // in the scan schema (pq_stage probe)
        uint32_t target_file_column; // in the file schema (pq_prepare pruning)

        ColumnType key_type;  // type keys are cast to before collecting/hashing
        uint32_t max_values;  // exact key set limit (0 -> none)
        uint64_t bloom_bits;  // Bloom filter size (0 -> none)

        SharedOpNode probe_input; // the join's probe-side input (for the exclusivity recheck)
        uint32_t probe_key;       // key column in probe_input's output
        bool prune;               // remote parquet leaf: also prune in pq_prepare
        double benefit;           // estimated fraction of probe rows removed (ranking)
    };

    // The accepted edges sharing build node, column and parameters (becomes a DynamicFilterBuildTemplate).
    struct DynamicFilterTap {
        uint32_t build_column;
        ColumnType key_type;
        uint32_t max_values;
        uint64_t bloom_bits;
        std::vector<uint32_t> edges; // indices into dynamic_filters_ of the edges it feeds
    };

    SharedOpNode Scan(std::shared_ptr<catalog::DataSource> data_src, Schema schema, 
        std::shared_ptr<std::vector<uint32_t>> pushed_projection, 
        std::shared_ptr<expr::ExprNode> pushed_filter);

    SharedOpNode After(SharedOpNode src, std::shared_ptr<exec::OperatorTemplate> templ, Schema output_schema);
    SharedOpNode Shuffle(SharedOpNode src, std::shared_ptr<exec::OperatorTemplate> templ, Schema output_schema, 
        std::vector<uint32_t> key_columns, uint32_t num_splits);
    SharedOpNode Join(SharedOpNode probe_side_op, SharedOpNode build_side_op,
        std::shared_ptr<exec::OperatorTemplate> templ, Schema output_schema,
        std::vector<uint32_t> probe_key_columns, std::vector<uint32_t> build_key_columns,
        uint32_t probe_num_splits, uint32_t build_num_splits);

    Stage *StageAt(size_t idx);
    LeafStage *LeafStageAt(size_t idx);

    std::pair<SharedOpNode, uint32_t> AppendColumn(SharedOpNode node, expr::ExprNode value, ColumnType type);

    struct LeafColumn {
        SharedOpNode node; // the leaf node
        LeafStage *leaf;   // its stage
        uint32_t scan_column;
        uint32_t file_column;
    };

    std::optional<LeafColumn> TraceLeafColumn(const SharedOpNode &node, uint32_t col_idx,
                                              std::vector<OperatorNode *> *path = nullptr);

    void AddDynamicFilter(DynamicFilterEdge edge);
    void ApplyDynamicFilters(const SharedOpNode &root, uint32_t max_chain_depth);
    
    const std::vector<DynamicFilterEdge> &DynamicFilters() const { return dynamic_filters_; }

    std::unique_ptr<PhysicalPlan> Export(SharedOpNode root);

private:
    size_t NewStage(Schema input_schema);
    size_t NewLeafStage(std::shared_ptr<catalog::DataSource> data_src, Schema input_schema);
    
    void EmitDynamicFilterTaps(const OperatorNode &node, size_t stage_idx);
    size_t MaterializeNode(SharedOpNode node);

    Result<void> RemoteRegistryAdd(SharedOpNode node);
    std::optional<SharedOpNode> RemoteRegistryLookup(std::shared_ptr<catalog::DataSource> data_src,
        std::shared_ptr<std::vector<uint32_t>> projection, std::shared_ptr<expr::ExprNode> filter);

    std::unique_ptr<PhysicalPlan> plan_ = std::make_unique<PhysicalPlan>();

    bool optimize_remote_fetching_ = true;
    std::unordered_multimap<std::string, SharedOpNode> remote_registry_;

    std::vector<DynamicFilterEdge> dynamic_filters_;
    std::vector<DynamicFilterTap> dynamic_filter_taps_;
};

} // namespace plume::parser
