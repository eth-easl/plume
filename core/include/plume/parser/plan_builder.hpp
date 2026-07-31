#pragma once

#include "plume/parser/physical_plan.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

namespace plume::parser {

constexpr size_t kUnmaterialized = std::numeric_limits<std::size_t>::max();

class PlanBuilder {
public:
    PlanBuilder() {}

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

    SharedOpNode Scan(std::shared_ptr<catalog::DataSource> data_src, Schema schema);

    SharedOpNode After(SharedOpNode src, std::shared_ptr<exec::OperatorTemplate> templ, Schema output_schema);
    SharedOpNode Shuffle(SharedOpNode src, std::shared_ptr<exec::OperatorTemplate> templ, Schema output_schema, 
        std::vector<uint32_t> key_columns, uint32_t num_splits);
    // `probe_num_splits`/`build_num_splits` are independent: a co-partitioned
    // equi join passes the same value for both, while a broadcast join (see
    // Converter::BuildCrossProduct) splits only the probe side and pins the
    // build side at 1 (so it is wired as a broadcast edge to every probe
    // partition, see composition.cpp).
    SharedOpNode Join(SharedOpNode probe_side_op, SharedOpNode build_side_op,
        std::shared_ptr<exec::OperatorTemplate> templ, Schema output_schema,
        std::vector<uint32_t> probe_key_columns, std::vector<uint32_t> build_key_columns,
        uint32_t probe_num_splits, uint32_t build_num_splits);

    Stage *StageAt(size_t idx);
    LeafStage *LeafStageAt(size_t idx);

    PhysicalPlan Export(SharedOpNode root);

private:
    size_t NewStage(Schema input_schema);
    size_t NewLeafStage(std::shared_ptr<catalog::DataSource> data_src, Schema input_schema);
    size_t MaterializeNode(SharedOpNode node);

    PhysicalPlan plan_;
};

} // namespace plume::parser
