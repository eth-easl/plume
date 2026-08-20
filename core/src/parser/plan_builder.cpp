#include "plume/parser/plan_builder.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/operator.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/parser/physical_plan.hpp"

#include <memory>

namespace plume::parser {

using catalog::DataSource;

namespace {

PlanBuilder::SharedOpNode AddShuffle(PlanBuilder::SharedOpNode n, std::vector<uint32_t> key_columns, uint32_t num_splits) {
    // Make sure number of splits is at least set to one which indicates a shuffle.
    num_splits = num_splits == 0 ? 1 : num_splits;
    
    if (n->CanSplit()) {
        n->key_columns = std::move(key_columns);
        n->num_splits = num_splits;
        return n;
    }
    if (n->IsSameSplit(key_columns, num_splits)) {
        return n;
    }

    if (n->num_splits > 0) {
        // Need to insert new dummy node that only does the shuffle that's already assigned to
        // the previous node.
        auto prev_splitter = std::make_shared<PlanBuilder::OperatorNode>();
        prev_splitter->output_schema = n->output_schema;
        prev_splitter->key_columns = std::move(n->key_columns);
        prev_splitter->num_splits = n->num_splits;
        prev_splitter->sources[0] = n;
        n->num_splits = 0;
        for (auto &c : n->consumers) {
            if (c->sources[0] == n) {
                c->sources[0] = prev_splitter;
            } else {
                c->sources[1] = prev_splitter;
            }
        }
        n->consumers = { prev_splitter };
    }

    auto new_splitter = std::make_shared<PlanBuilder::OperatorNode>();
    new_splitter->output_schema = n->output_schema;
    new_splitter->key_columns = std::move(key_columns);
    new_splitter->num_splits = num_splits;
    new_splitter->sources[0] = n;
    n->consumers.push_back(new_splitter);
    return new_splitter;
}

bool ExprNodeEquals(const expr::ExprNode &a, const expr::ExprNode &b) {
    if (a.kind != b.kind) return false;
    if (!(a.return_type == b.return_type)) return false;
    if (a.ref_index != b.ref_index) return false;
    if (a.constant.IsNull() != b.constant.IsNull()) return false;
    if (!a.constant.IsNull() && !(a.constant == b.constant)) return false;
    if (a.func_name != b.func_name) return false;
    if (a.expr_type != b.expr_type) return false;
    if (a.try_cast != b.try_cast) return false;
    if (a.children.size() != b.children.size()) return false;
    for (size_t i = 0; i < a.children.size(); i++) {
        if (!ExprNodeEquals(a.children[i], b.children[i])) return false;
    }
    return true;
}

} // namespace

PlanBuilder::SharedOpNode PlanBuilder::Scan(std::shared_ptr<DataSource> data_src, Schema schema,
        std::shared_ptr<std::vector<uint32_t>> pushed_projection, std::shared_ptr<expr::ExprNode> pushed_filter) {
    auto n = std::make_shared<PlanBuilder::OperatorNode>();
    n->output_schema = schema;

    bool use_registry = optimize_remote_fetching_ && data_src->IsRemote();
    if (use_registry) {
        std::optional<size_t> cached = RemoteRegistryLookup(data_src, pushed_projection, pushed_filter);
        if (cached.has_value()) {
            n->stage_idx = cached.value();
            return n;
        }
    }

    n->stage_idx = NewLeafStage(std::move(data_src), schema);
    LeafStage *leaf_stage = LeafStageAt(n->stage_idx);
    leaf_stage->projection = std::move(pushed_projection);
    leaf_stage->pushed_filter = std::move(pushed_filter);

    if (use_registry) {
        // ignoring any error here
        RemoteRegistryAdd(n->stage_idx);
    }

    return n;
}

PlanBuilder::SharedOpNode PlanBuilder::After(PlanBuilder::SharedOpNode src,
        std::shared_ptr<exec::OperatorTemplate> templ, Schema output_schema) {
    if (!src->consumers.empty()) {
        src = AddShuffle(std::move(src), {}, 1);
    }
    auto n = std::make_shared<PlanBuilder::OperatorNode>();
    n->output_schema = output_schema;
    n->templ = std::move(templ);
    src->consumers.push_back(n);
    n->sources[0] = std::move(src);
    return n;
}

PlanBuilder::SharedOpNode PlanBuilder::Shuffle(PlanBuilder::SharedOpNode src, 
        std::shared_ptr<exec::OperatorTemplate> templ, Schema output_schema, 
        std::vector<uint32_t> key_columns, uint32_t num_splits) {
    // Add shuffle to source node.
    PlanBuilder::SharedOpNode splitter = AddShuffle(src, std::move(key_columns), num_splits);
    
    auto n = std::make_shared<PlanBuilder::OperatorNode>();
    n->output_schema = output_schema;
    n->templ = std::move(templ);
    splitter->consumers.push_back(n);
    n->sources[0] = std::move(splitter);

    return n;
}

PlanBuilder::SharedOpNode PlanBuilder::Join(PlanBuilder::SharedOpNode probe_side_op,
        PlanBuilder::SharedOpNode build_side_op, std::shared_ptr<exec::OperatorTemplate> templ,
        Schema output_schema, std::vector<uint32_t> probe_key_columns,
        std::vector<uint32_t> build_key_columns, uint32_t probe_num_splits, uint32_t build_num_splits) {
    // Add shuffles to both source sides.
    PlanBuilder::SharedOpNode probe_side_splitter =
        AddShuffle(probe_side_op, std::move(probe_key_columns), probe_num_splits);
    PlanBuilder::SharedOpNode build_side_splitter =
        AddShuffle(build_side_op, std::move(build_key_columns), build_num_splits);

    auto n = std::make_shared<PlanBuilder::OperatorNode>();
    n->output_schema = output_schema;
    n->templ = std::move(templ);
    probe_side_splitter->consumers.push_back(n);
    build_side_splitter->consumers.push_back(n);
    n->sources[0] = std::move(probe_side_splitter); // left side is probe
    n->sources[1] = std::move(build_side_splitter); // right side is build
    return n;
}

Stage *PlanBuilder::StageAt(size_t idx) {
    if (idx >= plan_->stages.size()) {
        return nullptr;
    }
    return plan_->stages[idx].get();
}

LeafStage *PlanBuilder::LeafStageAt(size_t idx) {
    Stage *s = StageAt(idx);
    if (!s || !s->is_leaf()) {
        return nullptr;
    }
    return static_cast<LeafStage*>(s);
}

std::unique_ptr<PhysicalPlan> PlanBuilder::Export(PlanBuilder::SharedOpNode root) {
    size_t root_idx = MaterializeNode(root);
    Stage *root_stage = StageAt(root_idx);
    plan_->root_idx = root_idx;
    plan_->output_schema = root_stage->output_schema;
    return std::move(plan_);
}

size_t PlanBuilder::NewStage(Schema input_schema) {
    size_t idx = plan_->stages.size();
    auto s = std::make_unique<Stage>(StageType::COMMON, idx);
    s->pipeline.input_schema = std::move(input_schema);
    plan_->stages.push_back(std::move(s));
    return idx;
}

size_t PlanBuilder::NewLeafStage(std::shared_ptr<DataSource> data_src, Schema input_schema) {
    size_t idx = plan_->stages.size();
    auto s = std::make_unique<LeafStage>(idx);
    s->data_source = std::move(data_src);
    s->pipeline.input_schema = std::move(input_schema);
    plan_->stages.push_back(std::move(s));
    return idx;
}

size_t PlanBuilder::MaterializeNode(SharedOpNode node) {
    if (node->stage_idx != kUnmaterialized) {
        return node->stage_idx;
    }

    size_t left_id = MaterializeNode(node->sources[0]);
    size_t right_id = kUnmaterialized;
    if (node->sources[1]) {
        right_id = MaterializeNode(node->sources[1]);
    }

    auto make_split = [](std::vector<uint32_t> keys, uint32_t num_splits) {
        if (num_splits <= 1) {
            return exec::OutputSplit{};
        }
        return exec::OutputSplit{std::move(keys), num_splits};
    };

    size_t cur_id;
    if (node->sources[0]->num_splits > 0) {
        plan_->stages[left_id]->pipeline.output_split =
            make_split(node->sources[0]->key_columns, node->sources[0]->num_splits);
        Schema left_out = plan_->stages[left_id]->output_schema;
        cur_id = NewStage(left_out);
        Stage *cur = plan_->stages[cur_id].get();
        cur->input_stages.push_back(left_id);
        cur->output_schema = node->output_schema;
    } else {
        cur_id = left_id;
        plan_->stages[cur_id]->output_schema = node->output_schema;
    }

    if (right_id != kUnmaterialized) {
        plan_->stages[right_id]->pipeline.output_split =
            make_split(node->sources[1]->key_columns, node->sources[1]->num_splits);
        Stage *cur = plan_->stages[cur_id].get();
        cur->type = StageType::JOIN;
        cur->input_stages.push_back(right_id);
    }

    if (node->templ) {
        plan_->stages[cur_id]->pipeline.operators.push_back(node->templ);
    }
    node->stage_idx = cur_id;

    return cur_id;
}

Result<void> PlanBuilder::RemoteRegistryAdd(size_t stage_idx) {
    LeafStage *leaf_stage = LeafStageAt(stage_idx);
    if (!leaf_stage) {
        return Error("Invalid stage_idx.", ErrorKind::InvalidInput);
    }
    remote_registry_.insert({leaf_stage->data_source->name, stage_idx});
    return Ok();
}

std::optional<size_t> PlanBuilder::RemoteRegistryLookup(std::shared_ptr<catalog::DataSource> data_src,
        std::shared_ptr<std::vector<uint32_t>> projection, std::shared_ptr<expr::ExprNode> filter) {
    auto same_projection = [](const std::shared_ptr<std::vector<uint32_t>> &a,
                              const std::shared_ptr<std::vector<uint32_t>> &b) {
        if (a == b) return true; // covers pointer identity and both-nullptr
        if (!a || !b) return false;
        return *a == *b;
    };
    auto same_filter = [](const std::shared_ptr<expr::ExprNode> &a, const std::shared_ptr<expr::ExprNode> &b) {
        if (a == b) return true; // covers pointer identity and both-nullptr
        if (!a || !b) return false;
        return ExprNodeEquals(*a, *b);
    };

    auto [begin, end] = remote_registry_.equal_range(data_src->name);
    for (auto it = begin; it != end; it++) {
        LeafStage *leaf_stage = LeafStageAt(it->second);
        if (!leaf_stage || leaf_stage->data_source != data_src) {
            continue;
        }
        if (!same_projection(leaf_stage->projection, projection)) {
            continue;
        }
        if (!same_filter(leaf_stage->pushed_filter, filter)) {
            continue;
        }
        return it->second;
    }
    return std::nullopt;
}

} // namespace plume::parser
