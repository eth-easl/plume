#include "plume/parser/plan_builder.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/operator.hpp"
#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/execution/operators/sort.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/parser/physical_plan.hpp"

#include <algorithm>
#include <memory>
#include <unordered_map>

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

    if (!n->consumers.empty()) {
        auto prev_splitter = std::make_shared<PlanBuilder::OperatorNode>();
        prev_splitter->output_schema = n->output_schema;
        prev_splitter->key_columns = std::move(n->key_columns);
        prev_splitter->num_splits = n->num_splits > 0 ? n->num_splits : 1;
        prev_splitter->sources[0] = n;
        n->num_splits = 0;
        for (auto &c : n->consumers) {
            if (c->sources[0] == n) {
                c->sources[0] = prev_splitter;
            } else {
                c->sources[1] = prev_splitter;
            }
        }
        prev_splitter->consumers = std::move(n->consumers);
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

PlanBuilder::SharedOpNode FindEquivalentConsumer(const PlanBuilder::SharedOpNode &src,
        const exec::OperatorTemplate &templ, const Schema &output_schema) {
    for (auto &c : src->consumers) {
        if (c->templ) {
            if (c->templ->Equals(templ) && c->output_schema.Equals(output_schema)) {
                return c;
            }
        } else if (auto found = FindEquivalentConsumer(c, templ, output_schema)) {
            return found;
        }
    }
    return nullptr;
}

} // namespace

PlanBuilder::SharedOpNode PlanBuilder::Scan(std::shared_ptr<DataSource> data_src, Schema schema,
        std::shared_ptr<std::vector<uint32_t>> pushed_projection, std::shared_ptr<expr::ExprNode> pushed_filter) {
    bool use_registry = optimize_remote_fetching_ && data_src->IsRemote();
    if (use_registry) {
        std::optional<SharedOpNode> cached = RemoteRegistryLookup(data_src, pushed_projection, pushed_filter);
        if (cached.has_value()) {
            return cached.value();
        }
    }

    auto n = std::make_shared<PlanBuilder::OperatorNode>();
    n->output_schema = schema;
    n->stage_idx = NewLeafStage(std::move(data_src), schema);
    LeafStage *leaf_stage = LeafStageAt(n->stage_idx);
    leaf_stage->projection = std::move(pushed_projection);
    leaf_stage->pushed_filter = std::move(pushed_filter);

    if (use_registry) {
        // ignoring any error here
        RemoteRegistryAdd(n);
    }

    return n;
}

PlanBuilder::SharedOpNode PlanBuilder::After(PlanBuilder::SharedOpNode src,
        std::shared_ptr<exec::OperatorTemplate> templ, Schema output_schema) {
    if (!src->consumers.empty()) {
        if (templ) {
            auto equiv_consumer = FindEquivalentConsumer(src, *templ, output_schema);
            if (equiv_consumer) { return equiv_consumer; }
        }
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
    if (!s || !s->IsLeaf()) {
        return nullptr;
    }
    return static_cast<LeafStage*>(s);
}

std::pair<PlanBuilder::SharedOpNode, uint32_t> PlanBuilder::AppendColumn(PlanBuilder::SharedOpNode node,
        expr::ExprNode value, ColumnType type) {
    auto t = std::make_shared<exec::ProjectionTemplate>();
    Schema out = node->output_schema;
    for (uint32_t i = 0; i < out.columns.size(); i++) {
        t->projections.push_back(expr::ExprNode::Reference(i, out.columns[i].type));
    }
    const uint32_t new_idx = static_cast<uint32_t>(out.columns.size());
    t->projections.push_back(std::move(value));
    out.columns.push_back({"__key" + std::to_string(new_idx), type, /*nullable=*/true});
    return std::make_pair(After(std::move(node), std::move(t), std::move(out)), new_idx);
}

std::optional<PlanBuilder::ColumnSource> PlanBuilder::TraceColumnStep(const OperatorNode &node, uint32_t col_idx) {
    // A node without a template is either a source (nothing to step to) or a splitter node.
    if (!node.templ) {
        if (node.sources[0]) {
            return ColumnSource{0, col_idx}; // a splitter: passthrough, same columns
        }
        return std::nullopt;
    }

    // passthrough operators (filters, sort)
    const auto *templ = node.templ.get();
    if (dynamic_cast<const exec::FilterTemplate *>(templ) || dynamic_cast<const exec::SortTemplate *>(templ) ||
        dynamic_cast<const exec::DynamicFilterBuildTemplate *>(templ)) {
        return ColumnSource{0, col_idx}; // passthrough, same columns
    }

    // projection
    if (auto *proj = dynamic_cast<const exec::ProjectionTemplate *>(templ)) {
        if (col_idx >= proj->projections.size()) {
            return std::nullopt;
        }
        const expr::ExprNode &e = proj->projections[col_idx];
        if (e.kind != expr::ExprKind::REFERENCE) {
            return std::nullopt; // a computed column -- can't trace a value back through it
        }
        return ColumnSource{0, static_cast<uint32_t>(e.ref_index)};
    }

    // aggregate
    if (auto *agg = dynamic_cast<const exec::AggregateTemplate *>(templ)) {
        // The output leads with the group keys; a group's rows all share its key.
        if (col_idx >= agg->group_keys.size() || agg->group_keys[col_idx].kind != expr::ExprKind::REFERENCE) {
            return std::nullopt;
        }
        return ColumnSource{0, static_cast<uint32_t>(agg->group_keys[col_idx].ref_index)};
    }

    // join
    if (auto *join = dynamic_cast<const exec::JoinTemplate *>(templ)) {
        // The natural layout (exec::JoinSchema): left columns, right columns, marker.
        using K = exec::JoinKind;
        const bool has_left = join->kind != K::RIGHT_SEMI && join->kind != K::RIGHT_ANTI;
        const bool has_right = join->kind != K::SEMI && join->kind != K::ANTI && join->kind != K::MARK;
        const auto left_size = static_cast<uint32_t>(node.sources[0]->output_schema.columns.size());
        if (has_left && col_idx < left_size) {
            return ColumnSource{0, col_idx};
        }
        const uint32_t right_idx = has_left ? col_idx - left_size : col_idx;
        if (has_right && join->kind != K::SINGLE && right_idx < join->right_schema.columns.size()) {
            return ColumnSource{1, right_idx};
        }
        return std::nullopt; // the marker, or a SINGLE join's build side
    }
    return std::nullopt; // anything else (limit, top-n, ...) -- filtering its input changes its output
}

std::optional<PlanBuilder::LeafColumn> PlanBuilder::TraceLeafColumn(const SharedOpNode &node, uint32_t col_idx) {
    SharedOpNode cur = node;
    uint32_t col = col_idx;
    while (true) {
        if (!cur->templ && !cur->sources[0]) {
            break; // a source
        }
        auto step = TraceColumnStep(*cur, col);
        if (!step) {
            return std::nullopt;
        }
        col = step->column;
        cur = cur->sources[step->src_idx];
    }

    LeafStage *leaf = LeafStageAt(cur->stage_idx);
    if (!leaf) {
        return std::nullopt;
    }
    uint32_t file_idx = col;
    if (leaf->projection && !leaf->projection->empty()) {
        if (col >= leaf->projection->size()) {
            return std::nullopt;
        }
        file_idx = (*leaf->projection)[col];
    }
    return LeafColumn{cur, leaf, col, file_idx};
}

void PlanBuilder::AddDynamicFilter(DynamicFilterEdge edge) {
    dynamic_filters_.push_back(std::move(edge));
}

void PlanBuilder::AttachDynamicFilters() {
    // Edges with the same build node, column and parameters share one tap.
    dynamic_filter_taps_.clear();
    for (uint32_t i = 0; i < dynamic_filters_.size(); i++) {
        const auto &e = dynamic_filters_[i];
        auto &builds = e.build->dynamic_filter_builds;
        auto shared = std::find_if(builds.begin(), builds.end(), [&](uint32_t t) {
            const auto &tap = dynamic_filter_taps_[t];
            return tap.build_column == e.build_column && tap.key_type == e.key_type &&
                   tap.max_values == e.max_values && tap.bloom_bits == e.bloom_bits;
        });
        if (shared != builds.end()) {
            dynamic_filter_taps_[*shared].edges.push_back(i);
            continue;
        }
        builds.push_back(static_cast<uint32_t>(dynamic_filter_taps_.size()));
        dynamic_filter_taps_.push_back({e.build_column, e.key_type, e.max_values, e.bloom_bits, {i}});
    }
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

void PlanBuilder::EmitDynamicFilterTaps(const OperatorNode &node, size_t stage_idx) {
    Stage *producer = StageAt(stage_idx);
    for (uint32_t df_idx : node.dynamic_filter_builds) {
        const auto &tap = dynamic_filter_taps_[df_idx];

        producer->pipeline.operators.push_back(std::make_shared<exec::DynamicFilterBuildTemplate>(
            tap.build_column, tap.key_type, tap.max_values, tap.bloom_bits, df_idx));
        producer->dynamic_filter_outputs.push_back(df_idx);

        for (uint32_t i : tap.edges) {
            const auto &filter = dynamic_filters_[i];
            // leaf targets always have a stage
            Stage *consumer = StageAt(filter.target->stage_idx);
            consumer->dynamic_filter_inputs.push_back(DynamicFilterInput{
                df_idx, static_cast<uint32_t>(stage_idx), filter.target_column, filter.target_file_column, filter.prune});
            exec::DynamicFilterProbeSpec spec{df_idx, filter.target_column};
            auto &probes = consumer->pipeline.dynamic_filter_probes;
            if (std::find(probes.begin(), probes.end(), spec) == probes.end()) {
                probes.push_back(spec);
            }
        }
    }
}

size_t PlanBuilder::MaterializeNode(SharedOpNode node) {
    if (node->stage_idx != kUnmaterialized) {
        if (!node->materialized) {
            node->materialized = true;
            EmitDynamicFilterTaps(*node, node->stage_idx);
        }
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
    node->materialized = true;
    EmitDynamicFilterTaps(*node, cur_id);

    return cur_id;
}

Result<void> PlanBuilder::RemoteRegistryAdd(SharedOpNode node) {
    LeafStage *leaf_stage = LeafStageAt(node->stage_idx);
    if (!leaf_stage) {
        return Error("Invalid stage_idx.", ErrorKind::InvalidInput);
    }
    remote_registry_.insert({leaf_stage->data_source->name, std::move(node)});
    return Ok();
}

std::optional<PlanBuilder::SharedOpNode> PlanBuilder::RemoteRegistryLookup(std::shared_ptr<catalog::DataSource> data_src,
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
        return a->Equals(*b);
    };

    auto [begin, end] = remote_registry_.equal_range(data_src->name);
    for (auto it = begin; it != end; it++) {
        LeafStage *leaf_stage = LeafStageAt(it->second->stage_idx);
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
