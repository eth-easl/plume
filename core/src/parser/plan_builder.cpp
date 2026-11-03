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

// A fixed-size bitset over leaf ids.
class LeafSet {
public:
    explicit LeafSet(size_t n = 0) : words_((n + 63) / 64, 0) {}
    
    void Set(size_t i) { words_[i >> 6] |= uint64_t{1} << (i & 63); }
    bool Test(size_t i) const { return i < words_.size() * 64 && ((words_[i >> 6] >> (i & 63)) & 1); }
    void Clear() { std::fill(words_.begin(), words_.end(), 0); }
    
    LeafSet &operator|=(const LeafSet &o) {
        for (size_t k = 0; k < words_.size(); k++) {
            words_[k] |= o.words_[k];
        }
        return *this;
    }

    template <class F>
    void ForEach(F &&f) const {
        for (size_t k = 0; k < words_.size(); k++) {
            for (uint64_t w = words_[k]; w; w &= w - 1) {
                f(k * 64 + static_cast<size_t>(__builtin_ctzll(w)));
            }
        }
    }

private:
    std::vector<uint64_t> words_;
};

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

std::optional<PlanBuilder::LeafColumn> PlanBuilder::TraceLeafColumn(const SharedOpNode &node, uint32_t col_idx,
        std::vector<OperatorNode *> *path) {
    if (path) {
        path->push_back(node.get());
    }
    auto trace = [&](uint32_t source, uint32_t idx) {
        return TraceLeafColumn(node->sources[source], idx, path);
    };

    // A node without a template is either a source or a splitter node.
    if (!node->templ) {
        if (node->sources[0]) {
            return trace(0, col_idx); // a splitter: passthrough, same columns
        }
        
        LeafStage *leaf = LeafStageAt(node->stage_idx);
        if (!leaf) {
            return std::nullopt;
        }
        uint32_t file_idx = col_idx;
        if (leaf->projection && !leaf->projection->empty()) {
            if (col_idx >= leaf->projection->size()) {
                return std::nullopt;
            }
            file_idx = (*leaf->projection)[col_idx];
        }
        return LeafColumn{node, leaf, col_idx, file_idx};
    }

    // passthrough operators (filters, sort)
    const auto *templ = node->templ.get();
    if (dynamic_cast<const exec::FilterTemplate *>(templ) || dynamic_cast<const exec::SortTemplate *>(templ) ||
        dynamic_cast<const exec::DynamicFilterBuildTemplate *>(templ)) {
        return trace(0, col_idx); // passthrough, same columns
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
        return trace(0, e.ref_index);
    }

    // aggregate
    if (auto *agg = dynamic_cast<const exec::AggregateTemplate *>(templ)) {
        // The output leads with the group keys; a group's rows all share its key.
        if (col_idx >= agg->group_keys.size() || agg->group_keys[col_idx].kind != expr::ExprKind::REFERENCE) {
            return std::nullopt;
        }
        return trace(0, agg->group_keys[col_idx].ref_index);
    }

    // join
    if (auto *join = dynamic_cast<const exec::JoinTemplate *>(templ)) {
        // The natural layout (exec::JoinSchema): left columns, right columns, marker.
        using K = exec::JoinKind;
        const bool has_left = join->kind != K::RIGHT_SEMI && join->kind != K::RIGHT_ANTI;
        const bool has_right = join->kind != K::SEMI && join->kind != K::ANTI && join->kind != K::MARK;
        const auto left_size = static_cast<uint32_t>(node->sources[0]->output_schema.columns.size());
        if (has_left && col_idx < left_size) {
            return trace(0, col_idx);
        }
        const uint32_t right_idx = has_left ? col_idx - left_size : col_idx;
        if (has_right && join->kind != K::SINGLE && right_idx < join->right_schema.columns.size()) {
            return trace(1, right_idx);
        }
        return std::nullopt; // the marker, or a SINGLE join's build side
    }
    return std::nullopt; // anything else (limit, top-n, ...) -- filtering its input changes its output
}

void PlanBuilder::AddDynamicFilter(DynamicFilterEdge edge) {
    dynamic_filters_.push_back(std::move(edge));
}

void PlanBuilder::ApplyDynamicFilters(const SharedOpNode &root, uint32_t max_chain_depth) {
    const size_t n = dynamic_filters_.size();
    if (n == 0) {
        return;
    }

    // process order
    std::vector<uint32_t> order;
    order.reserve(n);
    for (uint32_t i = 0; i < n; i++) {
        const auto &e = dynamic_filters_[i];
        std::vector<OperatorNode *> path;
        auto traced = TraceLeafColumn(e.probe_input, e.probe_key, &path);
        const bool exclusive = traced && traced->node == e.target && traced->scan_column == e.target_column &&
            std::all_of(path.begin(), path.end(),
                        [&](const OperatorNode *p) { return p->consumers.size() == 1 && p != root.get(); });
        if (exclusive) {
            order.push_back(i);
        }
    }
    std::stable_sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
        return dynamic_filters_[a].benefit > dynamic_filters_[b].benefit;
    });

    // Before Export() only Scan() has created stages, so every node with a stage is a leaf and
    // its stage index is a dense leaf id. Targets are leaves (TraceLeafColumn returned them).
    const size_t num_leaves = plan_->stages.size();

    std::unordered_map<const OperatorNode *, LeafSet> upstream;
    auto upstream_of = [&](auto &self, const OperatorNode *node) -> const LeafSet & {
        if (auto it = upstream.find(node); it != upstream.end()) {
            return it->second;
        }

        LeafSet set(num_leaves);
        if (node->stage_idx < num_leaves) {
            set.Set(node->stage_idx);
        }
        for (const auto &src : node->sources) {
            if (src) {
                set |= self(self, src.get());
            }
        }
        return upstream.emplace(node, std::move(set)).first->second;
    };

    // awaits[l] is the leaves leaf l transitively waits for.
    std::vector<LeafSet> awaits(num_leaves, LeafSet(num_leaves));
    for (size_t l = 0; l < num_leaves; l++) {
        awaits[l].Set(l);
    }

    // chain relation among edges
    auto precedes = [&](uint32_t e, uint32_t o) {
        return upstream_of(upstream_of, dynamic_filters_[e].build.get())
            .Test(dynamic_filters_[o].target->stage_idx);
    };

    // For the accepted edges (parallel to `accepted`): the longest chain ending / starting at it.
    // The cycle check keeps the chain relation acyclic.
    std::vector<uint32_t> accepted;
    std::vector<uint32_t> up_depth;
    std::vector<uint32_t> down_depth;
    auto recompute_depths = [&]() {
        const size_t a = accepted.size();
        up_depth.assign(a, 0);
        down_depth.assign(a, 0);
        auto up = [&](auto &self, size_t k) -> uint32_t {
            if (up_depth[k] == 0) {
                uint32_t d = 1;
                for (size_t j = 0; j < a; j++) {
                    if (j != k && precedes(accepted[j], accepted[k])) {
                        d = std::max(d, self(self, j) + 1);
                    }
                }
                up_depth[k] = d;
            }
            return up_depth[k];
        };
        auto down = [&](auto &self, size_t k) -> uint32_t {
            if (down_depth[k] == 0) {
                uint32_t d = 1;
                for (size_t j = 0; j < a; j++) {
                    if (j != k && precedes(accepted[k], accepted[j])) {
                        d = std::max(d, self(self, j) + 1);
                    }
                }
                down_depth[k] = d;
            }
            return down_depth[k];
        };
        for (size_t k = 0; k < a; k++) {
            up(up, k);
            down(down, k);
        }
    };

    LeafSet reach(num_leaves);
    for (uint32_t i : order) {
        const auto &filter = dynamic_filters_[i];
        const size_t target_idx = filter.target->stage_idx;

        // Check if build waits for scan which would lead to cycle.
        reach.Clear();
        upstream_of(upstream_of, filter.build.get()).ForEach([&](size_t l) { reach |= awaits[l]; });
        if (reach.Test(target_idx)) {
            continue;
        }

        // Every chain avoiding i is within the limit already, so only check the longest through i.
        uint32_t up_i = 1;
        uint32_t down_i = 1;
        for (size_t k = 0; k < accepted.size(); k++) {
            if (precedes(accepted[k], i)) {
                up_i = std::max(up_i, up_depth[k] + 1);
            }
            if (precedes(i, accepted[k])) {
                down_i = std::max(down_i, down_depth[k] + 1);
            }
        }
        if (up_i + down_i - 1 > max_chain_depth) {
            continue;
        }

        accepted.push_back(i);
        for (size_t l = 0; l < num_leaves; l++) {
            if (awaits[l].Test(target_idx)) {
                awaits[l] |= reach;
            }
        }
        recompute_depths();
    }

    // Keep the accepted edges in their original order.
    std::sort(accepted.begin(), accepted.end());
    std::vector<DynamicFilterEdge> kept;
    kept.reserve(accepted.size());
    for (uint32_t i : accepted) {
        kept.push_back(std::move(dynamic_filters_[i]));
    }
    dynamic_filters_ = std::move(kept);

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
