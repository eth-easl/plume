// Regression tests for PlanBuilder's remote-registry sharing (Scan()/RemoteRegistry* in
// plan_builder.cpp): a second Scan() over the same remote data source with the same pushed
// projection/filter must reuse the first Scan()'s exact leaf node (not just its stage), so that
// a differing residual filter/projection chained after it (via After()) forks into its own
// downstream stage instead of being fused onto the shared leaf's pipeline - and an identical
// residual chain gets deduplicated onto the same downstream stage instead of building a
// redundant duplicate.

#include "test_util.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/expression/expression.hpp"
#include "plume/parser/physical_plan.hpp"
#include "plume/parser/plan_builder.hpp"

#include "duckdb/common/types/value.hpp"

using namespace plume;
using namespace plume::parser;
using namespace plume::exec;
using namespace plume::expr;
using duckdb::ExpressionType;
using duckdb::Value;
using plume::catalog::DataSource;
using plume::catalog::DataSourceType;
using plume::catalog::RemoteResolver;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema TestSchema() {
    return Schema{{{"a", I32(), true}, {"b", I32(), true}}};
}

// Minimal concrete remote source. Resolve() is never exercised here - these tests only cover
// PlanBuilder's graph bookkeeping, so the schema/cardinality are just set up-front.
struct FakeRemoteSource : DataSource {
    FakeRemoteSource() : DataSource(DataSourceType::REMOTE_PARQUET) {
        name = "fake_remote";
        schema = TestSchema();
        cardinality_total = 100;
    }
    Result<void> Resolve(const RemoteResolver &) override { return Ok(); }
};

std::shared_ptr<FilterTemplate> EqFilter(int32_t k) {
    auto pred = ExprNode::Comparison(ExpressionType::COMPARE_EQUAL, ExprNode::Reference(0, I32()),
                                     ExprNode::Constant(Value::INTEGER(k), I32()));
    return std::make_shared<FilterTemplate>(std::move(pred));
}

// Every non-leaf stage's single FILTER operator's constant, for stages that have exactly one.
std::vector<int32_t> FilterConstants(const PhysicalPlan &plan, size_t leaf_idx) {
    std::vector<int32_t> out;
    for (auto &s : plan.stages) {
        if (s->idx == leaf_idx) continue;
        if (s->pipeline.operators.size() != 1) continue;
        auto *f = dynamic_cast<FilterTemplate *>(s->pipeline.operators[0].get());
        if (!f) continue;
        CHECK(s->input_stages.size() == 1);
        CHECK(s->input_stages[0] == leaf_idx);
        out.push_back(f->filter.children[1].constant.GetValue<int32_t>());
    }
    return out;
}

} // namespace

TEST_CASE("plan_builder: registry Scan() hit returns the exact same leaf node") {
    auto src = std::make_shared<FakeRemoteSource>();
    PlanBuilder builder(/*optimize_remote_fetching=*/true);

    auto n1 = builder.Scan(src, TestSchema(), nullptr, nullptr);
    auto n2 = builder.Scan(src, TestSchema(), nullptr, nullptr);
    CHECK(n1 == n2); // same node, not just the same stage_idx
}

TEST_CASE("plan_builder: differing residual filters over a shared leaf fork into separate "
          "downstream stages, leaving the leaf's own pipeline untouched") {
    auto src = std::make_shared<FakeRemoteSource>();
    PlanBuilder builder(/*optimize_remote_fetching=*/true);

    auto leaf1 = builder.Scan(src, TestSchema(), nullptr, nullptr);
    auto branch1 = builder.After(leaf1, EqFilter(1), TestSchema());

    auto leaf2 = builder.Scan(src, TestSchema(), nullptr, nullptr);
    auto branch2 = builder.After(leaf2, EqFilter(2), TestSchema());

    CHECK(leaf1 == leaf2);
    CHECK(branch1 != branch2);

    // Join the two branches together purely to give them a single reachable root - the join
    // itself is semantically irrelevant to what's being tested.
    auto join_templ = std::make_shared<JoinTemplate>();
    auto root = builder.Join(branch1, branch2, join_templ, TestSchema(), {}, {}, 0, 0);

    auto plan = builder.Export(root);

    const Stage *leaf_stage = nullptr;
    int leaf_count = 0;
    for (auto &s : plan->stages) {
        if (s->IsLeaf()) {
            leaf_stage = s.get();
            leaf_count++;
        }
    }
    CHECK(leaf_count == 1); // the source was fetched exactly once
    CHECK(leaf_stage != nullptr);
    CHECK(leaf_stage->pipeline.operators.empty()); // no residual filter fused onto the shared leaf

    auto constants = FilterConstants(*plan, leaf_stage->idx);
    CHECK(constants.size() == 2);
    CHECK(((constants[0] == 1 && constants[1] == 2) || (constants[0] == 2 && constants[1] == 1)));
}

TEST_CASE("plan_builder: an identical residual filter over a shared leaf is deduplicated") {
    auto src = std::make_shared<FakeRemoteSource>();
    PlanBuilder builder(/*optimize_remote_fetching=*/true);

    auto leaf1 = builder.Scan(src, TestSchema(), nullptr, nullptr);
    auto branch1 = builder.After(leaf1, EqFilter(1), TestSchema());

    auto leaf2 = builder.Scan(src, TestSchema(), nullptr, nullptr);
    auto branch2 = builder.After(leaf2, EqFilter(1), TestSchema()); // same predicate as branch1

    CHECK(branch1 == branch2); // reused outright, no duplicate stage built

    auto plan = builder.Export(branch1);
    int leaf_count = 0;
    int filter_stage_count = 0;
    for (auto &s : plan->stages) {
        if (s->IsLeaf()) leaf_count++;
        if (s->pipeline.operators.size() == 1 &&
            dynamic_cast<FilterTemplate *>(s->pipeline.operators[0].get())) {
            filter_stage_count++;
        }
    }
    CHECK(leaf_count == 1);
    CHECK(filter_stage_count == 1);
}

int main() {
    return plume_test::RunAll();
}

// --- dynamic filters (AddDynamicFilter / AttachDynamicFilters / Export) -----------------------

namespace {

struct NamedRemoteSource : DataSource {
    explicit NamedRemoteSource(std::string n) : DataSource(DataSourceType::REMOTE_PARQUET) {
        name = std::move(n);
        schema = TestSchema();
        cardinality_total = 100;
    }
    Result<void> Resolve(const RemoteResolver &) override { return Ok(); }
};

PlanBuilder::SharedOpNode ScanOf(PlanBuilder &builder, const std::string &name) {
    return builder.Scan(std::make_shared<NamedRemoteSource>(name), TestSchema(), nullptr, nullptr);
}

// An INNER join of `probe` and `build` on column 0 of each.
PlanBuilder::SharedOpNode JoinOf(PlanBuilder &builder, PlanBuilder::SharedOpNode probe,
                                 PlanBuilder::SharedOpNode build) {
    auto t = std::make_shared<JoinTemplate>(std::vector<uint32_t>{0}, std::vector<uint32_t>{0}, build->output_schema,
                                            JoinKind::INNER);
    Schema out = JoinSchema(*t, probe->output_schema);
    return builder.Join(std::move(probe), std::move(build), std::move(t), std::move(out), {0}, {0}, 1, 1);
}

// The keys of `build`'s column 0 filter `target`'s column 0.
PlanBuilder::DynamicFilterEdge EdgeOf(PlanBuilder::SharedOpNode build, PlanBuilder::SharedOpNode target,
                                      uint32_t max_values = 16) {
    PlanBuilder::DynamicFilterEdge e;
    e.build = std::move(build);
    e.build_column = 0;
    e.target = std::move(target);
    e.target_column = 0;
    e.target_file_column = 0;
    e.key_type = I32();
    e.max_values = max_values;
    e.bloom_bits = 0;
    e.prune = false;
    return e;
}

std::vector<const DynamicFilterBuildTemplate *> Taps(const Stage &stage) {
    std::vector<const DynamicFilterBuildTemplate *> out;
    for (auto &op : stage.pipeline.operators) {
        if (auto *d = dynamic_cast<DynamicFilterBuildTemplate *>(op.get())) {
            out.push_back(d);
        }
    }
    return out;
}

} // namespace

TEST_CASE("plan_builder: a splitter inserted before a shared node's consumers records them") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto t = ScanOf(builder, "t");
    auto b = ScanOf(builder, "b");
    auto j = JoinOf(builder, t, b); // t itself carries the join's split
    // A second consumer: the join's split moves to a splitter in front of the join.
    auto other = builder.After(t, EqFilter(1), TestSchema());
    const auto &splitter = j->sources[0];
    CHECK(splitter != t);
    CHECK(splitter->sources[0] == t);
    CHECK(splitter->consumers.size() == 1);
    CHECK(splitter->consumers[0] == j);
    CHECK(t->consumers.size() == 2);
    CHECK(other->sources[0]->sources[0] == t);
}

TEST_CASE("plan_builder: an attached dynamic filter links the build stage to the target leaf") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto t = ScanOf(builder, "t");
    auto b = ScanOf(builder, "b");
    auto j = JoinOf(builder, t, b);
    builder.AddDynamicFilter(EdgeOf(b, t));
    builder.AttachDynamicFilters();
    CHECK(builder.DynamicFilters().size() == 1);
    auto plan = builder.Export(j);

    const Stage &t_stage = *plan->stages[t->stage_idx];
    const Stage &b_stage = *plan->stages[b->stage_idx];
    // A leaf build source still gets its tap.
    auto taps = Taps(b_stage);
    CHECK(taps.size() == 1);
    CHECK(taps[0]->column == 0);
    CHECK(taps[0]->max_values == 16);
    CHECK(b_stage.dynamic_filter_outputs == std::vector<uint32_t>{taps[0]->filter_id});
    CHECK(t_stage.dynamic_filter_inputs.size() == 1);
    CHECK(t_stage.dynamic_filter_inputs[0].filter_id == taps[0]->filter_id);
    CHECK(t_stage.dynamic_filter_inputs[0].source_stage == b->stage_idx);
    CHECK((t_stage.pipeline.dynamic_filter_probes ==
           std::vector<DynamicFilterProbeSpec>{{taps[0]->filter_id, 0}}));
}

TEST_CASE("plan_builder: edges with the same build node and parameters share one tap") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto t1 = ScanOf(builder, "t1");
    auto t2 = ScanOf(builder, "t2");
    auto b = ScanOf(builder, "b");
    auto j1 = JoinOf(builder, t1, b);
    auto j2 = JoinOf(builder, t2, b);
    auto root = JoinOf(builder, j1, j2);
    builder.AddDynamicFilter(EdgeOf(b, t1));
    builder.AddDynamicFilter(EdgeOf(b, t2));
    // Different parameters: a tap of its own.
    builder.AddDynamicFilter(EdgeOf(b, t2, /*max_values=*/0));
    builder.AttachDynamicFilters();
    CHECK(builder.DynamicFilters().size() == 3);
    auto plan = builder.Export(root);

    auto taps = Taps(*plan->stages[b->stage_idx]);
    CHECK(taps.size() == 2);
    const auto &in1 = plan->stages[t1->stage_idx]->dynamic_filter_inputs;
    const auto &in2 = plan->stages[t2->stage_idx]->dynamic_filter_inputs;
    CHECK(in1.size() == 1);
    CHECK(in2.size() == 2);
    CHECK(in1[0].filter_id == in2[0].filter_id); // the shared tap
    CHECK(in2[1].filter_id != in2[0].filter_id);
}
