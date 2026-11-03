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

// --- dynamic filters (AddDynamicFilter / ApplyDynamicFilters / Export) -----------------------

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

// The keys of `build`'s column 0 filter `target`'s column 0, which `probe_input` (the join's probe
// input) traces back to through its column 0.
PlanBuilder::DynamicFilterEdge EdgeOf(PlanBuilder::SharedOpNode build, PlanBuilder::SharedOpNode target,
                                      PlanBuilder::SharedOpNode probe_input, double benefit,
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
    e.probe_input = std::move(probe_input);
    e.probe_key = 0;
    e.prune = false;
    e.benefit = benefit;
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

TEST_CASE("plan_builder: an accepted dynamic filter links the build stage to the target leaf") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto t = ScanOf(builder, "t");
    auto b = ScanOf(builder, "b");
    auto j = JoinOf(builder, t, b);
    builder.AddDynamicFilter(EdgeOf(b, t, t, 0.9));
    builder.ApplyDynamicFilters(j, 2);
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
    builder.AddDynamicFilter(EdgeOf(b, t1, t1, 0.9));
    builder.AddDynamicFilter(EdgeOf(b, t2, t2, 0.8));
    // Different parameters: a tap of its own.
    builder.AddDynamicFilter(EdgeOf(b, t2, t2, 0.7, /*max_values=*/0));
    builder.ApplyDynamicFilters(root, 2);
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

TEST_CASE("plan_builder: a dynamic filter on a shared scan is dropped") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto t = ScanOf(builder, "t");
    auto b = ScanOf(builder, "b");
    auto j = JoinOf(builder, t, b);
    // The scan also feeds a filter (e.g. a CTE's other reference).
    auto other = builder.After(t, EqFilter(1), TestSchema());
    auto root = JoinOf(builder, j, other);
    builder.AddDynamicFilter(EdgeOf(b, t, t, 0.9));
    builder.ApplyDynamicFilters(root, 2);
    CHECK(builder.DynamicFilters().empty());
    auto plan = builder.Export(root);
    for (auto &s : plan->stages) {
        CHECK(Taps(*s).empty());
        CHECK(!s->ConsumesDynFilter());
        CHECK(!s->ProducesDynFilter());
    }
}

TEST_CASE("plan_builder: a dynamic filter whose build waits for its target is dropped") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto t = ScanOf(builder, "t");
    auto b = ScanOf(builder, "b");
    auto j = JoinOf(builder, t, b);
    // The join's output reads the target itself: the filtered scan would wait on itself.
    builder.AddDynamicFilter(EdgeOf(j, t, t, 0.9));
    builder.ApplyDynamicFilters(j, 2);
    CHECK(builder.DynamicFilters().empty());
}

TEST_CASE("plan_builder: chained dynamic filters are capped by the chain depth, weakest dropped") {
    auto build_chain = [](PlanBuilder &builder, std::vector<PlanBuilder::SharedOpNode> &targets) {
        // t1 JOIN (t2 JOIN (t3 JOIN b3)): each level's build side is the next level's join.
        auto t1 = ScanOf(builder, "t1");
        auto t2 = ScanOf(builder, "t2");
        auto t3 = ScanOf(builder, "t3");
        auto b3 = ScanOf(builder, "b3");
        auto j3 = JoinOf(builder, t3, b3);
        auto j2 = JoinOf(builder, t2, j3);
        auto j1 = JoinOf(builder, t1, j2);
        builder.AddDynamicFilter(EdgeOf(j2, t1, t1, 0.9));
        builder.AddDynamicFilter(EdgeOf(j3, t2, t2, 0.5));
        builder.AddDynamicFilter(EdgeOf(b3, t3, t3, 0.7));
        targets = {t1, t2, t3};
        return j1;
    };

    {
        PlanBuilder builder(false);
        std::vector<PlanBuilder::SharedOpNode> targets;
        auto root = build_chain(builder, targets);
        builder.ApplyDynamicFilters(root, 3);
        CHECK(builder.DynamicFilters().size() == 3);
    }
    {
        // Depth 2: t1 <- j2 <- t2 <- j3 <- t3 is 3 long; the weakest edge (into t2) goes, which
        // breaks the chain in the middle and keeps both others.
        PlanBuilder builder(false);
        std::vector<PlanBuilder::SharedOpNode> targets;
        auto root = build_chain(builder, targets);
        builder.ApplyDynamicFilters(root, 2);
        const auto &kept = builder.DynamicFilters();
        CHECK(kept.size() == 2);
        CHECK(kept[0].target == targets[0]);
        CHECK(kept[1].target == targets[2]);
        auto plan = builder.Export(root);
        CHECK(plan->stages[targets[0]->stage_idx]->ConsumesDynFilter());
        CHECK(!plan->stages[targets[1]->stage_idx]->ConsumesDynFilter());
        CHECK(plan->stages[targets[2]->stage_idx]->ConsumesDynFilter());
    }
    {
        PlanBuilder builder(false);
        std::vector<PlanBuilder::SharedOpNode> targets;
        auto root = build_chain(builder, targets);
        builder.ApplyDynamicFilters(root, 1);
        // Benefit order: t1 (0.9) accepted. t3 (0.7) chains with it too: t1's build j2 reads the
        // scan t3, which would wait for b3. t2 (0.5) chains directly.
        CHECK(builder.DynamicFilters().size() == 1);
        CHECK(builder.DynamicFilters()[0].target == targets[0]);
    }
}
