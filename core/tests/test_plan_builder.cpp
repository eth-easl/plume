#include "test_util.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/execution/operators/aggregate.hpp"
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

// --- early aggregates (FindCoPartitioning / UndoEarlyAggregate / CollapseEarlyAggregates) ------

namespace {

using SharedOpNode = PlanBuilder::SharedOpNode;

ColumnType I64() { return {TypeId::INT64}; }

// sum(column 1) grouped by `group_col`.
std::shared_ptr<AggregateTemplate> SumBy(uint32_t group_col, uint32_t sum_col = 1) {
    std::vector<AggregateSpec> aggs{{"sum", I64(), {ExprNode::Reference(sum_col, I32())}}};
    return std::make_shared<AggregateTemplate>(std::move(aggs),
                                               std::vector<ExprNode>{ExprNode::Reference(group_col, I32())});
}

// A two-phase sum grouped by `group_col` over `src`, built like Converter::BuildAggregate does:
// partial (After) -> shuffle by the group key -> combine, with `early_agg` on the combine.
SharedOpNode TwoPhaseSum(PlanBuilder &builder, SharedOpNode src, uint32_t group_col, uint32_t partitions = 4) {
    auto partial = SumBy(group_col);
    Schema partial_out = AggregateSchema(*partial, src->output_schema);
    auto early = std::make_shared<PlanBuilder::EarlyAggregate>();
    early->single_phase = SumBy(group_col);
    early->partial = builder.After(src, partial, partial_out);

    std::vector<AggregateSpec> combines{{"sum", I64(), {ExprNode::Reference(1, I64())}}};
    auto combine = std::make_shared<AggregateTemplate>(std::move(combines),
                                                       std::vector<ExprNode>{ExprNode::Reference(0, I32())});
    Schema combine_out = AggregateSchema(*combine, partial_out);
    auto last = builder.Shuffle(early->partial, combine, combine_out, {0}, partitions);
    last->early_agg = early;
    return last;
}

// A join of `kind` of `probe` and `build` on column 0 of each, both sides split `splits` ways.
SharedOpNode KeyedJoin(PlanBuilder &builder, SharedOpNode probe, SharedOpNode build, JoinKind kind,
                       uint32_t splits = 4) {
    auto t = std::make_shared<JoinTemplate>(std::vector<uint32_t>{0}, std::vector<uint32_t>{0}, build->output_schema,
                                            kind);
    Schema out = JoinSchema(*t, probe->output_schema);
    return builder.Join(std::move(probe), std::move(build), std::move(t), std::move(out), {0}, {0}, splits, splits);
}

// The partial of a sum grouped by `group_col` over `src` (FindCoPartitioning's input).
SharedOpNode PartialOver(PlanBuilder &builder, SharedOpNode src, uint32_t group_col) {
    return TwoPhaseSum(builder, std::move(src), group_col)->early_agg->partial;
}

size_t CountAggregates(const PhysicalPlan &plan) {
    size_t n = 0;
    for (auto &s : plan.stages) {
        for (auto &op : s->pipeline.operators) {
            n += op->type == OpType::AGGREGATE;
        }
    }
    return n;
}

} // namespace

TEST_CASE("plan_builder: FindCoPartitioning after an INNER join accepts the probe or the build key") {
    for (uint32_t group_col : {0u, 2u}) { // probe key, build key
        PlanBuilder builder(/*optimize_remote_fetching=*/false);
        auto j = KeyedJoin(builder, ScanOf(builder, "t"), ScanOf(builder, "b"), JoinKind::INNER);
        auto partial = PartialOver(builder, j, group_col);
        CHECK(PlanBuilder::FindCoPartitioning(partial) == j);
    }
    // A non-key column isn't partitioned.
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto j = KeyedJoin(builder, ScanOf(builder, "t"), ScanOf(builder, "b"), JoinKind::INNER);
    CHECK(PlanBuilder::FindCoPartitioning(PartialOver(builder, j, 1)) == nullptr);
}

TEST_CASE("plan_builder: FindCoPartitioning only counts join sides whose keys can't be NULL-extended") {
    struct Case {
        JoinKind kind;
        bool probe_ok;
        bool build_ok;
    };
    for (Case c : {Case{JoinKind::LEFT, true, false}, Case{JoinKind::RIGHT, false, true},
                   Case{JoinKind::OUTER, false, false}}) {
        for (bool probe_key : {true, false}) {
            PlanBuilder builder(/*optimize_remote_fetching=*/false);
            auto j = KeyedJoin(builder, ScanOf(builder, "t"), ScanOf(builder, "b"), c.kind);
            auto partial = PartialOver(builder, j, probe_key ? 0 : 2);
            const bool expected = probe_key ? c.probe_ok : c.build_ok;
            CHECK((PlanBuilder::FindCoPartitioning(partial) == j) == expected);
            if (!expected) {
                CHECK(PlanBuilder::FindCoPartitioning(partial) == nullptr);
            }
        }
    }
}

TEST_CASE("plan_builder: FindCoPartitioning traces the group keys through a passthrough filter") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto j = KeyedJoin(builder, ScanOf(builder, "t"), ScanOf(builder, "b"), JoinKind::INNER);
    auto f = builder.After(j, EqFilter(1), j->output_schema);
    CHECK(PlanBuilder::FindCoPartitioning(PartialOver(builder, f, 0)) == f);
}

TEST_CASE("plan_builder: FindCoPartitioning skips a partial that something else reads too") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto j = KeyedJoin(builder, ScanOf(builder, "t"), ScanOf(builder, "b"), JoinKind::INNER);
    auto partial = PartialOver(builder, j, 0);
    // A second reader of the partial.
    builder.After(partial, EqFilter(1), partial->output_schema);
    CHECK(PlanBuilder::FindCoPartitioning(partial) == nullptr);
}

TEST_CASE("plan_builder: FindCoPartitioning needs more than one partition") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto j = KeyedJoin(builder, ScanOf(builder, "t"), ScanOf(builder, "b"), JoinKind::INNER, /*splits=*/1);
    CHECK(PlanBuilder::FindCoPartitioning(PartialOver(builder, j, 0)) == nullptr);
}

TEST_CASE("plan_builder: FindCoPartitioning reuses an existing keyed split of the partial's input") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto t = ScanOf(builder, "t");
    builder.After(t, EqFilter(1), TestSchema()); // t has a consumer: the partials get stages of their own
    auto by_key = PartialOver(builder, t, 0);
    auto by_other = PartialOver(builder, t, 1);
    auto j = KeyedJoin(builder, t, ScanOf(builder, "b"), JoinKind::INNER); // splits t by column 0

    auto split = PlanBuilder::FindCoPartitioning(by_key);
    CHECK(split != nullptr);
    CHECK(split == j->sources[0]);
    CHECK(split->key_columns == std::vector<uint32_t>{0});
    CHECK(split->num_splits == 4);
    CHECK(PlanBuilder::FindCoPartitioning(by_other) == nullptr);
}

TEST_CASE("plan_builder: CollapseEarlyAggregates reuses a split a later join makes of the scan") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto t = ScanOf(builder, "t");
    builder.After(t, EqFilter(1), TestSchema());
    auto agg = TwoPhaseSum(builder, t, 0);
    auto j = KeyedJoin(builder, t, ScanOf(builder, "b"), JoinKind::INNER);
    auto root = JoinOf(builder, agg, j);
    auto single = agg->early_agg->single_phase;
    auto split = j->sources[0];

    PlanBuilder::CollapseEarlyAggregates(root);
    CHECK(agg->early_agg == nullptr);
    CHECK(agg->templ == single);
    CHECK(agg->sources[0] == split);
    CHECK(std::find(split->consumers.begin(), split->consumers.end(), agg) != split->consumers.end());

    auto plan = builder.Export(root);
    CHECK(CountAggregates(*plan) == 1);
    const Stage &agg_stage = *plan->stages[agg->stage_idx];
    CHECK(agg_stage.input_stages == std::vector<size_t>{t->stage_idx});
    CHECK(plan->stages[t->stage_idx]->pipeline.output_split.partitions == 4);
    CHECK(plan->stages[t->stage_idx]->pipeline.output_split.key_columns == std::vector<uint32_t>{0});
}

TEST_CASE("plan_builder: CollapseEarlyAggregates fuses the aggregate into a keyed join stage") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto j = KeyedJoin(builder, ScanOf(builder, "t"), ScanOf(builder, "b"), JoinKind::INNER);
    auto agg = TwoPhaseSum(builder, j, 0);
    auto single = agg->early_agg->single_phase;

    PlanBuilder::CollapseEarlyAggregates(agg);
    CHECK(agg->templ == single);
    CHECK(agg->sources[0] == j);
    CHECK(j->consumers == std::vector<SharedOpNode>{agg});

    auto plan = builder.Export(agg);
    CHECK(plan->stages.size() == 3); // two leaves + the join stage
    const Stage &join_stage = *plan->stages[plan->root_idx];
    CHECK(join_stage.pipeline.operators.size() == 2);
    CHECK(join_stage.pipeline.operators[0]->type == OpType::JOIN);
    CHECK(join_stage.pipeline.operators[1]->type == OpType::AGGREGATE);
    CHECK(join_stage.output_schema.Equals(AggregateSchema(*single, j->output_schema)));
}

TEST_CASE("plan_builder: CollapseEarlyAggregates keeps an unpartitioned input two-phase") {
    PlanBuilder builder(/*optimize_remote_fetching=*/false);
    auto t = ScanOf(builder, "t");
    auto agg = TwoPhaseSum(builder, t, 0);
    auto partial = agg->early_agg->partial;
    auto combine = agg->templ;
    auto combine_src = agg->sources[0];

    PlanBuilder::CollapseEarlyAggregates(agg);
    CHECK(agg->early_agg == nullptr);
    CHECK(agg->templ == combine);
    CHECK(agg->sources[0] == combine_src);
    CHECK(partial->sources[0] == t);
    CHECK(t->consumers == std::vector<SharedOpNode>{partial});

    auto plan = builder.Export(agg);
    CHECK(CountAggregates(*plan) == 2);
    CHECK(plan->stages.size() == 2);
}
