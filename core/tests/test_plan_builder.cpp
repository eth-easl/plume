// Regression tests for PlanBuilder's remote-registry sharing (Scan()/RemoteRegistry* in
// plan_builder.cpp): a second Scan() over the same remote data source with the same pushed
// projection/filter must reuse the first Scan()'s exact leaf node (not just its stage), so that
// a differing residual filter/projection chained after it (via After()) forks into its own
// downstream stage instead of being fused onto the shared leaf's pipeline - and an identical
// residual chain gets deduplicated onto the same downstream stage instead of building a
// redundant duplicate.

#include "test_util.hpp"

#include "plume/catalog/catalog.hpp"
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
        if (s->is_leaf()) {
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
        if (s->is_leaf()) leaf_count++;
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
