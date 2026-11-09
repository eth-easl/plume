// Unit tests for the predicate transfer dynamic filter planner (parser/transfer/, see
// predicate-transfer.md): relation discovery, join graph edges and their closure, key class
// domains, the forward pass, and the planner's emitted dynamic filters. The node graphs
// are built by hand through PlanBuilder, as the converter would.

#include "test_util.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/expression/expression.hpp"
#include "plume/parser/converter.hpp"
#include "plume/parser/dynamic_filter_planning.hpp"
#include "plume/parser/plan_builder.hpp"
#include "plume/parser/transfer/join_graph.hpp"
#include "plume/parser/transfer/relation.hpp"

#include "duckdb/common/types/value.hpp"

#include <algorithm>
#include <limits>

using namespace plume;
using namespace plume::parser;
using namespace plume::parser::transfer;
using namespace plume::exec;
using namespace plume::expr;
using duckdb::ExpressionType;
using duckdb::Value;
using plume::catalog::DataSource;
using plume::catalog::DataSourceType;
using plume::catalog::RemoteResolver;

namespace {

using SharedOpNode = PlanBuilder::SharedOpNode;

ColumnType I32() { return {TypeId::INT32}; }

// Every table has two INT32 key columns (a, b).
Schema TwoColumns() { return Schema{{{"a", I32(), true}, {"b", I32(), true}}}; }

// A remote parquet source without row group statistics (never prunable).
struct FakeParquet : catalog::RemoteParquetDataSource {
    FakeParquet(std::string n, uint64_t rows) {
        name = std::move(n);
        schema = TwoColumns();
        cardinality_total = rows;
        paths = {"fake://" + name + ".parquet"};
        metadata.push_back({});
    }
    Result<void> Resolve(const RemoteResolver &) override { return Ok(); }
};

// A local table: it may send filters but never receives them.
struct FakeLocal : DataSource {
    explicit FakeLocal(std::string n) : DataSource(DataSourceType::LOCAL_TABLE) {
        name = std::move(n);
        schema = TwoColumns();
    }
    Result<void> Resolve(const RemoteResolver &) override { return Ok(); }
};

std::shared_ptr<FilterTemplate> EqFilter(int32_t k) {
    auto pred = ExprNode::Comparison(ExpressionType::COMPARE_EQUAL, ExprNode::Reference(1, I32()),
                                     ExprNode::Constant(Value::INTEGER(k), I32()));
    return std::make_shared<FilterTemplate>(std::move(pred));
}

// Builds a node graph and records what the converter would for dynamic filter planning.
struct Graph {
    PlanBuilder builder{/*optimize_remote_fetching=*/false};
    DynamicFilterContext ctx;
    int32_t next_constant = 0;

    SharedOpNode Parquet(const std::string &name, uint64_t rows, uint64_t estimate) {
        auto n = builder.Scan(std::make_shared<FakeParquet>(name, rows), TwoColumns(), nullptr, nullptr);
        ctx.node_card[n.get()] = estimate;
        return n;
    }
    SharedOpNode Local(const std::string &name, uint64_t estimate) {
        auto n = builder.Scan(std::make_shared<FakeLocal>(name), TwoColumns(), nullptr, nullptr);
        ctx.node_card[n.get()] = estimate;
        return n;
    }
    // A filter on column 1 (each a different constant: equal filters on one node are deduplicated).
    SharedOpNode Filter(SharedOpNode src, uint64_t estimate) {
        auto n = builder.After(std::move(src), EqFilter(next_constant++), TwoColumns());
        ctx.node_card[n.get()] = estimate;
        return n;
    }
    // A join of `left` (probe) and `right` (build) on the given key columns.
    SharedOpNode Join(SharedOpNode left, SharedOpNode right, JoinKind kind, std::vector<uint32_t> lk,
                      std::vector<uint32_t> rk, bool match_null = false) {
        auto t = std::make_shared<JoinTemplate>(lk, rk, right->output_schema, kind);
        Schema out = JoinSchema(*t, left->output_schema);
        auto n = builder.Join(left, right, std::move(t), std::move(out), lk, rk, 1, 1);
        ctx.joins.push_back(EquiJoin{left, right, kind, lk, rk, std::vector<bool>(lk.size(), match_null)});
        return n;
    }
    SharedOpNode Join(SharedOpNode left, SharedOpNode right, uint32_t lk = 0, uint32_t rk = 0) {
        return Join(std::move(left), std::move(right), JoinKind::INNER, {lk}, {rk});
    }
};

uint32_t RelOf(const RelationSet &rels, const SharedOpNode &top) {
    auto r = rels.FindByTop(top.get());
    CHECK(r.has_value());
    return *r;
}

bool HasEdge(const std::vector<TransferEdge> &edges, RelationColumn source, RelationColumn target) {
    return std::find(edges.begin(), edges.end(), TransferEdge{source, target}) != edges.end();
}

bool HasClosedEdge(const JoinGraph &graph, RelationColumn source, RelationColumn target) {
    return HasEdge(graph.EdgesFrom(source.relation), source, target);
}

ForwardPassOptions Opts(uint32_t max_depth = 2) {
    ForwardPassOptions o;
    o.max_match_fraction = 0.5;
    o.max_card_fraction = 0.2;
    o.max_depth = max_depth;
    return o;
}

} // namespace

// --- relations ----------------------------------------------------------------------------

TEST_CASE("transfer: a relation is a scan plus its single-input chain, up to the first join") {
    Graph g;
    auto t = g.Parquet("t", 1000, 1000);
    auto b = g.Parquet("b", 1000, 1000);
    auto bf = g.Filter(b, 40);
    auto root = g.Join(t, bf);

    auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
    CHECK(rels.size() == 2);
    const Relation &rb = rels[RelOf(rels, bf)];
    CHECK(rb.leaf == b);
    CHECK(rb.top == bf);
    CHECK(rb.card == 40); // the chain top's estimate
    CHECK(rb.filterable);
    // Both columns pass the filter unchanged: they map back to the scan.
    auto leaf = rels.LeafColumnOf({rb.id, 1});
    CHECK(leaf && leaf->node == b && leaf->scan_column == 1);
    CHECK(rels[RelOf(rels, t)].top == t);
}

TEST_CASE("transfer: a shared scan forks into relations that send but never receive") {
    Graph g;
    auto s = g.Parquet("s", 1000, 1000);
    auto s1 = g.Filter(s, 10);
    auto s2 = g.Filter(s, 500); // the scan now feeds two branches
    auto t = g.Parquet("t", 1000, 1000);
    auto root = g.Join(g.Join(t, s1), s2);

    auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
    const Relation &r1 = rels[RelOf(rels, s1)];
    const Relation &r2 = rels[RelOf(rels, s2)];
    CHECK(r1.leaf == s);
    CHECK(r2.leaf == s);
    CHECK(!r1.filterable);
    CHECK(!r2.filterable);
    CHECK(r1.card == 10);
    CHECK(rels[RelOf(rels, t)].filterable);

    // The forked branch's filter still reaches the other scan.
    auto graph = JoinGraph::Build(rels, g.ctx.joins, root);
    auto transfers = graph.ForwardPass(rels, Opts());
    bool found = false;
    for (const auto &x : transfers) {
        found |= x.edge.source.relation == r1.id && x.edge.target.relation == RelOf(rels, t);
        CHECK(x.edge.target.relation != r1.id && x.edge.target.relation != r2.id);
    }
    CHECK(found);
}

// --- join graph ---------------------------------------------------------------------------

TEST_CASE("transfer: edge directions follow the sides a join drops unmatched rows of") {
    auto edges_of = [](JoinKind kind, bool match_null = false) {
        Graph g;
        auto l = g.Parquet("l", 1000, 1000);
        auto r = g.Parquet("r", 1000, 1000);
        auto root = g.Join(l, r, kind, {0}, {0}, match_null);
        auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
        auto graph = JoinGraph::Build(rels, g.ctx.joins, root);
        const RelationColumn lc{RelOf(rels, l), 0}, rc{RelOf(rels, r), 0};
        return std::make_pair(HasEdge(graph.JoinEdges(), lc, rc), HasEdge(graph.JoinEdges(), rc, lc));
    };
    // {left filters right, right filters left}
    CHECK((edges_of(JoinKind::INNER) == std::make_pair(true, true)));
    CHECK((edges_of(JoinKind::SEMI) == std::make_pair(true, true)));
    CHECK((edges_of(JoinKind::LEFT) == std::make_pair(true, false)));  // left rows are preserved
    CHECK((edges_of(JoinKind::RIGHT) == std::make_pair(false, true))); // right rows are preserved
    CHECK((edges_of(JoinKind::OUTER) == std::make_pair(false, false)));
    CHECK((edges_of(JoinKind::ANTI) == std::make_pair(false, false)));
    CHECK((edges_of(JoinKind::MARK) == std::make_pair(false, false)));
    // IS NOT DISTINCT FROM: NULL keys match, which a filter would reject.
    CHECK((edges_of(JoinKind::INNER, /*match_null=*/true) == std::make_pair(false, false)));
}

TEST_CASE("transfer: equal keys close transitively, never across a preserved side") {
    // (a JOIN b ON a.0 = b.0) JOIN c ON b.0 = c.0: a filters c through b.
    {
        Graph g;
        auto a = g.Parquet("a", 1000, 1000);
        auto b = g.Parquet("b", 1000, 1000);
        auto c = g.Parquet("c", 1000, 1000);
        auto ab = g.Join(b, a);
        auto root = g.Join(c, ab, 0, /*b.0 in a JOIN b's output*/ 0);
        auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
        auto graph = JoinGraph::Build(rels, g.ctx.joins, root);
        const RelationColumn ac{RelOf(rels, a), 0}, cc{RelOf(rels, c), 0};
        CHECK(!HasEdge(graph.JoinEdges(), ac, cc)); // no join between them...
        CHECK(HasClosedEdge(graph, ac, cc));        // ...but transitively
        CHECK(HasClosedEdge(graph, cc, ac));
    }
    // b LEFT JOIN a, then JOIN c on b's key: a only filters b's side through b, and nothing
    // filters b itself from a's side; c reaches b (inner) but not a through b (b -> a blocked).
    {
        Graph g;
        auto a = g.Parquet("a", 1000, 1000);
        auto b = g.Parquet("b", 1000, 1000);
        auto c = g.Parquet("c", 1000, 1000);
        auto ba = g.Join(b, a, JoinKind::LEFT, {0}, {0});
        auto root = g.Join(c, ba, 0, 0);
        auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
        auto graph = JoinGraph::Build(rels, g.ctx.joins, root);
        const RelationColumn ac{RelOf(rels, a), 0}, bc{RelOf(rels, b), 0}, cc{RelOf(rels, c), 0};
        CHECK(HasClosedEdge(graph, bc, ac)); // preserved b filters the nullable a
        CHECK(!HasClosedEdge(graph, ac, bc)); // but not the other way
        CHECK(HasClosedEdge(graph, cc, bc));
        CHECK(HasClosedEdge(graph, cc, ac)); // c -> b -> a
        CHECK(!HasClosedEdge(graph, ac, cc)); // a -> b is blocked, so a never reaches c
    }
}

TEST_CASE("transfer: a scan feeding two consumers receives no edges") {
    Graph g;
    auto t = g.Parquet("t", 1000, 1000);
    auto b = g.Parquet("b", 1000, 1000);
    auto j = g.Join(t, b);
    auto other = g.Filter(t, 10); // a second consumer of the scan t
    auto root = g.Join(j, other);
    auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
    auto graph = JoinGraph::Build(rels, g.ctx.joins, root);
    const uint32_t rt = RelOf(rels, t);
    for (const auto &e : graph.JoinEdges()) {
        CHECK(e.target.relation != rt);
    }
}

TEST_CASE("transfer: a key domain is the smallest known domain of its class") {
    Graph g;
    auto fact = g.Parquet("fact", 6'000'000, 6'000'000); // a foreign key: domain = row count
    auto dim = g.Parquet("dim", 10'000, 10'000);         // its primary key
    auto loc = g.Local("loc", 5);                        // unknown domain
    auto root = g.Join(g.Join(fact, dim), loc);
    auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
    auto graph = JoinGraph::Build(rels, g.ctx.joins, root);
    const RelationColumn fc{RelOf(rels, fact), 0}, dc{RelOf(rels, dim), 0}, lc{RelOf(rels, loc), 0};
    CHECK(graph.KeyDomain(fc) == 10'000);
    CHECK(graph.KeyDomain(lc) == 10'000); // loc's own is unknown: it takes the class's
    CHECK(graph.KeyDomain(lc, /*exclude=*/dc) == 6'000'000);
    CHECK(graph.KeyDomain({RelOf(rels, fact), 1}) == 0); // not a join key
}

// --- forward pass -----------------------------------------------------------------------------

TEST_CASE("transfer: a filtered relation filters a larger one, each target column keeps its best") {
    Graph g;
    auto fact = g.Parquet("fact", 100'000, 100'000);
    auto d1 = g.Filter(g.Parquet("d1", 1000, 1000), 100); // keeps 10% of its keys
    auto d2 = g.Filter(g.Local("d2", 1000), 300); // keeps 30%; local, so d1 cannot filter it
    // fact.0 = d1.0 and fact.0 = d2.0: two offers for one target column.
    auto root = g.Join(g.Join(fact, d1), d2, 0, 0);
    auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
    auto graph = JoinGraph::Build(rels, g.ctx.joins, root);
    auto transfers = graph.ForwardPass(rels, Opts());

    const uint32_t rf = RelOf(rels, fact), r1 = RelOf(rels, d1);
    std::vector<Transfer> into_fact;
    for (const auto &x : transfers) {
        if (x.edge.target.relation == rf) {
            into_fact.push_back(x);
        }
    }
    CHECK(into_fact.size() == 1);
    CHECK(into_fact[0].edge.source.relation == r1);
    CHECK(into_fact[0].fraction == 0.1);
    CHECK(into_fact[0].depth == 1);
    // Smaller first: the fact table is scheduled after d1, so it never filters d1.
    for (const auto &x : transfers) {
        CHECK(x.edge.target.relation != r1);
    }
}

TEST_CASE("transfer: an unselective relation sends nothing") {
    Graph g;
    auto fact = g.Parquet("fact", 100'000, 100'000);
    auto dim = g.Parquet("dim", 1000, 1000); // all of its keys
    auto root = g.Join(fact, dim);
    auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
    auto graph = JoinGraph::Build(rels, g.ctx.joins, root);
    CHECK(graph.ForwardPass(rels, Opts()).empty());
}

TEST_CASE("transfer: chains of transfers carry filters through relations, capped by depth") {
    // small.0 = mid.0, mid.1 = big.1: small filters mid, mid (filtered) filters big.
    auto run = [](uint32_t max_depth) {
        Graph g;
        auto small = g.Filter(g.Parquet("small", 1000, 1000), 10);
        auto mid = g.Parquet("mid", 1000, 2000);
        auto big = g.Parquet("big", 1000, 100'000);
        auto sm = g.Join(mid, small, 0, 0);
        auto root = g.Join(big, sm, 1, /*mid.1*/ 1);
        auto rels = RelationSet::Discover(g.builder, root, g.ctx.node_card);
        auto graph = JoinGraph::Build(rels, g.ctx.joins, root);
        auto transfers = graph.ForwardPass(rels, Opts(max_depth));
        std::vector<std::pair<uint32_t, uint32_t>> pairs;
        for (const auto &x : transfers) {
            pairs.push_back({x.edge.source.relation, x.edge.target.relation});
        }
        return std::make_tuple(pairs, RelOf(rels, small), RelOf(rels, mid), RelOf(rels, big));
    };
    {
        auto [pairs, small, mid, big] = run(2);
        CHECK(pairs.size() == 2);
        CHECK((std::find(pairs.begin(), pairs.end(), std::make_pair(small, mid)) != pairs.end()));
        CHECK((std::find(pairs.begin(), pairs.end(), std::make_pair(mid, big)) != pairs.end()));
    }
    {
        auto [pairs, small, mid, big] = run(1);
        CHECK(pairs.size() == 1);
        CHECK((pairs[0] == std::make_pair(small, mid)));
    }
}

// --- planner ------------------------------------------------------------------------------

TEST_CASE("transfer: the planner taps the source relation's top and links the target scan") {
    Graph g;
    auto t = g.Parquet("t", 100'000, 100'000);
    auto b = g.Parquet("b", 1000, 1000);
    auto bf = g.Filter(b, 50);
    auto root = g.Join(t, bf);

    ConverterConfig cfg;
    PlanDynamicFilters(g.builder, root, g.ctx, cfg);
    const auto &edges = g.builder.DynamicFilters();
    CHECK(edges.size() == 1);
    CHECK(edges[0].build == bf);
    CHECK(edges[0].target == t);
    CHECK(edges[0].max_values == cfg.dynamic_filter_max_values);
    CHECK(!edges[0].prune); // no row group statistics

    auto plan = g.builder.Export(root);
    const Stage &t_stage = *plan->stages[t->stage_idx];
    CHECK(t_stage.dynamic_filter_inputs.size() == 1);
    const Stage &b_stage = *plan->stages[t_stage.dynamic_filter_inputs[0].source_stage];
    // The tap follows the filter in the build relation's stage.
    const auto &ops = b_stage.pipeline.operators;
    CHECK(!ops.empty());
    CHECK(dynamic_cast<const FilterTemplate *>(ops[ops.size() - 2].get()) != nullptr);
    CHECK(dynamic_cast<const DynamicFilterBuildTemplate *>(ops.back().get()) != nullptr);
}

int main() {
    return plume_test::RunAll();
}
