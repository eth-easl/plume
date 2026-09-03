#include "duckdb.hpp"
#include "mock_runtime.hpp"
#include "test_util.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/catalog/plume_remote.hpp"
#include "plume/dandelion/composition.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/parser/converter.hpp"
#include "plume/parser/physical_plan.hpp"

#include "duckdb/common/types/data_chunk.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

using namespace plume;
using namespace plume::catalog;
using namespace plume::client;
using namespace plume::parser;

namespace {

// Render one result row (a DuckDB DataChunk row) as a tab-joined string using
// DuckDB's own Value formatting, so Plume's and DuckDB's outputs format alike.
std::string RowString(duckdb::DataChunk &chunk, duckdb::idx_t row) {
    std::string s;
    for (duckdb::idx_t c = 0; c < chunk.ColumnCount(); c++) {
        if (c > 0) {
            s += '\t';
        }
        s += chunk.GetValue(c, row).ToString();
    }
    return s;
}

// Decode the mock runtime's output blocks into row strings.
std::vector<std::string> PlumeRows(const BlockSet &blocks) {
    std::vector<std::string> rows;
    for (auto &b : blocks) {
        Schema schema;
        auto chunks =
            memory::ImportBlockChunks(const_cast<uint8_t *>(b.data()), b.size(), schema).unwrap();
        for (auto &c : chunks) {
            for (duckdb::idx_t r = 0; r < c->size(); r++) {
                rows.push_back(RowString(*c, r));
            }
        }
    }
    return rows;
}

// Run a query directly in DuckDB and collect its rows.
std::vector<std::string> DuckRows(duckdb::Connection &con, const std::string &sql) {
    auto result = con.Query(sql);
    if (result->HasError()) {
        throw std::runtime_error("duckdb query failed: " + result->GetError());
    }
    std::vector<std::string> rows;
    while (auto chunk = result->Fetch()) {
        for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
            rows.push_back(RowString(*chunk, r));
        }
    }
    return rows;
}

// Build a ConverterConfig from the handful of knobs these tests vary; projection/
// filter pushdown stay at their (enabled) default since no in-memory-table test
// needs to toggle them.
ConverterConfig Cfg(bool pre_aggregate, uint32_t max_splits = 1, uint64_t target_rows_per_split = 1u << 20) {
    ConverterConfig cfg;
    cfg.early_aggregation = pre_aggregate;
    cfg.max_splits = max_splits;
    cfg.target_rows_per_split = target_rows_per_split;
    return cfg;
}

// Compare Plume's result to DuckDB's. When `ordered` is false (no ORDER BY) the
// rows are compared as multisets.
void ExpectMatch(duckdb::Connection &con, const std::string &sql, bool ordered,
                 const SourceCatalog &sources = {}, bool pre_aggregate = true, uint32_t max_splits = 1,
                 uint64_t target_rows_per_split = 1u << 20) {
    auto plan = BuildPhysicalPlan(con, sql, sources, Cfg(pre_aggregate, max_splits, target_rows_per_split)).unwrap();
    MockRuntime runtime(con);
    auto plume_rows = PlumeRows(runtime.Run(*plan));
    auto duck_rows = DuckRows(con, sql);

    if (!ordered) {
        std::sort(plume_rows.begin(), plume_rows.end());
        std::sort(duck_rows.begin(), duck_rows.end());
    }
    if (plume_rows != duck_rows) {
        std::string msg = "result mismatch for: " + sql + "\n  plume (" + std::to_string(plume_rows.size()) +
                          " rows):\n";
        for (auto &r : plume_rows) {
            msg += "    " + r + "\n";
        }
        msg += "  duckdb (" + std::to_string(duck_rows.size()) + " rows):\n";
        for (auto &r : duck_rows) {
            msg += "    " + r + "\n";
        }
        throw plume_test::Failure{msg};
    }
}

duckdb::Connection MakeDb(duckdb::DuckDB &db) {
    duckdb::Connection con(db);
    con.Query("CREATE TABLE orders(o_id INTEGER, cust INTEGER, amount INTEGER, region VARCHAR)");
    con.Query("INSERT INTO orders VALUES "
              "(1, 10, 100, 'east'), (2, 10, 50, 'east'), (3, 20, 30, 'west'), "
              "(4, 30, 70, 'west'), (5, 20, 25, 'east'), (6, 10, 0, 'north'), "
              "(7, 30, 12, 'south'), (8, 40, 200, 'west')");
    con.Query("CREATE TABLE customers(c_id INTEGER, name VARCHAR, tier INTEGER)");
    con.Query("INSERT INTO customers VALUES "
              "(10, 'alice', 1), (20, 'bob', 2), (30, 'carol', 1), (50, 'dave', 3)");
    con.Query("CREATE TABLE tiers(t_id INTEGER, label VARCHAR)");
    con.Query("INSERT INTO tiers VALUES (1, 'gold'), (2, 'silver'), (3, 'bronze')");
    return con;
}

// --- dynamic filter (join build -> remote parquet probe scan pruning) helpers ----------

// A resolved (schema + cardinality already set) fake REMOTE_PARQUET source, so tests can
// exercise Converter's dynamic-filter attachment without a real remote fetch/resolve.
struct FakeRemoteOrders : RemoteParquetDataSource {
    explicit FakeRemoteOrders(uint64_t card) {
        name = "remote_orders";
        schema = Schema{{{"o_id", {TypeId::INT32}, false}, {"cust", {TypeId::INT32}, false}}};
        cardinality_total = card;
        // Never actually parsed by these tests (Resolve() is stubbed out below), but
        // BuildParquetPrepareInputs/BuildParquetStageInputs index into paths/footer_bytes
        // 1:1, so they need to be present and the same size.
        paths = {"fake://remote_orders.parquet"};
        footer_bytes = {{0}};
        metadata.push_back({});
    }
    Result<void> Resolve(const RemoteResolver &) override { return Ok(); }
};

// A fresh in-memory DB with a small local `customers` table and `remote_orders` registered
// as a plume_remote(...) source of `orders_cardinality` estimated rows.
duckdb::Connection MakeRemoteJoinDb(duckdb::DuckDB &db, uint64_t orders_cardinality) {
    duckdb::Connection con(db);
    con.Query("CREATE TABLE customers(cust INTEGER, name VARCHAR)");
    con.Query("INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')");

    auto catalog_ptr = std::make_shared<SourceCatalog>();
    CHECK(catalog_ptr->Add(std::make_shared<FakeRemoteOrders>(orders_cardinality)).is_ok());
    auto registry = duckdb::make_shared_ptr<PlumeRemoteInfo>();
    registry->catalog = catalog_ptr;
    RegisterPlumeRemote(con, std::move(registry));
    return con;
}

const LeafStage *FindLeaf(const PhysicalPlan &plan, const std::string &source_name) {
    for (auto &s : plan.stages) {
        if (!s->IsLeaf()) continue;
        auto *leaf = static_cast<const LeafStage *>(s.get());
        if (leaf->data_source->name == source_name) {
            return leaf;
        }
    }
    return nullptr;
}

size_t CountLeaves(const PhysicalPlan &plan, DataSourceType type) {
    size_t n = 0;
    for (auto &s : plan.stages) {
        if (s->IsLeaf() && static_cast<const LeafStage &>(*s).data_source->type == type) {
            n++;
        }
    }
    return n;
}

// Every stage's serialized pipeline template: BuildDandelionComposition names each
// one `st_<idx>` in the DSL, so pull those names back out via
// ParseCompositionInputNames and match them 1:1 against comp.in_sets.
std::vector<const dandelion::BinaryData *> StageTemplates(const dandelion::DandelionComposition &comp) {
    auto names = dandelion::ParseCompositionInputNames(comp).unwrap();
    CHECK(names.size() == comp.in_sets.size());
    std::vector<const dandelion::BinaryData *> out;
    for (size_t i = 0; i < names.size() && i < comp.in_sets.size(); i++) {
        if (names[i].rfind("st_", 0) == 0 && !comp.in_sets[i].empty()) {
            out.push_back(&comp.in_sets[i][0].data);
        }
    }
    return out;
}

// The pipeline's sole DynamicFilterBuildTemplate, or nullptr if it has none.
const exec::DynamicFilterBuildTemplate *FindDynamicFilterBuild(const Stage &stage) {
    const exec::DynamicFilterBuildTemplate *found = nullptr;
    for (auto &op : stage.pipeline.operators) {
        if (auto *d = dynamic_cast<exec::DynamicFilterBuildTemplate *>(op.get())) {
            CHECK(found == nullptr); // at most one
            found = d;
        }
    }
    return found;
}

} // namespace

TEST_CASE("e2e: projection + filter") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con, "SELECT o_id, amount FROM orders WHERE amount > 20", false);
}

TEST_CASE("e2e: group by aggregate") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con, "SELECT region, sum(amount), count(*), min(amount), max(amount) "
                     "FROM orders GROUP BY region",
                false);
}

TEST_CASE("e2e: global aggregate") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con, "SELECT sum(amount), avg(amount), count(*) FROM orders", false);
}

TEST_CASE("e2e: order by + limit") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con, "SELECT o_id, amount FROM orders ORDER BY amount DESC, o_id LIMIT 4", true);
}

// Two-phase (pre-)aggregation: each algebraic aggregate is partially aggregated in
// the producing stage and combined after the shuffle. Run every aggregate kind
// with both pre_aggregate on (default) and off, asserting both match DuckDB.
TEST_CASE("e2e: grouped aggregate decompositions (pre-aggregate on and off)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    // sum/count(*)/avg/min/max in one query, plus count(col) separately (DuckDB's
    // resolver mis-binds count(*) and count(col) together, independent of Plume).
    for (const char *sql :
         {"SELECT region, sum(amount), count(*), avg(amount), min(amount), max(amount) "
          "FROM orders GROUP BY region",
          "SELECT region, count(amount), sum(amount) FROM orders GROUP BY region"}) {
        ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/true);
        ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/false);
    }
}

TEST_CASE("e2e: global aggregate decompositions (pre-aggregate on and off)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    for (const char *sql :
         {"SELECT sum(amount), count(*), avg(amount), min(amount), max(amount) FROM orders",
          "SELECT count(amount), sum(amount) FROM orders"}) {
        ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/true);
        ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/false);
    }
}

// Decimal sum/avg must at least *decompose* without tripping over DuckDB's unbound
// decimal-sum return type (an unset width/scale used to crash FromLogicalType). The
// shape TPC-H Q1 hits. (Decimal aggregate *execution* needs an aggregate bind the
// instance-free engine doesn't run, so this is a plan-build check only.)
TEST_CASE("structure: decimal aggregate decompositions build (pre-aggregate on and off)") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE li(flag VARCHAR, qty DECIMAL(15,2), price DECIMAL(15,2), disc DECIMAL(15,2))");
    const char *sql = "SELECT flag, sum(qty), sum(price * (1 - disc)), avg(qty), avg(disc), "
                      "min(price), max(price), count(*) FROM li GROUP BY flag";
    for (bool pa : {true, false}) {
        CHECK(BuildPhysicalPlan(con, sql, {}, Cfg(pa)).is_ok());
    }
}

// avg(DECIMAL) must come out correctly scaled even without pre-aggregation. The
// single-phase aggregate resolves DuckDB's native decimal avg via the physical
// storage-type fallback in ResolveAggregateFunction, which needs an
// AverageDecimalBindData (the decimal's scale) at Finalize time; Plume's aggregate
// operator never runs DuckDB's bind step (a null AggregateInputData), so without
// special-casing this the result comes back unscaled (10^scale too large).
TEST_CASE("e2e: decimal avg (pre-aggregate on and off)") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE li(flag VARCHAR, qty DECIMAL(15,2))");
    con.Query("INSERT INTO li VALUES ('A', 17.50), ('A', 22.00), ('A', 8.25), "
              "('B', 5.00), ('B', 100.00)");
    const char *sql = "SELECT flag, avg(qty), sum(qty), count(*) FROM li GROUP BY flag";
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/true);
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/false);
}

// count(*) references no column; DuckDB keeps a dummy column for it, but Plume's
// pre-aggregation makes it a partial count so nothing extra crosses the shuffle.
TEST_CASE("e2e: count(*) over a join, group by (pre-aggregate on and off)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    const char *sql = "SELECT c.name, count(*) FROM orders o JOIN customers c "
                      "ON o.cust = c.c_id GROUP BY c.name";
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/true);
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/false);
}

TEST_CASE("e2e: inner join") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con,
                "SELECT o.o_id, c.name, o.amount FROM orders o JOIN customers c ON o.cust = c.c_id",
                false);
}

TEST_CASE("e2e: join then group by") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con,
                "SELECT c.name, sum(o.amount) FROM orders o JOIN customers c ON o.cust = c.c_id "
                "GROUP BY c.name",
                false);
}

TEST_CASE("e2e: arithmetic projection + CASE") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con,
                "SELECT o_id, amount * 2 + 1 AS adj, "
                "CASE WHEN amount > 50 THEN 'big' ELSE 'small' END AS bucket "
                "FROM orders",
                false);
}

TEST_CASE("e2e: filter on varchar + BETWEEN") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con, "SELECT o_id, region FROM orders WHERE region = 'west' AND amount BETWEEN 20 AND 100",
                false);
}

TEST_CASE("e2e: IN predicate (optional scan filter skipped, OR filter applied)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con, "SELECT o_id, region FROM orders WHERE region IN ('west', 'south')", false);
}

TEST_CASE("e2e: multi-key group by with having-style filter") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con,
                "SELECT region, cust, sum(amount) AS s FROM orders GROUP BY region, cust HAVING sum(amount) > 20",
                false);
}

TEST_CASE("e2e: join with downstream filter + projection") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con,
                "SELECT c.name, o.amount FROM orders o JOIN customers c ON o.cust = c.c_id "
                "WHERE c.tier = 1 AND o.amount > 10",
                false);
}

TEST_CASE("e2e: three-table join (two join stages)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con,
                "SELECT o.o_id, c.name, t.label FROM orders o "
                "JOIN customers c ON o.cust = c.c_id "
                "JOIN tiers t ON c.tier = t.t_id",
                false);
}

TEST_CASE("e2e: cross join (explicit CROSS JOIN, no condition)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con, "SELECT o.o_id, t.label FROM orders o CROSS JOIN tiers t", false);
}

TEST_CASE("e2e: cross join (comma FROM with no correlating predicate)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    // No WHERE at all — DuckDB plans this as LOGICAL_CROSS_PRODUCT, not a
    // LogicalComparisonJoin with empty conditions (unlike the comma-FROM-with-a-
    // WHERE-equi-condition case, which is a real equi join).
    ExpectMatch(con, "SELECT o.o_id, t.label FROM orders o, tiers t", false);
}

TEST_CASE("e2e: cross join with a residual (non-equi) filter downstream") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con, "SELECT o.o_id, t.label FROM orders o CROSS JOIN tiers t WHERE o.amount > t.t_id * 10", false);
}

TEST_CASE("e2e: cross join column order is preserved regardless of which side is bigger") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    // `tiers` (3 rows) is written first, `orders` (8 rows) second — the bigger
    // relation still ends up on the probe side internally (see the composition
    // test below), but SELECT * must reflect tiers++orders, the SQL order.
    ExpectMatch(con, "SELECT * FROM tiers t CROSS JOIN orders o", false);
    ExpectMatch(con, "SELECT * FROM orders o CROSS JOIN tiers t", false);
}

TEST_CASE("e2e: join + filter + group by + order + limit") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con,
                "SELECT t.label, sum(o.amount) AS total FROM orders o "
                "JOIN customers c ON o.cust = c.c_id "
                "JOIN tiers t ON c.tier = t.t_id "
                "WHERE o.amount > 10 GROUP BY t.label ORDER BY total DESC LIMIT 2",
                true);
}

namespace {

bool Contains(const std::string &haystack, const std::string &needle) {
    return haystack.find(needle) != std::string::npos;
}

// `keyed` and `anyKeyed` are both valid sharding keywords (group by key; `anyKeyed`
// additionally allows combining groups, `keyed` doesn't) — the runtime functions
// handle either, so composition tests accept whichever the composer emits.
bool ContainsKeyed(const std::string &haystack, const std::string &before, const std::string &after) {
    return Contains(haystack, before + "keyed" + after) || Contains(haystack, before + "anyKeyed" + after);
}

} // namespace

TEST_CASE("composition: structure for a single source stage") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    auto plan = BuildPhysicalPlan(con, "SELECT o_id, amount FROM orders WHERE amount > 20").unwrap();
    auto comp = dandelion::BuildDandelionComposition(con, *plan, "Q", ConverterConfig{}).unwrap();

    CHECK(CountLeaves(*plan, DataSourceType::LOCAL_TABLE) == 1); // one base table (orders)
    CHECK(FindLeaf(*plan, "orders") != nullptr);
    CHECK(Contains(comp.dsl, "composition Q ("));
    // The template is broadcast to every invocation (`all`); a LOCAL_TABLE source
    // reads its host-provided blocks keyed by block index (`keyed`/`anyKeyed`).
    CHECK(ContainsKeyed(comp.dsl, "plume_stage (template = all st_0, inData = ", " tin_0)"));
    // Every stage's template set is a valid serialized pipeline template.
    auto templates = StageTemplates(comp);
    CHECK(templates.size() == plan->stages.size());
    for (auto &st : templates) {
        CHECK(plume::DeserializePipeline(*st).is_ok());
    }
}

TEST_CASE("composition: three-table join wiring") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    auto plan = BuildPhysicalPlan(con,
                                  "SELECT o.o_id, c.name, t.label FROM orders o "
                                  "JOIN customers c ON o.cust = c.c_id "
                                  "JOIN tiers t ON c.tier = t.t_id")
                    .unwrap();
    auto comp = dandelion::BuildDandelionComposition(con, *plan, "J3", ConverterConfig{}).unwrap();

    CHECK(CountLeaves(*plan, DataSourceType::LOCAL_TABLE) == 3); // orders, customers, tiers
    CHECK(StageTemplates(comp).size() == plan->stages.size());
    CHECK(Contains(comp.dsl, "inData2 = all"));     // joins wired with a build side
    // Two join stages -> two `inData2` references.
    size_t joins = 0, pos = 0;
    while ((pos = comp.dsl.find("inData2 = all", pos)) != std::string::npos) {
        joins++;
        pos += 1;
    }
    CHECK(joins == 2);
}

TEST_CASE("composition: data parallelism emits keyed shardings") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    // max_splits>1 makes the co-partitioned join read both inputs keyed, and the
    // two-phase grouped aggregate read its shuffled partials keyed (`keyed`/`anyKeyed`).
    auto plan = BuildPhysicalPlan(con,
                                  "SELECT c.name, sum(o.amount) FROM orders o JOIN customers c "
                                  "ON o.cust = c.c_id GROUP BY c.name",
                                  {}, Cfg(true, /*max_splits=*/4, /*target_rows_per_split=*/1))
                    .unwrap();
    auto comp = dandelion::BuildDandelionComposition(con, *plan, "P", Cfg(true, /*max_splits=*/4,
                                                                          /*target_rows_per_split=*/1))
                    .unwrap();

    // The join's two inputs are both consumed keyed (co-partitioned on the join key).
    CHECK(ContainsKeyed(comp.dsl, "inData = ", ""));
    CHECK(ContainsKeyed(comp.dsl, "inData2 = ", ""));
    // The join strategy clause is part of the statement (before the `;`), never on a
    // dangling line of its own.
    CHECK(Contains(comp.dsl, ") by inData inner inData2;"));
    CHECK(!Contains(comp.dsl, ");\n by inData"));

    // At a single split every edge is a plain gather (`all`), no keyed sharding.
    auto serial_plan = BuildPhysicalPlan(con,
                                        "SELECT c.name, sum(o.amount) FROM orders o JOIN customers c "
                                        "ON o.cust = c.c_id GROUP BY c.name")
                          .unwrap();
    auto serial = dandelion::BuildDandelionComposition(con, *serial_plan, "S", ConverterConfig{}).unwrap();
    CHECK(!ContainsKeyed(serial.dsl, "", " out_"));
}

TEST_CASE("composition: cross join gives the probe side keyed sharding, "
          "the build side all, and no by-clause") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    // `orders` (8 rows) is bigger than `tiers` (3 rows), so orders is expected to
    // end up as probe (keyed, split) and tiers as build (all, broadcast) — even
    // though composition.cpp only looks at each producer's own output_split, not
    // which relation is "orders" vs "tiers" by name.
    auto plan = BuildPhysicalPlan(con, "SELECT o.o_id, t.label FROM orders o CROSS JOIN tiers t", {},
                                  Cfg(true, /*max_splits=*/4, /*target_rows_per_split=*/1))
                    .unwrap();

    const Stage *join = nullptr;
    for (const auto &s : plan->stages) {
        if (s->LeadsWithJoin()) {
            join = s.get();
        }
    }
    CHECK(join != nullptr);
    if (join != nullptr) {
        const auto &probe_split = plan->stages[join->input_stages[0]]->pipeline.output_split;
        const auto &build_split = plan->stages[join->input_stages[1]]->pipeline.output_split;
        CHECK(probe_split.partitions > 1);        // the bigger side is split...
        CHECK(!probe_split.key_columns.empty());  // ...by (all of) its own real columns
        CHECK(build_split.partitions <= 1);       // the smaller side stays single-partition
    }

    auto comp = dandelion::BuildDandelionComposition(con, *plan, "X",
                                                     Cfg(true, /*max_splits=*/4, /*target_rows_per_split=*/1))
                    .unwrap();
    CHECK(ContainsKeyed(comp.dsl, "inData = ", ""));  // probe: keyed
    CHECK(Contains(comp.dsl, "inData2 = all "));      // build: broadcast to every invocation
    // No pairing clause: a broadcast edge already reaches every invocation, there's
    // nothing to pair by key.
    CHECK(!Contains(comp.dsl, "by inData"));
}

namespace {

size_t CountOps(const PhysicalPlan &plan, exec::OpType type) {
    size_t n = 0;
    for (const auto &stage : plan.stages) {
        for (const auto &op : stage->pipeline.operators) {
            if (op->type == type) {
                n++;
            }
        }
    }
    return n;
}

// True if any join stage's *output* carries a column with the given name — i.e. a
// dead column that survives the join and would cross the next shuffle. (A join
// key legitimately crosses into the join itself as an input, so we only inspect
// join stages' outputs, not source stages.)
bool AnyJoinOutputCarries(const PhysicalPlan &plan, const std::string &col_name) {
    for (const auto &stage : plan.stages) {
        if (!stage->LeadsWithJoin()) {
            continue;
        }
        for (const auto &col : stage->output_schema.columns) {
            if (col.name == col_name) {
                return true;
            }
        }
    }
    return false;
}

} // namespace

TEST_CASE("structure: two-phase aggregation splits into partial + final") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    const char *sql = "SELECT region, sum(amount) FROM orders GROUP BY region";

    auto with = BuildPhysicalPlan(con, sql, {}, Cfg(/*pre_aggregate=*/true)).unwrap();
    CHECK(CountOps(*with, exec::OpType::AGGREGATE) == 2); // partial (producing) + final

    auto without = BuildPhysicalPlan(con, sql, {}, Cfg(/*pre_aggregate=*/false)).unwrap();
    CHECK(CountOps(*without, exec::OpType::AGGREGATE) == 1); // single post-shuffle aggregate
}

TEST_CASE("structure: DISTINCT aggregate parallelism is driven by input, not output, cardinality") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    // Few output groups (5), many input rows (2000) to dedup per group — a DISTINCT
    // aggregate's per-partition cost tracks the latter.
    con.Query("CREATE TABLE big(g INTEGER, v INTEGER)");
    con.Query("INSERT INTO big SELECT (i % 5), i FROM range(2000) t(i)");

    ConverterConfig cfg = Cfg(true, /*max_splits=*/100, /*target_rows_per_split=*/1);

    auto MaxPartitions = [](const PhysicalPlan &plan) {
        uint32_t n = 0;
        for (const auto &s : plan.stages) {
            n = std::max(n, s->pipeline.output_split.partitions);
        }
        return n;
    };

    // count(DISTINCT v): forced single-phase (DISTINCT has no algebraic combine). The
    // shuffle must size off the ~2000-row input estimate (capped at max_splits=100),
    // not the ~5-group output estimate.
    auto distinct_plan = BuildPhysicalPlan(con, "SELECT g, count(DISTINCT v) FROM big GROUP BY g", {}, cfg).unwrap();
    CHECK(MaxPartitions(*distinct_plan) > 5);

    // first(v): also forced single-phase (not in the two-phase whitelist), but with no
    // DISTINCT involved the shuffle must still size off the (small) output-group
    // estimate, same as before this fix — not the 2000-row input.
    auto first_plan = BuildPhysicalPlan(con, "SELECT g, first(v) FROM big GROUP BY g", {}, cfg).unwrap();
    CHECK(MaxPartitions(*first_plan) <= 10);
}

TEST_CASE("split: data-parallel execution matches DuckDB (mock runtime)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    // max_splits=4 with target_rows_per_split=1 forces every parallelizable shuffle
    // to actually fan out (multiple invocations); the mock runtime must still produce
    // DuckDB's answer. Run each query at both 1 and 4 splits.
    for (uint32_t splits : {1u, 4u}) {
        // filter + projection (source partitions its output)
        ExpectMatch(con, "SELECT o_id, amount FROM orders WHERE amount > 20", false, {}, true, splits, 1);
        // grouped aggregate (two-phase, keyed by group)
        ExpectMatch(con, "SELECT region, sum(amount), count(*), min(amount), max(amount) FROM orders "
                         "GROUP BY region",
                    false, {}, true, splits, 1);
        // global aggregate (gathers to one invocation)
        ExpectMatch(con, "SELECT sum(amount), avg(amount), count(*) FROM orders", false, {}, true, splits, 1);
        // inner join (co-partitioned on the join key), then a grouped aggregate
        ExpectMatch(con, "SELECT c.name, sum(o.amount) FROM orders o JOIN customers c ON o.cust = c.c_id "
                         "GROUP BY c.name",
                    false, {}, true, splits, 1);
        // three-table join
        ExpectMatch(con, "SELECT o.o_id, c.name, t.label FROM orders o JOIN customers c ON o.cust = c.c_id "
                         "JOIN tiers t ON c.tier = t.t_id",
                    false, {}, true, splits, 1);
        // left outer join (unmatched left rows must appear exactly once)
        ExpectMatch(con, "SELECT c.name, o.o_id FROM customers c LEFT JOIN orders o ON o.cust = c.c_id",
                    false, {}, true, splits, 1);
        // order by + limit (top-n gathers to one invocation)
        ExpectMatch(con, "SELECT o_id, amount FROM orders ORDER BY amount DESC, o_id LIMIT 4", true, {},
                    true, splits, 1);
        // correlated subquery (DELIM join — its LHS is forced serial, join runs serially)
        ExpectMatch(con, "SELECT o_id, (SELECT max(amount) FROM orders o2 WHERE o2.cust = o.cust) "
                         "FROM orders o",
                    false, {}, true, splits, 1);
        // cross join (probe side split, build side broadcast to every invocation —
        // exercises MockRuntime::RunBlockStage's broadcast path at splits > 1)
        ExpectMatch(con, "SELECT o.o_id, t.label FROM orders o CROSS JOIN tiers t", false, {}, true, splits, 1);
        ExpectMatch(con, "SELECT o.o_id, t.label FROM tiers t CROSS JOIN orders o", false, {}, true, splits, 1);
        // uncorrelated scalar subquery comparison (Q22-shaped) — same broadcast
        // treatment, reached through BuildJoin's fallback instead of BuildCrossProduct
        ExpectMatch(con, "SELECT o_id, amount FROM orders WHERE amount > (SELECT avg(amount) FROM orders)", false,
                    {}, true, splits, 1);
    }
}

TEST_CASE("split: max_splits=1 leaves every stage unpartitioned") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    // The default cap of 1 must reproduce the previous (single-invocation) plan: no
    // producer carries a key or a partition count > 1, anywhere.
    auto plan = BuildPhysicalPlan(con,
                                  "SELECT region, sum(amount) FROM orders o JOIN customers c "
                                  "ON o.cust = c.c_id GROUP BY region ORDER BY region")
                    .unwrap();
    for (const auto &s : plan->stages) {
        CHECK(s->pipeline.output_split.partitions == 1);
        CHECK(s->pipeline.output_split.key_columns.empty());
    }
}

TEST_CASE("split: join inputs co-partition by their join keys") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    const uint32_t cap = 8;
    // target_rows_per_split=1 with the cap pushes parallelizable shuffles to the
    // cardinality estimate (>=1), bounded by the cap.
    auto plan = BuildPhysicalPlan(con,
                                  "SELECT o.o_id, c.name FROM orders o JOIN customers c ON o.cust = c.c_id",
                                  {}, Cfg(true, cap, 1))
                    .unwrap();

    const Stage *join = nullptr;
    for (const auto &s : plan->stages) {
        if (s->LeadsWithJoin()) {
            join = s.get();
        }
    }
    CHECK(join != nullptr);
    if (join != nullptr) {
        const auto &lp = plan->stages[join->input_stages[0]]->pipeline.output_split;
        const auto &rp = plan->stages[join->input_stages[1]]->pipeline.output_split;
        // Both inputs partition by a (non-empty) key, into the SAME number of parts.
        CHECK(!lp.key_columns.empty());
        CHECK(!rp.key_columns.empty());
        CHECK(lp.partitions == rp.partitions);
        CHECK(lp.partitions > 1);          // both base tables estimate > 1 row
        CHECK(lp.partitions <= cap);       // capped by max_splits
    }
}

TEST_CASE("split: grouped aggregate keys by its group columns; sort gathers serially") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    const uint32_t cap = 8;

    // Two-phase grouped aggregate: the producing (partial) stage hash-partitions by
    // the leading group-key column [0].
    {
        auto plan = BuildPhysicalPlan(con, "SELECT region, sum(amount) FROM orders GROUP BY region",
                                      {}, Cfg(/*pre_aggregate=*/true, cap, 1))
                        .unwrap();
        bool keyed = false;
        for (const auto &s : plan->stages) {
            const auto &sp = s->pipeline.output_split;
            if (!sp.key_columns.empty()) {
                keyed = true;
                CHECK(sp.key_columns == std::vector<uint32_t>{0});
                CHECK(sp.partitions >= 1);
                CHECK(sp.partitions <= cap);
            }
        }
        CHECK(keyed);
    }

    // Single-phase grouped aggregate (pre_aggregate off): the producer hash-partitions
    // by the group-key column [0] so equal groups meet in a single aggregate invocation.
    {
        auto plan = BuildPhysicalPlan(con, "SELECT region, sum(amount) FROM orders GROUP BY region",
                                      {}, Cfg(/*pre_aggregate=*/false, cap, 1))
                        .unwrap();
        bool keyed = false;
        for (const auto &s : plan->stages) {
            const auto &sp = s->pipeline.output_split;
            if (!sp.key_columns.empty()) {
                keyed = true;
                CHECK(sp.key_columns == std::vector<uint32_t>{0});
                CHECK(sp.partitions >= 1);
                CHECK(sp.partitions <= cap);
            }
        }
        CHECK(keyed);
    }

    // A global sort gathers every row into one invocation: its producer is serial.
    {
        auto plan =
            BuildPhysicalPlan(con, "SELECT o_id FROM orders ORDER BY o_id", {}, Cfg(true, cap, 1))
                .unwrap();
        for (const auto &s : plan->stages) {
            CHECK(s->pipeline.output_split.partitions == 1);
            CHECK(s->pipeline.output_split.key_columns.empty());
        }
    }
}

TEST_CASE("structure: dead join-key columns are pruned before the aggregate shuffle") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    // count(*) reads no column and `cust`/`c_id` (the join keys) are dead after the
    // join. With pre-aggregate on, the partial aggregate consumes them inside the join
    // stage, so they never cross the shuffle. (Pre-aggregate off carries the dead join
    // output across the shuffle until the input-pruning pass lands — see the TODO in
    // BuildAggregate.)
    const char *sql = "SELECT c.name, count(*) FROM orders o JOIN customers c "
                      "ON o.cust = c.c_id GROUP BY c.name";
    auto plan = BuildPhysicalPlan(con, sql, {}, Cfg(/*pre_aggregate=*/true)).unwrap();
    CHECK(!AnyJoinOutputCarries(*plan, "cust"));
    CHECK(!AnyJoinOutputCarries(*plan, "c_id"));
}

// NOTE: file-source (CSV/parquet, local or remote) tests used to live here. They
// depended on the SQL FROM/JOIN source-detection + rewrite pass that lived in the
// now-deleted client/src/compiler.cpp (scan FROM/JOIN tokens, classify local file
// vs. remote URL, rewrite to plume_remote(...) / load as LOCAL_TABLE). That pass
// was never ported to the new client.cpp when the client was rebuilt around the
// Client class — see PROGRESS.md / ask the person who ran this refactor. Once it's
// restored, these tests (CSV/parquet LOCAL_TABLE and REMOTE_CSV/REMOTE_PARQUET
// sources, bracket-lists, comma-FROM local files, sources nested in a subquery,
// pushdown on/off equivalence) should come back.

TEST_CASE("e2e: uncorrelated scalar subquery comparison (Q22-shaped)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    ExpectMatch(con, "SELECT o_id, amount FROM orders WHERE amount > (SELECT avg(amount) FROM orders)", false);
}

TEST_CASE("composition: uncorrelated scalar subquery comparison gives the "
          "bigger side keyed sharding, the subquery side all") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    auto plan = BuildPhysicalPlan(con, "SELECT o_id, amount FROM orders WHERE amount > (SELECT avg(amount) FROM orders)",
                                  {}, Cfg(true, /*max_splits=*/8, /*target_rows_per_split=*/1))
                    .unwrap();

    const Stage *join = nullptr;
    for (const auto &s : plan->stages) {
        if (s->LeadsWithJoin()) {
            join = s.get();
        }
    }
    CHECK(join != nullptr);
    if (join != nullptr) {
        auto jt = std::static_pointer_cast<exec::JoinTemplate>(join->pipeline.operators[0]);
        CHECK(jt->kind == exec::JoinKind::INNER);

        const auto &probe_split = plan->stages[join->input_stages[0]]->pipeline.output_split;
        const auto &build_split = plan->stages[join->input_stages[1]]->pipeline.output_split;
        CHECK(probe_split.partitions > 1);        // orders (the bigger side) is split...
        CHECK(!probe_split.key_columns.empty());  // ...by its own real columns, not the dummy join key
        CHECK(build_split.partitions <= 1);       // the one-row avg subquery stays single-partition
    }

    auto comp = dandelion::BuildDandelionComposition(con, *plan, "Y",
                                                     Cfg(true, /*max_splits=*/8, /*target_rows_per_split=*/1))
                    .unwrap();
    CHECK(ContainsKeyed(comp.dsl, "inData = ", ""));
    CHECK(Contains(comp.dsl, "inData2 = all "));
    CHECK(!Contains(comp.dsl, "by inData"));
}

TEST_CASE("e2e: correlated scalar subquery (DELIM join)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    // Each order whose amount equals the maximum amount among its customer's orders.
    const char *sql = "SELECT o_id, cust, amount FROM orders o "
                      "WHERE amount = (SELECT max(amount) FROM orders o2 WHERE o2.cust = o.cust)";
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/true);
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/false);
}

// A correlated subquery whose outer row may have no matching inner rows, so the
// LEFT (single) delim join must keep it (subquery -> NULL, predicate false).
TEST_CASE("e2e: correlated subquery over a join (DELIM join)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    const char *sql =
        "SELECT o.o_id, o.amount FROM orders o JOIN customers c ON o.cust = c.c_id "
        "WHERE o.amount > (SELECT avg(amount) FROM orders o2 WHERE o2.cust = o.cust)";
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/true);
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/false);
}

// The non-inner join kinds, each matched against DuckDB. The query syntax maps to
// the join types DuckDB's planner/optimizer emits: FULL OUTER -> OUTER, EXISTS ->
// RIGHT_SEMI, NOT EXISTS -> RIGHT_ANTI, IN -> SEMI, NOT IN -> MARK + filter. (A
// RIGHT JOIN is flipped to LEFT by the optimizer.)
TEST_CASE("e2e: outer joins (LEFT / RIGHT / FULL OUTER)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    for (const char *sql :
         {"SELECT o.o_id, c.name FROM orders o LEFT JOIN customers c ON o.cust = c.c_id",
          "SELECT o.o_id, c.name FROM orders o RIGHT JOIN customers c ON o.cust = c.c_id",
          "SELECT o.o_id, c.name FROM orders o FULL OUTER JOIN customers c ON o.cust = c.c_id"}) {
        ExpectMatch(con, sql, /*ordered=*/false);
    }
}

TEST_CASE("e2e: semi/anti joins (EXISTS / NOT EXISTS / IN / NOT IN)") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    for (const char *sql :
         {"SELECT o_id FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.c_id = o.cust)",
          "SELECT o_id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.c_id = o.cust)",
          "SELECT o_id FROM orders o WHERE cust IN (SELECT c_id FROM customers)",
          "SELECT o_id FROM orders o WHERE cust NOT IN (SELECT c_id FROM customers)"}) {
        ExpectMatch(con, sql, /*ordered=*/false);
    }
}

// NOT IN where the subquery yields a NULL: the MARK join's marker must be NULL
// (not false) for non-matching probe rows, so SQL's three-valued NOT IN returns
// no rows. Validates the null-aware marker against DuckDB.
TEST_CASE("e2e: NOT IN with a NULL in the subquery (null-aware mark join)") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE t(x INTEGER)");
    con.Query("INSERT INTO t VALUES (1),(2),(3),(8)");
    con.Query("CREATE TABLE s(y INTEGER)");
    con.Query("INSERT INTO s VALUES (2),(NULL)");
    ExpectMatch(con, "SELECT x FROM t WHERE x NOT IN (SELECT y FROM s)", /*ordered=*/false);
    ExpectMatch(con, "SELECT x FROM t WHERE x IN (SELECT y FROM s)", /*ordered=*/false);
}

// A correlated subquery the deliminator keeps as a real DELIM join (the
// correlated column is used as a join key inside the subquery): exercises
// BuildDelimJoin + the duplicate-eliminated DISTINCT + DELIM_GET.
TEST_CASE("e2e: correlated subquery kept as a DELIM join") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    const char *sql = "SELECT o.o_id FROM orders o "
                      "WHERE o.amount = (SELECT max(amount) FROM orders o2, customers c2 "
                      "WHERE o2.cust = c2.c_id AND c2.c_id = o.cust)";
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/true);
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/false);
}

// A TPC-H Q2-shaped query: the outer query and the correlated subquery share a
// common sub-plan, so DuckDB materializes it as a CTE and keeps a DELIM join over
// it. Exercises the full path: materialized CTE + CTE_SCAN fan-out + DELIM_JOIN +
// DELIM_GET, matched against DuckDB.
TEST_CASE("e2e: Q2-shaped correlated subquery (CTE + DELIM join)") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE part(p_id INTEGER, sz INTEGER)");
    con.Query("INSERT INTO part VALUES (1,15),(2,15),(3,20),(4,15)");
    con.Query("CREATE TABLE ps(p_id INTEGER, s_id INTEGER, cost INTEGER)");
    con.Query("INSERT INTO ps VALUES (1,10,100),(1,11,80),(2,10,50),(2,12,55),(3,10,30),(4,11,200),(4,12,150)");
    con.Query("CREATE TABLE supp(s_id INTEGER, nat INTEGER)");
    con.Query("INSERT INTO supp VALUES (10,1),(11,1),(12,2)");
    const char *sql = "SELECT p.p_id, ps.cost FROM part p, ps, supp "
                      "WHERE p.p_id = ps.p_id AND ps.s_id = supp.s_id AND p.sz = 15 "
                      "AND ps.cost = (SELECT min(ps2.cost) FROM ps ps2, supp supp2 "
                      "WHERE p.p_id = ps2.p_id AND ps2.s_id = supp2.s_id)";
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/true);
    ExpectMatch(con, sql, /*ordered=*/false, {}, /*pre_aggregate=*/false);
}

// A correlated-subquery plan reuses a stage's output as the input of more than
// one downstream stage (the CTE feeds the outer probe and the delim DISTINCT).
// Confirm the decomposer produces that fan-out and the dandelion composition
// still builds valid, deserializable stage templates over it.
TEST_CASE("composition: CTE/DELIM plan wires stage-output fan-out") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE part(p_id INTEGER, sz INTEGER)");
    con.Query("INSERT INTO part VALUES (1,15),(2,15),(3,20),(4,15)");
    con.Query("CREATE TABLE ps(p_id INTEGER, s_id INTEGER, cost INTEGER)");
    con.Query("INSERT INTO ps VALUES (1,10,100),(1,11,80),(2,10,50),(2,12,55),(3,10,30),(4,11,200),(4,12,150)");
    con.Query("CREATE TABLE supp(s_id INTEGER, nat INTEGER)");
    con.Query("INSERT INTO supp VALUES (10,1),(11,1),(12,2)");
    const char *sql = "SELECT p.p_id, ps.cost FROM part p, ps, supp "
                      "WHERE p.p_id = ps.p_id AND ps.s_id = supp.s_id AND p.sz = 15 "
                      "AND ps.cost = (SELECT min(ps2.cost) FROM ps ps2, supp supp2 "
                      "WHERE p.p_id = ps2.p_id AND ps2.s_id = supp2.s_id)";
    auto plan = BuildPhysicalPlan(con, sql).unwrap();

    // Some stage's output is consumed by 2+ downstream stages (fan-out).
    std::map<size_t, int> consumers;
    for (const auto &stage : plan->stages) {
        for (size_t in : stage->input_stages) {
            consumers[in]++;
        }
    }
    bool fan_out = false;
    for (auto &[id, n] : consumers) {
        fan_out = fan_out || n >= 2;
    }
    CHECK(fan_out);

    auto comp = dandelion::BuildDandelionComposition(con, *plan, "Q2", ConverterConfig{}).unwrap();
    auto templates = StageTemplates(comp);
    CHECK(templates.size() == plan->stages.size());
    for (auto &st : templates) {
        CHECK(plume::DeserializePipeline(*st).is_ok());
    }
}

// A TPC-H Q15-shaped query: two textually identical (non-correlated) grouped-
// aggregate subqueries get de-duplicated by DuckDB into one materialized CTE,
// referenced once directly (feeding a JOIN, which needs a real hash-shuffle of
// the CTE's output) and once through a plain projection down to a single column
// (feeding a scalar MAX subquery, which needs no shuffle at all). Regression
// test for a bug where the projection-only reference got fused onto whichever
// stage the JOIN's shuffle produced, silently collapsing that stage's schema
// and leaving the JOIN's shuffle key pointing past the end of the row.
TEST_CASE("e2e: Q15-shaped shared aggregate (CTE feeds join + scalar subquery)") {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE supp(supp INTEGER, name VARCHAR)");
    con.Query("INSERT INTO supp VALUES (1,'a'),(2,'b'),(3,'c')");
    con.Query("CREATE TABLE lit(supp INTEGER, amt INTEGER)");
    con.Query("INSERT INTO lit VALUES (1,10),(1,15),(2,40),(2,5),(3,1)");
    const char *sql =
        "SELECT s.supp, s.name, rev.total FROM supp s, "
        "(SELECT supp AS c2, sum(amt) AS total FROM lit GROUP BY c2) rev "
        "WHERE s.supp = rev.c2 AND rev.total = (SELECT max(total) FROM "
        "(SELECT supp AS c2, sum(amt) AS total FROM lit GROUP BY c2) rev2) "
        "ORDER BY s.supp";
    ExpectMatch(con, sql, /*ordered=*/true, {}, /*pre_aggregate=*/true, /*max_splits=*/4);
    ExpectMatch(con, sql, /*ordered=*/true, {}, /*pre_aggregate=*/false, /*max_splits=*/4);
}

// --- dynamic filter (join build -> remote parquet probe scan pruning) ------------------

TEST_CASE("dynamic filter: attaches to an INNER join's build side, marks the remote probe leaf") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeRemoteJoinDb(db, /*orders_cardinality=*/1'000'000);
    auto plan = BuildPhysicalPlan(con, "SELECT o.o_id, c.name FROM plume_remote('remote_orders') o "
                                       "JOIN customers c ON o.cust = c.cust")
                    .unwrap();

    const LeafStage *orders = FindLeaf(*plan, "remote_orders");
    CHECK(orders != nullptr);
    CHECK(orders->HasDynFilter());
    CHECK(orders->dynamic_filter_column == 1); // "cust" is file column 1 of (o_id, cust)
    CHECK(orders->dynamic_filter_source_stage != kNoDynamicFilter);

    const Stage &build_stage = *plan->stages[orders->dynamic_filter_source_stage];
    const auto *dyn = FindDynamicFilterBuild(build_stage);
    CHECK(dyn != nullptr);
    CHECK(dyn->column == 0); // "cust" is column 0 of customers(cust, name)

    // The producer -> consumer link is the reverse of the consumer -> producer one.
    CHECK(build_stage.ProducesDynFilter());
    CHECK(build_stage.dynamic_filter_consumer_stage == orders->idx);
}

// Regression test: the dynamic filter's build side above is a plain local table, so the
// producing stage is a LOCAL_TABLE *leaf* stage, not a COMMON/JOIN one -- the common
// case, not an edge case (a join's build side isn't restricted to any particular
// stage type). BuildDandelionComposition must still bind that stage's dynFilter
// output: every leaf-stage branch used to skip the ProducesDynFilter() check (only the
// non-leaf branch did it), so plume_pq_prepare's `dynFilter = all dfltr_N` referenced a
// variable nothing in the composition body ever produced.
TEST_CASE("dynamic filter: producing leaf stage's dynFilter output is actually bound in the dsl") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeRemoteJoinDb(db, /*orders_cardinality=*/1'000'000);
    auto plan = BuildPhysicalPlan(con, "SELECT o.o_id, c.name FROM plume_remote('remote_orders') o "
                                       "JOIN customers c ON o.cust = c.cust")
                    .unwrap();

    const LeafStage *orders = FindLeaf(*plan, "remote_orders");
    CHECK(orders != nullptr);
    CHECK(orders->HasDynFilter());
    const Stage &build_stage = *plan->stages[orders->dynamic_filter_source_stage];
    CHECK(build_stage.ProducesDynFilter());
    CHECK(build_stage.IsLeaf()); // the case this regression test targets

    auto comp = dandelion::BuildDandelionComposition(con, *plan, "DF", ConverterConfig{}).unwrap();

    const std::string dyn_var = "dfltr_" + std::to_string(build_stage.idx);
    // plume_pq_prepare consumes it by name...
    CHECK(Contains(comp.dsl, "dynFilter = all " + dyn_var));
    // ...and the producing stage's own function application must actually bind it as an
    // output, or the reference above is dangling.
    CHECK(Contains(comp.dsl, dyn_var + " = dynFilter"));
    // The function it's declared on must expose the output at all.
    CHECK(Contains(comp.dsl, "=> (outData, dynFilter"));
}

TEST_CASE("dynamic filter: disabled via ConverterConfig -> leaf isn't marked") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeRemoteJoinDb(db, /*orders_cardinality=*/1'000'000);
    ConverterConfig cfg;
    cfg.dynamic_filter = false;
    auto plan = BuildPhysicalPlan(con,
                                  "SELECT o.o_id, c.name FROM plume_remote('remote_orders') o "
                                  "JOIN customers c ON o.cust = c.cust",
                                  {}, cfg)
                    .unwrap();

    const LeafStage *orders = FindLeaf(*plan, "remote_orders");
    CHECK(orders != nullptr);
    CHECK(!orders->HasDynFilter());
    CHECK(orders->dynamic_filter_source_stage == kNoDynamicFilter);
    for (auto &s : plan->stages) {
        CHECK(FindDynamicFilterBuild(*s) == nullptr);
    }
}

TEST_CASE("dynamic filter: build side too large relative to the probe -> not attached") {
    duckdb::DuckDB db(nullptr);
    // The probe (remote_orders) is only 10 rows; customers' 2 rows are not <= 10 * 0.2 = 2...
    // make it unambiguous by using a threshold no real cardinality clears.
    auto con = MakeRemoteJoinDb(db, /*orders_cardinality=*/10);
    ConverterConfig cfg;
    cfg.dynamic_filter_selectivity_threshold = 0.0; // nothing clears this but an empty build side
    auto plan = BuildPhysicalPlan(con,
                                  "SELECT o.o_id, c.name FROM plume_remote('remote_orders') o "
                                  "JOIN customers c ON o.cust = c.cust",
                                  {}, cfg)
                    .unwrap();

    const LeafStage *orders = FindLeaf(*plan, "remote_orders");
    CHECK(orders != nullptr);
    CHECK(!orders->HasDynFilter());
}

TEST_CASE("dynamic filter: not attached for a non-INNER join") {
    duckdb::DuckDB db(nullptr);
    auto con = MakeRemoteJoinDb(db, /*orders_cardinality=*/1'000'000);
    auto plan = BuildPhysicalPlan(con, "SELECT o.o_id, c.name FROM plume_remote('remote_orders') o "
                                       "LEFT JOIN customers c ON o.cust = c.cust")
                    .unwrap();

    const LeafStage *orders = FindLeaf(*plan, "remote_orders");
    CHECK(orders != nullptr);
    CHECK(!orders->HasDynFilter());
}

TEST_CASE("dynamic filter: not attached when the probe side isn't a remote parquet leaf") {
    // Both sides local tables -> the probe side never traces to a REMOTE_PARQUET leaf.
    duckdb::DuckDB db(nullptr);
    auto con = MakeDb(db);
    auto plan = BuildPhysicalPlan(con, "SELECT o.o_id, c.name FROM orders o JOIN customers c "
                                       "ON o.cust = c.c_id")
                    .unwrap();
    for (auto &s : plan->stages) {
        CHECK(FindDynamicFilterBuild(*s) == nullptr);
    }
}

int main() { return plume_test::RunAll(); }
