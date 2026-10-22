// Dynamic filter tests:
//  (1) DynamicFilterBuildTemplate survives pipeline serialize/deserialize
//  (2) DynamicFilterBuildOperator passes every row through unchanged while collecting the
//      key range, retrievable via Executor::FindDynamicFilterBuild() once the collection ended
//  (3) DynamicFilter key-range merging
//  (4) BuildRangeFilter produces a predicate that actually prunes as expected
//  (5) BloomFilter: no false negatives, expected false positive rate, merge, wire format
//  (6) DynamicFilter: exact key set, conversion to a Bloom filter, key casting/typing, merging
//  (7) DynamicFilterProbe: per-row evaluation on the probe side, self-disabling

#include "exec_test_util.hpp"
#include "test_util.hpp"

#include "plume/common/serial.hpp"
#include "plume/execution/bloom_filter.hpp"
#include "plume/execution/executor.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/expression/expression.hpp"
#include "plume/expression/expression_builder.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <algorithm>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

using namespace plume;
using namespace plume::exec;
using namespace plume::expr;
using namespace plume::memory;
using duckdb::DataChunk;
using duckdb::ExpressionExecutor;
using duckdb::FlatVector;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema AB() {
    Schema s;
    s.columns = {{"a", I32(), false}, {"b", I32(), true}};
    return s;
}

// Build an int32 block from column-major data; a negative sentinel in the (nullable)
// second column marks a NULL.
OwnedBlock MakeBlock(Allocator &alloc, const Schema &schema, const std::vector<std::vector<int32_t>> &cols,
                     int32_t null_sentinel = -1000) {
    duckdb::vector<LogicalType> types;
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }
    idx_t rows = cols.empty() ? 0 : cols[0].size();
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), types);
    for (size_t c = 0; c < cols.size(); c++) {
        for (idx_t r = 0; r < rows; r++) {
            if (cols[c][r] == null_sentinel) {
                chunk.SetValue(c, r, Value(LogicalType::INTEGER));
            } else {
                chunk.SetValue(c, r, Value::INTEGER(cols[c][r]));
            }
        }
    }
    chunk.SetCardinality(rows);
    return ExportChunk(schema, chunk, alloc).unwrap();
}

ColumnType Varchar() { return {TypeId::VARCHAR}; }

Schema OneVarchar() {
    Schema s;
    s.columns = {{"s", Varchar(), true}};
    return s;
}

OwnedBlock MakeVarcharBlock(Allocator &alloc, const Schema &schema,
                           const std::vector<std::optional<std::string>> &vals) {
    duckdb::vector<LogicalType> types = {LogicalType::VARCHAR};
    idx_t rows = vals.size();
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), types);
    for (idx_t r = 0; r < rows; r++) {
        if (vals[r].has_value()) {
            chunk.SetValue(0, r, Value(*vals[r]));
        } else {
            chunk.SetValue(0, r, Value(LogicalType::VARCHAR));
        }
    }
    chunk.SetCardinality(rows);
    return ExportChunk(schema, chunk, alloc).unwrap();
}

} // namespace

TEST_CASE("DynamicFilterBuildTemplate: pipeline serialize/deserialize round-trip") {
    PipelineTemplate p;
    p.input_schema = AB();

    auto dyn = std::make_shared<DynamicFilterBuildTemplate>(1, I32(), /*max_values=*/16, /*bloom_bits=*/1024);
    p.operators = {dyn};

    auto bytes = plume::SerializePipeline(p);
    auto q = plume::DeserializePipeline(bytes).unwrap();

    CHECK(q.operators.size() == 1);
    CHECK(q.operators[0]->type == OpType::DYNAMIC_FILTER_BUILD);
    auto &t = static_cast<const DynamicFilterBuildTemplate &>(*q.operators[0]);
    CHECK(t.column == 1);
    CHECK(t.key_type == I32());
    CHECK(t.max_values == 16);
    CHECK(t.bloom_bits == 1024);
    CHECK(t.Equals(*dyn));

    auto bytes2 = plume::SerializePipeline(q);
    CHECK_MSG(bytes == bytes2, "re-serialized pipeline differs");
}

TEST_CASE("DynamicFilterBuildOperator: passthrough + running min/max across blocks") {
    Allocator alloc;
    auto schema = AB();

    // column 0 is the one we're building bounds over.
    auto b0 = MakeBlock(alloc, schema, {{10, -5, 30}, {1, 2, 3}});
    auto b1 = MakeBlock(alloc, schema, {{100, -1000, 7}, {4, 5, 6}}); // row 1's col0 is NULL

    PipelineTemplate desc;
    desc.input_schema = schema;
    desc.operators = {std::make_shared<DynamicFilterBuildTemplate>(0, I32())};

    // Drive the Executor by hand (rather than exec_test_util's RunBlocks) so we can check that
    // reading the filter ends the collection.
    std::vector<OwnedBlock> out;
    exec::OutputEmit emit = [&](const std::string &, size_t, DataBuffer buf, size_t) {
        OwnedBlock ob;
        ob.size = buf.size();
        ob.data = alloc.Allocate(buf.size());
        std::memcpy(ob.data, buf.data(), buf.size());
        out.push_back(ob);
    };

    Executor exec(alloc);
    exec.Build(desc, emit).unwrap();

    auto *dyn = exec.FindDynamicFilterBuild();
    CHECK(dyn != nullptr);

    exec.PushBlocks({{b0.data, b0.size}}).unwrap();
    exec.PushBlocks({{b1.data, b1.size}}).unwrap();
    exec.Finish().unwrap();

    const auto &bounds = dyn->Filter();
    CHECK(bounds.has_bounds);
    // NULL (-1000 sentinel) skipped; min/max over {10, -5, 30, 100, 7} = -5 / 100
    CHECK(bounds.min == Value::INTEGER(-5));
    CHECK(bounds.max == Value::INTEGER(100));

    idx_t total_rows = 0;
    for (auto &block : out) {
        Schema out_schema;
        auto chunks = ImportBlockChunks(block.data, block.size, out_schema).unwrap();
        for (auto &chunk : chunks) {
            total_rows += chunk->size();
        }
    }
    CHECK(total_rows == 6);

    for (auto &block : out) {
        alloc.Free(block.data, block.size);
    }
    alloc.Free(b0.data, b0.size);
    alloc.Free(b1.data, b1.size);
}

TEST_CASE("DynamicFilterBuildOperator: VARCHAR min/max, including a non-inlined string") {
    Allocator alloc;
    auto schema = OneVarchar();

    // string_t inlines up to 12 bytes; this one is long enough to force a heap-backed
    // string, exercising the copy-into-an-owned-Value path across chunk lifetimes.
    std::string long_str(40, 'z');
    auto b0 = MakeVarcharBlock(alloc, schema, {std::string("banana"), std::nullopt, std::string("apple"), long_str});

    PipelineTemplate desc;
    desc.input_schema = schema;
    desc.operators = {std::make_shared<DynamicFilterBuildTemplate>(0, Varchar())};

    Executor exec(alloc);
    auto out = plume_test::RunBlocks(exec, desc, {{b0.data, b0.size}}, alloc);

    auto *dyn = exec.FindDynamicFilterBuild();
    CHECK(dyn != nullptr);
    const auto &bounds = dyn->Filter();
    CHECK(bounds.has_bounds);
    CHECK(bounds.min == Value(std::string("apple")));
    CHECK(bounds.max == Value(long_str));

    for (auto &block : out) {
        alloc.Free(block.data, block.size);
    }
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("DynamicFilterBuildOperator: empty input yields an invalid bounds") {
    Allocator alloc;
    auto schema = AB();

    PipelineTemplate desc;
    desc.input_schema = schema;
    desc.operators = {std::make_shared<DynamicFilterBuildTemplate>(0, I32())};

    Executor exec(alloc);
    plume_test::RunBlocks(exec, desc, {}, alloc);

    auto *dyn = exec.FindDynamicFilterBuild();
    CHECK(dyn != nullptr);
    CHECK_MSG(!dyn->Filter().has_bounds, "no rows ever pushed -> bounds must stay invalid");
}

TEST_CASE("DynamicFilter: MergeBounds/Merge widen to the union, no bounds is the identity") {
    auto make = [](int32_t lo, int32_t hi) {
        DynamicFilter f;
        f.MergeBounds(Value::INTEGER(lo), Value::INTEGER(hi));
        return f;
    };

    DynamicFilter merged;
    merged.Merge(DynamicFilter()); // no bounds into no bounds -> still none
    CHECK(!merged.has_bounds);

    merged.Merge(make(10, 20));
    CHECK(merged.has_bounds);
    CHECK(merged.min == Value::INTEGER(10));
    CHECK(merged.max == Value::INTEGER(20));

    merged.Merge(DynamicFilter()); // an empty build instance doesn't change anything
    CHECK(merged.min == Value::INTEGER(10));
    CHECK(merged.max == Value::INTEGER(20));

    merged.Merge(make(-5, 15));
    merged.MergeBounds(Value::INTEGER(12), Value::INTEGER(40));
    CHECK(merged.min == Value::INTEGER(-5));
    CHECK(merged.max == Value::INTEGER(40));
}

TEST_CASE("BuildRangeFilter: no bounds -> no filter") {
    DynamicFilter f;
    CHECK(!BuildRangeFilter(0, I32(), f).has_value());
}

TEST_CASE("BuildRangeFilter: min == max -> single equality, prunes correctly") {
    DynamicFilter f;
    f.MergeBounds(Value::INTEGER(42), Value::INTEGER(42));

    auto node = BuildRangeFilter(0, I32(), f);
    CHECK(node.has_value());
    CHECK(node->kind == ExprKind::COMPARISON);

    Allocator alloc;
    DataChunk input;
    input.Initialize(alloc.Get(), {LogicalType::INTEGER});
    const idx_t n = 3;
    auto col = FlatVector::GetData<int32_t>(input.data[0]);
    col[0] = 41;
    col[1] = 42;
    col[2] = 43;
    input.SetCardinality(n);

    auto expr = BuildExpression(*node).unwrap();
    ExpressionExecutor executor;
    executor.AddExpression(*expr);
    DataChunk result;
    result.Initialize(alloc.Get(), {LogicalType::BOOLEAN});
    executor.Execute(input, result);
    auto pred = FlatVector::GetData<bool>(result.data[0]);
    CHECK(!pred[0]);
    CHECK(pred[1]);
    CHECK(!pred[2]);
}

TEST_CASE("BuildRangeFilter: min != max -> closed range, prunes correctly") {
    DynamicFilter f;
    f.MergeBounds(Value::INTEGER(10), Value::INTEGER(20));

    auto node = BuildRangeFilter(0, I32(), f);
    CHECK(node.has_value());
    CHECK(node->kind == ExprKind::CONJUNCTION);
    CHECK(node->children.size() == 2);

    Allocator alloc;
    DataChunk input;
    input.Initialize(alloc.Get(), {LogicalType::INTEGER});
    const idx_t n = 5;
    auto col = FlatVector::GetData<int32_t>(input.data[0]);
    col[0] = 5;
    col[1] = 10;
    col[2] = 15;
    col[3] = 20;
    col[4] = 25;
    input.SetCardinality(n);

    auto expr = BuildExpression(*node).unwrap();
    ExpressionExecutor executor;
    executor.AddExpression(*expr);
    DataChunk result;
    result.Initialize(alloc.Get(), {LogicalType::BOOLEAN});
    executor.Execute(input, result);
    auto pred = FlatVector::GetData<bool>(result.data[0]);
    CHECK(!pred[0]);
    CHECK(pred[1]);
    CHECK(pred[2]);
    CHECK(pred[3]);
    CHECK(!pred[4]);
}

// --- Bloom filter ------------------------------------------------------------

TEST_CASE("BloomFilter: no false negatives, ~1% false positives at 10 bits/key") {
    BloomFilter empty;
    CHECK(empty.Empty());

    const uint64_t n = 10000;
    BloomFilter bf(n * 10);
    CHECK(!bf.Empty());
    for (uint64_t i = 0; i < n; i++) {
        bf.Insert(duckdb::Hash<int64_t>(static_cast<int64_t>(i)));
    }
    for (uint64_t i = 0; i < n; i++) {
        CHECK(bf.MayContain(duckdb::Hash<int64_t>(static_cast<int64_t>(i))));
    }
    uint64_t false_positives = 0;
    for (uint64_t i = n; i < 11 * n; i++) {
        false_positives += bf.MayContain(duckdb::Hash<int64_t>(static_cast<int64_t>(i))) ? 1 : 0;
    }
    // ~1.2% expected for a split-block filter at 10 bits/key; 100k probes.
    CHECK_MSG(false_positives < 2 * n / 10, "false positive rate above 2% at 10 bits/key");
}

TEST_CASE("BloomFilter: merge ORs equal sizes, refuses mismatched sizes; wire round-trip") {
    BloomFilter a(4096), b(4096), c(8192);
    a.Insert(duckdb::Hash<int32_t>(1));
    b.Insert(duckdb::Hash<int32_t>(2));
    CHECK(!a.MayContain(duckdb::Hash<int32_t>(2)));
    CHECK(a.Merge(b));
    CHECK(a.MayContain(duckdb::Hash<int32_t>(1)));
    CHECK(a.MayContain(duckdb::Hash<int32_t>(2)));
    CHECK(!a.Merge(c));

    auto buf = SerializeToBuffer(a);
    auto a2 = DeserializeFromBytes<BloomFilter>(buf.data(), buf.size());
    CHECK(a2.NumBlocks() == a.NumBlocks());
    CHECK(a2.MayContain(duckdb::Hash<int32_t>(1)));
    CHECK(a2.MayContain(duckdb::Hash<int32_t>(2)));

    auto ebuf = SerializeToBuffer(BloomFilter{});
    CHECK(DeserializeFromBytes<BloomFilter>(ebuf.data(), ebuf.size()).Empty());
}

// --- DynamicFilter: exact values + Bloom filter ----------------------------------

namespace {

// Run a single DynamicFilterBuild over `blocks` and return its finished filter.
DynamicFilter BuildFilter(Allocator &alloc, const Schema &schema, std::shared_ptr<DynamicFilterBuildTemplate> templ,
                          const std::vector<OwnedBlock> &blocks) {
    PipelineTemplate desc;
    desc.input_schema = schema;
    desc.operators = {std::move(templ)};
    std::vector<InputBlock> in;
    for (auto &b : blocks) {
        in.push_back({b.data, b.size});
    }
    Executor exec(alloc);
    plume_test::RunBlocks(exec, desc, in, alloc);
    return exec.FindDynamicFilterBuild()->Filter();
}

// One-column block of arbitrary Values (a NULL Value marks a NULL row).
OwnedBlock MakeValueBlock(Allocator &alloc, const Schema &schema, const std::vector<Value> &vals) {
    duckdb::vector<LogicalType> types = {ToLogicalType(schema.columns[0].type)};
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), types);
    for (idx_t r = 0; r < vals.size(); r++) {
        chunk.SetValue(0, r, vals[r].DefaultCastAs(types[0]));
    }
    chunk.SetCardinality(vals.size());
    return ExportChunk(schema, chunk, alloc).unwrap();
}

std::vector<int32_t> SortedInts(const std::vector<Value> &vals) {
    std::vector<int32_t> out;
    for (auto &v : vals) {
        out.push_back(v.GetValue<int32_t>());
    }
    std::sort(out.begin(), out.end());
    return out;
}

} // namespace

TEST_CASE("DynamicFilterBuildOperator: exact distinct keys, NULLs skipped, no Bloom filter") {
    Allocator alloc;
    auto schema = AB();
    // Keys in the nullable column b.
    auto b0 = MakeBlock(alloc, schema, {{0, 0, 0}, {7, 3, 7}});
    auto b1 = MakeBlock(alloc, schema, {{0, 0, 0}, {-1000, 3, 42}}); // first b is NULL

    auto f = BuildFilter(alloc, schema,
        std::make_shared<DynamicFilterBuildTemplate>(1, I32(), /*max_values=*/8, /*bloom_bits=*/1024), {b0, b1});
    CHECK(f.has_bounds);
    CHECK(f.has_values);
    CHECK(f.max_values == 8);
    CHECK(f.bloom_bits == 1024);
    CHECK(SortedInts(f.values) == std::vector<int32_t>({3, 7, 42}));
    CHECK_MSG(f.bloom.Empty(), "the exact set fits -> no Bloom filter is built");
}

TEST_CASE("DynamicFilterBuildOperator: exact set converts to a Bloom filter past max_values") {
    Allocator alloc;
    auto schema = AB();
    // The set overflows in the second block; the keys of the first must still be in the filter.
    auto b0 = MakeBlock(alloc, schema, {{1, 2, 1}, {0, 0, 0}});
    auto b1 = MakeBlock(alloc, schema, {{3, 4, 5}, {0, 0, 0}});

    auto f = BuildFilter(alloc, schema,
        std::make_shared<DynamicFilterBuildTemplate>(0, I32(), /*max_values=*/3, /*bloom_bits=*/1024), {b0, b1});
    CHECK(f.has_bounds);
    CHECK(f.min == Value::INTEGER(1));
    CHECK(f.max == Value::INTEGER(5));
    CHECK(!f.has_values);
    CHECK(f.values.empty());
    CHECK(!f.bloom.Empty());
    CHECK(f.bloom.NumBlocks() == 1024 / BloomFilter::kBitsPerBlock);
    for (int32_t k : {1, 2, 3, 4, 5}) {
        CHECK(f.bloom.MayContain(duckdb::Hash<int32_t>(k)));
    }

    // max_values = 0 -> straight to the Bloom filter.
    auto g = BuildFilter(alloc, schema,
        std::make_shared<DynamicFilterBuildTemplate>(0, I32(), /*max_values=*/0, /*bloom_bits=*/1024), {b0});
    CHECK(!g.has_values);
    CHECK(g.bloom.MayContain(duckdb::Hash<int32_t>(2)));

    // An empty build still allocates its (zeroed) Bloom filter, so it merges as the identity.
    auto e = BuildFilter(alloc, schema,
        std::make_shared<DynamicFilterBuildTemplate>(0, I32(), /*max_values=*/0, /*bloom_bits=*/1024), {});
    CHECK(!e.has_bounds);
    CHECK(!e.bloom.Empty());
}

TEST_CASE("DynamicFilterBuildOperator: VARCHAR keys convert to the same hashes as string_t") {
    Allocator alloc;
    auto schema = OneVarchar();
    std::string long_str(40, 'q');
    auto b0 = MakeVarcharBlock(alloc, schema, {std::string("a"), long_str, std::nullopt, std::string("c")});

    auto f = BuildFilter(alloc, schema,
        std::make_shared<DynamicFilterBuildTemplate>(0, Varchar(), /*max_values=*/2, /*bloom_bits=*/1024), {b0});
    CHECK(!f.has_values);
    for (const std::string &k : {std::string("a"), long_str, std::string("c")}) {
        CHECK(f.bloom.MayContain(duckdb::Hash<duckdb::string_t>(duckdb::string_t(k.data(), k.size()))));
    }
}

TEST_CASE("DynamicFilterBuildOperator: exact set dropped past max_values without a Bloom filter") {
    Allocator alloc;
    auto schema = AB();
    auto b0 = MakeBlock(alloc, schema, {{1, 2, 3, 4}, {0, 0, 0, 0}});

    auto f = BuildFilter(alloc, schema, std::make_shared<DynamicFilterBuildTemplate>(0, I32(), /*max_values=*/3), {b0});
    CHECK(f.has_bounds); // the range survives
    CHECK(!f.has_values);
    CHECK(f.values.empty());
    CHECK(f.bloom.Empty()); // not requested

    auto off = BuildFilter(alloc, schema, std::make_shared<DynamicFilterBuildTemplate>(0, I32()), {b0});
    CHECK(!off.has_values);
}

TEST_CASE("DynamicFilterBuildOperator: no push after the filter was read") {
    Allocator alloc;
    auto schema = AB();
    auto b0 = MakeBlock(alloc, schema, {{1}, {0}});

    PipelineTemplate desc;
    desc.input_schema = schema;
    desc.operators = {std::make_shared<DynamicFilterBuildTemplate>(0, I32())};
    exec::OutputEmit emit = [](const std::string &, size_t, DataBuffer, size_t) {};
    Executor exec(alloc);
    exec.Build(desc, emit).unwrap();

    CHECK(!exec.FindDynamicFilterBuild()->Filter().has_bounds); // e.g. a stage without input
    CHECK(exec.PushBlocks({{b0.data, b0.size}}).is_error());
    alloc.Free(b0.data, b0.size);
}

TEST_CASE("DynamicFilterBuildOperator: keys are cast to key_type before collecting/hashing") {
    // INT32 column, BIGINT key type -> BIGINT values and Hash<int64_t> in the Bloom filter.
    Allocator alloc;
    auto schema = AB();
    auto b0 = MakeBlock(alloc, schema, {{5, -2}, {0, 0}});

    auto f = BuildFilter(alloc, schema,
        std::make_shared<DynamicFilterBuildTemplate>(0, ColumnType{TypeId::INT64}, 8, 1024), {b0});
    CHECK(f.min == Value::BIGINT(-2));
    CHECK(f.max == Value::BIGINT(5));
    CHECK(f.values.size() == 2);
    for (auto &v : f.values) {
        CHECK(v.type() == LogicalType::BIGINT);
    }

    auto g = BuildFilter(alloc, schema,
        std::make_shared<DynamicFilterBuildTemplate>(0, ColumnType{TypeId::INT64}, 0, 1024), {b0});
    CHECK(g.bloom.MayContain(duckdb::Hash<int64_t>(5)));
    CHECK(g.bloom.MayContain(duckdb::Hash<int64_t>(-2)));
}

TEST_CASE("DynamicFilterBuildOperator: DATE and DECIMAL keys keep their logical type") {
    Allocator alloc;

    Schema dates;
    dates.columns = {{"d", ColumnType{TypeId::DATE}, true}};
    auto db = MakeValueBlock(alloc, dates, {Value::DATE(duckdb::date_t(100)), Value::DATE(duckdb::date_t(20))});
    auto df = BuildFilter(alloc, dates, std::make_shared<DynamicFilterBuildTemplate>(0, dates.columns[0].type, 8), {db});
    CHECK(df.min == Value::DATE(duckdb::date_t(20)));
    CHECK(df.max == Value::DATE(duckdb::date_t(100)));
    for (auto &v : df.values) {
        CHECK(v.type() == LogicalType::DATE);
    }

    Schema decs;
    const ColumnType dec_type{TypeId::DECIMAL, 15, 2};
    decs.columns = {{"x", dec_type, true}};
    auto xb = MakeValueBlock(alloc, decs, {Value::DECIMAL(int64_t(12345), 15, 2), Value::DECIMAL(int64_t(-50), 15, 2)});
    auto xf = BuildFilter(alloc, decs, std::make_shared<DynamicFilterBuildTemplate>(0, dec_type, 8), {xb});
    // 123.45 / -0.50, not the raw 12345 / -50 as plain integers.
    CHECK(xf.max == Value::DECIMAL(int64_t(12345), 15, 2));
    CHECK(xf.min == Value::DECIMAL(int64_t(-50), 15, 2));
}

TEST_CASE("Value::Hash matches duckdb::Hash<T> of the physical key") {
    // DynamicFilter::Merge inserts exact Values into a Bloom filter built from duckdb::Hash<T>.
    std::string long_str(40, 'q');
    CHECK(Value::INTEGER(42).Hash() == duckdb::Hash<int32_t>(42));
    CHECK(Value::BIGINT(-7).Hash() == duckdb::Hash<int64_t>(-7));
    CHECK(Value::DATE(duckdb::date_t(100)).Hash() == duckdb::Hash<int32_t>(100));
    CHECK(Value::DECIMAL(int64_t(12345), 15, 2).Hash() == duckdb::Hash<int64_t>(12345));
    CHECK(Value(std::string("abc")).Hash() == duckdb::Hash<duckdb::string_t>(duckdb::string_t("abc")));
    CHECK(Value(long_str).Hash() == duckdb::Hash<duckdb::string_t>(duckdb::string_t(long_str.data(), long_str.size())));
}

TEST_CASE("DynamicFilter: Merge unions values, converts to a Bloom filter, drops what one side lacks") {
    auto values = [](std::vector<int32_t> keys, uint32_t max_values = 4, uint64_t bloom_bits = 1024) {
        DynamicFilter f;
        f.max_values = max_values;
        f.bloom_bits = bloom_bits;
        f.has_values = true;
        for (int32_t k : keys) {
            f.MergeBounds(Value::INTEGER(k), Value::INTEGER(k));
            f.values.push_back(Value::INTEGER(k));
        }
        return f;
    };
    auto bloom = [](std::vector<int32_t> keys, uint64_t bloom_bits = 1024) {
        DynamicFilter f;
        f.max_values = 4;
        f.bloom_bits = bloom_bits;
        f.bloom = BloomFilter(bloom_bits);
        for (int32_t k : keys) {
            f.MergeBounds(Value::INTEGER(k), Value::INTEGER(k));
            f.bloom.Insert(duckdb::Hash<int32_t>(k));
        }
        return f;
    };
    auto contains_all = [](const DynamicFilter &f, std::vector<int32_t> keys) {
        for (int32_t k : keys) {
            if (!f.bloom.MayContain(duckdb::Hash<int32_t>(k))) {
                return false;
            }
        }
        return true;
    };

    // values + values within max_values -> union
    auto a = values({1, 2});
    a.Merge(values({2, 9}));
    CHECK(a.min == Value::INTEGER(1));
    CHECK(a.max == Value::INTEGER(9));
    CHECK(a.has_values);
    CHECK(SortedInts(a.values) == std::vector<int32_t>({1, 2, 9}));
    CHECK(a.bloom.Empty());

    // An empty build instance (no rows: empty set) is the identity.
    a.Merge(values({}));
    CHECK(SortedInts(a.values) == std::vector<int32_t>({1, 2, 9}));

    // values + values past max_values -> Bloom filter over the union
    a.Merge(values({3, 4})); // 5 distinct > 4
    CHECK(!a.has_values);
    CHECK(a.values.empty());
    CHECK(!a.bloom.Empty());
    CHECK(contains_all(a, {1, 2, 3, 4, 9}));

    // ... or dropped without a configured Bloom filter
    auto n = values({1, 2, 3}, 4, 0);
    n.Merge(values({4, 5}, 4, 0));
    CHECK(!n.has_values);
    CHECK(n.bloom.Empty());
    CHECK(n.has_bounds);

    // values + Bloom filter (either order) -> Bloom filter with the values inserted
    auto vb = values({1, 2});
    vb.Merge(bloom({7}));
    CHECK(!vb.has_values);
    CHECK(contains_all(vb, {1, 2, 7}));
    auto bv = bloom({7});
    bv.Merge(values({1, 2}));
    CHECK(!bv.has_values);
    CHECK(contains_all(bv, {1, 2, 7}));
    CHECK(bv.min == Value::INTEGER(1));

    // Bloom + Bloom -> OR; mismatched sizes -> dropped
    auto bb = bloom({1});
    bb.Merge(bloom({2}));
    CHECK(contains_all(bb, {1, 2}));
    auto mismatch = bloom({1});
    mismatch.Merge(bloom({2}, 2048));
    CHECK(mismatch.bloom.Empty());

    // A side without any membership structure drops it from the result.
    DynamicFilter none;
    none.MergeBounds(Value::INTEGER(0), Value::INTEGER(0));
    auto v = values({1});
    v.Merge(none);
    CHECK(!v.has_values);
    CHECK(v.bloom.Empty());
    CHECK(v.min == Value::INTEGER(0));
    auto b2 = bloom({1});
    b2.Merge(none);
    CHECK(b2.bloom.Empty());
}

TEST_CASE("DynamicFilter: wire round-trip") {
    DynamicFilter f;
    f.MergeBounds(Value::INTEGER(1), Value::INTEGER(9));
    f.has_values = true;
    f.values = {Value::INTEGER(1), Value::INTEGER(9)};
    f.max_values = 16;
    f.bloom_bits = 512;
    f.bloom = BloomFilter(512);
    f.bloom.Insert(duckdb::Hash<int32_t>(9));

    auto buf = SerializeToBuffer(f);
    auto g = DeserializeFromBytes<DynamicFilter>(buf.data(), buf.size());
    CHECK(g.has_bounds);
    CHECK(g.min == Value::INTEGER(1));
    CHECK(g.max == Value::INTEGER(9));
    CHECK(g.has_values);
    CHECK(g.values.size() == 2);
    CHECK(g.max_values == 16);
    CHECK(g.bloom_bits == 512);
    CHECK(g.bloom.NumBlocks() == f.bloom.NumBlocks());
    CHECK(g.bloom.MayContain(duckdb::Hash<int32_t>(9)));

    auto empty_buf = SerializeToBuffer(DynamicFilter());
    auto empty = DeserializeFromBytes<DynamicFilter>(empty_buf.data(), empty_buf.size());
    CHECK(!empty.has_bounds);
    CHECK(!empty.has_values);
    CHECK(empty.bloom.Empty());
}

int main() { return plume_test::RunAll(); }

// --- DynamicFilterProbe ------------------------------------------------------

namespace {

// Runs `probe` over an INT32 vector (std::nullopt -> NULL), starting from the rows in `candidates`
// (all rows if empty), and returns the surviving keys.
std::vector<int32_t> ProbeInts(DynamicFilterProbe &probe, const std::vector<std::optional<int32_t>> &keys,
                               std::vector<idx_t> candidates = {}) {
    duckdb::Vector vec(LogicalType::INTEGER, keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        vec.SetValue(i, keys[i] ? Value::INTEGER(*keys[i]) : Value(LogicalType::INTEGER));
    }
    if (candidates.empty()) {
        for (idx_t i = 0; i < keys.size(); i++) {
            candidates.push_back(i);
        }
    }
    duckdb::SelectionVector sel(keys.size());
    for (idx_t i = 0; i < candidates.size(); i++) {
        sel.set_index(i, candidates[i]);
    }
    const idx_t passed = probe.Select(vec, keys.size(), sel, candidates.size());
    std::vector<int32_t> out;
    for (idx_t i = 0; i < passed; i++) {
        out.push_back(*keys[sel.get_index(i)]);
    }
    return out;
}

DynamicFilter IntRange(int32_t lo, int32_t hi) {
    DynamicFilter f;
    f.MergeBounds(Value::INTEGER(lo), Value::INTEGER(hi));
    return f;
}

} // namespace

TEST_CASE("DynamicFilterProbe: key range only, NULLs rejected, candidate selection honored") {
    auto probe = DynamicFilterProbe::Create(IntRange(3, 7), LogicalType::INTEGER).unwrap();
    CHECK(probe != nullptr);
    CHECK(ProbeInts(*probe, {1, 3, std::nullopt, 5, 7, 8}) == std::vector<int32_t>({3, 5, 7}));
    // Only rows 1 and 2 are candidates (e.g. what the scan filter let through).
    CHECK(ProbeInts(*probe, {4, 5, 9, 6}, {1, 2}) == std::vector<int32_t>({5}));
}

TEST_CASE("DynamicFilterProbe: exact key set") {
    auto f = IntRange(2, 9);
    f.has_values = true;
    f.values = {Value::INTEGER(2), Value::INTEGER(5), Value::INTEGER(9)};
    auto probe = DynamicFilterProbe::Create(std::move(f), LogicalType::INTEGER).unwrap();
    CHECK(ProbeInts(*probe, {1, 2, 3, 5, 6, 9, 10}) == std::vector<int32_t>({2, 5, 9}));
}

TEST_CASE("DynamicFilterProbe: Bloom filter has no false negatives and rejects most other keys") {
    auto f = IntRange(0, 1999);
    f.bloom = BloomFilter(1000 * 10);
    std::vector<std::optional<int32_t>> keys;
    for (int32_t k = 0; k < 2000; k++) {
        if (k % 2 == 0) {
            f.bloom.Insert(duckdb::Hash<int32_t>(k));
        }
        keys.push_back(k);
    }
    auto probe = DynamicFilterProbe::Create(std::move(f), LogicalType::INTEGER).unwrap();
    auto passed = ProbeInts(*probe, keys);
    size_t odd = 0;
    for (int32_t k : passed) {
        odd += k % 2;
    }
    CHECK(passed.size() - odd == 1000); // every inserted key passes
    CHECK_MSG(odd < 50, "more than 5% false positives at 10 bits/key");
}

TEST_CASE("DynamicFilterProbe: VARCHAR exact keys and bounds") {
    std::string long_str(40, 'm');
    DynamicFilter f;
    f.MergeBounds(Value("b"), Value(long_str));
    f.has_values = true;
    f.values = {Value("b"), Value(long_str)};
    auto probe = DynamicFilterProbe::Create(std::move(f), LogicalType::VARCHAR).unwrap();

    std::vector<std::string> keys = {"a", "b", "c", long_str, std::string(41, 'm')};
    duckdb::Vector vec(LogicalType::VARCHAR, keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        vec.SetValue(i, Value(keys[i]));
    }
    duckdb::SelectionVector sel(keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        sel.set_index(i, i);
    }
    CHECK(probe->Select(vec, keys.size(), sel, keys.size()) == 2);
    CHECK(sel.get_index(0) == 1);
    CHECK(sel.get_index(1) == 3);
}

TEST_CASE("DynamicFilterProbe: dictionary entries are checked once, rows by lookup") {
    auto f = IntRange(10, 30);
    f.has_values = true;
    f.values = {Value::INTEGER(20)};
    auto probe = DynamicFilterProbe::Create(std::move(f), LogicalType::INTEGER).unwrap();

    // Dictionary [10, 20, 30, NULL].
    duckdb::Vector dict(LogicalType::INTEGER, 4);
    auto *d = FlatVector::GetData<int32_t>(dict);
    d[0] = 10;
    d[1] = 20;
    d[2] = 30;
    FlatVector::Validity(dict).SetInvalid(3);
    CHECK(probe->SetDictionary(&dict, 4) == 1);

    auto select = [&](duckdb::Vector &keys, idx_t n) {
        duckdb::SelectionVector sel(n);
        for (idx_t i = 0; i < n; i++) {
            sel.set_index(i, i);
        }
        std::vector<idx_t> rows;
        const idx_t passed = probe->Select(keys, n, sel, n);
        for (idx_t i = 0; i < passed; i++) {
            rows.push_back(sel.get_index(i));
        }
        return rows;
    };
    // Rows referencing entries 0, 1, 2, 3 (NULL), 1.
    duckdb::SelectionVector idx(5);
    const std::vector<idx_t> entries = {0, 1, 2, 3, 1};
    for (idx_t i = 0; i < entries.size(); i++) {
        idx.set_index(i, entries[i]);
    }
    duckdb::Vector keys(LogicalType::INTEGER);
    keys.Dictionary(dict, 4, idx, 5);
    CHECK(select(keys, 5) == std::vector<idx_t>({1, 4}));

    // Keys over another dictionary, or flat keys, are checked row by row.
    duckdb::Vector other(LogicalType::INTEGER, 4);
    auto *o = FlatVector::GetData<int32_t>(other);
    o[0] = 20;
    o[1] = 40;
    o[2] = 20;
    o[3] = 10;
    duckdb::Vector other_keys(LogicalType::INTEGER);
    other_keys.Dictionary(other, 4, idx, 5);
    CHECK(select(other_keys, 5) == std::vector<idx_t>({0, 2}));
    CHECK(select(other, 4) == std::vector<idx_t>({0, 2}));

    // Forgetting the dictionary goes back to checking rows.
    CHECK(probe->SetDictionary(nullptr, 0) == 0);
    d[0] = 20; // the dictionary's own rows now pass by value
    CHECK(select(keys, 5) == std::vector<idx_t>({0, 1, 4}));
}

TEST_CASE("DynamicFilterProbe: nothing to check -> no probe") {
    CHECK(DynamicFilterProbe::Create(DynamicFilter(), LogicalType::INTEGER).unwrap() == nullptr);
    // Build keys typed BIGINT can't be checked against an INTEGER probe column.
    DynamicFilter f;
    f.MergeBounds(Value::BIGINT(1), Value::BIGINT(2));
    CHECK(DynamicFilterProbe::Create(std::move(f), LogicalType::INTEGER).unwrap() == nullptr);
}

TEST_CASE("DynamicFilterProbe: turns itself off if it lets through too many rows") {
    const idx_t n = STANDARD_VECTOR_SIZE;
    std::vector<std::optional<int32_t>> keys;
    for (idx_t i = 0; i < n; i++) {
        keys.push_back(static_cast<int32_t>(i));
    }

    // Everything passes -> off after the warmup.
    auto all = DynamicFilterProbe::Create(IntRange(0, int32_t(n)), LogicalType::INTEGER).unwrap();
    for (uint64_t checked = 0; checked < DynamicFilterProbe::kWarmupRows; checked += n) {
        CHECK(all->Active());
        ProbeInts(*all, keys);
    }
    CHECK(!all->Active());

    // Half passes -> stays on.
    auto half = DynamicFilterProbe::Create(IntRange(0, int32_t(n / 2) - 1), LogicalType::INTEGER).unwrap();
    for (uint64_t checked = 0; checked < 2 * DynamicFilterProbe::kWarmupRows; checked += n) {
        ProbeInts(*half, keys);
    }
    CHECK(half->Active());
}
