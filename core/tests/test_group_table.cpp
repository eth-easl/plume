#include "test_util.hpp"

#include "plume/execution/group_table.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <set>
#include <string>
#include <vector>

using namespace plume;
using namespace plume::exec;
using duckdb::data_ptr_t;
using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::SelectionVector;
using duckdb::Value;
using duckdb::Vector;

namespace {

// One chunk of `rows` (each a row of Values) with the given column types.
std::unique_ptr<DataChunk> MakeChunk(memory::Allocator &alloc, const duckdb::vector<LogicalType> &types,
                                     const std::vector<std::vector<Value>> &rows) {
    auto chunk = std::make_unique<DataChunk>();
    chunk->Initialize(alloc.Get(), types);
    for (idx_t r = 0; r < rows.size(); r++) {
        for (idx_t c = 0; c < types.size(); c++) {
            chunk->data[c].SetValue(r, rows[r][c]);
        }
    }
    chunk->SetChildCardinality(rows.size());
    return chunk;
}

struct Lookup {
    std::vector<data_ptr_t> payloads = std::vector<data_ptr_t>(STANDARD_VECTOR_SIZE);
    SelectionVector new_groups = SelectionVector(STANDARD_VECTOR_SIZE);
    idx_t new_count = 0;
};

Lookup FindOrCreate(GroupTable &table, DataChunk &keys) {
    Lookup out;
    Vector hashes(LogicalType::HASH, keys.size());
    table.Hash(keys, hashes, keys.size());
    out.new_count = table.FindOrCreate(keys, hashes, keys.size(), out.payloads.data(), out.new_groups);
    return out;
}

std::vector<data_ptr_t> Find(GroupTable &table, DataChunk &keys) {
    std::vector<data_ptr_t> payloads(STANDARD_VECTOR_SIZE);
    Vector hashes(LogicalType::HASH, keys.size());
    table.Hash(keys, hashes, keys.size());
    table.Find(keys, hashes, keys.size(), payloads.data());
    return payloads;
}

} // namespace

TEST_CASE("group table: many groups across resizes keep their payloads") {
    memory::Allocator alloc;
    GroupTable table({LogicalType::BIGINT}, 8, alloc);
    const idx_t n = 100000;
    std::vector<data_ptr_t> first(n);
    for (idx_t base = 0; base < n; base += STANDARD_VECTOR_SIZE) {
        std::vector<std::vector<Value>> rows;
        for (idx_t i = base; i < std::min(n, base + STANDARD_VECTOR_SIZE); i++) {
            rows.push_back({Value::BIGINT(static_cast<int64_t>(i * 7919))});
        }
        auto chunk = MakeChunk(alloc, {LogicalType::BIGINT}, rows);
        auto res = FindOrCreate(table, *chunk);
        CHECK(res.new_count == rows.size());
        for (idx_t r = 0; r < rows.size(); r++) {
            duckdb::Store<int64_t>(static_cast<int64_t>(base + r), res.payloads[r]);
            first[base + r] = res.payloads[r];
        }
    }
    CHECK(table.Count() == n);
    // A second pass finds every key again, at the same payload, creating nothing.
    for (idx_t base = 0; base < n; base += STANDARD_VECTOR_SIZE) {
        std::vector<std::vector<Value>> rows;
        for (idx_t i = base; i < std::min(n, base + STANDARD_VECTOR_SIZE); i++) {
            rows.push_back({Value::BIGINT(static_cast<int64_t>(i * 7919))});
        }
        auto chunk = MakeChunk(alloc, {LogicalType::BIGINT}, rows);
        auto res = FindOrCreate(table, *chunk);
        CHECK(res.new_count == 0);
        for (idx_t r = 0; r < rows.size(); r++) {
            CHECK(res.payloads[r] == first[base + r]);
            CHECK(duckdb::Load<int64_t>(res.payloads[r]) == static_cast<int64_t>(base + r));
        }
    }
}

TEST_CASE("group table: composite keys with NULLs, duplicates within a chunk, long strings") {
    memory::Allocator alloc;
    const duckdb::vector<LogicalType> types = {LogicalType::INTEGER, LogicalType::VARCHAR};
    GroupTable table(types, 0, alloc);
    const std::string long_a = "a string that is clearly longer than twelve bytes";
    const std::string long_b = "a string that is clearly longer than twelve bytez";
    const Value null_int(LogicalType::INTEGER);
    const Value null_str(LogicalType::VARCHAR);
    std::vector<std::vector<Value>> rows = {
        {Value::INTEGER(1), Value(long_a)}, {Value::INTEGER(1), Value(long_b)}, {null_int, Value("x")},
        {null_int, null_str},              {Value::INTEGER(1), null_str},       {Value::INTEGER(1), Value(long_a)},
        {null_int, null_str},              {Value::INTEGER(0), Value("")},      {Value::INTEGER(0), null_str},
    };
    Lookup res;
    {
        auto chunk = MakeChunk(alloc, types, rows);
        res = FindOrCreate(table, *chunk);
    } // the source strings are gone: the table must have copied the long ones
    CHECK(res.new_count == 7);
    CHECK(table.Count() == 7);
    CHECK(res.payloads[0] == res.payloads[5]);
    CHECK(res.payloads[3] == res.payloads[6]);
    std::set<data_ptr_t> distinct(res.payloads.begin(), res.payloads.begin() + rows.size());
    CHECK(distinct.size() == 7);

    // Scanned keys outlive the table (they reference its string arena).
    DataChunk out;
    out.Initialize(alloc.Get(), types);
    {
        GroupTable copy(types, 0, alloc);
        auto chunk = MakeChunk(alloc, types, rows);
        FindOrCreate(copy, *chunk);
        std::vector<data_ptr_t> payloads(STANDARD_VECTOR_SIZE);
        copy.ScanKeys(0, copy.Count(), out, payloads.data());
        out.SetChildCardinality(copy.Count());
    }
    std::multiset<std::string> scanned;
    for (idx_t r = 0; r < out.size(); r++) {
        scanned.insert(out.GetValue(0, r).ToString() + "|" + out.GetValue(1, r).ToString());
    }
    const std::multiset<std::string> expected = {"1|" + long_a, "1|" + long_b, "NULL|x", "NULL|NULL",
                                                 "1|NULL",      "0|",          "0|NULL"};
    CHECK(scanned == expected);
}

TEST_CASE("group table: Find only looks up") {
    memory::Allocator alloc;
    GroupTable table({LogicalType::VARCHAR}, 8, alloc);
    CHECK(Find(table, *MakeChunk(alloc, {LogicalType::VARCHAR}, {{Value("a")}}))[0] == nullptr); // empty table
    auto res = FindOrCreate(table, *MakeChunk(alloc, {LogicalType::VARCHAR}, {{Value("a")}, {Value("bb")}}));
    auto found = Find(table, *MakeChunk(alloc, {LogicalType::VARCHAR},
                                        {{Value("bb")}, {Value("zz")}, {Value("a")}, {Value(LogicalType::VARCHAR)}}));
    CHECK(found[0] == res.payloads[1]);
    CHECK(found[1] == nullptr);
    CHECK(found[2] == res.payloads[0]);
    CHECK(found[3] == nullptr);
    CHECK(table.Count() == 2);
}

TEST_CASE("group table: constant and dictionary key vectors") {
    memory::Allocator alloc;
    GroupTable table({LogicalType::INTEGER, LogicalType::VARCHAR}, 0, alloc);
    DataChunk keys;
    keys.Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::VARCHAR});
    duckdb::ConstantVector::Reference(keys.data[0], Value::INTEGER(42), duckdb::count_t(4)); // constant
    Vector dict(LogicalType::VARCHAR, 3);
    dict.SetValue(0, Value("left"));
    dict.SetValue(1, Value("a fairly long right-hand value"));
    dict.SetValue(2, Value("left"));
    duckdb::FlatVector::SetSize(dict, 3);
    SelectionVector sel(4);
    for (idx_t i = 0; i < 4; i++) {
        sel.set_index(i, i % 3); // left, right, left, left
    }
    keys.data[1].Slice(dict, sel, 4);
    keys.SetChildCardinality(4);
    auto res = FindOrCreate(table, keys);
    CHECK(res.new_count == 2);
    CHECK(res.payloads[0] == res.payloads[2]);
    CHECK(res.payloads[0] == res.payloads[3]);
    CHECK(res.payloads[0] != res.payloads[1]);
}

TEST_CASE("group table: floating point, decimal, hugeint, date, boolean keys") {
    memory::Allocator alloc;
    const duckdb::vector<LogicalType> types = {LogicalType::DOUBLE, LogicalType::DECIMAL(30, 4), LogicalType::HUGEINT,
                                               LogicalType::DATE, LogicalType::BOOLEAN};
    CHECK(GroupTable::SupportsType(LogicalType::DECIMAL(30, 4)));
    CHECK(!GroupTable::SupportsType(LogicalType::LIST(LogicalType::INTEGER)));
    GroupTable table(types, 0, alloc);
    auto row = [](double d, const char *dec, int64_t h, int32_t days, bool b) {
        return std::vector<Value> {Value::DOUBLE(d), Value(dec).DefaultCastAs(LogicalType::DECIMAL(30, 4)),
                                   Value::HUGEINT(duckdb::hugeint_t(h) * duckdb::hugeint_t(1000000000000LL)),
                                   Value::DATE(duckdb::date_t(days)), Value::BOOLEAN(b)};
    };
    auto res = FindOrCreate(table, *MakeChunk(alloc, types,
                                              {row(0.0, "1.5", 3, 10, true), row(-0.0, "1.5", 3, 10, true),
                                               row(0.0, "1.5000", 3, 10, false), row(1.0, "123456789.1234", -3, 10, true),
                                               row(1.0, "123456789.1234", -3, 10, true)}));
    CHECK(res.payloads[0] == res.payloads[1]); // -0.0 groups with 0.0, as in DuckDB
    CHECK(res.payloads[0] != res.payloads[2]);
    CHECK(res.payloads[3] == res.payloads[4]);
    CHECK(res.new_count == 3);
}

int main() { return plume_test::RunAll(); }
