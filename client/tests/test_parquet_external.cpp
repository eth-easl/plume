#include "duckdb.hpp"
#include "test_util.hpp"

#include "plume/parquet/encoder.hpp"

#include "duckdb/common/types/date.hpp"

#include "parquet_extension.hpp"

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

using namespace plume;
using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;

namespace {

Schema ExternalSchema() {
    Schema s;
    s.columns = {
        {"id", {TypeId::INT32}, false},
        {"label", {TypeId::VARCHAR}, true},
        {"big", {TypeId::INT64}, false},
        {"price", {TypeId::DECIMAL, 9, 2}, false},
        {"dt", {TypeId::DATE}, false},
    };
    return s;
}

} // namespace

namespace {

// Write the known 3-row table with `codec` and read it back with a real DuckDB
// instance + the parquet extension, checking values, types, and the preserved NULL.
void CheckExternalRead(int32_t codec, const std::string &path) {
    auto schema = ExternalSchema();
    duckdb::vector<LogicalType> types;
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }

    // Three rows; label row 1 is NULL.
    DataChunk chunk;
    duckdb::Allocator alloc;
    chunk.Initialize(alloc, types);
    const int n = 3;
    const char *labels[] = {"alpha", nullptr, "gamma"};
    for (int r = 0; r < n; r++) {
        chunk.SetValue(0, r, Value::INTEGER(r + 1));
        chunk.SetValue(1, r, labels[r] ? Value(std::string(labels[r])) : Value(LogicalType::VARCHAR));
        chunk.SetValue(2, r, Value::BIGINT(int64_t(r + 1) * 1000000000));
        chunk.SetValue(3, r, Value::DOUBLE(10.0 + r + 0.25).DefaultCastAs(LogicalType::DECIMAL(9, 2)));
        chunk.SetValue(4, r, Value::DATE(duckdb::Date::FromDate(2021, 3, r + 1)));
    }
    chunk.SetCardinality(n);

    auto buf = parquet::WriteParquet(schema, {&chunk}, codec).unwrap();
    {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        f.write(reinterpret_cast<const char *>(buf.data()), buf.size());
    }

    duckdb::DuckDB db(nullptr); // duckdb_glue's LoadAllExtensions registers parquet
    duckdb::Connection con(db);

    auto res = con.Query("SELECT id, label, big, price, dt FROM read_parquet('" + path + "') ORDER BY id");
    CHECK_MSG(!res->HasError(), res->HasError() ? res->GetError().c_str() : "");
    CHECK(res->RowCount() == 3);

    // Spot-check values, types, and the preserved NULL.
    CHECK(res->GetValue(0, 0).GetValue<int32_t>() == 1);
    CHECK(res->GetValue(1, 0).ToString() == "alpha");
    CHECK(res->GetValue(1, 1).IsNull());
    CHECK(res->GetValue(2, 2).GetValue<int64_t>() == int64_t(3) * 1000000000);
    CHECK(res->GetValue(3, 0).ToString() == "10.25"); // DECIMAL(9,2) round-trip
    CHECK(res->GetValue(4, 2).ToString() == "2021-03-03");

    std::remove(path.c_str());
}

} // namespace

TEST_CASE("uncompressed parquet output is readable by DuckDB's parquet reader") {
    CheckExternalRead(parquet::WRITE_UNCOMPRESSED, "plume_external_test.parquet");
}

TEST_CASE("Snappy parquet output is readable by DuckDB's parquet reader") {
    CheckExternalRead(parquet::WRITE_SNAPPY, "plume_external_snappy.parquet");
}

int main() { return plume_test::RunAll(); }
