// Milestone-2 round-trip test: build a DataChunk covering all v1 types (incl.
// nulls, short/long/empty varchar) -> ExportChunk to a block -> ImportBlock back
// -> ExportChunk again, asserting value equality and byte-for-byte block
// stability, plus the zero-copy property for fixed-width columns.

#include "test_util.hpp"

#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/memory/block.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"

#include <cstring>

using namespace plume;
using namespace plume::memory;
using duckdb::DataChunk;
using duckdb::Value;
using duckdb::LogicalType;
using duckdb::FlatVector;
using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::hugeint_t;
using duckdb::idx_t;

namespace {

Schema MakeSchema() {
    Schema s;
    s.columns = {
        {"c_bool", {TypeId::BOOLEAN}, true},
        {"c_i32", {TypeId::INT32}, true},
        {"c_i64", {TypeId::INT64}, false},
        {"c_f32", {TypeId::FLOAT}, true},
        {"c_f64", {TypeId::DOUBLE}, false},
        {"c_str", {TypeId::VARCHAR}, true},
        {"c_dec", {TypeId::DECIMAL, 9, 2}, true},
        {"c_date", {TypeId::DATE}, true},
        {"c_time", {TypeId::TIME}, false},
        {"c_hugeint", {TypeId::HUGEINT}, true},
        {"c_i8", {TypeId::INT8}, true},
        {"c_i16", {TypeId::INT16}, false},
    };
    return s;
}

// Build the canonical 6-row chunk. Nulls are placed in nullable columns.
void FillChunk(DataChunk &chunk, Allocator &alloc) {
    auto schema = MakeSchema();
    duckdb::vector<LogicalType> types;
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }
    chunk.Initialize(alloc.Get(), types);
    const idx_t n = 6;

    const char *strs[n] = {"hi",                              // inlined short
                           "a-much-longer-string-past-12b",   // non-inlined
                           "",                                 // empty
                           nullptr,                            // null
                           "exactly12chr",                     // 12 bytes (inlined boundary)
                           "thirteen_chars"};                  // 14 bytes non-inlined
    for (idx_t r = 0; r < n; r++) {
        chunk.SetValue(0, r, (r % 2 == 0) ? Value::BOOLEAN(true) : Value::BOOLEAN(false));
        chunk.SetValue(1, r, Value::INTEGER(int32_t(r) * 100 - 250));
        chunk.SetValue(2, r, Value::BIGINT(int64_t(r) * 1'000'000'000LL));
        chunk.SetValue(3, r, Value::FLOAT(float(r) + 0.5f));
        chunk.SetValue(4, r, Value::DOUBLE(double(r) * 3.14159));
        if (strs[r]) {
            chunk.SetValue(5, r, Value(std::string(strs[r])));
        } else {
            FlatVector::SetNull(chunk.data[5], r, true);
        }
        chunk.SetValue(6, r, Value::DECIMAL(int32_t(r) * 125, uint8_t(9), uint8_t(2)));
        chunk.SetValue(7, r, Value::DATE(date_t(int32_t(19000 + r))));
        chunk.SetValue(8, r, Value::TIME(dtime_t(int64_t(r) * 3'600'000'000LL)));
        // A value wider than 64 bits to exercise the full INT128 layout.
        chunk.SetValue(9, r, Value::HUGEINT(hugeint_t(int64_t(r) + 1) * hugeint_t(1'000'000'000'000'000'000LL) *
                                            hugeint_t(100)));
        chunk.SetValue(10, r, Value::TINYINT(int8_t(int32_t(r) * 20 - 60)));  // c_i8
        chunk.SetValue(11, r, Value::SMALLINT(int16_t(int32_t(r) * 5000 - 15000))); // c_i16
    }
    // Inject nulls into nullable fixed-width columns too.
    FlatVector::SetNull(chunk.data[0], 1, true); // c_bool
    FlatVector::SetNull(chunk.data[1], 3, true); // c_i32
    FlatVector::SetNull(chunk.data[3], 4, true); // c_f32
    FlatVector::SetNull(chunk.data[6], 2, true); // c_dec
    FlatVector::SetNull(chunk.data[7], 5, true); // c_date
    FlatVector::SetNull(chunk.data[9], 0, true);  // c_hugeint
    FlatVector::SetNull(chunk.data[10], 3, true); // c_i8
    chunk.SetCardinality(n);
}

bool ValuesEqual(DataChunk &a, DataChunk &b) {
    if (a.size() != b.size() || a.ColumnCount() != b.ColumnCount()) {
        return false;
    }
    for (idx_t c = 0; c < a.ColumnCount(); c++) {
        for (idx_t r = 0; r < a.size(); r++) {
            Value va = a.GetValue(c, r);
            Value vb = b.GetValue(c, r);
            if (va.IsNull() != vb.IsNull()) {
                return false;
            }
            if (!va.IsNull() && !Value::NotDistinctFrom(va, vb)) {
                return false;
            }
        }
    }
    return true;
}

} // namespace

TEST_CASE("export/import round-trip preserves values and is byte-stable") {
    Allocator alloc;
    auto schema = MakeSchema();

    DataChunk original;
    FillChunk(original, alloc);

    // chunk -> blockA
    OwnedBlock blockA = ExportChunk(schema, original, alloc).unwrap();
    CHECK(blockA.data != nullptr);
    CHECK(blockA.size >= sizeof(BlockHeader));

    // header sanity
    auto *h = reinterpret_cast<const BlockHeader *>(blockA.data);
    CHECK(h->magic == kBlockMagic);
    CHECK(h->format_version == kFormatVersion);
    CHECK(h->row_count == 6);
    CHECK(h->column_count == schema.size());
    CHECK(h->total_size == blockA.size);

    // schema survives serialization
    Schema parsed = ParseSchema(blockA.data, blockA.size).unwrap();
    CHECK(parsed.size() == schema.size());
    for (size_t i = 0; i < schema.size(); i++) {
        CHECK(parsed.columns[i].name == schema.columns[i].name);
        CHECK(parsed.columns[i].type == schema.columns[i].type);
        CHECK(parsed.columns[i].nullable == schema.columns[i].nullable);
    }

    // Snapshot blockA before import: ImportBlock swizzles its varchar offsets to
    // absolute pointers in place, so the byte-stability check below must compare
    // against the pristine pre-import bytes.
    std::vector<uint8_t> blockA_snapshot(blockA.data, blockA.data + blockA.size);

    // blockA -> chunkB  (zero-copy import; mutates blockA's varchar offsets)
    DataChunk chunkB;
    Schema schemaB;
    ImportBlock(blockA.data, blockA.size, chunkB, schemaB).unwrap();
    CHECK_MSG(ValuesEqual(original, chunkB), "imported values differ from original");

    // zero-copy: a fixed-width column's data must lie inside blockA's buffer
    auto *i64_ptr = reinterpret_cast<const uint8_t *>(FlatVector::GetData<int64_t>(chunkB.data[2]));
    auto *base = reinterpret_cast<const uint8_t *>(blockA.data);
    CHECK_MSG(i64_ptr >= base && i64_ptr < base + blockA.size, "fixed-width import was not zero-copy");

    // chunkB -> blockB, must be byte-identical to blockA (deterministic layout)
    OwnedBlock blockB = ExportChunk(schemaB, chunkB, alloc).unwrap();
    CHECK(blockB.size == blockA_snapshot.size());
    CHECK_MSG(std::memcmp(blockA_snapshot.data(), blockB.data, blockB.size) == 0,
              "re-exported block differs byte-wise from pristine original");

    alloc.Free(blockA.data, blockA.size);
    alloc.Free(blockB.data, blockB.size);
}

TEST_CASE("all-valid non-nullable columns store no validity mask") {
    Allocator alloc;
    Schema s;
    s.columns = {{"x", {TypeId::INT32}, false}};
    duckdb::vector<LogicalType> types = {LogicalType::INTEGER};
    DataChunk c;
    c.Initialize(alloc.Get(), types);
    for (idx_t r = 0; r < 4; r++) {
        c.SetValue(0, r, Value::INTEGER(int32_t(r)));
    }
    c.SetCardinality(4);

    OwnedBlock b = ExportChunk(s, c, alloc).unwrap();
    auto *descs = reinterpret_cast<const ColumnDescriptor *>(b.data + sizeof(BlockHeader));
    CHECK_MSG(descs[0].validity_offset == 0, "non-nullable column should have no validity region");
    alloc.Free(b.data, b.size);
}

int main() {
    return plume_test::RunAll();
}
