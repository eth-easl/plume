#include "test_util.hpp"

#include "plume/execution/operators/output.hpp"
#include "plume/execution/sink.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/memory/block.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <cstdlib>
#include <cstring>
#include <functional>
#include <set>
#include <string>

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
        chunk.data[0].SetValue(r, (r % 2 == 0) ? Value::BOOLEAN(true) : Value::BOOLEAN(false));
        chunk.data[1].SetValue(r, Value::INTEGER(int32_t(r) * 100 - 250));
        chunk.data[2].SetValue(r, Value::BIGINT(int64_t(r) * 1'000'000'000LL));
        chunk.data[3].SetValue(r, Value::FLOAT(float(r) + 0.5f));
        chunk.data[4].SetValue(r, Value::DOUBLE(double(r) * 3.14159));
        if (strs[r]) {
            chunk.data[5].SetValue(r, Value(std::string(strs[r])));
        } else {
            FlatVector::SetNull(chunk.data[5], r, true);
        }
        chunk.data[6].SetValue(r, Value::DECIMAL(int32_t(r) * 125, uint8_t(9), uint8_t(2)));
        chunk.data[7].SetValue(r, Value::DATE(date_t(int32_t(19000 + r))));
        chunk.data[8].SetValue(r, Value::TIME(dtime_t(int64_t(r) * 3'600'000'000LL)));
        // A value wider than 64 bits to exercise the full INT128 layout.
        chunk.data[9].SetValue(r, Value::HUGEINT(hugeint_t(int64_t(r) + 1) * hugeint_t(1'000'000'000'000'000'000LL) *
                                            hugeint_t(100)));
        chunk.data[10].SetValue(r, Value::TINYINT(int8_t(int32_t(r) * 20 - 60)));  // c_i8
        chunk.data[11].SetValue(r, Value::SMALLINT(int16_t(int32_t(r) * 5000 - 15000))); // c_i16
    }
    // Inject nulls into nullable fixed-width columns too.
    FlatVector::SetNull(chunk.data[0], 1, true); // c_bool
    FlatVector::SetNull(chunk.data[1], 3, true); // c_i32
    FlatVector::SetNull(chunk.data[3], 4, true); // c_f32
    FlatVector::SetNull(chunk.data[6], 2, true); // c_dec
    FlatVector::SetNull(chunk.data[7], 5, true); // c_date
    FlatVector::SetNull(chunk.data[9], 0, true);  // c_hugeint
    FlatVector::SetNull(chunk.data[10], 3, true); // c_i8
    chunk.SetChildCardinality(n);
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
        c.data[0].SetValue(r, Value::INTEGER(int32_t(r)));
    }
    c.SetChildCardinality(4);

    OwnedBlock b = ExportChunk(s, c, alloc).unwrap();
    auto *descs = reinterpret_cast<const ColumnDescriptor *>(b.data + sizeof(BlockHeader));
    CHECK_MSG(descs[0].validity_offset == 0, "non-nullable column should have no validity region");
    alloc.Free(b.data, b.size);
}

namespace {

std::string RenderRow(DataChunk &chunk, idx_t r) {
    std::string s;
    for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
        auto v = chunk.GetValue(c, r);
        s += (v.IsNull() ? std::string("NULL") : v.ToString()) + "|";
    }
    return s;
}

} // namespace

TEST_CASE("split output: flat/dictionary/constant inputs, NULLs and long strings, 1..64 partitions") {
    Allocator alloc;
    Schema schema;
    schema.columns = {{"k", {TypeId::INT64}, false},
                      {"i", {TypeId::INT32}, true},
                      {"s", {TypeId::VARCHAR}, true},
                      {"h", {TypeId::HUGEINT}, true},
                      {"b", {TypeId::BOOLEAN}, true}};
    const duckdb::vector<LogicalType> types = {LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::VARCHAR,
                                               LogicalType::HUGEINT, LogicalType::BOOLEAN};
    auto flat = [&](idx_t rows, int64_t seed) {
        auto chunk = std::make_unique<DataChunk>();
        chunk->Initialize(alloc.Get(), types, rows);
        for (idx_t r = 0; r < rows; r++) {
            const int64_t v = seed + int64_t(r);
            chunk->data[0].SetValue(r, Value::BIGINT(v * 7919 % 1000));
            chunk->data[1].SetValue(r, v % 5 == 0 ? Value(LogicalType::INTEGER) : Value::INTEGER(int32_t(v)));
            chunk->data[2].SetValue(r, v % 7 == 0   ? Value(LogicalType::VARCHAR)
                                       : v % 2 == 0 ? Value("s" + std::to_string(v))
                                                    : Value("a long string that is not inlined " + std::to_string(v)));
            chunk->data[3].SetValue(r, v % 3 == 0 ? Value(LogicalType::HUGEINT) : Value::HUGEINT(hugeint_t(v) * 1000003));
            chunk->data[4].SetValue(r, v % 11 == 0 ? Value(LogicalType::BOOLEAN) : Value::BOOLEAN(v % 2 == 0));
        }
        chunk->SetChildCardinality(rows);
        return chunk;
    };
    // Each generator makes a fresh input chunk (pushing consumes it).
    std::vector<std::function<std::unique_ptr<DataChunk>()>> inputs = {
        [&] { return flat(5000, 0); }, // larger than a vector
        [&] {                          // every column a dictionary (every third row, reversed)
            auto chunk = flat(STANDARD_VECTOR_SIZE, 10000);
            duckdb::SelectionVector sel(STANDARD_VECTOR_SIZE);
            idx_t n = 0;
            for (idx_t r = STANDARD_VECTOR_SIZE; r-- > 0;) {
                if (r % 3 == 0) {
                    sel.set_index(n++, r);
                }
            }
            chunk->Slice(sel, n);
            return chunk;
        },
        [&] { // constant payload columns
            auto chunk = flat(1000, 20000);
            chunk->data[1].Reference(Value::INTEGER(7), duckdb::count_t(1000));
            chunk->data[2].Reference(Value("a constant string too long to be inlined"), duckdb::count_t(1000));
            chunk->data[3].Reference(Value(LogicalType::HUGEINT), duckdb::count_t(1000));
            chunk->data[4].Reference(Value::BOOLEAN(true), duckdb::count_t(1000));
            return chunk;
        },
        [&] { // constant key: a single partition gets the whole chunk
            auto chunk = flat(777, 30000);
            chunk->data[0].Reference(Value::BIGINT(42), duckdb::count_t(777));
            return chunk;
        },
        [&] { return flat(63, 40000); }, // a row count off the validity word size
    };
    std::multiset<std::string> expected;
    for (auto &make : inputs) {
        auto chunk = make();
        for (idx_t r = 0; r < chunk->size(); r++) {
            expected.insert(RenderRow(*chunk, r));
        }
    }

    for (uint32_t partitions : {1u, 7u, 64u}) {
        std::vector<std::pair<size_t, DataBuffer>> out;
        auto sink = std::make_unique<exec::BlockSink>(
            [&](const std::string &, size_t, DataBuffer buf, size_t key) { out.emplace_back(key, std::move(buf)); },
            16u << 10); // small blocks: several flushes per partition
        auto op = exec::MakeOutputOperator(schema, {0}, partitions, std::move(sink));
        for (auto &make : inputs) {
            op->Push(make()).unwrap();
        }
        op->Finish().unwrap();

        std::multiset<std::string> got;
        for (auto &[key, buf] : out) {
            DataChunk chunk;
            Schema parsed;
            ImportBlock(buf.mutable_data(), buf.size(), chunk, parsed).unwrap();
            CHECK(parsed.size() == schema.size());
            for (idx_t r = 0; r < chunk.size(); r++) {
                got.insert(RenderRow(chunk, r));
                if (partitions > 1) {
                    CHECK(key == exec::PartitionOf(chunk.GetValue(0, r).Hash(), partitions));
                }
            }
        }
        CHECK_MSG(got == expected, "split output rows differ from the input at " + std::to_string(partitions) +
                                       " partitions");
    }
}

TEST_CASE("export zeroes every byte outside the written regions") {
    Allocator alloc;
    Schema s;
    s.columns = {{"a", {TypeId::INT8}, true}, {"b", {TypeId::VARCHAR}, true}, {"c", {TypeId::INT32}, true}};
    duckdb::vector<LogicalType> types = {LogicalType::TINYINT, LogicalType::VARCHAR, LogicalType::INTEGER};
    DataChunk c;
    c.Initialize(alloc.Get(), types);
    for (idx_t r = 0; r < 13; r++) {
        c.data[0].SetValue(r, Value::TINYINT(int8_t(r)));
        c.data[1].SetValue(r, Value("string number " + std::to_string(r)));
        c.data[2].SetValue(r, Value::INTEGER(int32_t(r)));
    }
    c.SetChildCardinality(13);
    // Export again right after freeing a junk-filled buffer of the same size, which malloc will
    // likely hand back: the bytes must still match the first export.
    OwnedBlock clean = ExportChunk(s, c, alloc).unwrap();
    std::vector<ChunkSelection> parts = {{&c, nullptr, 13}};
    void *junk = std::malloc(clean.size);
    std::memset(junk, 0xAB, clean.size);
    std::free(junk);
    DataBuffer dirty = ExportSelection(s, parts).unwrap();
    CHECK(dirty.size() == clean.size);
    CHECK_MSG(std::memcmp(dirty.data(), clean.data, clean.size) == 0, "export left stale bytes in the block");
    alloc.Free(clean.data, clean.size);
}

TEST_CASE("BlockWriter: growing, full, heap-limited, oversized-row and partial blocks match the reference export") {
    Allocator alloc;
    for (auto [max_bytes, total] : std::vector<std::pair<size_t, int>> {{8u << 10, 3000}, {256u << 10, 30000}}) {
        Schema schema;
        schema.columns = {{"a", {TypeId::INT8}, true}, {"s", {TypeId::VARCHAR}, true}, {"b", {TypeId::INT64}, false}};
        const duckdb::vector<LogicalType> types = {LogicalType::TINYINT, LogicalType::VARCHAR, LogicalType::BIGINT};
        // Short strings first (the heap estimate starts low), then long ones (the heap fills before the
        // row capacity), one string larger than a whole block, then short ones again.
        std::vector<std::string> strs;
        for (int i = 0; i < total; i++) {
            strs.push_back(i < total / 3 ? "s" + std::to_string(i)
                                    : (i == 2001 ? std::string(20000, 'z') : std::string(i % 2 ? 40 : 3, 'x') + std::to_string(i)));
        }
        memory::BlockWriter writer(schema, max_bytes);
        std::vector<DataBuffer> blocks;
        std::multiset<std::string> expected;
        for (idx_t begin = 0; begin < strs.size(); begin += 700) {
            const idx_t n = std::min<idx_t>(700, strs.size() - begin);
            DataChunk chunk;
            chunk.Initialize(alloc.Get(), types, n);
            for (idx_t r = 0; r < n; r++) {
                const idx_t i = begin + r;
                chunk.data[0].SetValue(r, i % 9 == 0 ? Value(LogicalType::TINYINT) : Value::TINYINT(int8_t(i % 100)));
                chunk.data[1].SetValue(r, i % 13 == 0 ? Value(LogicalType::VARCHAR) : Value(strs[i]));
                chunk.data[2].SetValue(r, Value::BIGINT(int64_t(i)));
            }
            chunk.SetChildCardinality(n);
            // Append every other row through a selection, starting at odd offsets.
            std::vector<duckdb::sel_t> sel;
            for (idx_t r = 1; r < n; r += 2) {
                sel.push_back(duckdb::sel_t(r));
                expected.insert(RenderRow(chunk, r));
            }
            duckdb::UnifiedVectorFormat formats[3];
            for (idx_t c = 0; c < 3; c++) {
                chunk.data[c].ToUnifiedFormat(formats[c]);
            }
            for (idx_t done = 0; done < sel.size();) {
                done += writer.Append(formats, sel.data(), done, sel.size() - done);
                if (done < sel.size()) {
                    blocks.push_back(writer.Finish());
                }
            }
        }
        blocks.push_back(writer.Finish());
        CHECK(blocks.size() > 3);

        std::multiset<std::string> got;
        for (auto &block : blocks) {
            auto *h = reinterpret_cast<const BlockHeader *>(block.data());
            CHECK(h->total_size == block.size());
            CHECK_MSG(block.size() <= max_bytes || h->row_count == 1, "block over the cap with more than one row");
            std::vector<uint8_t> pristine(block.data(), block.data() + block.size());
            DataChunk chunk;
            Schema parsed;
            ImportBlock(block.mutable_data(), block.size(), chunk, parsed).unwrap();
            for (idx_t r = 0; r < chunk.size(); r++) {
                got.insert(RenderRow(chunk, r));
            }
            OwnedBlock again = ExportChunk(parsed, chunk, alloc).unwrap();
            CHECK(again.size == pristine.size());
            CHECK_MSG(again.size == pristine.size() && std::memcmp(again.data, pristine.data(), again.size) == 0,
                      "BlockWriter block differs from the reference export of its rows");
            alloc.Free(again.data, again.size);
        }
        CHECK_MSG(got == expected, "BlockWriter lost or changed rows");
    }
}

int main() {
    return plume_test::RunAll();
}
