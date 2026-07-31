// Parquet decoder + plume_pq_stage. Column chunks (PLAIN, dictionary, nullable
// with definition levels, Snappy-compressed, and V2 pages) are hand-encoded so
// the expected values are known, then decoded directly and through pq_stage.

#include "abi_mock.hpp"
#include "parquet_encode.hpp"
#include "test_util.hpp"

#include "plume/execution/pipeline.hpp"
#include "plume/functions/parquet.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/parquet/decoder.hpp"
#include "plume/parquet/parquet.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

using namespace plume;
using namespace plume::exec;
using namespace pqtest;
using duckdb::idx_t;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

// Differential check: the streaming ColumnChunkReader (reading `step` rows at a
// time, so page/chunk boundaries fall at every offset as `step` varies) must
// reproduce exactly what the whole-column DecodeColumnChunk reference produces.
void CheckReaderMatches(const std::vector<uint8_t> &chunk, ColumnType type, bool nullable, int codec, size_t step) {
    using duckdb::FlatVector;
    CHECK(step <= STANDARD_VECTOR_SIZE);

    auto ref = parquet::DecodeColumnChunk(chunk.data(), chunk.size(), type, nullable, codec).unwrap();
    parquet::ColumnChunkReader r(chunk.data(), chunk.size(), type, nullable, codec);
    CHECK(r.TotalRows() == ref.size());

    const size_t stride = parquet::ColumnPhysicalWidth(type);
    size_t got = 0;
    while (!r.Done()) {
        const size_t n = std::min(step, r.RowsRemaining());
        duckdb::Vector vec(ToLogicalType(type));
        const size_t produced = r.ReadInto(vec, n);
        CHECK(produced == n);
        // ReadInto may emit a DICTIONARY_VECTOR (dictionary fast path); flatten to
        // materialize, mirroring what ExportChunks does before serialization.
        vec.Flatten(produced);
        auto &validity = FlatVector::Validity(vec);
        for (size_t i = 0; i < produced; i++) {
            const bool ref_null = ref.IsNull(got + i);
            CHECK(validity.RowIsValid(i) == !ref_null);
            if (ref_null) {
                continue;
            }
            if (type.id == TypeId::VARCHAR) {
                auto *d = FlatVector::GetData<duckdb::string_t>(vec);
                CHECK(std::string_view(d[i].GetData(), d[i].GetSize()) == ref.Str(got + i));
            } else {
                CHECK(std::memcmp(FlatVector::GetData(vec) + i * stride, ref.raw.data() + (got + i) * stride,
                                  stride) == 0);
            }
        }
        got += produced;
    }
    CHECK(got == ref.size());
}

} // namespace

TEST_CASE("decoder: PLAIN required int32") {
    auto chunk = PlainInt32Chunk({10, 20, 30, 40});
    auto out = parquet::DecodeColumnChunk(chunk.data(), chunk.size(), I32(), /*nullable=*/false).unwrap();
    CHECK(out.size() == 4);
    CHECK(out.Get<int32_t>(0) == 10);
    CHECK(out.Get<int32_t>(3) == 40);
}

TEST_CASE("decoder: dictionary-encoded int32") {
    auto chunk = DictInt32Chunk({10, 20, 30}, {0, 2, 1, 0}, /*bit_width=*/2);
    auto out = parquet::DecodeColumnChunk(chunk.data(), chunk.size(), I32(), /*nullable=*/false).unwrap();
    CHECK(out.size() == 4);
    CHECK(out.Get<int32_t>(0) == 10);
    CHECK(out.Get<int32_t>(1) == 30);
    CHECK(out.Get<int32_t>(2) == 20);
    CHECK(out.Get<int32_t>(3) == 10);
}

TEST_CASE("decoder: nullable int32 with definition levels") {
    auto chunk = NullableInt32Chunk({1, 0, 1, 1}, {5, 7, 9});
    auto out = parquet::DecodeColumnChunk(chunk.data(), chunk.size(), I32(), /*nullable=*/true).unwrap();
    CHECK(out.size() == 4);
    CHECK(out.Get<int32_t>(0) == 5);
    CHECK(out.IsNull(1));
    CHECK(out.Get<int32_t>(2) == 7);
    CHECK(out.Get<int32_t>(3) == 9);
}

TEST_CASE("decoder: Snappy-compressed PLAIN int32 (V1)") {
    auto chunk = PlainInt32Chunk({11, 22, 33, 44, 55}, CODEC_SNAPPY);
    auto out = parquet::DecodeColumnChunk(chunk.data(), chunk.size(), I32(), /*nullable=*/false, CODEC_SNAPPY).unwrap();
    CHECK(out.size() == 5);
    CHECK(out.Get<int32_t>(0) == 11);
    CHECK(out.Get<int32_t>(4) == 55);
}

TEST_CASE("decoder: V2 data page, nullable, Snappy values") {
    auto chunk = NullableInt32ChunkV2({1, 1, 0, 1}, {3, 6, 9}, CODEC_SNAPPY);
    auto out = parquet::DecodeColumnChunk(chunk.data(), chunk.size(), I32(), /*nullable=*/true, CODEC_SNAPPY).unwrap();
    CHECK(out.size() == 4);
    CHECK(out.Get<int32_t>(0) == 3);
    CHECK(out.Get<int32_t>(1) == 6);
    CHECK(out.IsNull(2));
    CHECK(out.Get<int32_t>(3) == 9);
}

TEST_CASE("decoder: int8 / int16 / date (parquet INT32 physical)") {
    auto i8 = PlainInt32Chunk({-5, 0, 127});
    auto o8 = parquet::DecodeColumnChunk(i8.data(), i8.size(), {TypeId::INT8}, false).unwrap();
    CHECK(o8.size() == 3);
    CHECK(o8.Get<int8_t>(0) == -5);
    CHECK(o8.Get<int8_t>(2) == 127);

    auto i16 = PlainInt32Chunk({-1000, 12345});
    auto o16 = parquet::DecodeColumnChunk(i16.data(), i16.size(), {TypeId::INT16}, false).unwrap();
    CHECK(o16.Get<int16_t>(0) == -1000);
    CHECK(o16.Get<int16_t>(1) == 12345);

    // DATE is stored as int32_t days-since-epoch in DuckDB physical layout.
    auto d = PlainInt32Chunk({19000, 19500});
    auto od = parquet::DecodeColumnChunk(d.data(), d.size(), {TypeId::DATE}, false).unwrap();
    CHECK(od.Get<int32_t>(0) == 19000);
    CHECK(od.Get<int32_t>(1) == 19500);
}

TEST_CASE("decoder: time (INT64) + decimal (INT32 and INT64 backed)") {
    // TIME is stored as int64_t microseconds in DuckDB physical layout.
    auto t = PlainInt64Chunk({3600000000LL, 0});
    auto ot = parquet::DecodeColumnChunk(t.data(), t.size(), {TypeId::TIME}, false).unwrap();
    CHECK(ot.Get<int64_t>(0) == 3600000000LL);

    // DECIMAL(5,2) -> parquet INT32 -> DuckDB int32_t. Unscaled 123 == 1.23.
    auto d32 = PlainInt32Chunk({123, -45, 6789});
    auto od32 = parquet::DecodeColumnChunk(d32.data(), d32.size(), {TypeId::DECIMAL, 5, 2}, false).unwrap();
    CHECK(od32.Get<int32_t>(0) == 123);
    CHECK(od32.Get<int32_t>(2) == 6789);

    // DECIMAL(12,3) -> parquet INT64 -> DuckDB int64_t.
    auto d64 = PlainInt64Chunk({123456789LL});
    auto od64 = parquet::DecodeColumnChunk(d64.data(), d64.size(), {TypeId::DECIMAL, 12, 3}, false).unwrap();
    CHECK(od64.Get<int64_t>(0) == 123456789LL);
}

TEST_CASE("plume_pq_stage: decode a region + run pipeline") {
    mock::Reset();

    auto id_chunk = PlainInt32Chunk({1, 2, 3, 4});
    auto amt_chunk = DictInt32Chunk({100, 200, 300}, {0, 1, 2, 0}, 2); // 100,200,300,100

    parquet::Region region;
    parquet::DataChunk dc0;
    dc0.row_group_idx = 0;
    dc0.column_idx = 0;
    dc0.size = int64_t(id_chunk.size());
    dc0.req_idx = 0;
    dc0.req_offset = 0;
    parquet::DataChunk dc1;
    dc1.row_group_idx = 0;
    dc1.column_idx = 1;
    dc1.size = int64_t(amt_chunk.size());
    dc1.req_idx = 1;
    dc1.req_offset = 0;
    region.row_groups.push_back({dc0, dc1});
    region.requests.push_back({0, dc0.size});
    region.requests.push_back({0, dc1.size});

    PipelineTemplate desc;
    desc.input_schema.columns = {{"id", I32(), false}, {"amount", I32(), false}};
    auto blob = plume::SerializePipeline(desc);

    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(blob.data(), blob.size()));
        return v;
    }());
    mock::SetInput(1, [&] {
        std::vector<DataBuffer> v;
        v.push_back(region.Serialize());
        return v;
    }());
    // Data buffers tagged with (key=region, ident=request index) so the stage can
    // match each chunk to its request.
    mock::AddInput(2, mock::MakeBuffer(id_chunk.data(), id_chunk.size()), "0", 0);
    mock::AddInput(2, mock::MakeBuffer(amt_chunk.data(), amt_chunk.size()), "1", 0);

    CHECK(fn::RunParquetStage().is_ok());

    std::vector<std::pair<int32_t, int32_t>> rows;
    for (auto *out : mock::OutputsForSet(0)) {
        Schema schema;
        auto chunks = memory::ImportBlockChunks(const_cast<uint8_t *>(out->buffer.data()), out->buffer.size(), schema).unwrap();
        for (auto &c : chunks) {
            for (idx_t r = 0; r < c->size(); r++) {
                rows.emplace_back(c->GetValue(0, r).GetValue<int32_t>(), c->GetValue(1, r).GetValue<int32_t>());
            }
        }
    }
    CHECK(rows.size() == 4);
    CHECK(rows[0] == std::make_pair(1, 100));
    CHECK(rows[1] == std::make_pair(2, 200));
    CHECK(rows[2] == std::make_pair(3, 300));
    CHECK(rows[3] == std::make_pair(4, 100));
}

TEST_CASE("ColumnChunkReader matches DecodeColumnChunk (fixed-width, boundaries)") {
    // Steps chosen so page/chunk boundaries land at every alignment: 1 (per-row),
    // 700 (arbitrary), and STANDARD_VECTOR_SIZE (the production chunk size).
    const std::vector<size_t> steps = {1, 700, STANDARD_VECTOR_SIZE};

    // Large single PLAIN page (>2048 rows): exercises intra-page chunk boundaries.
    std::vector<int32_t> big;
    for (int32_t i = 0; i < 5000; i++) {
        big.push_back(i * 3 - 100);
    }
    auto big_plain = PlainInt32Chunk(big);

    // Multi-page PLAIN column (two 1500-row pages): a 2048 chunk spans the page
    // boundary, and a 700 step lands mid-page on both.
    auto multi_page = Concat(PlainInt32Chunk({big.begin(), big.begin() + 1500}),
                             PlainInt32Chunk({big.begin() + 1500, big.begin() + 3000}));

    // Large nullable column (every 3rd row null), plus small hand-built cases.
    std::vector<uint32_t> def;
    std::vector<int32_t> nn;
    for (int32_t i = 0; i < 3000; i++) {
        if (i % 3 == 0) {
            def.push_back(0);
        } else {
            def.push_back(1);
            nn.push_back(i);
        }
    }
    auto big_nullable = NullableInt32Chunk(def, nn);

    auto dict = DictInt32Chunk({10, 20, 30}, {0, 2, 1, 0}, 2);
    auto small_nullable = NullableInt32Chunk({1, 0, 1, 1}, {5, 7, 9});
    auto snappy = PlainInt32Chunk(big, CODEC_SNAPPY);
    auto v2 = NullableInt32ChunkV2({1, 1, 0, 1}, {3, 6, 9}, CODEC_SNAPPY);
    auto i64 = PlainInt64Chunk({1, -2, 3000000000LL, 0, -9});

    for (size_t step : steps) {
        CheckReaderMatches(big_plain, I32(), false, CODEC_UNCOMPRESSED, step);
        CheckReaderMatches(multi_page, I32(), false, CODEC_UNCOMPRESSED, step);
        CheckReaderMatches(big_nullable, I32(), true, CODEC_UNCOMPRESSED, step);
        CheckReaderMatches(dict, I32(), false, CODEC_UNCOMPRESSED, step);
        CheckReaderMatches(small_nullable, I32(), true, CODEC_UNCOMPRESSED, step);
        CheckReaderMatches(snappy, I32(), false, CODEC_SNAPPY, step);
        CheckReaderMatches(v2, I32(), true, CODEC_SNAPPY, step);
        CheckReaderMatches(i64, {TypeId::INT64}, false, CODEC_UNCOMPRESSED, step);
    }
}

TEST_CASE("decoder: Skip advances the cursor without materializing") {
    using duckdb::FlatVector;
    // Two PLAIN int32 data pages (0..9, 10..19) → a 20-row, 2-page column chunk.
    std::vector<int32_t> p0, p1;
    for (int32_t i = 0; i < 10; i++) {
        p0.push_back(i);
        p1.push_back(10 + i);
    }
    auto chunk = Concat(PlainInt32Chunk(p0), PlainInt32Chunk(p1));
    const auto t = I32();
    const auto lt = ToLogicalType(t);

    auto read_from = [&](parquet::ColumnChunkReader &r, size_t n, int32_t first) {
        duckdb::Vector vec(lt);
        const size_t got = r.ReadInto(vec, n);
        CHECK(got == n);
        vec.Flatten(got);
        auto *d = FlatVector::GetData<int32_t>(vec);
        for (size_t i = 0; i < n; i++) {
            CHECK(d[i] == first + static_cast<int32_t>(i));
        }
    };

    // Skip within the first page, then read the rest.
    {
        parquet::ColumnChunkReader r(chunk.data(), chunk.size(), t, false);
        CHECK(r.TotalRows() == 20);
        r.Skip(3);
        CHECK(r.RowsRemaining() == 17);
        read_from(r, 17, 3);
    }
    // Skip across the page boundary (all of page 0 + 3 of page 1) by header only.
    {
        parquet::ColumnChunkReader r(chunk.data(), chunk.size(), t, false);
        r.Skip(13);
        CHECK(r.RowsRemaining() == 7);
        read_from(r, 7, 13);
    }
    // Interleave read / skip / read, mirroring pq_stage's per-window alternation.
    {
        parquet::ColumnChunkReader r(chunk.data(), chunk.size(), t, false);
        read_from(r, 4, 0); // 0..3
        r.Skip(4);          // 4..7
        read_from(r, 12, 8); // 8..19
        CHECK(r.Done());
    }
}

TEST_CASE("decoder: selective ReadInto materializes only kept rows") {
    using duckdb::FlatVector;
    std::vector<int32_t> vals;
    for (int32_t i = 0; i < 10; i++) {
        vals.push_back(i * 5);
    }
    auto chunk = PlainInt32Chunk(vals);
    const auto t = I32();

    // Keep the even positions; the odd ones must be dropped after Slice.
    const std::vector<uint8_t> keep = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    duckdb::SelectionVector sel(10);
    size_t sc = 0;
    for (size_t i = 0; i < keep.size(); i++) {
        if (keep[i]) {
            sel.set_index(sc++, i);
        }
    }

    parquet::ColumnChunkReader r(chunk.data(), chunk.size(), t, false);
    duckdb::Vector vec(ToLogicalType(t));
    r.ReadInto(vec, 10, keep.data());
    vec.Slice(sel, sc);
    vec.Flatten(sc);
    auto *d = FlatVector::GetData<int32_t>(vec);
    CHECK(sc == 5);
    for (size_t i = 0; i < sc; i++) {
        CHECK(d[i] == static_cast<int32_t>(i * 2 * 5)); // 0,10,20,30,40
    }
}

int main() { return plume_test::RunAll(); }
