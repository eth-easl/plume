// Parquet decoder + plume_pq_stage. Column chunks (PLAIN, dictionary, nullable
// with definition levels, Snappy-compressed, and V2 pages) are hand-encoded so
// the expected values are known, then decoded directly and through pq_stage.

#include "abi_mock.hpp"
#include "parquet_encode.hpp"
#include "test_util.hpp"

#include "plume/common/serial.hpp"
#include "plume/execution/bloom_filter.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/parquet.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/parquet/decoder.hpp"
#include "plume/parquet/parquet.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
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

TEST_CASE("plume_pq_stage: emits dynamic filter bounds when the pipeline builds one") {
    mock::Reset();

    auto amt_chunk = PlainInt32Chunk({70, -3, 12});
    parquet::Region region;
    parquet::DataChunk dc;
    dc.row_group_idx = 0;
    dc.column_idx = 0;
    dc.size = int64_t(amt_chunk.size());
    dc.req_idx = 0;
    dc.req_offset = 0;
    region.row_groups.push_back({dc});
    region.requests.push_back({0, dc.size});

    PipelineTemplate desc;
    desc.input_schema.columns = {{"amount", I32(), false}};
    desc.operators = {std::make_shared<DynamicFilterBuildTemplate>(/*column=*/0, I32())};
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
    mock::AddInput(2, mock::MakeBuffer(amt_chunk.data(), amt_chunk.size()), "0", 0);

    CHECK(fn::RunParquetStage().is_ok());

    auto dyn_outs = mock::OutputsForSet(1);
    CHECK(dyn_outs.size() == 1);
    auto bounds =
        DeserializeFromBytes<DynamicFilter>(dyn_outs[0]->buffer.data(), dyn_outs[0]->buffer.size());
    CHECK(bounds.has_bounds);
    CHECK(bounds.min == duckdb::Value::INTEGER(-3));
    CHECK(bounds.max == duckdb::Value::INTEGER(70));
}

TEST_CASE("plume_pq_stage: emits invalid dynamic filter bounds without any region") {
    mock::Reset();

    PipelineTemplate desc;
    desc.input_schema.columns = {{"amount", I32(), false}};
    desc.operators = {std::make_shared<DynamicFilterBuildTemplate>(/*column=*/0, I32())};
    auto blob = plume::SerializePipeline(desc);

    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(blob.data(), blob.size()));
        return v;
    }());
    mock::SetInput(1, std::vector<DataBuffer>{});
    mock::SetInput(2, std::vector<DataBuffer>{});

    CHECK(fn::RunParquetStage().is_ok());

    auto dyn_outs = mock::OutputsForSet(1);
    CHECK(dyn_outs.size() == 1);
    auto bounds =
        DeserializeFromBytes<DynamicFilter>(dyn_outs[0]->buffer.data(), dyn_outs[0]->buffer.size());
    CHECK(!bounds.has_bounds);
}

namespace {

// Runs pq_stage over one row group of (id, amount) INT32 columns, with the given pipeline
// operators and the dynamic filter attached to the region on column `dyn_col`; returns the rows.
// With `build_filters`, the region only names the column and the filters arrive per build
// instance on input set 3 instead (no pq_prepare in between).
// Runs pq_stage over one row group of the (id, amount) INT32 column chunks.
std::vector<std::pair<int32_t, int32_t>> RunChunksStage(const std::vector<uint8_t> &id_chunk,
        const std::vector<uint8_t> &amt_chunk, std::vector<std::shared_ptr<OperatorTemplate>> operators,
        const DynamicFilter *dyn_filter, int32_t dyn_col = 0,
        const std::optional<std::vector<DynamicFilter>> &build_filters = std::nullopt) {
    mock::Reset();

    parquet::Region region;
    parquet::DataChunk dc0;
    dc0.row_group_idx = 0;
    dc0.column_idx = 0;
    dc0.size = int64_t(id_chunk.size());
    dc0.req_idx = 0;
    dc0.req_offset = 0;
    parquet::DataChunk dc1 = dc0;
    dc1.column_idx = 1;
    dc1.size = int64_t(amt_chunk.size());
    dc1.req_idx = 1;
    region.row_groups.push_back({dc0, dc1});
    region.requests.push_back({0, dc0.size});
    region.requests.push_back({0, dc1.size});
    if (dyn_filter) {
        auto buf = SerializeToBuffer(*dyn_filter);
        region.dynamic_filter_column = dyn_col;
        region.dynamic_filter.assign(buf.data(), buf.data() + buf.size());
    }
    if (build_filters) {
        region.dynamic_filter_column = dyn_col;
    }

    PipelineTemplate desc;
    desc.input_schema.columns = {{"id", I32(), false}, {"amount", I32(), false}};
    desc.operators = std::move(operators);
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
    mock::AddInput(2, mock::MakeBuffer(id_chunk.data(), id_chunk.size()), "0", 0);
    mock::AddInput(2, mock::MakeBuffer(amt_chunk.data(), amt_chunk.size()), "1", 0);
    if (build_filters) {
        for (const auto &f : *build_filters) {
            mock::AddInput(3, SerializeToBuffer(f), "dynamic_filter", 0);
        }
    }
    CHECK(fn::RunParquetStage().is_ok());

    std::vector<std::pair<int32_t, int32_t>> rows;
    for (auto *out : mock::OutputsForSet(0)) {
        Schema schema;
        auto chunks =
            memory::ImportBlockChunks(const_cast<uint8_t *>(out->buffer.data()), out->buffer.size(), schema).unwrap();
        for (auto &c : chunks) {
            for (idx_t r = 0; r < c->size(); r++) {
                rows.emplace_back(c->GetValue(0, r).GetValue<int32_t>(), c->GetValue(1, r).GetValue<int32_t>());
            }
        }
    }
    return rows;
}

std::vector<std::pair<int32_t, int32_t>> RunIdAmountStage(const std::vector<int32_t> &ids,
        const std::vector<int32_t> &amounts, std::vector<std::shared_ptr<OperatorTemplate>> operators,
        const DynamicFilter *dyn_filter, int32_t dyn_col = 0,
        const std::optional<std::vector<DynamicFilter>> &build_filters = std::nullopt) {
    return RunChunksStage(PlainInt32Chunk(ids), PlainInt32Chunk(amounts), std::move(operators), dyn_filter, dyn_col,
                          build_filters);
}

DynamicFilter ExactInts(std::vector<int32_t> keys) {
    DynamicFilter f;
    f.max_values = 16;
    f.has_values = true;
    for (int32_t k : keys) {
        f.MergeBounds(duckdb::Value::INTEGER(k), duckdb::Value::INTEGER(k));
        f.values.push_back(duckdb::Value::INTEGER(k));
    }
    return f;
}

using Rows = std::vector<std::pair<int32_t, int32_t>>;

} // namespace

TEST_CASE("plume_pq_stage: dynamic filter drops probe rows without a join partner") {
    const std::vector<int32_t> ids = {1, 2, 3, 4, 5, 6};
    const std::vector<int32_t> amounts = {10, 20, 30, 40, 50, 60};

    auto exact = ExactInts({2, 5, 6});
    CHECK(RunIdAmountStage(ids, amounts, {}, &exact) == Rows({{2, 20}, {5, 50}, {6, 60}}));

    // Key column is not the first column.
    auto by_amount = ExactInts({30});
    CHECK(RunIdAmountStage(ids, amounts, {}, &by_amount, 1) == Rows({{3, 30}}));

    // Bloom filter.
    DynamicFilter bloom;
    bloom.MergeBounds(duckdb::Value::INTEGER(1), duckdb::Value::INTEGER(4));
    bloom.bloom = BloomFilter(1024);
    bloom.bloom.Insert(duckdb::Hash<int32_t>(1));
    bloom.bloom.Insert(duckdb::Hash<int32_t>(4));
    auto rows = RunIdAmountStage(ids, amounts, {}, &bloom);
    CHECK(std::find(rows.begin(), rows.end(), std::make_pair(1, 10)) != rows.end());
    CHECK(std::find(rows.begin(), rows.end(), std::make_pair(4, 40)) != rows.end());
    for (auto &r : rows) {
        CHECK(r.first >= 1 && r.first <= 4); // the key range always applies
    }

    // No row survives.
    auto none = ExactInts({100});
    CHECK(RunIdAmountStage(ids, amounts, {}, &none).empty());

    // No dynamic filter -> everything.
    CHECK(RunIdAmountStage(ids, amounts, {}, nullptr).size() == ids.size());
}

TEST_CASE("ColumnChunkReader: dictionary of a fully dictionary-encoded chunk") {
    auto chunk = DictInt32Chunk({10, 20, 30}, {0, 2, 1, 0}, 2);
    parquet::ColumnChunkReader r(chunk.data(), chunk.size(), I32(), /*nullable=*/false);
    const auto *dict = r.Dictionary(duckdb::LogicalType::INTEGER);
    CHECK(dict != nullptr);
    CHECK(r.DictionaryVectorSize() == 4); // + the NULL sentinel
    CHECK(duckdb::FlatVector::GetData<int32_t>(*dict)[1] == 20);

    // Reads reference the same dictionary.
    duckdb::Vector vec(duckdb::LogicalType::INTEGER);
    CHECK(r.ReadInto(vec, 4) == 4);
    CHECK(vec.GetVectorType() == duckdb::VectorType::DICTIONARY_VECTOR);
    CHECK(duckdb::FlatVector::GetData<int32_t>(duckdb::DictionaryVector::Child(vec)) ==
          duckdb::FlatVector::GetData<int32_t>(*dict));
    CHECK(vec.GetValue(1) == duckdb::Value::INTEGER(30));

    // A PLAIN chunk has none.
    auto plain = PlainInt32Chunk({1, 2});
    parquet::ColumnChunkReader p(plain.data(), plain.size(), I32(), false);
    CHECK(p.Dictionary(duckdb::LogicalType::INTEGER) == nullptr);
}

TEST_CASE("plume_pq_stage: dynamic filter checks a dictionary-encoded key column's dictionary") {
    // ids 10, 30, 20, 10, 20
    auto ids = DictInt32Chunk({10, 20, 30}, {0, 2, 1, 0, 1}, 2);
    auto amounts = PlainInt32Chunk({1, 2, 3, 4, 5});
    auto twenty = ExactInts({20});
    CHECK(RunChunksStage(ids, amounts, {}, &twenty) == Rows({{20, 3}, {20, 5}}));

    // No dictionary entry passes: the row group is skipped before decoding anything else. The scan
    // filter would decode amount first otherwise, whose unsupported page encoding fails the stage.
    auto bad_amounts = Concat(DataPageV1Header(5, /*encoding=*/99, 20, 20), std::vector<uint8_t>(20, 0));
    auto scan_filter = std::make_shared<FilterTemplate>();
    scan_filter->filter = expr::ExprNode::Comparison(duckdb::ExpressionType::COMPARE_GREATERTHAN,
                                                     expr::ExprNode::Reference(1, I32()),
                                                     expr::ExprNode::Constant(duckdb::Value::INTEGER(0), I32()));
    auto fifteen = ExactInts({15});
    CHECK(RunChunksStage(ids, bad_amounts, {scan_filter}, &fifteen).empty());
}

namespace {

// A multi-page INT32 column chunk; `offsets` gets each page's offset in it.
std::vector<uint8_t> PagedInt32Chunk(const std::vector<std::vector<int32_t>> &pages, std::vector<int64_t> &offsets) {
    std::vector<uint8_t> chunk;
    for (const auto &p : pages) {
        offsets.push_back(int64_t(chunk.size()));
        auto page = PlainInt32Chunk(p);
        chunk.insert(chunk.end(), page.begin(), page.end());
    }
    offsets.push_back(int64_t(chunk.size()));
    return chunk;
}

// Runs pq_stage over one row group of which only the given page runs of the (id, amount) chunks
// are read; runs are {first page, last page} of the column's pages.
Rows RunPagedStage(const std::vector<std::vector<int32_t>> &id_pages, const std::vector<std::pair<int, int>> &id_runs,
                   const std::vector<std::vector<int32_t>> &amt_pages, const std::vector<std::pair<int, int>> &amt_runs,
                   const DynamicFilter *dyn_filter = nullptr) {
    mock::Reset();
    std::vector<int64_t> id_offsets, amt_offsets;
    auto id_chunk = PagedInt32Chunk(id_pages, id_offsets);
    auto amt_chunk = PagedInt32Chunk(amt_pages, amt_offsets);

    auto column = [](uint64_t col, uint64_t req, const std::vector<std::vector<int32_t>> &pages,
                     const std::vector<int64_t> &offsets, const std::vector<std::pair<int, int>> &runs) {
        parquet::DataChunk dc;
        dc.row_group_idx = 0;
        dc.column_idx = col;
        dc.offset = 0;
        dc.size = 0; // no dictionary page
        dc.req_idx = req;
        dc.req_offset = 0;
        uint64_t first_row = 0;
        std::vector<uint64_t> page_rows;
        for (const auto &p : pages) {
            page_rows.push_back(first_row);
            first_row += p.size();
        }
        page_rows.push_back(first_row);
        for (auto [first, last] : runs) {
            parquet::PageRun run;
            run.offset = offsets[first];
            run.size = offsets[last + 1] - offsets[first];
            run.first_row = page_rows[first];
            run.num_rows = page_rows[last + 1] - page_rows[first];
            run.req_idx = req;
            run.req_offset = uint64_t(offsets[first]);
            dc.pages.push_back(run);
        }
        return dc;
    };
    parquet::Region region;
    region.row_groups.push_back({column(0, 0, id_pages, id_offsets, id_runs),
                                 column(1, 1, amt_pages, amt_offsets, amt_runs)});
    region.requests.push_back({0, int64_t(id_chunk.size())});
    region.requests.push_back({0, int64_t(amt_chunk.size())});
    if (dyn_filter) {
        auto buf = SerializeToBuffer(*dyn_filter);
        region.dynamic_filter_column = 0;
        region.dynamic_filter.assign(buf.data(), buf.data() + buf.size());
    }

    PipelineTemplate desc;
    desc.input_schema.columns = {{"id", I32(), false}, {"amount", I32(), false}};
    auto blob = plume::SerializePipeline(desc);
    mock::AddInput(0, mock::MakeBuffer(blob.data(), blob.size()), "template", 0);
    mock::AddInput(1, region.Serialize(), "region", 0);
    mock::AddInput(2, mock::MakeBuffer(id_chunk.data(), id_chunk.size()), "0", 0);
    mock::AddInput(2, mock::MakeBuffer(amt_chunk.data(), amt_chunk.size()), "1", 0);
    CHECK(fn::RunParquetStage().is_ok());

    Rows rows;
    for (auto *out : mock::OutputsForSet(0)) {
        Schema schema;
        auto chunks =
            memory::ImportBlockChunks(const_cast<uint8_t *>(out->buffer.data()), out->buffer.size(), schema).unwrap();
        for (auto &c : chunks) {
            for (idx_t r = 0; r < c->size(); r++) {
                rows.emplace_back(c->GetValue(0, r).GetValue<int32_t>(), c->GetValue(1, r).GetValue<int32_t>());
            }
        }
    }
    return rows;
}

const std::vector<std::vector<int32_t>> kIdPages = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
const std::vector<std::vector<int32_t>> kAmountPages = {{10, 20}, {30, 40, 50, 60}, {70, 80, 90}};

} // namespace

TEST_CASE("plume_pq_stage: reads only the rows every column's selected pages hold") {
    // id page 1 holds rows [3, 6); amount page 1 rows [2, 6).
    CHECK(RunPagedStage(kIdPages, {{1, 1}}, kAmountPages, {{1, 1}}) == Rows({{4, 40}, {5, 50}, {6, 60}}));
    // id pages 0 and 2: rows [0, 3) and [6, 9) of the amount chunk read whole.
    CHECK(RunPagedStage(kIdPages, {{0, 0}, {2, 2}}, kAmountPages, {{0, 2}}) ==
          Rows({{1, 10}, {2, 20}, {3, 30}, {7, 70}, {8, 80}, {9, 90}}));
    // Same, with amount runs [0, 2) + [2, 6) and [6, 9) apart.
    CHECK(RunPagedStage(kIdPages, {{0, 0}, {2, 2}}, kAmountPages, {{0, 1}, {2, 2}}) ==
          Rows({{1, 10}, {2, 20}, {3, 30}, {7, 70}, {8, 80}, {9, 90}}));
    // Together with the dynamic filter.
    auto eight = ExactInts({8});
    CHECK(RunPagedStage(kIdPages, {{0, 0}, {2, 2}}, kAmountPages, {{0, 2}}, &eight) == Rows({{8, 80}}));
}

TEST_CASE("plume_pq_stage: dynamic filter combines with the pushed-down scan filter") {
    const std::vector<int32_t> ids = {1, 2, 3, 4, 5, 6};
    const std::vector<int32_t> amounts = {10, 20, 30, 40, 50, 60};
    // amount > 25, and id in {2, 3, 5}
    auto scan_filter = std::make_shared<FilterTemplate>();
    scan_filter->filter = expr::ExprNode::Comparison(duckdb::ExpressionType::COMPARE_GREATERTHAN,
                                                     expr::ExprNode::Reference(1, I32()),
                                                     expr::ExprNode::Constant(duckdb::Value::INTEGER(25), I32()));
    auto exact = ExactInts({2, 3, 5});
    CHECK(RunIdAmountStage(ids, amounts, {scan_filter}, &exact) == Rows({{3, 30}, {5, 50}}));
}

TEST_CASE("plume_pq_stage: merges the build instances' dynamic filters from input set 3") {
    const std::vector<int32_t> ids = {1, 2, 3, 4, 5, 6};
    const std::vector<int32_t> amounts = {10, 20, 30, 40, 50, 60};

    CHECK(RunIdAmountStage(ids, amounts, {}, nullptr, 0, std::vector<DynamicFilter>{ExactInts({2}), ExactInts({5, 6})}) ==
          Rows({{2, 20}, {5, 50}, {6, 60}}));

    // Every build instance empty -> the inner join can't produce anything: no rows at all.
    DynamicFilter empty;
    empty.has_values = true;
    CHECK(RunIdAmountStage(ids, amounts, {}, nullptr, 0, std::vector<DynamicFilter>{empty, empty}).empty());

    // The region names a column but no filter arrives -> everything.
    CHECK(RunIdAmountStage(ids, amounts, {}, nullptr, 0, std::vector<DynamicFilter>{}).size() == ids.size());
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
