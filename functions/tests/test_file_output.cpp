// File output: run plume_stage (passthrough pipeline) with output_sink set to CSV
// and to PARQUET, and verify the emitted host files. CSV is checked as text and
// re-parsed via csv::ParseCSVChunk; parquet is round-tripped through Plume's own
// reader (parquet::ParseFooter + DecodeColumnChunk).

#include "abi_mock.hpp"
#include "test_util.hpp"

#include "plume/csv/csv.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/stage.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/parquet/decoder.hpp"
#include "plume/parquet/encoder.hpp"
#include "plume/parquet/metadata.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"

#include <string>
#include <string_view>
#include <vector>

using namespace plume;
using namespace plume::exec;
using namespace plume::memory;
using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;


namespace {

ColumnType I32() { return {TypeId::INT32}; }
ColumnType STR() { return {TypeId::VARCHAR}; }

// region INT32 (required), name VARCHAR (nullable), amount INT32 (nullable).
Schema MixedSchema() {
    Schema s;
    s.columns = {{"region", I32(), false}, {"name", STR(), true}, {"amount", I32(), true}};
    return s;
}

duckdb::vector<LogicalType> MixedTypes() {
    return {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER};
}

// Serialize a passthrough pipeline (no operators) with the given output sink.
std::vector<uint8_t> PassthroughTemplate(const OutputSink &sink, uint32_t partitions = 1) {
    PipelineTemplate desc;
    desc.input_schema = MixedSchema();
    desc.output_split.key_columns = {0};
    desc.output_split.partitions = partitions;
    desc.output_sink = sink;
    return plume::SerializePipeline(desc);
}

// One input block; `name`/`amount` use the NULL sentinel "" / INT_MIN.
DataBuffer MakeBlock(Allocator &alloc, const std::vector<int32_t> &region, const std::vector<std::string> &name,
                     const std::vector<int32_t> &amount) {
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), MixedTypes());
    for (idx_t r = 0; r < region.size(); r++) {
        chunk.SetValue(0, r, Value::INTEGER(region[r]));
        chunk.SetValue(1, r, name[r].empty() ? Value(LogicalType::VARCHAR) : Value(name[r]));
        chunk.SetValue(2, r, amount[r] == INT32_MIN ? Value(LogicalType::INTEGER) : Value::INTEGER(amount[r]));
    }
    chunk.SetCardinality(region.size());
    auto block = ExportChunk(MixedSchema(), chunk, alloc).unwrap();
    auto buf = mock::MakeBuffer(block.data, block.size);
    alloc.Free(block.data, block.size);
    return buf;
}

void SetStageInputs(const std::vector<uint8_t> &blob, DataBuffer block) {
    mock::Reset();
    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(blob.data(), blob.size()));
        return v;
    }());
    mock::SetInput(1, [&] {
        std::vector<DataBuffer> v;
        v.push_back(std::move(block));
        return v;
    }());
}

} // namespace

TEST_CASE("file output: CSV (header, quoting, nulls) via plume_stage") {
    Allocator alloc;
    OutputSink sink;
    sink.format = OutputFormat::CSV;
    sink.csv.has_header = true;
    sink.csv.delimiter = ',';
    auto blob = PassthroughTemplate(sink);

    // Row 1 has a name needing quoting ("a,b"); row 2 has NULL name + NULL amount.
    SetStageInputs(blob, MakeBlock(alloc, {1, 2}, {"a,b", ""}, {100, INT32_MIN}));
    CHECK(fn::RunStage().is_ok());

    auto outs = mock::OutputsForSet(0);
    CHECK(outs.size() == 1); // one partition -> one file
    std::string text(reinterpret_cast<const char *>(outs[0]->buffer.data()), outs[0]->buffer.size());
    CHECK(text == "region,name,amount\n1,\"a,b\",100\n2,,\n");

    // Re-parse the emitted CSV to round-trip back to rows.
    csv::CSVRegionInfo info;
    info.schema = MixedSchema();
    info.options = sink.csv;
    info.logical_end = outs[0]->buffer.size();
    // ParseCSVChunk skips the first line as a partial record, so it naturally drops
    // the header row here.
    int rows = 0;
    csv::ParseCSVChunk(outs[0]->buffer.data(), outs[0]->buffer.size(), info, alloc,
                       [&](std::unique_ptr<DataChunk> c) -> Result<void> {
                           for (idx_t r = 0; r < c->size(); r++) {
                               rows++;
                           }
                           return Ok();
                       })
        .unwrap();
    CHECK(rows == 2);
}

TEST_CASE("file output: CSV partitions -> one file per partition") {
    Allocator alloc;
    OutputSink sink;
    sink.format = OutputFormat::CSV;
    sink.csv.has_header = false;
    auto blob = PassthroughTemplate(sink, /*partitions=*/3);

    SetStageInputs(blob, MakeBlock(alloc, {1, 2, 3, 4, 5, 6}, {"a", "b", "c", "d", "e", "f"},
                                   {10, 20, 30, 40, 50, 60}));
    CHECK(fn::RunStage().is_ok());

    int total = 0;
    for (auto *out : mock::OutputsForSet(0)) {
        std::string t(reinterpret_cast<const char *>(out->buffer.data()), out->buffer.size());
        for (char c : t) {
            if (c == '\n') {
                total++;
            }
        }
    }
    CHECK(total == 6); // all 6 rows written across the partition files
    CHECK(mock::OutputsForSet(0).size() == 3);
}

namespace {

// Run plume_stage with PARQUET output at `codec`, then round-trip the emitted file
// through Plume's own reader and verify the known rows (incl. preserved nulls).
void RunParquetAndVerify(int32_t codec) {
    Allocator alloc;
    OutputSink sink;
    sink.format = OutputFormat::PARQUET;
    sink.parquet_codec = codec;
    auto blob = PassthroughTemplate(sink);

    SetStageInputs(blob, MakeBlock(alloc, {1, 2, 3}, {"x", "", "zz"}, {10, 20, INT32_MIN}));
    CHECK(fn::RunStage().is_ok());

    auto outs = mock::OutputsForSet(0);
    CHECK(outs.size() == 1);
    const auto &file = outs[0]->buffer;

    auto meta = parquet::ParseFooter(file.data(), file.size()).unwrap();
    CHECK(meta.row_groups.size() == 1);
    const auto &cols = meta.row_groups[0].columns;
    CHECK(cols.size() == 3);
    CHECK(cols[0].codec == codec); // the footer records the codec we asked for

    auto schema = MixedSchema();
    auto col_values = [&](size_t c) {
        return parquet::DecodeColumnChunk(file.data() + cols[c].offset, static_cast<size_t>(cols[c].size),
                                          schema.columns[c].type, schema.columns[c].nullable, cols[c].codec)
            .unwrap();
    };

    auto region = col_values(0);
    auto name = col_values(1);
    auto amount = col_values(2);
    CHECK(region.size() == 3);
    CHECK(region.Get<int32_t>(0) == 1);
    CHECK(region.Get<int32_t>(2) == 3);
    CHECK(name.Str(0) == "x");
    CHECK(name.IsNull(1));          // nullable VARCHAR null preserved
    CHECK(name.Str(2) == "zz");
    CHECK(amount.Get<int32_t>(0) == 10);
    CHECK(amount.IsNull(2));        // nullable INT32 null preserved

    // The streaming ColumnChunkReader must reproduce the same values decoding
    // straight into a DataChunk vector — including the nullable VARCHAR column.
    using duckdb::FlatVector;
    auto reader_col = [&](size_t c, duckdb::Vector &vec) {
        parquet::ColumnChunkReader r(file.data() + cols[c].offset, static_cast<size_t>(cols[c].size),
                                     schema.columns[c].type, schema.columns[c].nullable, cols[c].codec);
        CHECK(r.TotalRows() == 3);
        CHECK(r.ReadInto(vec, 3) == 3);
    };
    duckdb::Vector rvec(LogicalType::INTEGER), nvec(LogicalType::VARCHAR), avec(LogicalType::INTEGER);
    reader_col(0, rvec);
    reader_col(1, nvec);
    reader_col(2, avec);
    CHECK(FlatVector::GetData<int32_t>(rvec)[0] == 1);
    CHECK(FlatVector::GetData<int32_t>(rvec)[2] == 3);
    auto *names = FlatVector::GetData<duckdb::string_t>(nvec);
    CHECK(std::string_view(names[0].GetData(), names[0].GetSize()) == "x");
    CHECK(!FlatVector::Validity(nvec).RowIsValid(1)); // nullable VARCHAR null preserved
    CHECK(std::string_view(names[2].GetData(), names[2].GetSize()) == "zz");
    CHECK(FlatVector::GetData<int32_t>(avec)[0] == 10);
    CHECK(!FlatVector::Validity(avec).RowIsValid(2)); // nullable INT32 null preserved
}

} // namespace

TEST_CASE("file output: uncompressed Parquet round-trips through Plume's reader") {
    RunParquetAndVerify(parquet::WRITE_UNCOMPRESSED);
}

TEST_CASE("file output: Snappy Parquet round-trips through Plume's reader") {
    RunParquetAndVerify(parquet::WRITE_SNAPPY);
}

int main() { return plume_test::RunAll(); }
