// End-to-end parquet: a hand-built file (one uncompressed column, one Snappy
// column) flows through plume_pq_prepare (footer -> regions/requests), the test
// resolves each request by slicing the file (mimicking the runtime), then
// plume_pq_stage decodes the region and runs a pipeline. Values are known.

#include "abi_mock.hpp"
#include "parquet_encode.hpp"
#include "test_util.hpp"

#include "plume/common/serial.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/parquet.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/parquet/parquet.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <cstdint>
#include <vector>

using namespace plume;
using namespace plume::exec;
using namespace pqtest;
using duckdb::idx_t;

namespace {
ColumnType I32() { return {TypeId::INT32}; }
} // namespace

TEST_CASE("parquet e2e: prepare -> stage over a hand-built file") {
    // Two columns in one row group: id (uncompressed) and val (Snappy).
    auto file = BuildParquetFile({
        {PlainInt32Chunk({1, 2, 3, 4, 5}), CODEC_UNCOMPRESSED},
        {PlainInt32Chunk({10, 20, 30, 40, 50}, CODEC_SNAPPY), CODEC_SNAPPY},
    });

    // --- plume_pq_prepare: footer -> regions -------------------------------------
    mock::Reset();
    parquet::ParquetConfig cfg;
    cfg.num_splits = 1;
    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(SerializeToBuffer(cfg));
        return v;
    }());
    mock::SetInput(1, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(file.data(), file.size())); // whole file includes the footer
        return v;
    }());
    mock::SetInput(2, [&] {
        std::vector<DataBuffer> v;
        const char *url = "s3://b/f.parquet";
        v.push_back(mock::MakeBuffer(url, std::char_traits<char>::length(url)));
        return v;
    }());
    CHECK(fn::RunParquetPrepare().is_ok());

    auto region_outs = mock::OutputsForSet(0);
    auto req_outs = mock::OutputsForSet(1);
    CHECK(region_outs.size() == 1);
    const size_t region_key = region_outs[0]->key;
    const std::string region_ident = region_outs[0]->ident;
    auto region = parquet::Region::Deserialize(const_cast<DataBuffer &>(region_outs[0]->buffer));
    auto region_buf = mock::MakeBuffer(region_outs[0]->buffer.data(), region_outs[0]->buffer.size());

    // --- mimic the runtime: resolve each request output (preserving its ident +
    // key) by slicing the file at the byte range region.requests[ident] names ----
    struct Resolved {
        std::string ident;
        size_t key;
        DataBuffer buf;
    };
    std::vector<Resolved> resolved;
    for (auto *ro : req_outs) {
        const auto &range = region.requests[std::stoull(ro->ident)];
        resolved.push_back({ro->ident, ro->key,
                            mock::MakeBuffer(file.data() + range.offset, static_cast<size_t>(range.size))});
    }

    // --- plume_pq_stage: decode + run -----------------------------------------
    PipelineTemplate desc;
    desc.input_schema.columns = {{"id", I32(), false}, {"val", I32(), false}};
    auto blob = plume::SerializePipeline(desc);

    mock::Reset();
    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(blob.data(), blob.size()));
        return v;
    }());
    mock::AddInput(1, std::move(region_buf), region_ident, region_key);
    for (auto &r : resolved) {
        mock::AddInput(2, std::move(r.buf), r.ident, r.key);
    }

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
    CHECK(rows.size() == 5);
    for (int i = 0; i < 5; i++) {
        CHECK(rows[i] == std::make_pair(i + 1, (i + 1) * 10));
    }
}

int main() { return plume_test::RunAll(); }
