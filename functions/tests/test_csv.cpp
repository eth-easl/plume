// CSV path end to end through the mock host ABI: csv_prepare defines byte-range
// splits, the test mimics the runtime resolving each request from the file, then
// csv_stage parses each split and runs a pass-through pipeline. Verifies every
// data row is parsed exactly once across the split boundary.

#include "abi_mock.hpp"
#include "test_util.hpp"

#include "plume/common/serial.hpp"
#include "plume/csv/csv.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/csv.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <algorithm>
#include <string>
#include <vector>

using namespace plume;
using namespace plume::exec;
using duckdb::idx_t;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema RegionAmount() {
    Schema s;
    s.columns = {{"region", I32(), false}, {"amount", I32(), false}};
    return s;
}

std::string RowKey(int32_t a, int32_t b) { return std::to_string(a) + "," + std::to_string(b); }

// Mimic the runtime resolving an S3 GET request: parse "Range: bytes=START-END"
// into the half-open file byte range [START, END+1).
std::pair<uint64_t, uint64_t> ParseRange(const DataBuffer &req) {
    std::string s(reinterpret_cast<const char *>(req.data()), req.size());
    auto pos = s.find("bytes=");
    CHECK_MSG(pos != std::string::npos, "request has no Range header");
    pos += std::char_traits<char>::length("bytes=");
    auto dash = s.find('-', pos);
    uint64_t start = std::stoull(s.substr(pos, dash - pos));
    uint64_t end_inclusive = std::stoull(s.substr(dash + 1));
    return {start, end_inclusive + 1};
}

} // namespace

TEST_CASE("csv path: prepare splits + stage parses every row once across the boundary") {
    mock::Reset();
    memory::Allocator alloc;

    const std::string csv = "region,amount\n"
                            "1,100\n2,50\n1,-5\n3,30\n2,25\n1,0\n3,70\n1,12\n2,8\n3,99\n";
    const std::vector<std::pair<int32_t, int32_t>> expected = {{1, 100}, {2, 50}, {1, -5}, {3, 30}, {2, 25},
                                                               {1, 0},   {3, 70}, {1, 12}, {2, 8},  {3, 99}};

    // --- csv_prepare: define 2 splits over the file -----------------------------
    csv::CSVConfig config;
    config.num_splits = 2;
    config.total_size = csv.size();
    config.line_margin = 4096;
    config.options.has_header = true;
    config.schema = RegionAmount();

    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(SerializeToBuffer(config));
        return v;
    }());
    mock::SetInput(1, [&] { // header buffer (unused here; names come from config)
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(csv.data(), std::min<size_t>(csv.size(), 64)));
        return v;
    }());
    mock::SetInput(2, [&] {
        std::vector<DataBuffer> v;
        const char *url = "s3://bucket/data.csv";
        v.push_back(mock::MakeBuffer(url, std::char_traits<char>::length(url)));
        return v;
    }());

    CHECK(fn::RunCSVPrepare().is_ok());

    auto chunk_infos = mock::OutputsForSet(0);
    auto prepare_reqs = mock::OutputsForSet(1);
    CHECK(chunk_infos.size() == 2); // two splits -> two chunk infos
    CHECK(prepare_reqs.size() == 2);   // two splits -> two fetch requests

    // schema name carried through from config.
    auto first_info = DeserializeFromBytes<csv::CSVRegionInfo>(chunk_infos[0]->buffer.data(),
                                                              chunk_infos[0]->buffer.size());
    CHECK(first_info.schema.columns[0].name == "region");

    // --- mimic the runtime: resolve each request to the file byte range, keeping
    // the chunk key so csv_stage can match each buffer to its chunk info ---------
    struct Resolved {
        size_t key;
        DataBuffer buf;
    };
    std::vector<Resolved> resolved;
    for (auto *r : prepare_reqs) {
        auto [start, end] = ParseRange(r->buffer);
        resolved.push_back({r->key, mock::MakeBuffer(csv.data() + start, end - start)});
    }
    std::vector<Resolved> info_bufs;
    for (auto *ci : chunk_infos) {
        info_bufs.push_back({ci->key, mock::MakeBuffer(ci->buffer.data(), ci->buffer.size())});
    }

    // --- csv_stage: pass-through pipeline over the parsed rows ----------------
    PipelineTemplate desc;
    desc.input_schema = RegionAmount(); // no operators: identity
    auto blob = plume::SerializePipeline(desc);

    mock::Reset(); // clear prepare outputs; set up stage inputs
    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(blob.data(), blob.size()));
        return v;
    }());
    // Each chunk info and its resolved csv buffer share the split's chunk key.
    for (auto &i : info_bufs) {
        mock::AddInput(1, std::move(i.buf), "chunk_info", i.key);
    }
    for (auto &r : resolved) {
        mock::AddInput(2, std::move(r.buf), "csv_chunk", r.key);
    }

    CHECK(fn::RunCSVStage().is_ok());

    std::vector<std::string> got;
    for (auto *out : mock::OutputsForSet(0)) {
        Schema schema;
        auto chunks = memory::ImportBlockChunks(const_cast<uint8_t *>(out->buffer.data()), out->buffer.size(), schema).unwrap();
        for (auto &c : chunks) {
            for (idx_t r = 0; r < c->size(); r++) {
                got.push_back(RowKey(c->GetValue(0, r).GetValue<int32_t>(), c->GetValue(1, r).GetValue<int32_t>()));
            }
        }
    }

    std::vector<std::string> want;
    for (auto &[a, b] : expected) {
        want.push_back(RowKey(a, b));
    }
    std::sort(got.begin(), got.end());
    std::sort(want.begin(), want.end());
    // The first split owns the "region,amount" header line. It must be skipped,
    // not parsed as a data row — otherwise we'd get 11 rows (the extra one being
    // garbage from parsing "region"/"amount" as int32). Pin the count explicitly.
    CHECK_MSG(got.size() == 10, "header row must not appear in the output");
    CHECK_MSG(got.size() == want.size(),
              "row count: got " + std::to_string(got.size()) + " want " + std::to_string(want.size()));
    CHECK(got == want);
}

TEST_CASE("csv_prepare: resolve column names from the header") {
    mock::Reset();
    const std::string csv = "the_region,the_amount\n1,100\n2,50\n";

    csv::CSVConfig config;
    config.num_splits = 1;
    config.total_size = csv.size();
    config.options.has_header = true;
    config.resolve_names_from_header = true;
    config.schema = RegionAmount(); // types matter; names should be overridden

    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(SerializeToBuffer(config));
        return v;
    }());
    mock::SetInput(1, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(csv.data(), csv.size()));
        return v;
    }());
    mock::SetInput(2, [&] {
        std::vector<DataBuffer> v;
        const char *url = "s3://b/x.csv";
        v.push_back(mock::MakeBuffer(url, std::char_traits<char>::length(url)));
        return v;
    }());

    CHECK(fn::RunCSVPrepare().is_ok());
    auto out = mock::OutputsForSet(0);
    CHECK(out.size() == 1);
    auto info = DeserializeFromBytes<csv::CSVRegionInfo>(out[0]->buffer.data(), out[0]->buffer.size());
    CHECK(info.schema.columns[0].name == "the_region");
    CHECK(info.schema.columns[1].name == "the_amount");
}

TEST_CASE("csv projection pushdown: ParseCSVChunk reads only the projected fields, reordered") {
    // Three source fields per row; project fields {2, 0} into a 2-column output.
    // ParseCSVChunk always skips the first physical line (the header / partial
    // leading record), so prepend a throwaway header row.
    const std::string csv = "a,b,c\n10,20,30\n40,50,60\n";
    csv::CSVRegionInfo info;
    info.schema.columns = {{"f2", I32(), false}, {"f0", I32(), false}};
    info.options.has_header = true;
    info.logical_end = csv.size();
    info.projection = {2, 0}; // output col 0 <- field 2, output col 1 <- field 0

    memory::Allocator alloc;
    std::vector<std::string> rows;
    auto emit = [&](std::unique_ptr<duckdb::DataChunk> chunk) -> Result<void> {
        for (idx_t r = 0; r < chunk->size(); r++) {
            rows.push_back(RowKey(chunk->GetValue(0, r).GetValue<int32_t>(),
                                  chunk->GetValue(1, r).GetValue<int32_t>()));
        }
        return Ok();
    };
    CHECK(csv::ParseCSVChunk(reinterpret_cast<const uint8_t *>(csv.data()), csv.size(), info, alloc, emit).is_ok());

    CHECK(rows.size() == 2);
    CHECK(rows[0] == RowKey(30, 10)); // (field2, field0)
    CHECK(rows[1] == RowKey(60, 40));
}

int main() { return plume_test::RunAll(); }
