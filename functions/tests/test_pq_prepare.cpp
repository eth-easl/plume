// plume_pq_prepare: parse a parquet footer into regions + fetch requests. Uses a
// hand-encoded FileMetaData (full control of offsets/sizes) for exact checks,
// plus a real parquet file for a structural smoke test.

#include "abi_mock.hpp"
#include "test_util.hpp"

#include "plume/common/serial.hpp"
#include "plume/expression/expression.hpp"
#include "plume/functions/parquet.hpp"
#include "plume/parquet/metadata.hpp"
#include "plume/parquet/parquet.hpp"
#include "plume/parquet/prune.hpp"

#include "duckdb/common/types/value.hpp"

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

using namespace plume;
using plume::parquet::Region;

namespace {

// Minimal Thrift compact-protocol encoder for building a FileMetaData footer.
struct ThriftWriter {
    std::vector<uint8_t> b;
    void Byte(uint8_t v) { b.push_back(v); }
    void Varint(uint64_t v) {
        while (v > 0x7f) {
            b.push_back(static_cast<uint8_t>((v & 0x7f) | 0x80));
            v >>= 7;
        }
        b.push_back(static_cast<uint8_t>(v));
    }
    void ZigZag(int64_t n) { Varint((static_cast<uint64_t>(n) << 1) ^ static_cast<uint64_t>(n >> 63)); }
    void Field(uint8_t delta, uint8_t type) { Byte(static_cast<uint8_t>((delta << 4) | type)); }
    void ListHeader(uint8_t size, uint8_t elem) { Byte(static_cast<uint8_t>((size << 4) | elem)); }
    void Stop() { Byte(0); }
};

constexpr uint8_t I64 = 6, LIST = 9, STRUCT = 12;

// One column's ColumnChunk { meta_data { total_compressed=7, data_page=9, dict=11 } }.
void EncodeColumn(ThriftWriter &w, int64_t total, int64_t data_page, int64_t dict_page) {
    w.Field(3, STRUCT); // ColumnChunk.meta_data
    w.Field(7, I64);
    w.ZigZag(total);
    w.Field(2, I64); // delta 2 -> field 9 (data_page_offset)
    w.ZigZag(data_page);
    if (dict_page >= 0) {
        w.Field(2, I64); // delta 2 -> field 11 (dictionary_page_offset)
        w.ZigZag(dict_page);
    }
    w.Stop(); // ColumnMetaData
    w.Stop(); // ColumnChunk
}

// Footer = [FileMetaData][i32 len][PAR1]. Two row groups, two columns each.
DataBuffer EncodeFooter() {
    ThriftWriter w;
    w.Field(4, LIST); // FileMetaData.row_groups
    w.ListHeader(2, STRUCT);
    // row group 0
    w.Field(1, LIST); // RowGroup.columns
    w.ListHeader(2, STRUCT);
    EncodeColumn(w, /*total=*/200, /*data=*/100, /*dict=*/4); // chunk [4, 204)
    EncodeColumn(w, /*total=*/50, /*data=*/210, /*dict=*/-1); // chunk [210, 260)
    w.Stop();
    // row group 1
    w.Field(1, LIST);
    w.ListHeader(2, STRUCT);
    EncodeColumn(w, /*total=*/100, /*data=*/1000, /*dict=*/900); // chunk [900, 1000)
    EncodeColumn(w, /*total=*/40, /*data=*/1010, /*dict=*/-1);   // chunk [1010, 1050)
    w.Stop();
    w.Stop(); // FileMetaData

    std::vector<uint8_t> footer = w.b;
    uint32_t len = static_cast<uint32_t>(w.b.size());
    for (int i = 0; i < 4; i++) {
        footer.push_back(static_cast<uint8_t>((len >> (8 * i)) & 0xff)); // little-endian
    }
    const char *magic = "PAR1";
    footer.insert(footer.end(), magic, magic + 4);
    return mock::MakeBuffer(footer.data(), footer.size());
}

// Run pq_prepare with a fully-specified config (projection / filter pushdown).
void RunprepareCfg(parquet::ParquetConfig cfg, DataBuffer footer) {
    mock::Reset();
    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(SerializeToBuffer(cfg));
        return v;
    }());
    mock::SetInput(1, [&] {
        std::vector<DataBuffer> v;
        v.push_back(std::move(footer));
        return v;
    }());
    mock::SetInput(2, [&] {
        std::vector<DataBuffer> v;
        const char *url = "s3://b/f.parquet";
        v.push_back(mock::MakeBuffer(url, std::char_traits<char>::length(url)));
        return v;
    }());
    CHECK(fn::RunParquetPrepare().is_ok());
}

void Runprepare(uint32_t num_splits, uint64_t coalesce, DataBuffer footer) {
    parquet::ParquetConfig cfg;
    cfg.num_splits = num_splits;
    cfg.coalesce_distance = coalesce;
    RunprepareCfg(std::move(cfg), std::move(footer));
}

Region RegionAt(size_t i) {
    auto outs = mock::OutputsForSet(0);
    return Region::Deserialize(const_cast<DataBuffer &>(outs[i]->buffer));
}

} // namespace

TEST_CASE("pq_prepare: regions + requests from a hand-encoded footer (no coalesce)") {
    Runprepare(2, 0, EncodeFooter());

    auto regions = mock::OutputsForSet(0);
    auto requests = mock::OutputsForSet(1);
    CHECK(regions.size() == 2);   // 2 row groups, 2 splits -> 1 row group each
    CHECK(requests.size() == 4);  // 2 columns per region, no coalescing

    Region r0 = RegionAt(0);
    CHECK(r0.row_groups.size() == 1);
    CHECK(r0.row_groups[0].size() == 2);
    auto &c00 = r0.row_groups[0][0];
    CHECK(c00.row_group_idx == 0);
    CHECK(c00.column_idx == 0);
    CHECK(c00.offset == 4);
    CHECK(c00.size == 200);
    CHECK(c00.req_idx == 0);
    CHECK(c00.req_offset == 0);
    auto &c01 = r0.row_groups[0][1];
    CHECK(c01.offset == 210);
    CHECK(c01.size == 50);
    CHECK(c01.req_idx == 1);
    CHECK(c01.req_offset == 0);
    CHECK(r0.requests.size() == 2);
    CHECK(r0.requests[0].offset == 4);
    CHECK(r0.requests[0].size == 200);
    CHECK(r0.requests[1].offset == 210);

    Region r1 = RegionAt(1);
    CHECK(r1.row_groups[0][0].row_group_idx == 1);
    CHECK(r1.row_groups[0][0].offset == 900);

    // requests are tagged with their region key.
    int region0_reqs = 0, region1_reqs = 0;
    for (auto *req : requests) {
        if (req->key == 0) region0_reqs++;
        if (req->key == 1) region1_reqs++;
    }
    CHECK(region0_reqs == 2);
    CHECK(region1_reqs == 2);
}

TEST_CASE("pq_prepare: coalescing merges nearby chunks into one request") {
    Runprepare(2, /*coalesce=*/100, EncodeFooter());
    Region r0 = RegionAt(0);
    // [4,204) and [210,260): gap 6 <= 100 -> one request [4, 260).
    CHECK(r0.requests.size() == 1);
    CHECK(r0.requests[0].offset == 4);
    CHECK(r0.requests[0].size == 256);
    CHECK(r0.row_groups[0][0].req_idx == 0);
    CHECK(r0.row_groups[0][0].req_offset == 0);
    CHECK(r0.row_groups[0][1].req_idx == 0);
    CHECK(r0.row_groups[0][1].req_offset == 206); // 210 - 4
}

TEST_CASE("pq_prepare: real parquet file parses into structurally valid regions") {
    std::ifstream f(PLUME_TEST_PARQUET, std::ios::binary);
    CHECK_MSG(f.good(), "cannot open " PLUME_TEST_PARQUET);
    std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    const int64_t file_size = static_cast<int64_t>(bytes.size());

    Runprepare(3, 1 << 20, mock::MakeBuffer(bytes.data(), bytes.size()));

    auto region_outs = mock::OutputsForSet(0);
    CHECK(region_outs.size() >= 1);
    CHECK(region_outs.size() <= 3);

    size_t col_count = 0;
    for (size_t i = 0; i < region_outs.size(); i++) {
        Region region = RegionAt(i);
        CHECK(!region.row_groups.empty());
        for (auto &rg : region.row_groups) {
            if (col_count == 0) {
                col_count = rg.size();
            }
            CHECK(rg.size() == col_count); // all row groups share the column count
            for (auto &dc : rg) {
                CHECK(dc.size > 0);
                CHECK(dc.offset >= 0);
                CHECK(dc.offset + dc.size <= file_size);
                // the chunk lies within its fetch request.
                CHECK(dc.req_idx < region.requests.size());
                auto &req = region.requests[dc.req_idx];
                CHECK(dc.offset >= req.offset);
                CHECK(dc.offset + dc.size <= req.offset + req.size);
                CHECK(dc.req_offset == static_cast<uint64_t>(dc.offset - req.offset));
            }
        }
    }
    CHECK(col_count > 0);
}

namespace {

constexpr uint8_t I32 = 5, BINARY = 8;

void StringElem(ThriftWriter &w, const char *s) {
    size_t n = std::char_traits<char>::length(s);
    w.Varint(n);
    for (size_t i = 0; i < n; i++) {
        w.Byte(static_cast<uint8_t>(s[i]));
    }
}

void PlainI32(ThriftWriter &w, int32_t v) {
    w.Varint(4); // binary length
    uint32_t u = static_cast<uint32_t>(v);
    for (int i = 0; i < 4; i++) {
        w.Byte(static_cast<uint8_t>(u >> (8 * i)));
    }
}

// A footer with a 1-column (INT32 "v") schema, num_rows=42, and one row group whose
// column carries Statistics { null_count=2, distinct_count=5, max=999, min=-7 }.
DataBuffer EncodeStatsFooter() {
    ThriftWriter w;
    // FileMetaData.schema (field 2): [root group, leaf "v"].
    w.Field(2, LIST);
    w.ListHeader(2, STRUCT);
    w.Field(4, BINARY); // root: name
    StringElem(w, "schema");
    w.Field(1, I32); // delta 1 -> field 5 num_children
    w.ZigZag(1);
    w.Stop(); // root element
    w.Field(1, I32); // leaf: field 1 type = INT32 (physical 1)
    w.ZigZag(1);
    w.Field(2, I32); // delta 2 -> field 3 repetition_type = OPTIONAL
    w.ZigZag(1);
    w.Field(1, BINARY); // delta 1 -> field 4 name
    StringElem(w, "v");
    w.Stop(); // leaf element

    w.Field(1, I64); // delta 1 -> field 3 num_rows
    w.ZigZag(42);

    w.Field(1, LIST); // delta 1 -> field 4 row_groups
    w.ListHeader(1, STRUCT);
    w.Field(1, LIST); // RowGroup.columns
    w.ListHeader(1, STRUCT);
    w.Field(3, STRUCT); // ColumnChunk.meta_data
    w.Field(7, I64);    // total_compressed_size
    w.ZigZag(100);
    w.Field(2, I64); // delta 2 -> field 9 data_page_offset
    w.ZigZag(10);
    w.Field(3, STRUCT); // delta 3 -> field 12 statistics
    w.Field(3, I64);    // null_count
    w.ZigZag(2);
    w.Field(1, I64); // delta 1 -> field 4 distinct_count
    w.ZigZag(5);
    w.Field(1, BINARY); // delta 1 -> field 5 max_value
    PlainI32(w, 999);
    w.Field(1, BINARY); // delta 1 -> field 6 min_value
    PlainI32(w, -7);
    w.Stop(); // statistics
    w.Stop(); // ColumnMetaData
    w.Stop(); // ColumnChunk
    w.Stop(); // RowGroup
    w.Stop(); // FileMetaData

    std::vector<uint8_t> footer = w.b;
    uint32_t len = static_cast<uint32_t>(w.b.size());
    for (int i = 0; i < 4; i++) {
        footer.push_back(static_cast<uint8_t>((len >> (8 * i)) & 0xff));
    }
    const char *magic = "PAR1";
    footer.insert(footer.end(), magic, magic + 4);
    return mock::MakeBuffer(footer.data(), footer.size());
}

} // namespace

TEST_CASE("ParseFooter: schema + num_rows + column statistics") {
    auto footer = EncodeStatsFooter();
    auto res = parquet::ParseFooter(footer.data(), footer.size());
    CHECK(res.is_ok());
    auto fm = res.unwrap();

    CHECK(fm.num_rows == 42);
    CHECK(fm.schema.columns.size() == 1);
    CHECK(fm.schema.columns[0].name == "v");
    CHECK(fm.schema.columns[0].type.id == TypeId::INT32);
    CHECK(fm.schema.columns[0].nullable == true);

    CHECK(fm.row_groups.size() == 1);
    CHECK(fm.row_groups[0].columns.size() == 1);
    const auto &st = fm.row_groups[0].columns[0].stats;
    CHECK(st.has_null_count && st.null_count == 2);
    CHECK(st.has_distinct_count && st.distinct_count == 5);
    CHECK(st.has_min && st.min_value.size() == 4);
    CHECK(st.has_max && st.max_value.size() == 4);
    // max=999 -> 0xE7 0x03 0x00 0x00 ; min=-7 -> 0xF9 0xFF 0xFF 0xFF (LE int32)
    CHECK(st.max_value[0] == 0xE7 && st.max_value[1] == 0x03);
    CHECK(st.min_value[0] == 0xF9 && st.min_value[1] == 0xFF);
}

// --- projection pushdown ---------------------------------------------------

TEST_CASE("pq_prepare: projection pushdown reads only the selected column") {
    // EncodeFooter has 2 columns/row group; project just column 1 (the second).
    parquet::ParquetConfig cfg;
    cfg.num_splits = 2;
    cfg.coalesce_distance = 0;
    cfg.projection = {1};
    RunprepareCfg(cfg, EncodeFooter());

    auto regions = mock::OutputsForSet(0);
    auto requests = mock::OutputsForSet(1);
    CHECK(regions.size() == 2);  // still 2 row groups / 2 splits
    CHECK(requests.size() == 2); // only one (projected) column fetched per region

    Region r0 = RegionAt(0);
    CHECK(r0.row_groups.size() == 1);
    CHECK(r0.row_groups[0].size() == 1);     // only the projected column
    CHECK(r0.row_groups[0][0].column_idx == 0); // remapped to output position 0
    CHECK(r0.row_groups[0][0].offset == 210);   // column 1's chunk in row group 0
    CHECK(r0.row_groups[0][0].size == 50);

    Region r1 = RegionAt(1);
    CHECK(r1.row_groups[0].size() == 1);
    CHECK(r1.row_groups[0][0].offset == 1010); // column 1's chunk in row group 1
}

// --- filter pushdown (row-group pruning) -----------------------------------

namespace {
using plume::expr::ExprNode;
using duckdb::Value;

// `v <op> rhs` over the single INT32 column of EncodeStatsFooter (file column 0).
ExprNode VCompare(duckdb::ExpressionType op, int32_t rhs) {
    return ExprNode::Comparison(op, ExprNode::Reference(0, {TypeId::INT32}),
                                ExprNode::Constant(Value::INTEGER(rhs), {TypeId::INT32}));
}
} // namespace

TEST_CASE("pq_prepare: filter pushdown prunes a row group proven empty by stats") {
    // EncodeStatsFooter's only row group has min=-7, max=999.
    parquet::ParquetConfig cfg;
    cfg.has_pushed_filter = true;
    cfg.pushed_filter = VCompare(duckdb::ExpressionType::COMPARE_GREATERTHAN, 1000); // v > 1000
    RunprepareCfg(cfg, EncodeStatsFooter());

    CHECK(mock::OutputsForSet(0).empty()); // pruned: no region emitted
    CHECK(mock::OutputsForSet(1).empty()); // and nothing to fetch
}

TEST_CASE("pq_prepare: filter pushdown keeps a row group that may match") {
    parquet::ParquetConfig cfg;
    cfg.has_pushed_filter = true;
    cfg.pushed_filter = VCompare(duckdb::ExpressionType::COMPARE_GREATERTHAN, 500); // v > 500 (max 999)
    RunprepareCfg(cfg, EncodeStatsFooter());

    CHECK(mock::OutputsForSet(0).size() == 1); // kept
}

TEST_CASE("RowGroupMayMatch: comparison decisions against INT32 [min,max] stats") {
    using duckdb::ExpressionType;
    auto footer = EncodeStatsFooter();
    auto meta = parquet::ParseFooter(footer.data(), footer.size()).unwrap(); // min=-7, max=999
    const auto &rgm = meta.row_groups[0];

    auto may = [&](ExpressionType op, int32_t rhs) {
        return parquet::RowGroupMayMatch(VCompare(op, rhs), rgm, meta.schema);
    };
    // provably empty -> prune (false)
    CHECK(!may(ExpressionType::COMPARE_GREATERTHAN, 999));   // v > 999, max is 999
    CHECK(!may(ExpressionType::COMPARE_LESSTHAN, -7));       // v < -7, min is -7
    CHECK(!may(ExpressionType::COMPARE_EQUAL, 5000));        // v = 5000, out of range
    // possibly matches -> keep (true)
    CHECK(may(ExpressionType::COMPARE_GREATERTHANOREQUALTO, 999)); // v >= 999
    CHECK(may(ExpressionType::COMPARE_EQUAL, 50));                 // v = 50, in range
    CHECK(may(ExpressionType::COMPARE_LESSTHAN, 0));               // v < 0, min -7
}

int main() { return plume_test::RunAll(); }
