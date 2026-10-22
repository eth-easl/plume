#include "plume/abi/abi.hpp"
#include "plume/common/serial.hpp"
#include "plume/dandelion/s3.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/functions/parquet.hpp"
#include "plume/parquet/metadata.hpp"
#include "plume/parquet/parquet.hpp"
#include "plume/parquet/prune.hpp"
#include "plume/parquet/regions.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

// The first input set (index 0) should have one item containing the parquet reading configuration.
#define SET_IDX_IN_CONFIG 0
// The second input set (index 1) should have items containing the parquet footers which are
// expected to contain the entire metadata of the parquet.
#define SET_IDX_IN_FOOTER 1
// The third input set (index 2) should have items containing source urls of the parquet.
#define SET_IDX_IN_URL 2
// The fourth input set (index 3) is optional and holds the DynamicFilter computed by an
// upstream stage's join build side, one item per build instance.
#define SET_IDX_IN_DYNAMIC_FILTER 3

// The first output set (index 0) contains the region infos.
#define SET_IDX_OUT_REGION 0
// The second output set (index 1) contains the data chunk requests to load the data.
#define SET_IDX_OUT_CHUNK_REQ 1

namespace plume::fn {

namespace {

// The bytes [offset, offset + length) of the file, if within the page index bytes.
struct PageIndexBytes {
    const uint8_t *data = nullptr;
    int64_t offset = 0;
    uint64_t size = 0;

    const uint8_t *Slice(int64_t at, int32_t length) const {
        if (!data || at < offset || length <= 0 || static_cast<uint64_t>(at - offset) + length > size) {
            return nullptr;
        }
        return data + (at - offset);
    }
};

// The pages of a read column chunk overlapping the selected rows.
struct ColumnPages {
    int64_t first_page_offset; // where its dictionary page (if any) ends
    std::vector<parquet::PageRun> runs; // runs of consecutive pages
};

std::optional<ColumnPages> PageRunsFor(const parquet::ColumnChunkMeta &cc, uint64_t rg_rows,
                                       const PageIndexBytes &page_index, const std::vector<parquet::RowRange> &rows) {
    const uint8_t *oi = page_index.Slice(cc.offset_index_offset, cc.offset_index_length);
    if (!oi) {
        return std::nullopt;
    }
    auto pages = parquet::ParseOffsetIndex(oi, static_cast<size_t>(cc.offset_index_length));
    if (pages.is_error() || pages.unwrap().empty()) {
        return std::nullopt;
    }
    const auto &locs = pages.unwrap();
    std::vector<parquet::PageRun> runs;
    size_t r = 0;
    for (size_t p = 0; p < locs.size(); p++) {
        const uint64_t begin = static_cast<uint64_t>(locs[p].first_row);
        const uint64_t end = p + 1 < locs.size() ? static_cast<uint64_t>(locs[p + 1].first_row) : rg_rows;
        while (r < rows.size() && rows[r].end <= begin) {
            r++;
        }
        if (r == rows.size() || rows[r].begin >= end) {
            continue; // no selected row in the page
        }
        if (!runs.empty() && runs.back().first_row + runs.back().num_rows == begin &&
            static_cast<int64_t>(runs.back().end()) == locs[p].offset) {
            runs.back().size += locs[p].size;
            runs.back().num_rows += end - begin;
        } else {
            runs.push_back({locs[p].offset, locs[p].size, begin, end - begin, 0, 0});
        }
    }
    return ColumnPages{locs.front().offset, std::move(runs)};
}

// Narrows the row groups to read by the page index of the dynamic filter's key column: drops the
// row groups none of whose pages can match and selects the pages to read of the others.
void SelectPages(const parquet::FileMeta &meta, const parquet::ParquetConfig &config, const expr::ExprNode *filter,
                 const std::vector<duckdb::Value> *dyn_values, const PageIndexBytes &page_index,
                 std::vector<uint32_t> &row_groups, std::map<uint32_t, parquet::RowGroupPages> &selection) {
    const uint32_t key_col = static_cast<uint32_t>(config.dynamic_filter_column);
    std::vector<uint32_t> read_cols = config.projection;
    if (read_cols.empty()) {
        read_cols.resize(meta.schema.columns.size());
        std::iota(read_cols.begin(), read_cols.end(), 0u);
    }

    std::vector<uint32_t> kept;
    for (uint32_t rg : row_groups) {
        const auto &rgm = meta.row_groups[rg];
        const uint64_t rg_rows = static_cast<uint64_t>(std::max<int64_t>(rgm.num_rows, 0));
        const auto &key_cc = rgm.columns[key_col];
        const uint8_t *ci = page_index.Slice(key_cc.column_index_offset, key_cc.column_index_length);
        const uint8_t *oi = page_index.Slice(key_cc.offset_index_offset, key_cc.offset_index_length);
        if (!ci || !oi) {
            kept.push_back(rg);
            continue;
        }
        auto index = parquet::ParseColumnIndex(ci, static_cast<size_t>(key_cc.column_index_length));
        auto pages = parquet::ParseOffsetIndex(oi, static_cast<size_t>(key_cc.offset_index_length));
        if (index.is_error() || pages.is_error()) {
            kept.push_back(rg);
            continue;
        }
        auto rows = parquet::SelectPageRows(filter, dyn_values, key_col, meta.schema, pages.unwrap(), index.unwrap(),
                                            rg_rows);
        if (!rows) {
            kept.push_back(rg);
            continue;
        }
        if (rows->empty()) {
            continue; // no page can match
        }
        kept.push_back(rg);
        if (rows->size() == 1 && rows->front() == parquet::RowRange{0, rg_rows}) {
            continue; // every page may match
        }

        parquet::RowGroupPages rg_pages;
        bool complete = true;
        for (uint32_t c : read_cols) {
            if (c >= rgm.columns.size()) {
                continue; // DefineRegions skips it too
            }
            auto pages = PageRunsFor(rgm.columns[c], rg_rows, page_index, *rows);
            if (!pages || pages->runs.empty()) {
                complete = false; // every column covers the selected rows, or none is narrowed
                break;
            }
            // The dictionary page leads the chunk, before its first data page.
            rg_pages.dict_sizes.push_back(std::max<int64_t>(0, pages->first_page_offset - rgm.columns[c].offset));
            rg_pages.runs.push_back(std::move(pages->runs));
        }
        if (complete) {
            selection[rg] = std::move(rg_pages);
        }
    }
    row_groups = std::move(kept);
}

} // namespace

Result<void> RunParquetPrepare() {
    TRY(auto config_itm, abi::GetInputSingleton(SET_IDX_IN_CONFIG));
    auto config = DeserializeFromBytes<parquet::ParquetConfig>(config_itm.buffer.data(), config_itm.buffer.size());
    const uint32_t num_splits = config.num_splits == 0 ? 1 : config.num_splits;

    std::optional<exec::DynamicFilter> dyn_filter = std::nullopt;
    auto dyn_filter_itms = abi::GetOptionalInputSet(SET_IDX_IN_DYNAMIC_FILTER);
    if (config.dynamic_filter_column >= 0 && dyn_filter_itms && !dyn_filter_itms->empty()) {
        auto &first = (*dyn_filter_itms)[0];
        auto merged = DeserializeFromBytes<exec::DynamicFilter>(first.buffer.data(), first.buffer.size());
        for (size_t i = 1; i < dyn_filter_itms->size(); i++) {
            auto &itm = (*dyn_filter_itms)[i];
            merged.Merge(DeserializeFromBytes<exec::DynamicFilter>(itm.buffer.data(), itm.buffer.size()));
        }
        if (!merged.has_bounds) {
            // The build side has no non-NULL key at all: the inner join can't produce any row,
            // so the entire probe scan is pruned.
            return Ok();
        }
        dyn_filter = std::move(merged);
    }

    // The merged filter goes along with every region, for pq_stage to evaluate on the key column
    // (an index into the scan schema, i.e. the projected columns).
    int32_t dyn_filter_scan_column = -1;
    std::vector<uint8_t> dyn_filter_bytes;
    if (dyn_filter) {
        dyn_filter_scan_column = parquet::ScanColumnIndex(config.projection, config.dynamic_filter_column);
        if (dyn_filter_scan_column >= 0) {
            auto buf = SerializeToBuffer(*dyn_filter);
            dyn_filter_bytes.assign(buf.data(), buf.data() + buf.size());
        }
    }

    TRY(auto footer_itms, abi::GetInputSet(SET_IDX_IN_FOOTER));
    TRY(auto url_itms, abi::GetInputSet(SET_IDX_IN_URL));
    if (footer_itms.size() != url_itms.size()) {
        return Error("Number of footer items does not match number of url items.", ErrorKind::InvalidInput);
    }
    if (footer_itms.empty()) {
        return Ok();
    }
    std::map<size_t, const DataBuffer *> url_buf_index;
    for (auto &url_buf : url_itms) {
        url_buf_index[url_buf.key] = &url_buf.buffer;
    }

    const auto &rg_offsets = config.file_row_group_offsets;
    const uint32_t num_files = rg_offsets.empty() ? 1 : static_cast<uint32_t>(rg_offsets.size());

    for (auto& footer_itm : footer_itms) {
        auto it = url_buf_index.find(footer_itm.key);
        if (it == url_buf_index.end()) {
            return Error("Did not find url item with matching key.");
        }
        const size_t file_idx = rg_offsets.empty() ? 0 : footer_itm.key;
        if (!rg_offsets.empty() && file_idx >= rg_offsets.size()) {
            return Error("Footer key exceeds ParquetConfig.file_row_group_offsets.", ErrorKind::OutOfRange);
        }
        auto url = std::string(reinterpret_cast<const char *>(it->second->data()), it->second->size());

        TRY(auto meta, parquet::ParseFooter(footer_itm.buffer.data(), footer_itm.buffer.size()));

        // combine any dynamic filter with static pushed filters
        bool has_filter = config.has_pushed_filter;
        expr::ExprNode filter = config.pushed_filter;
        std::optional<std::vector<duckdb::Value>> dyn_values;
        if (dyn_filter && config.dynamic_filter_column >= 0) {
            if (static_cast<size_t>(config.dynamic_filter_column) >= meta.schema.columns.size()) {
                return Error("ParquetConfig.dynamic_filter_column exceeds metadata schema columns.", ErrorKind::OutOfRange);
            }
            const auto &col_type = meta.schema.columns[config.dynamic_filter_column].type;
            auto range = exec::BuildRangeFilter(config.dynamic_filter_column, col_type, *dyn_filter);
            if (range) {
                filter = has_filter ? expr::ExprNode::Conjunction(
                             duckdb::ExpressionType::CONJUNCTION_AND, {std::move(filter), std::move(*range)}
                         ) : std::move(*range);
                has_filter = true;
            }
            // An exact key set additionally prunes the row groups none of its keys falls into.
            if (dyn_filter->has_values) {
                dyn_values = parquet::SortStatValues(dyn_filter->values, col_type);
            }
        }

        // filter pushdown
        const bool prune = has_filter || dyn_values;
        std::vector<uint32_t> keep_row_groups;
        if (prune) {
            for (uint32_t rg = 0; rg < meta.row_groups.size(); rg++) {
                const auto &rgm = meta.row_groups[rg];
                if ((!has_filter || parquet::RowGroupMayMatch(filter, rgm, meta.schema)) &&
                    (!dyn_values || parquet::RowGroupMayContainAny(rgm, meta.schema, config.dynamic_filter_column,
                                                                   *dyn_values))) {
                    keep_row_groups.push_back(rg);
                }
            }
            if (keep_row_groups.empty()) {
                continue; // entire file pruned for this footer
            }
        }

        // page index: only the pages of the row groups whose key may match
        std::map<uint32_t, parquet::RowGroupPages> page_selection;
        PageIndexBytes page_index;
        if (file_idx < config.page_index_sizes.size() && file_idx < config.page_index_offsets.size() &&
            config.page_index_sizes[file_idx] <= footer_itm.buffer.size()) {
            page_index = {footer_itm.buffer.data(), config.page_index_offsets[file_idx],
                          config.page_index_sizes[file_idx]};
        }
        const bool use_page_index = dyn_filter && config.dynamic_filter_column >= 0 && page_index.size > 0;
        if (use_page_index) {
            if (!prune) {
                keep_row_groups.resize(meta.row_groups.size());
                std::iota(keep_row_groups.begin(), keep_row_groups.end(), 0u);
            }
            SelectPages(meta, config, has_filter ? &filter : nullptr, dyn_values ? &*dyn_values : nullptr,
                        page_index, keep_row_groups, page_selection);
            if (keep_row_groups.empty()) {
                continue; // entire file pruned for this footer
            }
        }
        const bool narrowed = prune || use_page_index;

        // projection pushdown
        const size_t kept_rgs = narrowed ? keep_row_groups.size() : meta.row_groups.size();
        const uint32_t file_splits = parquet::FileSplitCount(num_splits, num_files, static_cast<uint32_t>(file_idx),
                                                             kept_rgs, meta.row_groups.size());
        auto regions = parquet::DefineRegions(meta, file_splits, config.coalesce_distance,
                                              config.projection, keep_row_groups, config.max_region_size,
                                              page_selection);

        const size_t key_base = rg_offsets.empty() ? 0 : rg_offsets[file_idx];
        for (size_t r = 0; r < regions.size(); r++) {
            const size_t key = key_base + r;
            if (dyn_filter_scan_column >= 0) {
                regions[r].dynamic_filter_column = dyn_filter_scan_column;
                regions[r].dynamic_filter = dyn_filter_bytes;
            }
            abi::AddOutput("region", SET_IDX_OUT_REGION, regions[r].Serialize(), key);
            for (size_t ri = 0; ri < regions[r].requests.size(); ri++) {
                const auto &req = regions[r].requests[ri];
                auto buf = s3::CreateGetRequest(url, req.size, req.offset);
                abi::AddOutput(std::to_string(ri), SET_IDX_OUT_CHUNK_REQ, std::move(buf), key);
            }
        }
    }

    return Ok();
}

} // namespace plume::fn
