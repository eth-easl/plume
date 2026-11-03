#include "plume/abi/abi.hpp"
#include "plume/common/serial.hpp"
#include "plume/dandelion/s3.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/util/dynamic_filter.hpp"
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
// The fourth input set (index 3) is optional and holds the dynamic filters computed by upstream
// stages.
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

// A dynamic filter's key column and, if it carries them, its exact keys (sorted, cast to the
// column type).
struct KeyFilter {
    uint32_t column;
    std::optional<std::vector<duckdb::Value>> values;
};

std::vector<parquet::RowRange> IntersectRows(const std::vector<parquet::RowRange> &a,
                                             const std::vector<parquet::RowRange> &b) {
    std::vector<parquet::RowRange> out;
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        const uint64_t begin = std::max(a[i].begin, b[j].begin);
        const uint64_t end = std::min(a[i].end, b[j].end);
        if (begin < end) {
            out.push_back({begin, end});
        }
        (a[i].end < b[j].end) ? i++ : j++;
    }
    return out;
}

// Narrows the row groups to read by the page indexes of the dynamic filters' key columns: drops
// the row groups none of whose pages can match and selects the pages to read of the others (the
// rows every key column's selected pages hold).
void SelectPages(const parquet::FileMeta &meta, const parquet::ParquetConfig &config, const expr::ExprNode *filter,
                 const std::vector<KeyFilter> &key_filters, const PageIndexBytes &page_index,
                 std::vector<uint32_t> &row_groups, std::map<uint32_t, parquet::RowGroupPages> &selection) {
    std::vector<uint32_t> read_cols = config.projection;
    if (read_cols.empty()) {
        read_cols.resize(meta.schema.columns.size());
        std::iota(read_cols.begin(), read_cols.end(), 0u);
    }

    std::vector<uint32_t> kept;
    for (uint32_t rg : row_groups) {
        const auto &rgm = meta.row_groups[rg];
        const uint64_t rg_rows = static_cast<uint64_t>(std::max<int64_t>(rgm.num_rows, 0));
        // A key column without a (usable) page index doesn't narrow the rows.
        std::optional<std::vector<parquet::RowRange>> rows;
        for (const auto &kf : key_filters) {
            if (kf.column >= rgm.columns.size()) {
                continue;
            }
            const auto &key_cc = rgm.columns[kf.column];
            const uint8_t *ci = page_index.Slice(key_cc.column_index_offset, key_cc.column_index_length);
            const uint8_t *oi = page_index.Slice(key_cc.offset_index_offset, key_cc.offset_index_length);
            if (!ci || !oi) {
                continue;
            }
            auto index = parquet::ParseColumnIndex(ci, static_cast<size_t>(key_cc.column_index_length));
            auto pages = parquet::ParseOffsetIndex(oi, static_cast<size_t>(key_cc.offset_index_length));
            if (index.is_error() || pages.is_error()) {
                continue;
            }
            auto key_rows = parquet::SelectPageRows(filter, kf.values ? &*kf.values : nullptr, kf.column, meta.schema,
                                                    pages.unwrap(), index.unwrap(), rg_rows);
            if (!key_rows) {
                continue;
            }
            rows = rows ? IntersectRows(*rows, *key_rows) : std::move(*key_rows);
            if (rows->empty()) {
                break;
            }
        }
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

    // The prune-mode dynamic filters (all ANDed) and their file key columns. A filter that isn't
    // bound has nothing to prune by.
    TRY(auto all_dyn_filters, util::ReadDynamicFilters(SET_IDX_IN_DYNAMIC_FILTER));
    std::vector<std::pair<uint32_t, const exec::DynamicFilter *>> dyn_filters;
    for (const auto &df : config.dynamic_filters) {
        auto it = all_dyn_filters.find(df.filter_id);
        if (it == all_dyn_filters.end()) {
            continue;
        }
        if (!it->second.has_bounds) {
            // The build side has no non-NULL key at all: the inner join can't produce any row,
            // so the entire probe scan is pruned.
            return Ok();
        }
        dyn_filters.emplace_back(df.file_column, &it->second);
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

        // combine the dynamic filters with static pushed filters
        bool has_filter = config.has_pushed_filter;
        expr::ExprNode filter = config.pushed_filter;
        std::vector<KeyFilter> key_filters;
        bool has_values = false;
        for (const auto &[key_col, dyn_filter] : dyn_filters) {
            if (key_col >= meta.schema.columns.size()) {
                return Error("ParquetConfig.dynamic_filters column exceeds metadata schema columns.",
                             ErrorKind::OutOfRange);
            }
            const auto &col_type = meta.schema.columns[key_col].type;
            auto range = exec::BuildRangeFilter(key_col, col_type, *dyn_filter);
            if (range) {
                filter = has_filter ? expr::ExprNode::Conjunction(
                             duckdb::ExpressionType::CONJUNCTION_AND, {std::move(filter), std::move(*range)}
                         ) : std::move(*range);
                has_filter = true;
            }
            // An exact key set additionally prunes the row groups none of its keys falls into.
            KeyFilter kf{key_col, std::nullopt};
            if (dyn_filter->has_values) {
                kf.values = parquet::SortStatValues(dyn_filter->values, col_type);
                has_values = has_values || kf.values.has_value();
            }
            key_filters.push_back(std::move(kf));
        }

        // filter pushdown
        const bool prune = has_filter || has_values;
        std::vector<uint32_t> keep_row_groups;
        if (prune) {
            for (uint32_t rg = 0; rg < meta.row_groups.size(); rg++) {
                const auto &rgm = meta.row_groups[rg];
                bool may_match = !has_filter || parquet::RowGroupMayMatch(filter, rgm, meta.schema);
                for (size_t k = 0; may_match && k < key_filters.size(); k++) {
                    const auto &kf = key_filters[k];
                    may_match = !kf.values || parquet::RowGroupMayContainAny(rgm, meta.schema, kf.column, *kf.values);
                }
                if (may_match) {
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
        const bool use_page_index = !key_filters.empty() && page_index.size > 0;
        if (use_page_index) {
            if (!prune) {
                keep_row_groups.resize(meta.row_groups.size());
                std::iota(keep_row_groups.begin(), keep_row_groups.end(), 0u);
            }
            SelectPages(meta, config, has_filter ? &filter : nullptr, key_filters, page_index, keep_row_groups,
                        page_selection);
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
