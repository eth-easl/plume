#include "plume/abi/abi.hpp"
#include "plume/common/serial.hpp"
#include "plume/dandelion/s3.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/functions/parquet.hpp"
#include "plume/parquet/metadata.hpp"
#include "plume/parquet/parquet.hpp"
#include "plume/parquet/prune.hpp"
#include "plume/parquet/regions.hpp"

#include <cstdint>
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
// The fourth input set (index 3) is optional and holds zero or one DynamicFilterBounds computed by
// an upstream stage's join build side.
#define SET_IDX_IN_DYNAMIC_FILTER 3

// The first output set (index 0) contains the region infos.
#define SET_IDX_OUT_REGION 0
// The second output set (index 1) contains the data chunk requests to load the data.
#define SET_IDX_OUT_CHUNK_REQ 1

namespace plume::fn {

Result<void> RunParquetPrepare() {
    TRY(auto config_itm, abi::GetInputSingleton(SET_IDX_IN_CONFIG));
    auto config = DeserializeFromBytes<parquet::ParquetConfig>(config_itm.buffer.data(), config_itm.buffer.size());
    const uint32_t num_splits = config.num_splits == 0 ? 1 : config.num_splits;

    std::optional<exec::DynamicFilterBounds> dyn_filter_bounds = std::nullopt;
    TRY(auto dyn_filter_item, abi::GetOptionalInputSingleton(SET_IDX_IN_DYNAMIC_FILTER));
    if (dyn_filter_item) {
        dyn_filter_bounds = std::make_optional(DeserializeFromBytes<exec::DynamicFilterBounds>(
            dyn_filter_item->buffer.data(), dyn_filter_item->buffer.size()));
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

    size_t region_key = 0;
    for (auto& footer_itm : footer_itms) {
        auto it = url_buf_index.find(footer_itm.key);
        if (it == url_buf_index.end()) {
            return Error("Did not find url item with matching key.");
        }
        auto url = std::string(reinterpret_cast<const char *>(it->second->data()), it->second->size());

        TRY(auto meta, parquet::ParseFooter(footer_itm.buffer.data(), footer_itm.buffer.size()));

        // combine any dynamic filter with static pushed filters
        bool has_filter = config.has_pushed_filter;
        expr::ExprNode filter = config.pushed_filter;
        if (dyn_filter_bounds && config.dynamic_filter_column >= 0) {
            if (static_cast<size_t>(config.dynamic_filter_column) >= meta.schema.columns.size()) {
                return Error("ParquetConfig.dynamic_filter_column exceeds metadata schema columns.", ErrorKind::OutOfRange);
            }
            const auto &col_type = meta.schema.columns[config.dynamic_filter_column].type;
            auto range = exec::BuildRangeFilter(config.dynamic_filter_column, col_type, *dyn_filter_bounds);
            if (range) {
                filter = has_filter ? expr::ExprNode::Conjunction(
                             duckdb::ExpressionType::CONJUNCTION_AND, {std::move(filter), std::move(*range)}
                         ) : std::move(*range);
                has_filter = true;
            }
        }

        // filter pushdown
        std::vector<uint32_t> keep_row_groups;
        if (has_filter) {
            for (uint32_t rg = 0; rg < meta.row_groups.size(); rg++) {
                if (parquet::RowGroupMayMatch(filter, meta.row_groups[rg], meta.schema)) {
                    keep_row_groups.push_back(rg);
                }
            }
            if (keep_row_groups.empty()) {
                continue; // entire file pruned for this footer
            }
        }

        // projection pushdown
        auto regions = parquet::DefineRegions(meta, num_splits, config.coalesce_distance,
                                              config.projection, keep_row_groups, config.max_region_size);

        for (size_t r = 0; r < regions.size(); r++) {
            const size_t key = region_key++;
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
