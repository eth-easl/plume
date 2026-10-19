#include "plume/abi/abi.hpp"
#include "plume/common/serial.hpp"
#include "plume/execution/executor.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/output_split.hpp"
#include "plume/functions/parquet.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/parquet/decoder.hpp"
#include "plume/parquet/parquet.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"

#include <algorithm>
#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// The first input set (index 0) should have one item containing the pipeline template.
#define SET_IDX_IN_PIPELINE_TEMPL 0
// The second input set (index 1) should have items containing the region infos.
#define SET_IDX_IN_REGION_INFO 1
// The third input set (index 2) should have items containing the resolved data chunk buffers.
#define SET_IDX_IN_DATA_CHUNKS 2
// The fourth input set (index 3) optionally holds the DynamicFilter of a join build side.
#define SET_IDX_IN_DYNAMIC_FILTER 3

// The first output set (index 0) contains the normal row output.
// The second output set (index 1) optionally contains a single serialized DynamicFilter item,
// emitted only if the pipeline contains a DynamicFilterBuildTemplate operator.
#define SET_IDX_OUT_DYNAMIC_FILTER 1

namespace plume::fn {

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;

namespace {

uint64_t ParseIdent(const std::string &s) { return s.empty() ? 0 : std::stoull(s); }

std::vector<uint32_t> RemainingColumns(uint32_t num_cols, const std::vector<uint8_t> &col_is_filter) {
    std::vector<uint32_t> rest;
    for (uint32_t c = 0; c < num_cols; c++) {
        if (col_is_filter[c] == 0) {
            rest.push_back(c);
        }
    }
    return rest;
}

} // namespace

Result<void> RunParquetStage() {
    TRY(auto template_data, abi::GetInputSingleton(SET_IDX_IN_PIPELINE_TEMPL));
    exec::PipelineTemplate desc;
    {
        PLUME_TRACE_STAGE(trace::Phase::DESERIALIZE);
        TRY(desc, plume::DeserializePipeline(template_data.buffer.data(), template_data.buffer.size()));
    }
    memory::Allocator alloc;
    exec::Executor executor(alloc);
    {
        PLUME_TRACE_STAGE(trace::Phase::SETUP);
        TRYV(executor.Build(desc, MakeSink(desc.output_sink, abi::AddOutput)));
    }

    TRY(auto region_itms, abi::GetInputSet(SET_IDX_IN_REGION_INFO));
    TRY(auto data_chunk_itms, abi::GetInputSet(SET_IDX_IN_DATA_CHUNKS));
    if (region_itms.empty()) {
        if (auto *dyn_filter = executor.FindDynamicFilterBuild()) {
            abi::AddOutput("dynamic_filter", SET_IDX_OUT_DYNAMIC_FILTER, SerializeToBuffer(dyn_filter->Filter()));
        }
        return Ok();
    }

    std::map<std::pair<size_t, uint64_t>, const DataBuffer *> data_chunk_index;
    for (auto &itm : data_chunk_itms) {
        data_chunk_index[{itm.key, ParseIdent(itm.ident)}] = &itm.buffer;
    }

    const Schema &schema = desc.input_schema;
    duckdb::vector<LogicalType> types;
    types.reserve(schema.size());
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }

    const auto num_cols = static_cast<uint32_t>(schema.size());
    TRY(exec::FilterOperator *scan_filter, executor.TakeLeadingScanFilter());
    std::vector<uint32_t> filter_cols;
    std::vector<uint8_t> col_is_filter(schema.size(), 0);
    if (scan_filter) {
        filter_cols = scan_filter->FilterColumns();
        bool in_bounds = true;
        for (auto c : filter_cols) {
            if (c < col_is_filter.size()) {
                col_is_filter[c] = 1;
            } else {
                in_bounds = false;
            }
        }
        if (!in_bounds || filter_cols.empty()) {
            // Predicate references something outside the scan schema -> decode fully.
            scan_filter = nullptr;
            filter_cols.clear();
            col_is_filter.assign(schema.size(), 0);
        }
    }
    std::vector<uint32_t> rest_cols = RemainingColumns(num_cols, col_is_filter);
    std::vector<uint32_t> probe_early_cols, probe_rest_cols;
    std::unique_ptr<exec::DynamicFilterProbe> probe;
    uint32_t probe_col = 0;
    bool probe_checked = false; // every region carries the same filter -> set up once
    std::vector<uint8_t> keep; // reused per window (survivor mask for late materialization)

    for (auto &region_itm : region_itms) {
        const size_t region_key = region_itm.key;
        auto region = parquet::Region::Deserialize(region_itm.buffer);

        if (!probe_checked && region.dynamic_filter_column >= 0) {
            probe_checked = true;
            if (static_cast<uint32_t>(region.dynamic_filter_column) >= num_cols) {
                return Error("Region.dynamic_filter_column exceeds the scan schema.", ErrorKind::OutOfRange);
            }
            probe_col = static_cast<uint32_t>(region.dynamic_filter_column);
            std::optional<exec::DynamicFilter> filter;
            if (!region.dynamic_filter.empty()) {
                filter = DeserializeFromBytes<exec::DynamicFilter>(region.dynamic_filter.data(),
                                                                   region.dynamic_filter.size());
            } else if (auto itms = abi::GetOptionalInputSet(SET_IDX_IN_DYNAMIC_FILTER); itms && !itms->empty()) {
                filter = DeserializeFromBytes<exec::DynamicFilter>((*itms)[0].buffer.data(), (*itms)[0].buffer.size());
                for (size_t i = 1; i < itms->size(); i++) {
                    auto &itm = (*itms)[i];
                    filter->Merge(DeserializeFromBytes<exec::DynamicFilter>(itm.buffer.data(), itm.buffer.size()));
                }
                if (!filter->has_bounds) {
                    // The build side has no non-NULL key at all: the inner join can't produce any
                    // row, so nothing of the scan is needed.
                    if (auto *dyn_filter = executor.FindDynamicFilterBuild()) {
                        abi::AddOutput("dynamic_filter", SET_IDX_OUT_DYNAMIC_FILTER, SerializeToBuffer(dyn_filter->Filter()));
                    }
                    return Ok();
                }
            }
            if (filter) {
                TRY(probe, exec::DynamicFilterProbe::Create(std::move(*filter), types[probe_col]));
            }
            probe_early_cols = filter_cols;
            if (col_is_filter[probe_col] == 0) {
                probe_early_cols.push_back(probe_col);
            }
            col_is_filter[probe_col] = 1;
            probe_rest_cols = RemainingColumns(num_cols, col_is_filter);
        }

        for (auto &rg : region.row_groups) {
            const size_t ncol = rg.size();
            if (ncol != schema.size()) {
                return Error("Region row group's column count does not match the scan schema.",
                             ErrorKind::InvalidInput);
            }

            std::vector<const uint8_t *> bases(ncol);
            for (size_t c = 0; c < ncol; c++) {
                auto &dc = rg[c];
                auto it = data_chunk_index.find({region_key, dc.req_idx});
                if (it == data_chunk_index.end()) {
                    return Error("Plume: no fetched buffer for parquet chunk request", ErrorKind::InvalidInput);
                }
                bases[c] = it->second->data() + dc.req_offset;
            }

            std::vector<parquet::ColumnChunkReader> readers;
            size_t num_rows = 0;
            TRYV(TryCatch([&] {
                readers.reserve(ncol);
                for (size_t c = 0; c < ncol; c++) {
                    const auto &col = schema.columns[c];
                    readers.emplace_back(bases[c], static_cast<size_t>(rg[c].size), col.type, col.nullable,
                                         rg[c].codec);
                    num_rows = std::max(num_rows, readers.back().TotalRows());
                }
            }));

            for (size_t start = 0; start < num_rows; start += STANDARD_VECTOR_SIZE) {
                const size_t n = std::min<size_t>(STANDARD_VECTOR_SIZE, num_rows - start);
                auto chunk = std::make_unique<DataChunk>();
                chunk->Initialize(alloc.Get(), types);

                const bool use_filter = scan_filter != nullptr;
                const bool use_probe = probe && probe->Active();
                const auto &early_cols = use_probe ? probe_early_cols : filter_cols;
                const auto &late_cols = use_probe ? probe_rest_cols : rest_cols;

                bool emit = true;
                TRYV(TryCatch([&] {
                    if (!use_filter && !use_probe) {
                        for (size_t c = 0; c < ncol; c++) {
                            readers[c].ReadInto(chunk->data[c], n);
                        }
                        chunk->SetCardinality(n);
                        return;
                    }

                    // 1. Decode the early columns and evaluate the scan and the dynamic filters.
                    for (auto c : early_cols) {
                        readers[c].ReadInto(chunk->data[c], n);
                    }
                    chunk->SetCardinality(n);
                    duckdb::SelectionVector sel(n);
                    duckdb::idx_t sc = n;
                    if (use_filter) {
                        sc = scan_filter->Select(*chunk, sel);
                    } else {
                        for (duckdb::idx_t j = 0; j < n; j++) {
                            sel.set_index(j, j);
                        }
                    }
                    if (use_probe && sc > 0) {
                        sc = probe->Select(chunk->data[probe_col], n, sel, sc);
                    }
                    if (sc == 0) {
                        // Whole window rejected: advance the other readers, emit nothing.
                        for (auto c : late_cols) {
                            readers[c].Skip(n);
                        }
                        emit = false;
                        return;
                    }

                    // 2. Late-materialize the remaining columns for survivors only.
                    keep.assign(n, 0);
                    for (duckdb::idx_t j = 0; j < sc; j++) {
                        keep[sel.get_index(j)] = 1;
                    }
                    for (auto c : late_cols) {
                        readers[c].ReadInto(chunk->data[c], n, keep.data());
                    }

                    // 3. Compact every column to the surviving rows.
                    chunk->Slice(sel, sc);
                }));
                if (!emit) {
                    continue;
                }
                TRYV(executor.Push(std::move(chunk)));
            }
        }
    }

    TRYV(executor.Finish());
    if (auto *dyn_filter = executor.FindDynamicFilterBuild()) {
        abi::AddOutput("dynamic_filter", SET_IDX_OUT_DYNAMIC_FILTER, SerializeToBuffer(dyn_filter->Filter()));
    }

#if defined(PLUME_VERBOSE) && PLUME_VERBOSE >= 1
    alloc.PrintStats();
#endif

    return Ok();
}

} // namespace plume::fn
