#include "plume/abi/abi.hpp"
#include "plume/execution/executor.hpp"
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
#include <string>
#include <utility>
#include <vector>

/// The first input set (index 0) should have one item containing the pipeline template.
#define SET_IDX_IN_PIPELINE_TEMPL 0
/// The second input set (index 1) should have items containing the region infos.
#define SET_IDX_IN_REGION_INFO 1
/// The third input set (index 2) should have items containing the resolved data chunk buffers.
#define SET_IDX_IN_DATA_CHUNKS 2

namespace plume::fn {

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;

namespace {

uint64_t ParseIdent(const std::string &s) { return s.empty() ? 0 : std::stoull(s); }

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

    // filter pushdown
    TRY(exec::FilterOperator *scan_filter, executor.TakeLeadingScanFilter());
    std::vector<uint32_t> filter_cols, rest_cols;
    if (scan_filter) {
        filter_cols = scan_filter->FilterColumns();
        std::vector<uint8_t> is_filter(schema.size(), 0);
        bool in_bounds = true;
        for (auto c : filter_cols) {
            if (c < is_filter.size()) {
                is_filter[c] = 1;
            } else {
                in_bounds = false;
            }
        }
        if (!in_bounds || filter_cols.empty()) {
            scan_filter = nullptr; // predicate references something outside the scan schema; decode fully
        } else {
            for (uint32_t c = 0; c < schema.size(); c++) {
                if (!is_filter[c]) {
                    rest_cols.push_back(c);
                }
            }
        }
    }
    std::vector<uint8_t> keep; // reused per window (survivor mask for late materialization)

    for (auto &region_itm : region_itms) {
        const size_t region_key = region_itm.key;
        auto region = parquet::Region::Deserialize(region_itm.buffer);

        for (auto &rg : region.row_groups) {
            const size_t ncol = rg.size();

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

            const bool use_filter = scan_filter != nullptr && ncol == schema.size();

            for (size_t start = 0; start < num_rows; start += STANDARD_VECTOR_SIZE) {
                const size_t n = std::min<size_t>(STANDARD_VECTOR_SIZE, num_rows - start);
                auto chunk = std::make_unique<DataChunk>();
                chunk->Initialize(alloc.Get(), types);

                bool emit = true;
                TRYV(TryCatch([&] {
                    if (!use_filter) {
                        for (size_t c = 0; c < ncol; c++) {
                            readers[c].ReadInto(chunk->data[c], n);
                        }
                        chunk->SetCardinality(n);
                        return;
                    }

                    // 1. Decode the filter columns and evaluate the predicate.
                    for (auto c : filter_cols) {
                        readers[c].ReadInto(chunk->data[c], n);
                    }
                    chunk->SetCardinality(n);
                    duckdb::SelectionVector sel(n);
                    const duckdb::idx_t sc = scan_filter->Select(*chunk, sel);
                    if (sc == 0) {
                        // Whole window rejected: advance the other readers, emit nothing.
                        for (auto c : rest_cols) {
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
                    for (auto c : rest_cols) {
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

#if defined(PLUME_VERBOSE) && PLUME_VERBOSE >= 1
    alloc.PrintStats();
#endif

    return Ok();
}

} // namespace plume::fn
