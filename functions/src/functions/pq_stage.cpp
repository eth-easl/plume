#include "plume/abi/abi.hpp"
#include "plume/execution/executor.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/util/dynamic_filter.hpp"
#include "plume/util/output_split.hpp"
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
// The fourth input set (index 3) is optional and holds the dynamic filters computed by upstream
// stages.
#define SET_IDX_IN_DYNAMIC_FILTER 3

// The first output set (index 0) contains the normal row output.
// The second output set (index 1) contains the dynamic filters each as a single item.
#define SET_IDX_OUT_DYNAMIC_FILTER 1

namespace plume::fn {

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;

namespace {

uint64_t ParseIdent(const std::string &s) { return s.empty() ? 0 : std::stoull(s); }

// Rows [first, second) of a row group, sorted and disjoint.
using RowRanges = std::vector<std::pair<uint64_t, uint64_t>>;

RowRanges Intersect(const RowRanges &a, const RowRanges &b) {
    RowRanges out;
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        const uint64_t begin = std::max(a[i].first, b[j].first);
        const uint64_t end = std::min(a[i].second, b[j].second);
        if (begin < end) {
            out.emplace_back(begin, end);
        }
        (a[i].second < b[j].second) ? i++ : j++;
    }
    return out;
}

// The number of rows a column chunk holding only the `covered` rows holds before `row`.
uint64_t StreamOffset(const RowRanges &covered, uint64_t row) {
    uint64_t offset = 0;
    for (auto [begin, end] : covered) {
        if (begin >= row) {
            break;
        }
        offset += std::min(end, row) - begin;
    }
    return offset;
}

std::vector<uint32_t> RemainingColumns(uint32_t num_cols, const std::vector<uint8_t> &col_is_filter) {
    std::vector<uint32_t> rest;
    for (uint32_t c = 0; c < num_cols; c++) {
        if (col_is_filter[c] == 0) {
            rest.push_back(c);
        }
    }
    return rest;
}

// A dynamic filter evaluated on a scan column.
struct ColumnProbe {
    uint32_t column;
    std::unique_ptr<exec::DynamicFilterProbe> probe;
};

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
        TRYV(executor.Build(desc, util::MakeSink(desc.output_sink, abi::AddOutput)));
    }

    TRY(auto region_itms, abi::GetInputSet(SET_IDX_IN_REGION_INFO));
    TRY(auto data_chunk_itms, abi::GetInputSet(SET_IDX_IN_DATA_CHUNKS));
    if (region_itms.empty()) {
        util::EmitDynamicFilters(executor, SET_IDX_OUT_DYNAMIC_FILTER);
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

    // One probe per dynamic filter (and column) the pipeline names. A filter that isn't bound has
    // nothing to apply.
    std::vector<ColumnProbe> probes;
    {
        TRY(auto dyn_filters, util::ReadDynamicFilters(SET_IDX_IN_DYNAMIC_FILTER));
        for (const auto &spec : desc.dynamic_filter_probes) {
            if (spec.column >= num_cols) {
                return Error("Dynamic filter probe column exceeds the scan schema.", ErrorKind::OutOfRange);
            }
            auto it = dyn_filters.find(spec.filter_id);
            if (it == dyn_filters.end()) {
                continue;
            }
            if (!it->second.has_bounds) {
                // The build side has no non-NULL key at all: the inner join can't produce any
                // row, so nothing of the scan is needed.
                util::EmitDynamicFilters(executor, SET_IDX_OUT_DYNAMIC_FILTER);
                return Ok();
            }
            TRY(auto probe, exec::DynamicFilterProbe::Create(it->second, types[spec.column]));
            if (probe) {
                probes.push_back({spec.column, std::move(probe)});
            }
        }
    }
    // The columns decoded before (early) and after (late) evaluating the scan filter and the
    // active probes, recomputed whenever a probe disables itself.
    std::vector<uint32_t> probe_early_cols, probe_rest_cols;
    std::vector<uint8_t> active_probes; // the probes the columns were computed for
    bool probe_cols_computed = false;
    auto update_probe_cols = [&]() {
        std::vector<uint8_t> active(probes.size());
        for (size_t p = 0; p < probes.size(); p++) {
            active[p] = probes[p].probe->Active() ? 1 : 0;
        }
        if (probe_cols_computed && active == active_probes) {
            return;
        }
        probe_cols_computed = true;
        active_probes = std::move(active);
        std::vector<uint8_t> is_early = col_is_filter;
        probe_early_cols = filter_cols;
        for (size_t p = 0; p < probes.size(); p++) {
            const uint32_t c = probes[p].column;
            if (active_probes[p] && is_early[c] == 0) {
                is_early[c] = 1;
                probe_early_cols.push_back(c);
            }
        }
        probe_rest_cols = RemainingColumns(num_cols, is_early);
    };
    std::vector<uint8_t> keep; // reused per window (survivor mask for late materialization)

    for (auto &region_itm : region_itms) {
        const size_t region_key = region_itm.key;
        auto region = parquet::Region::Deserialize(region_itm.buffer);

        for (auto &rg : region.row_groups) {
            const size_t ncol = rg.size();
            if (ncol != schema.size()) {
                return Error("Region row group's column count does not match the scan schema.",
                             ErrorKind::InvalidInput);
            }

            // A column chunk read only in part (DataChunk::pages) is assembled from its dictionary
            // page and page runs; it holds just the rows of its runs.
            std::vector<const uint8_t *> bases(ncol);
            std::vector<size_t> sizes(ncol);
            std::vector<std::vector<uint8_t>> assembled(ncol);
            std::vector<RowRanges> covered(ncol);
            bool partial = false;
            auto fetched = [&](uint64_t req_idx, uint64_t req_offset, int64_t size) -> Result<const uint8_t *> {
                auto it = data_chunk_index.find({region_key, req_idx});
                if (it == data_chunk_index.end() || req_offset + static_cast<uint64_t>(size) > it->second->size()) {
                    return Error("Plume: no fetched buffer for parquet chunk request", ErrorKind::InvalidInput);
                }
                return it->second->data() + req_offset;
            };
            for (size_t c = 0; c < ncol; c++) {
                auto &dc = rg[c];
                if (dc.pages.empty()) {
                    TRY(bases[c], fetched(dc.req_idx, dc.req_offset, dc.size));
                    sizes[c] = static_cast<size_t>(dc.size);
                    continue;
                }
                partial = true;
                auto &buf = assembled[c];
                if (dc.size > 0) {
                    TRY(auto dict, fetched(dc.req_idx, dc.req_offset, dc.size));
                    buf.insert(buf.end(), dict, dict + dc.size);
                }
                for (const auto &run : dc.pages) {
                    TRY(auto pages, fetched(run.req_idx, run.req_offset, run.size));
                    buf.insert(buf.end(), pages, pages + run.size);
                    covered[c].emplace_back(run.first_row, run.first_row + run.num_rows);
                }
                bases[c] = buf.data();
                sizes[c] = buf.size();
            }

            std::vector<parquet::ColumnChunkReader> readers;
            size_t num_rows = 0;
            TRYV(TryCatch([&] {
                readers.reserve(ncol);
                for (size_t c = 0; c < ncol; c++) {
                    const auto &col = schema.columns[c];
                    readers.emplace_back(bases[c], sizes[c], col.type, col.nullable, rg[c].codec);
                    num_rows = std::max(num_rows, readers.back().TotalRows());
                }
            }));

            // The rows to read: those every column holds.
            RowRanges target = {{0, num_rows}};
            if (partial) {
                std::optional<RowRanges> common;
                for (size_t c = 0; c < ncol; c++) {
                    if (!rg[c].pages.empty()) {
                        common = common ? Intersect(*common, covered[c]) : covered[c];
                    }
                }
                target = std::move(*common);
                for (size_t c = 0; c < ncol; c++) {
                    if (rg[c].pages.empty()) {
                        covered[c] = {{0, readers[c].TotalRows()}};
                    }
                    if (Intersect(target, covered[c]) != target) {
                        return Error("Region columns don't hold the same rows.", ErrorKind::InvalidInput);
                    }
                }
            }

            // A fully dictionary-encoded key column chunk holds no other values than its
            // dictionary: check the dictionary once instead of every row.
            bool rg_rejected = false;
            for (auto &[probe_col, probe] : probes) {
                const duckdb::Vector *dict = nullptr;
                if (probe->Active() && !rg_rejected) {
                    TRYV(TryCatch([&, col = probe_col] { dict = readers[col].Dictionary(types[col]); }));
                }
                const idx_t passed = probe->SetDictionary(dict, dict ? readers[probe_col].DictionaryVectorSize() : 0);
                if (dict && passed == 0) {
                    rg_rejected = true; // no key of the row group can find a join partner
                }
            }
            if (rg_rejected) {
                continue;
            }

            std::vector<uint64_t> stream_pos(ncol, 0); // rows of each column chunk consumed
            for (auto [range_begin, range_end] : target) {
                if (partial) {
                    // Skip the rows of each column chunk before the range.
                    TRYV(TryCatch([&, begin = range_begin] {
                        for (size_t c = 0; c < ncol; c++) {
                            const uint64_t at = StreamOffset(covered[c], begin);
                            readers[c].Skip(at - stream_pos[c]);
                            stream_pos[c] = at;
                        }
                    }));
                }
                for (size_t start = range_begin; start < range_end; start += STANDARD_VECTOR_SIZE) {
                    const size_t n = std::min<size_t>(STANDARD_VECTOR_SIZE, range_end - start);
                    for (auto &pos : stream_pos) {
                        pos += n;
                    }
                    auto chunk = std::make_unique<DataChunk>();
                    chunk->Initialize(alloc.Get(), types);

                    const bool use_filter = scan_filter != nullptr;
                    const bool use_probe = std::any_of(probes.begin(), probes.end(),
                                                       [](const ColumnProbe &p) { return p.probe->Active(); });
                    if (use_probe) {
                        update_probe_cols();
                    }
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
                        if (use_probe) {
                            for (size_t p = 0; p < probes.size() && sc > 0; p++) {
                                // Only the probes the early columns were decoded for.
                                if (active_probes[p]) {
                                    sc = probes[p].probe->Select(chunk->data[probes[p].column], n, sel, sc);
                                }
                            }
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
    }

    TRYV(executor.Finish());
    util::EmitDynamicFilters(executor, SET_IDX_OUT_DYNAMIC_FILTER);

#if defined(PLUME_VERBOSE) && PLUME_VERBOSE >= 1
    alloc.PrintStats();
#endif

    return Ok();
}

} // namespace plume::fn
