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

// A dynamic filter evaluated on a scan column.
struct DynamicFilterColProbe {
    uint32_t column;
    std::unique_ptr<exec::DynamicFilterProbe> probe;
};

// Tracks and applies a scan_filter and active dyanmic filters while decoding.
// Columns are divided into those decoded before (early) and after (late) evaluating the active 
// filters. Dynamic filters may disable themselves when they rarely reject anything.
class LateMaterializer {
public:
    LateMaterializer(uint32_t num_cols, exec::FilterOperator *scan_filter, std::vector<DynamicFilterColProbe> df_probes)
        : num_cols_(num_cols), scan_filter_(scan_filter), df_probes_(std::move(df_probes)) {
        if (scan_filter_) {
            const auto cols = scan_filter_->FilterColumns();
            const bool in_bounds =
                std::all_of(cols.begin(), cols.end(), [&](uint32_t c) { return c < num_cols_; });
            if (!in_bounds || cols.empty()) {
                // Predicate references something outside the scan schema -> decode fully.
                scan_filter_ = nullptr;
            }
        }
        Recompute();
    }

    // Recomputes the column split if a probe disabled itself since the last call.
    void Refresh() {
        for (size_t p = 0; p < df_probes_.size(); p++) {
            if (active_probes_[p] && !df_probes_[p].probe->Active()) {
                Recompute();
                return;
            }
        }
    }

    bool Active() const { return scan_filter_ != nullptr || num_active_probes_ > 0; }

    const std::vector<uint32_t> &EarlyColumns() const { return early_cols_; }
    const std::vector<uint32_t> &LateColumns() const { return late_cols_; }

    std::vector<DynamicFilterColProbe> &Probes() { return df_probes_; }

    // Evaluates the scan filter and the active probes on the `n` rows of `chunk`, of which (only)
    // the early columns must be decoded. Writes the surviving rows to `sel` and returns their count.
    duckdb::idx_t Select(DataChunk &chunk, idx_t n, duckdb::SelectionVector &sel) {
        idx_t count = n;
        if (scan_filter_) {
            count = scan_filter_->Select(chunk, sel);
        } else {
            for (idx_t j = 0; j < n; j++) {
                sel.set_index(j, j);
            }
        }
        for (size_t p = 0; p < df_probes_.size() && count > 0; p++) {
            if (active_probes_[p]) {
                count = df_probes_[p].probe->Select(chunk.data[df_probes_[p].column], n, sel, count);
            }
        }
        return count;
    }

    // The mask over the `n` rows marking the `count` survivors in `sel`. Valid until the next call.
    const uint8_t *SurvivorMask(const duckdb::SelectionVector &sel, idx_t count, idx_t n) {
        keep_.assign(n, 0);
        for (idx_t j = 0; j < count; j++) {
            keep_[sel.get_index(j)] = 1;
        }
        return keep_.data();
    }

private:
    void Recompute() {
        std::vector<uint8_t> is_early(num_cols_, 0);
        early_cols_.clear();
        late_cols_.clear();
        auto add_early = [&](uint32_t c) {
            if (is_early[c] == 0) {
                is_early[c] = 1;
                early_cols_.push_back(c);
            }
        };
        if (scan_filter_) {
            for (auto c : scan_filter_->FilterColumns()) {
                add_early(c);
            }
        }
        active_probes_.assign(df_probes_.size(), 0);
        num_active_probes_ = 0;
        for (size_t p = 0; p < df_probes_.size(); p++) {
            if (df_probes_[p].probe->Active()) {
                active_probes_[p] = 1;
                num_active_probes_++;
                add_early(df_probes_[p].column);
            }
        }
        for (uint32_t c = 0; c < num_cols_; c++) {
            if (is_early[c] == 0) {
                late_cols_.push_back(c);
            }
        }
    }

    uint32_t num_cols_;
    exec::FilterOperator *scan_filter_;
    std::vector<DynamicFilterColProbe> df_probes_;

    std::vector<uint8_t> active_probes_; // the probes the column split was computed for
    size_t num_active_probes_ = 0;
    std::vector<uint32_t> early_cols_, late_cols_;
    std::vector<uint8_t> keep_;
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
    const auto schema_ncol = static_cast<uint32_t>(schema.size());
    duckdb::vector<LogicalType> types;
    types.reserve(schema_ncol);
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }

    for (const auto &spec : desc.dynamic_filter_probes) {
        if (spec.column >= schema_ncol) {
            return Error("Dynamic filter probe column exceeds the scan schema.", ErrorKind::OutOfRange);
        }
    }
    TRY(auto dyn_filters, util::BindDynamicFilters(SET_IDX_IN_DYNAMIC_FILTER, desc.dynamic_filter_probes));
    if (!dyn_filters) {
        // An empty build side: nothing of the scan is needed.
        util::EmitDynamicFilters(executor, SET_IDX_OUT_DYNAMIC_FILTER);
        return Ok();
    }
    std::vector<DynamicFilterColProbe> probes;
    for (auto &[column, filter] : *dyn_filters) {
        TRY(auto probe, exec::DynamicFilterProbe::Create(std::move(filter), types[column]));
        if (probe) {
            probes.push_back({column, std::move(probe)});
        }
    }
    TRY(exec::FilterOperator *scan_filter, executor.TakeLeadingScanFilter());
    LateMaterializer late_mat(schema_ncol, scan_filter, std::move(probes));

    for (auto &region_itm : region_itms) {
        const size_t region_key = region_itm.key;
        auto region = parquet::Region::Deserialize(region_itm.buffer);

        for (auto &rg : region.row_groups) {
            const size_t ncol = rg.size();
            if (ncol != schema_ncol) {
                return Error("Region row group's column count does not match the scan schema.",
                             ErrorKind::InvalidInput);
            }

            // collect base pointers and sizes
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

            // column readers
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

            // row ranges to read
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

            // apply pushed filter probes
            bool rg_rejected = false;
            for (auto &[probe_col, probe] : late_mat.Probes()) {
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

            // decode remaining data
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

                    late_mat.Refresh();
                    bool emit = true;
                    TRYV(TryCatch([&] {
                        if (!late_mat.Active()) {
                            for (size_t c = 0; c < ncol; c++) {
                                readers[c].ReadInto(chunk->data[c], n);
                            }
                            chunk->SetCardinality(n);
                            return;
                        }

                        // 1. Decode the early columns and evaluate the scan and the dynamic filters.
                        for (auto c : late_mat.EarlyColumns()) {
                            readers[c].ReadInto(chunk->data[c], n);
                        }
                        chunk->SetCardinality(n);
                        duckdb::SelectionVector sel(n);
                        const idx_t sc = late_mat.Select(*chunk, n, sel);
                        if (sc == 0) {
                            // Whole window rejected: advance the other readers, emit nothing.
                            for (auto c : late_mat.LateColumns()) {
                                readers[c].Skip(n);
                            }
                            emit = false;
                            return;
                        }

                        // 2. Late-materialize the remaining columns for survivors only.
                        const uint8_t *keep = late_mat.SurvivorMask(sel, sc, n);
                        for (auto c : late_mat.LateColumns()) {
                            readers[c].ReadInto(chunk->data[c], n, keep);
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
