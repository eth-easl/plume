#include "plume/catalog/parquet.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/remote_resolver.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/dandelion/s3.hpp"
#include "plume/parquet/metadata.hpp"
#include "plume/parquet/prune.hpp"
#include "plume/parquet/regions.hpp"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <string>

namespace plume::catalog {

namespace {

constexpr int64_t kFooterProbe = 256 << 10; // 256 KiB

int64_t LE(const uint8_t *p, size_t n) {
    uint64_t v = 0;
    for (size_t i = 0; i < n; i++) {
        v |= static_cast<uint64_t>(p[i]) << (8 * i);
    }
    return static_cast<int64_t>(v);
}

// Decode a parquet PLAIN-encoded min/max value into a typed duckdb Value plus a sort key for 
// cross-row-group aggregation.
// TODO: missing temporal/decimal/string min/max support
bool DecodeNumeric(const ColumnType &t, const std::vector<uint8_t> &b, double &key, duckdb::Value &val) {
    switch (t.id) {
    case TypeId::INT8:
        if (b.size() < 4) return false;
        val = duckdb::Value::TINYINT(static_cast<int8_t>(LE(b.data(), 4)));
        key = static_cast<double>(static_cast<int8_t>(LE(b.data(), 4)));
        return true;
    case TypeId::INT16:
        if (b.size() < 4) return false;
        val = duckdb::Value::SMALLINT(static_cast<int16_t>(LE(b.data(), 4)));
        key = static_cast<double>(static_cast<int16_t>(LE(b.data(), 4)));
        return true;
    case TypeId::INT32:
        if (b.size() < 4) return false;
        val = duckdb::Value::INTEGER(static_cast<int32_t>(LE(b.data(), 4)));
        key = static_cast<double>(static_cast<int32_t>(LE(b.data(), 4)));
        return true;
    case TypeId::INT64:
        if (b.size() < 8) return false;
        val = duckdb::Value::BIGINT(LE(b.data(), 8));
        key = static_cast<double>(LE(b.data(), 8));
        return true;
    case TypeId::FLOAT: {
        if (b.size() < 4) return false;
        uint32_t bits = static_cast<uint32_t>(LE(b.data(), 4));
        float f;
        std::memcpy(&f, &bits, 4);
        val = duckdb::Value::FLOAT(f);
        key = f;
        return true;
    }
    case TypeId::DOUBLE: {
        if (b.size() < 8) return false;
        uint64_t bits = static_cast<uint64_t>(LE(b.data(), 8));
        double d;
        std::memcpy(&d, &bits, 8);
        val = duckdb::Value::DOUBLE(d);
        key = d;
        return true;
    }
    default:
        return false; // BOOLEAN/VARCHAR/DECIMAL/DATE/TIME/HUGEINT: distinct+null only
    }
}

RemoteColumnStats AggregateColumnStats(const ColumnType &type,
                                       const std::vector<const parquet::ColumnStats *> &chunks) {
    RemoteColumnStats out;
    double min_key = 0, max_key = 0;
    uint64_t null_sum = 0;
    bool all_have_null_count = !chunks.empty();
    for (const auto *cs : chunks) {
        if (cs->has_min) {
            double k;
            duckdb::Value v;
            if (DecodeNumeric(type, cs->min_value, k, v) && (!out.has_min || k < min_key)) {
                out.has_min = true;
                min_key = k;
                out.min_value = std::move(v);
            }
        }
        if (cs->has_max) {
            double k;
            duckdb::Value v;
            if (DecodeNumeric(type, cs->max_value, k, v) && (!out.has_max || k > max_key)) {
                out.has_max = true;
                max_key = k;
                out.max_value = std::move(v);
            }
        }
        if (cs->has_distinct_count) {
            out.has_distinct = true;
            out.distinct_count = std::max(out.distinct_count, static_cast<uint64_t>(cs->distinct_count));
        }
        if (cs->has_null_count) {
            null_sum += static_cast<uint64_t>(cs->null_count);
        } else {
            all_have_null_count = false;
        }
    }
    out.not_null = all_have_null_count && null_sum == 0;
    return out;
}

} // namespace

Result<void> RemoteParquetDataSource::Resolve(const RemoteResolver &resolver) {
    // schema acts to check if this data source has already been resolved.
    if (schema.has_value()) return Ok();

    metadata.assign(paths.size(), parquet::FileMeta());
    cardinalities.assign(paths.size(), 0);
    cardinality_total = 0;

    for (size_t i = 0; i < paths.size(); i++) {
        resolver.Add([this, &resolver, i]() -> Result<void> {
            const DataFetcher &fetch = resolver.fetcher();
            const std::string &url = paths[i];

            TRY(auto file_size, fetch.size(url));

            const int64_t probe = std::min<int64_t>(static_cast<int64_t>(file_size), kFooterProbe);
            TRY(auto tail, fetch.range(url, -probe, probe));

            auto parsed = parquet::ParseFooter(tail.data(), tail.size());
            if (parsed.is_error() && tail.size() >= 8) {
                uint32_t meta_len = 0;
                std::memcpy(&meta_len, tail.data() + tail.size() - 8, 4);
                const int64_t need = static_cast<int64_t>(meta_len) + 8;
                if (need > probe) {
                    TRY(tail, fetch.range(url, -need, need));
                    parsed = parquet::ParseFooter(tail.data(), tail.size());
                }
            }

            TRY(auto meta, std::move(parsed));

            std::vector<RemoteColumnStats> stats;
            stats.resize(meta.schema.columns.size());
            for (size_t c = 0; c < meta.schema.columns.size(); c++) {
                std::vector<const parquet::ColumnStats *> chunks;
                for (const auto &rg : meta.row_groups) {
                    if (c < rg.columns.size()) {
                        chunks.push_back(&rg.columns[c].stats);
                    }
                }
                stats[c] = AggregateColumnStats(meta.schema.columns[c].type, chunks);
            }

            const uint64_t rows = static_cast<uint64_t>(std::max<int64_t>(meta.num_rows, 0));
            {
                static std::mutex mu; // guards the fields shared across this source's path jobs
                std::lock_guard<std::mutex> lock(mu);
                
                if (!schema) {
                    schema = meta.schema;
                }
                if (col_stats.size() < stats.size()) {
                    col_stats.resize(stats.size());
                }
                cardinality_total += rows;

                for (size_t c = 0; c < stats.size(); c++) {
                    if (stats[c].has_min && (!col_stats[c].has_min || stats[c].min_value < col_stats[c].min_value)) {
                        col_stats[c].has_min = true;
                        col_stats[c].min_value = std::move(stats[c].min_value);
                    }
                    if (stats[c].has_max && (!col_stats[c].has_max || stats[c].max_value < col_stats[c].max_value)) {
                        col_stats[c].has_max = true;
                        col_stats[c].max_value = std::move(stats[c].max_value);
                    }
                    if (stats[c].has_distinct) {
                        col_stats[c].has_distinct = true;
                        col_stats[c].distinct_count = std::max(col_stats[c].distinct_count, stats[c].distinct_count);
                    }
                    col_stats[c].not_null = col_stats[c].not_null && stats[c].not_null;
                }
            }
            cardinalities[i] = rows;
            metadata[i] = std::move(meta);
            return Ok();
        });
    }

    return Ok();
}

std::shared_ptr<DataSource> CreateRemoteParquetSource(std::string name, std::vector<std::string> urls) {
    auto pq_src = std::make_shared<RemoteParquetDataSource>();
    pq_src->name = std::move(name);
    pq_src->paths = std::move(urls);
    return pq_src;
}

Result<ParquetStageInputs> BuildParquetStageInputs(const RemoteParquetDataSource &src,
        const std::vector<uint32_t> &projection, const ExprNode *pushed_filter,
        uint32_t num_splits, uint64_t coalesce_distance, uint64_t max_region_bytes) {
    if (!src.schema) {
        return Error("Parquet source has not been resolved (missing schema).", ErrorKind::InvalidInput);
    }
    if (src.metadata.size() != src.paths.size()) {
        return Error("Parquet source has no resolved footer for every file.", ErrorKind::InvalidInput);
    }

    // TODO: apply filter pushdown first, then split according to remaining row groups per file
    uint32_t total = num_splits == 0 ? 1 : num_splits;
    const uint32_t num_files = static_cast<uint32_t>(src.metadata.size());
    total = std::max<uint32_t>(num_files, total);
    const uint32_t base_splits = total / num_files;
    const uint32_t rounding_files = total - base_splits * num_files;

    ParquetStageInputs out;
    uint64_t region_key = 0;
    for (size_t i = 0; i < src.metadata.size(); i++) {
        const parquet::FileMeta &meta = src.metadata[i];
        const std::string &url = src.paths[i];

        // filter pushdown
        std::vector<uint32_t> keep_row_groups;
        if (pushed_filter) {
            for (uint32_t rg = 0; rg < meta.row_groups.size(); rg++) {
                if (parquet::RowGroupMayMatch(*pushed_filter, meta.row_groups[rg], meta.schema)) {
                    keep_row_groups.push_back(rg);
                }
            }
            if (keep_row_groups.empty()) {
                continue; // entire file pruned
            }
        }

        const uint32_t region_splits = i < rounding_files ? base_splits + 1: base_splits;
        auto regions = parquet::DefineRegions(meta, region_splits, coalesce_distance, projection, keep_row_groups, max_region_bytes);

        
        for (size_t r = 0; r < regions.size(); r++) {
            const uint64_t key = region_key++;
            auto region_buf = regions[r].Serialize();
            out.region_info.push_back({
                key, 
                "region", 
                dandelion::BinaryData(region_buf.data(), region_buf.data() + region_buf.size())
            });
            for (size_t ri = 0; ri < regions[r].requests.size(); ri++) {
                const auto &req = regions[r].requests[ri];
                auto req_buf = s3::CreateGetRequest(url, req.size, req.offset);
                out.chunk_reqs.push_back({
                    key, 
                    std::to_string(ri), 
                    dandelion::BinaryData(req_buf.data(), req_buf.data() + req_buf.size())
                });
            }
        }
    }
    return out;
}

} // namespace plume::catalog
