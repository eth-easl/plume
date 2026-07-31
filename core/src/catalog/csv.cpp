#include "plume/catalog/csv.hpp"
#include "plume/csv/csv.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/remote_resolver.hpp"
#include "plume/common/serial.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/dandelion/s3.hpp"

#include <algorithm>
#include <mutex>
#include <string>

namespace plume::catalog {

namespace {

constexpr int64_t kHeadProbe = 64 << 10; // 64 KiB — comfortably covers the header + a sample of rows.

Schema ProjectSchema(const Schema &full, const std::vector<uint32_t> &projection) {
    if (projection.empty()) {
        return full;
    }
    Schema out;
    out.columns.reserve(projection.size());
    for (uint32_t idx : projection) {
        out.columns.push_back(full.columns[idx]);
    }
    return out;
}

} // namespace

Result<void> RemoteCSVDataSource::Resolve(const RemoteResolver &resolver) {
    // schema acts to check if this data source has already been resolved.
    if (schema.has_value()) return Ok();

    file_sizes.assign(paths.size(), 0);
    cardinalities.assign(paths.size(), 0);
    cardinality_total = 0;
    total_size = 0;

    for (size_t i = 0; i < paths.size(); i++) {
        resolver.Add([this, &resolver, i]() -> Result<void> {
            const DataFetcher &fetch = resolver.fetcher();
            const std::string &url = paths[i];

            TRY(auto file_size, fetch.size(url));
            TRY(auto head, fetch.range(url, 0, std::min<int64_t>(static_cast<int64_t>(file_size), kHeadProbe)));

            TRY(auto file_schema, csv::SniffSchema(head.data(), head.size(), csv_options));

            // Estimate this file's row count from the average data-row byte length in the sampled head.
            size_t pos = 0;
            if (csv_options.has_header) {
                size_t line_end = 0;
                csv::SplitCSVLine(head.data(), head.size(), static_cast<char>(csv_options.delimiter), line_end);
                pos += line_end;
            }
            const size_t data_start = pos;
            size_t sampled_rows = 0;
            while (pos < head.size()) {
                size_t line_end = 0;
                csv::SplitCSVLine(head.data() + pos, head.size() - pos, static_cast<char>(csv_options.delimiter),
                                  line_end);
                if (line_end == 0) {
                    break;
                }
                pos += line_end;
                sampled_rows++;
            }
            uint64_t rows = 0;
            const size_t sampled_bytes = pos - data_start;
            if (sampled_rows > 0 && sampled_bytes > 0) {
                const double avg = static_cast<double>(sampled_bytes) / static_cast<double>(sampled_rows);
                const double data_bytes = static_cast<double>(file_size) - static_cast<double>(data_start);
                rows = static_cast<uint64_t>(std::max(1.0, data_bytes / avg));
            }

            {
                static std::mutex mu; // guards the fields shared across this source's path jobs
                std::lock_guard<std::mutex> lock(mu);
                if (!schema) {
                    schema = std::move(file_schema);
                }
                cardinality_total += rows;
                total_size += file_size;
            }
            file_sizes[i] = file_size;
            cardinalities[i] = rows;
            return Ok();
        });
    }

    return Ok();
}

std::shared_ptr<DataSource> CreateRemoteCSVSource(std::string name, std::vector<std::string> urls, 
        csv::CSVOptions opt) {
    auto csv_src = std::make_shared<RemoteCSVDataSource>();
    csv_src->name = std::move(name);
    csv_src->paths = std::move(urls);
    csv_src->csv_options = std::move(opt);
    return csv_src;
}

Result<CSVStageInputs> BuildCSVStageInputs(const RemoteCSVDataSource &src, const std::vector<uint32_t> &projection,
                                           uint32_t num_splits, uint64_t max_region_bytes) {
    if (!src.schema) {
        return Error("CSV source has not been resolved (missing schema).", ErrorKind::InvalidInput);
    }
    if (src.file_sizes.size() != src.paths.size()) {
        return Error("CSV source has no resolved size for every file.", ErrorKind::InvalidInput);
    }

    uint32_t total = num_splits == 0 ? 1 : num_splits;
    const uint32_t num_files = std::max<uint32_t>(1, static_cast<uint32_t>(src.paths.size()));
    total = std::max<uint32_t>(num_files, total);
    const uint32_t base_splits = total / num_files;
    const uint32_t rounding_files = total - base_splits * num_files;

    const Schema read_schema = ProjectSchema(*src.schema, projection);
    constexpr uint64_t kLineMargin = 1 << 20; // over-read per split to finish a straddling record

    CSVStageInputs out;
    uint64_t region_key = 0;
    for (size_t f = 0; f < src.paths.size(); f++) {
        const std::string &url = src.paths[f];
        const uint64_t total_size = src.file_sizes[f];

        uint32_t n = f < rounding_files ? base_splits + 1 : base_splits;
        // `max_region_bytes` takes precedence over `num_splits` and can force more splits than that.
        if (max_region_bytes > 0 && total_size > 0) {
            const uint64_t needed = (total_size + max_region_bytes - 1) / max_region_bytes;
            n = std::max<uint32_t>(n, static_cast<uint32_t>(std::min<uint64_t>(needed, total_size)));
        }
        n = std::max<uint32_t>(1, n);

        const uint64_t chunk = std::max<uint64_t>(1, (total_size + n - 1) / n);
        for (uint32_t i = 0; i < n; i++) {
            const uint64_t start = static_cast<uint64_t>(i) * chunk;
            if (start >= total_size) {
                break;
            }
            const uint64_t end = std::min(start + chunk, total_size);
            const uint64_t fetch_end = std::min(end + kLineMargin, total_size);
            const uint64_t key = region_key++;

            auto req = s3::CreateGetRequest(url, static_cast<int64_t>(fetch_end - start), static_cast<int64_t>(start));
            out.chunk_reqs.push_back(
                {key, "csv_chunk", dandelion::BinaryData(req.data(), req.data() + req.size())});

            csv::CSVRegionInfo region_info;
            region_info.schema = read_schema;
            region_info.options = src.csv_options;
            region_info.projection = projection;
            region_info.logical_end = end - start;
            auto info_buf = SerializeToBuffer(region_info);
            out.chunk_info.push_back(
                {key, "chunk_info", dandelion::BinaryData(info_buf.data(), info_buf.data() + info_buf.size())});
        }
    }
    return out;
}

} // namespace plume::catalog
