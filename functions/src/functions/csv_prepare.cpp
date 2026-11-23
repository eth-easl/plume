#include "plume/abi/abi.hpp"
#include "plume/common/serial.hpp"
#include "plume/csv/csv.hpp"
#include "plume/dandelion/s3.hpp"
#include "plume/functions/csv.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

// The first input set (index 0) should have one item containing the csv reading configuration.
#define SET_IDX_IN_CONFIG 0
// The second input set (index 1) should have one item containing the csv header which is expected 
// to contain the entire header column of the csv.
#define SET_IDX_IN_HEADER 1
// The third input set (index 2) should have one item containing url of the csv.
#define SET_IDX_IN_URL 2

// The first output set (index 0) contains the csv chunk infos.
#define SET_IDX_OUT_CHUNK_INFO 0
// The second output set (index 1) contains the csv chunk requests to load the data.
#define SET_IDX_OUT_CHUNK_REQ 1

namespace plume::fn {

namespace {

std::vector<std::string> HeaderNames(const DataBuffer &buf, char delim, size_t& end) {
    std::vector<std::string> names;
    const char *b = reinterpret_cast<const char *>(buf.data());
    size_t n = buf.size();
    end = 0;
    while (end < n && b[end] != '\n') {
        end++;
    }
    if (end > 0 && b[end - 1] == '\r') {
        end--;
    }
    size_t start = 0;
    for (size_t i = 0; i < end; i++) {
        if (b[i] == delim) {
            names.emplace_back(b + start, i - start);
            start = i + 1;
        }
    }
    names.emplace_back(b + start, end - start);
    return names;
}

} // namespace

Result<void> RunCSVPrepare() {
    TRY(auto config_set, abi::GetInputSingleton(SET_IDX_IN_CONFIG));
    auto config = DeserializeFromBytes<csv::CSVConfig>(config_set.buffer.data(), config_set.buffer.size());
    const uint32_t num_splits = config.num_splits == 0 ? 1 : config.num_splits;

    TRY(auto header_itms, abi::GetInputSet(SET_IDX_IN_HEADER));
    TRY(auto url_itms, abi::GetInputSet(SET_IDX_IN_URL));
    if (header_itms.size() != url_itms.size()) {
        return Error("Number of header items does not match number of url items.", ErrorKind::InvalidInput);
    }
    if (header_itms.empty()) {
        return Ok();
    }
    std::map<size_t, const DataBuffer *> url_buf_index;
    for (auto &url_buf : url_itms) {
        url_buf_index[url_buf.key] = &url_buf.buffer;
    }

    for (auto& header_itm : header_itms) {
        auto it = url_buf_index.find(header_itm.key);
        if (it == url_buf_index.end()) {
            return Error("Did not find url item with matching key.");
        }
        auto url = std::string(reinterpret_cast<const char *>(it->second->data()), it->second->size());

        // TODO: in the future resolve types from the first record.
        csv::CSVRegionInfo chunk_info;
        chunk_info.schema = config.schema;
        chunk_info.options = config.options;
        chunk_info.projection = config.projection; // projection pushdown (empty ⇒ read all fields)
        if (config.options.has_header && config.resolve_names_from_header) {
            size_t header_end = 0;
            auto names = HeaderNames(header_itm.buffer, static_cast<char>(config.options.delimiter), header_end);
            for (size_t i = 0; i < chunk_info.schema.columns.size(); i++) {
                const size_t src = config.projection.empty() ? i : config.projection[i];
                if (src < names.size()) {
                    chunk_info.schema.columns[i].name = names[src];
                }
            }
        }

        const uint64_t total_size = config.total_size;
        const uint64_t chunk = std::max<uint64_t>(1, (total_size + num_splits - 1) / num_splits);
        const uint32_t key = header_itm.key * num_splits;
        for (uint32_t i = 0; i < num_splits; i++) {
            const uint64_t start = static_cast<uint64_t>(i) * chunk;
            if (start >= total_size) {
                break;
            }
            const uint64_t end = std::min(start + chunk, total_size);
            const uint64_t fetch_end = std::min(end + config.line_margin, total_size);
            const size_t chunk_key = key + i;

            auto req = s3::CreateGetRequest(url, static_cast<int64_t>(fetch_end - start), static_cast<int64_t>(start));
            abi::AddOutput("csv_chunk", SET_IDX_OUT_CHUNK_REQ, std::move(req), chunk_key);

            chunk_info.logical_end = end - start;
            abi::AddOutput("chunk_info", SET_IDX_OUT_CHUNK_INFO, SerializeToBuffer(chunk_info), chunk_key);
        }
    }

    return Ok();
}

} // namespace plume::fn
