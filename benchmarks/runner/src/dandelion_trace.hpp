#pragma once

#include "plume/common/result.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace plume::bench {

// Collects the execution traces dandelion returns with each response and exports them in the 
// compact binary `.plmt` format.
class DandelionTraceCollector {
  public:
    explicit DandelionTraceCollector(std::vector<std::string> ts_names);
    
    // Fetches the timestamp names from `<base_url>/trace/timestamps`.
    static Result<DandelionTraceCollector> Init(const std::string &base_url, int timeout_ms = 10000);

    // Decodes one trace (a JSON invocation tree) and records it as a run of `query`.
    // Empty traces or errors are ignored and nothing is recorded.
    Result<void> AddTrace(const std::string &query, const std::string &trace);

    // Writes all collected runs gzip-compressed to `path` (conventionally `*.plmt.gz`).
    Result<void> ExportToFile(const std::string &path) const;

    size_t Len() const { return num_runs_; }
    bool Empty() const { return num_runs_ == 0; }

    const std::vector<std::string> &TimestampNames() const { return ts_names_; }

  private:
    static constexpr uint8_t kHasTs = 1u << 0;
    static constexpr uint8_t kHasData = 1u << 1;
    static constexpr uint32_t kNoParent = 0xFFFFFFFFu;

    struct DataEntry {
        uint32_t items = 0;
        uint64_t size = 0;
    };

    struct Record {
        uint32_t parent = kNoParent;
        uint16_t group = 0;
        uint16_t id = 0;
        uint32_t node = 0;
        uint32_t data_begin = 0; // index into Run::data
        uint16_t n_data = 0;
    };

    struct Run {
        uint8_t flags = 0;
        std::vector<std::string> ids;
        std::vector<Record> records;
        std::vector<uint64_t> ts;     // n_ts per record if kHasTs
        std::vector<DataEntry> data;  // n_data per record if kHasData
    };

    struct Query {
        std::string name;
        std::vector<Run> runs;
    };

    std::vector<std::string> ts_names_;
    std::vector<Query> queries_;                         // in order of first appearance
    std::unordered_map<std::string, size_t> query_idx_;  // name -> index into queries_
    size_t num_runs_ = 0;
};

} // namespace plume::bench
