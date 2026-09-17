#pragma once

#include "checksum.hpp"
#include "config.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <cpr/cpr.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace plume::bench {

class Runner {
  public:
    explicit Runner(BenchmarkConfig config);

    Result<void> Run();

  private:
    // A compiled query invocation.
    struct Invocation {
        std::string name;                     // display name (e.g. "q6" or "q6_sf1")
        dandelion::BinaryData body;           // dandelion invocation body
        Schema out_schema;                    // schema of the result
        int64_t planning_ms = 0;              // CompileQuery wall time
        std::vector<int64_t> latencies_ms;    // per-invocation execution latency
        std::vector<std::string> timestamps;  // per-invocation dandelion timestamps
        size_t num_invocations = 0;

        // Indicates into latencies_ms/timestamps marking the start of each run after the first.
        // Empty if everything belongs to a single run.
        std::vector<size_t> run_starts;

        // Checksums to check responses for correctness if set.
        std::optional<ChecksumEntry> expected_checksum;
        size_t checksum_failures = 0;
    };

    struct InvocationResponse {
        cpr::Response response;
        std::optional<int64_t> latency_ms;
    };

    struct ThroughputEvent {
        size_t run;
        double target_rps;
        size_t seq;
        bool success;
        double scheduled_s;
        double submitted_s;
        double completed_s;
    };

    Result<size_t> BuildInvocation(const std::string &key, const std::string &query_file, 
        const DataSource &source, const std::string &checksum_query, const std::string &checksum_scale_factor);

    Result<void> Invoke(size_t idx, std::string *resp_string = nullptr);
    Result<InvocationResponse> PerformRequest(size_t idx);

    Result<void> HandleResponse(size_t idx, const cpr::Response &resp,
                                std::string *resp_string = nullptr,
                                std::optional<int64_t> latency_ms = std::nullopt);

    Result<void> RunSingle(const SingleConfig &sc);
    Result<void> RunThroughput(const ThroughputConfig &tc);
    Result<void> RunTrace(const TraceConfig &trc);

    void StartNextRun();

    Result<void> ExportResults();

    template <typename... Args>
    void LogDebug(Args &&...args) const {
        if (config_.debug_prints) {
            (std::cout << ... << args) << std::endl;
        }
    }
    template <typename... Args>
    void LogInfo(Args &&...args) const {
        (std::cout << ... << args) << std::endl;
    }

    BenchmarkConfig config_;
    std::vector<Invocation> invocations_;
    std::unordered_map<std::string, size_t> by_key_; // dedup key -> invocations_ index
    std::optional<ChecksumFile> checksum_file_;      // loaded from config_.checksum_path, if set
    std::vector<ThroughputEvent> throughput_events_;

    duckdb::DuckDB db_;
    duckdb::Connection con_;
    std::shared_ptr<catalog::SourceCatalog> catalog_;
};

} // namespace plume::bench
