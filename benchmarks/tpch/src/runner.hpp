#pragma once

#include "checksum.hpp"
#include "config.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"
#include "plume/dandelion/composition.hpp"

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

    // Dispatch on the benchmark type, run it, and export the collected results.
    Result<void> Run();

  private:
    // A compiled query invocation (the dandelion request body) plus the wall time
    // its client-side compilation took.
    struct Invocation {
        std::string name;                             // display name (e.g. "q6" or "q6_sf1")
        dandelion::BinaryData body;           // dandelion invocation body
        int64_t planning_ms = 0;                      // CompileQuery wall time
        std::vector<int64_t> latencies_ms;            // per-invocation execution latency
        std::vector<std::string> timestamps;          // per-invocation dandelion timestamps
        size_t num_invocations = 0;

        // Indices into latencies_ms/timestamps marking the start of each run
        // after the first, populated by StartNextRun(). Empty means everything
        // recorded so far belongs to a single run.
        std::vector<size_t> run_starts;

        // Set when config_.checksum_path is non-empty and the checksum file has
        // an entry for this (query, scale factor); checked on every response.
        std::optional<ChecksumEntry> expected_checksum;
        size_t checksum_failures = 0;
    };

    // Compile `query_file` against `source`, returning the index of the resulting
    // Invocation in `invocations_`. Cached by `key` so a (query, scale factor)
    // pair is only compiled once. `checksum_query`/`checksum_scale_factor` name
    // the (query, scale factor) pair to look up in the checksum file — usually
    // the same as `key`'s components, but kept separate since `key` is a single
    // dedup string.
    Result<size_t> BuildInvocation(const std::string &key, const std::string &query_file, const DataSource &source,
                                   const std::string &checksum_query, const std::string &checksum_scale_factor);

    // POST an invocation body to the dandelion instance and hand the response to
    // HandleResponse.
    Result<void> Invoke(size_t idx);

    // Parse a dandelion response for the Invocation at `idx`: on a transport/HTTP
    // error, or a checksum mismatch (if `expected_checksum` is set), returns an
    // Error without recording a latency; otherwise appends the execution latency
    // + timestamps.
    Result<void> HandleResponse(size_t idx, const cpr::Response &resp);

    Result<void> RunSingle(const SingleConfig &sc);
    Result<void> RunThroughput(const ThroughputConfig &tc);
    Result<void> RunTrace(const TraceConfig &trc);

    // Marks the boundary between two runs of the same invocation(s) — e.g.
    // between successive rps values in a throughput sweep — so ExportResults
    // can report per-run stats instead of lumping everything together.
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
    std::optional<ChecksumFile> checksum_file_;       // loaded from config_.checksum_path, if set

    // One connection + catalog for the whole run, so a source referenced by more
    // than one query (or the same query recompiled across scale factors under a
    // different key) is only resolved once. See plume::parser::CompileQuery.
    duckdb::DuckDB db_;
    duckdb::Connection con_;
    std::shared_ptr<catalog::SourceCatalog> catalog_;
};

} // namespace plume::bench
