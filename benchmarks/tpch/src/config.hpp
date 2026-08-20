#pragma once

#include "plume/common/result.hpp"
#include "plume/parser/converter.hpp"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace plume::bench {

// A single-scale-factor data source. A `[table]` placeholder in a query is
// resolved to `<path_prefix>/<table>/<table><path_suffix>` for an un-split
// table, or to a bracket-list of `<...>/<table>.<n><path_suffix>` paths when the
// table is split into `num_files[table]` files (matching the layout the tpch
// data is published in).
struct DataSource {
    std::string path_prefix;              // e.g. https://host/tpch10
    std::string path_suffix = ".parquet"; // file extension incl. dot
    std::unordered_map<std::string, uint32_t> num_files; // table -> #split files
};

// Runs each configured query in turn, `repetitions` times.
struct SingleConfig {
    size_t repetitions = 1;
};

// Fires the same query every `interval_ms` for `duration_s` seconds without
// waiting for the previous invocation to finish — measures sustained load.
struct ThroughputConfig {
    std::string query;        // query file to hammer (defaults to queries[0])
    std::vector<double> rps_values = {};
    size_t duration_s = 60;   // total wall time to dispatch for
};

// Replays a redshift-style trace: each entry names a query and a scale factor,
// dispatched at the trace's recorded arrival offset. `scale_factors` maps each
// scale-factor label in the trace to the data source it should read from.
struct TraceConfig {
    std::string path; // trace csv
    std::unordered_map<std::string, DataSource> scale_factors;
    bool closed_loop;
};

struct BenchmarkConfig {
    enum class Type { kSingle, kThroughput, kTrace };

    // Parse + validate a config file. Expands leading `~` in path fields to $HOME.
    static Result<BenchmarkConfig> FromJsonFile(const std::string &path);

    std::vector<std::string> queries; // .sql file names (relative to query_dir)
    std::string query_dir;            // directory holding the .sql files
    std::string dandelion_url;        // empty -> compile only (no invocation)
    int request_timeout_s = 60;
    std::string results_prefix; // output dir for timings/timestamps/planning
    bool debug_prints = false;

    // Scale-factor label for single/throughput modes (e.g. "sf1"), used together
    // with a query's name to look up its expected checksum. Trace mode instead
    // carries a label per entry (see TraceConfig::scale_factors).
    std::string scale_factor;

    // Expected-checksum file (see checksum.hpp); empty -> no correctness check.
    std::string checksum_path;

    // CompileQuery options.
    parser::ConverterConfig converter_config;
    size_t fetcher_threads = 10; // remote-source resolver worker pool size

    DataSource source; // single scale factor (single + throughput modes)

    Type type = Type::kSingle;
    std::variant<SingleConfig, ThroughputConfig, TraceConfig> bench = SingleConfig{};
};

} // namespace plume::bench
