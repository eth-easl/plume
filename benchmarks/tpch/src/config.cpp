#include "config.hpp"

#include <nlohmann/json.hpp>

#include <cstdlib>
#include <fstream>

namespace plume::bench {

namespace {

using json = nlohmann::json;

// Expand a leading `~` to $HOME (mirrors the old python runner's path handling).
std::string ExpandHome(const std::string &p) {
    if (p.empty() || p[0] != '~') {
        return p;
    }
    const char *home = std::getenv("HOME");
    if (!home) {
        return p;
    }
    return std::string(home) + p.substr(1);
}

// Read a DataSource out of an object carrying tablePathPrefix / tablePathSuffix /
// tableNumFiles. Used both for the top-level source and for each trace scale factor.
DataSource ParseDataSource(const json &obj) {
    DataSource src;
    if (obj.contains("tablePathPrefix")) {
        src.path_prefix = ExpandHome(obj.at("tablePathPrefix").get<std::string>());
    }
    if (obj.contains("tablePathSuffix")) {
        src.path_suffix = obj.at("tablePathSuffix").get<std::string>();
    }
    if (obj.contains("tableNumFiles")) {
        for (auto &[table, n] : obj.at("tableNumFiles").items()) {
            src.num_files[table] = n.get<uint32_t>();
        }
    }
    return src;
}

} // namespace

Result<BenchmarkConfig> BenchmarkConfig::FromJsonFile(const std::string &path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        return Error("Could not open config file '" + path + "'.", ErrorKind::InvalidInput);
    }

    json data;
    try {
        file >> data;
    } catch (const std::exception &e) {
        return Error("Failed to parse config JSON: " + std::string(e.what()), ErrorKind::InvalidInput);
    }

    BenchmarkConfig cfg;
    try {
        if (data.contains("queries")) {
            cfg.queries = data.at("queries").get<std::vector<std::string>>();
        }
        if (data.contains("queryStoragePrefix")) {
            cfg.query_dir = ExpandHome(data.at("queryStoragePrefix").get<std::string>());
        }
        if (data.contains("dandelionUrl")) {
            cfg.dandelion_url = data.at("dandelionUrl").get<std::string>();
        }
        if (data.contains("requestTimeout")) {
            cfg.request_timeout_s = data.at("requestTimeout").get<int>();
        }
        if (data.contains("resultsPrefix")) {
            cfg.results_prefix = ExpandHome(data.at("resultsPrefix").get<std::string>());
        }
        if (data.contains("debugPrints")) {
            cfg.debug_prints = data.at("debugPrints").get<bool>();
        }
        if (data.contains("scaleFactor")) {
            cfg.scale_factor = data.at("scaleFactor").get<std::string>();
        }
        if (data.contains("checksumFile")) {
            cfg.checksum_path = ExpandHome(data.at("checksumFile").get<std::string>());
        }
        if (data.contains("preAggregate")) {
            cfg.converter_config.early_aggregation = data.at("preAggregate").get<bool>();
        }
        if (data.contains("projectionPushdown")) {
            cfg.converter_config.projection_pushdown = data.at("projectionPushdown").get<bool>();
        }
        if (data.contains("filterPushdown")) {
            cfg.converter_config.filter_pushdown = data.at("filterPushdown").get<bool>();
        }
        if (data.contains("maxSplits")) {
            cfg.converter_config.max_splits = data.at("maxSplits").get<uint32_t>();
        }
        if (data.contains("targetRowsPerSplit")) {
            cfg.converter_config.target_rows_per_split = data.at("targetRowsPerSplit").get<uint64_t>();
        }
        if (data.contains("maxRegionSize")) {
            cfg.converter_config.max_region_bytes = data.at("maxRegionSize").get<uint64_t>();
        }

        cfg.source = ParseDataSource(data);

        std::string type = data.value("benchmarkType", "single");
        const json bench = data.value("benchmarkConfig", json::object());
        if (type == "single") {
            cfg.type = Type::kSingle;
            SingleConfig sc;
            sc.repetitions = bench.value("repetitions", data.value("repetitions", 1));
            cfg.bench = sc;
        } else if (type == "throughput") {
            cfg.type = Type::kThroughput;
            ThroughputConfig tc;
            tc.query = bench.value("query", cfg.queries.empty() ? std::string() : cfg.queries.front());
            tc.rps_values = bench.value("rpsValues", std::vector<double>{});
            tc.duration_s = bench.value("durationSec", size_t{60});
            if (tc.query.empty()) {
                return Error("throughput benchmark needs a 'query' (or a non-empty queries list).",
                             ErrorKind::InvalidInput);
            }
            if (tc.rps_values.empty()) {
                return Error("throughput benchmark needs a non-empty 'rpsValues'.", ErrorKind::InvalidInput);
            }
            for (double rps : tc.rps_values) {
                if (rps <= 0.0) {
                    return Error("throughput benchmark 'rpsValues' entries must be > 0.", ErrorKind::InvalidInput);
                }
            }
            cfg.bench = tc;
        } else if (type == "trace") {
            cfg.type = Type::kTrace;
            TraceConfig trc;
            trc.path = ExpandHome(bench.value("path", std::string()));
            if (trc.path.empty()) {
                return Error("trace benchmark needs a 'path' to the trace csv.", ErrorKind::InvalidInput);
            }
            if (bench.contains("scaleFactors")) {
                for (auto &[label, obj] : bench.at("scaleFactors").items()) {
                    trc.scale_factors[label] = ParseDataSource(obj);
                }
            }
            if (trc.scale_factors.empty()) {
                return Error("trace benchmark needs a non-empty 'scaleFactors' map.", ErrorKind::InvalidInput);
            }
            trc.closed_loop = bench.value("closedLoop", false);
            cfg.bench = trc;
        } else {
            return Error("Unknown benchmarkType '" + type + "' (expected single|throughput|trace).",
                         ErrorKind::InvalidInput);
        }
    } catch (const std::exception &e) {
        return Error("Malformed config: " + std::string(e.what()), ErrorKind::InvalidInput);
    }

    if (cfg.queries.empty() && cfg.type != Type::kTrace) {
        return Error("Config has no queries.", ErrorKind::InvalidInput);
    }
    return cfg;
}

} // namespace plume::bench
