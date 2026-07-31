#include "runner/config.hpp"

#include <nlohmann/json.hpp>

#include <cstdlib>
#include <fstream>

namespace plume::ubench {

namespace {

using json = nlohmann::json;

// Expand a leading `~` to $HOME (mirrors the plume_bench runner's path handling).
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

} // namespace

Result<RunConfig> RunConfig::FromJsonFile(const std::string &path) {
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

    RunConfig cfg;
    try {
        if (!data.contains("plan")) {
            return Error("Config must name a 'plan' (.ub file to replay).", ErrorKind::InvalidInput);
        }
        cfg.plan_path = ExpandHome(data.at("plan").get<std::string>());

        if (data.contains("trace")) {
            cfg.trace_path = ExpandHome(data.at("trace").get<std::string>());
        }
        if (data.contains("threads")) {
            cfg.threads = data.at("threads").get<size_t>();
        }
        if (data.contains("coreOffset")) {
            cfg.core_offset = data.at("coreOffset").get<size_t>();
        }
        if (data.contains("repartition")) {
            cfg.repartition = data.at("repartition").get<uint32_t>();
        }
        if (data.contains("reps")) {
            cfg.reps = data.at("reps").get<uint32_t>();
        }
        if (data.contains("quiet")) {
            cfg.quiet = data.at("quiet").get<bool>();
        }
        if (data.contains("rewrites")) {
            for (auto &[from, to] : data.at("rewrites").items()) {
                cfg.rewrites.emplace_back(from, ExpandHome(to.get<std::string>()));
            }
        }
    } catch (const std::exception &e) {
        return Error("Invalid config field: " + std::string(e.what()), ErrorKind::InvalidInput);
    }

    if (cfg.reps == 0) {
        cfg.reps = 1;
    }
    return cfg;
}

} // namespace plume::ubench
