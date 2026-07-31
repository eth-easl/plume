#pragma once

#include "plume/common/result.hpp"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace plume::ubench {

struct RunConfig {
    // Parse + validate a config file. Expands a leading `~` in path fields to $HOME.
    static Result<RunConfig> FromJsonFile(const std::string &path);

    std::string plan_path;   // the compiled .ub plan to replay (required)
    std::string trace_path;  // where to write the trace; empty -> no trace written
    size_t threads = 0;      // worker threads (0 -> hardware concurrency), each pinned
    size_t core_offset = 0;  // first core to pin to
    uint32_t repartition = 0; // override every shuffle stage's fan-out (0 -> unchanged)
    uint32_t reps = 1;       // repeat the run this many times, reporting each wall time
    bool quiet = false;      // suppress the per-rep progress prints

    // url-prefix substitutions for local fetch resolution (FROM -> TO).
    std::vector<std::pair<std::string, std::string>> rewrites;
};

} // namespace plume::ubench
