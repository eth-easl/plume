#pragma once

#include "plume/common/result.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace plume::bench {

struct TraceEntry {
    uint64_t offset_ms;       // arrival offset relative to the first entry
    std::string query;        // query name (e.g. "q6")
    std::string scale_factor; // scale-factor label (e.g. "sf1")
};

Result<std::vector<TraceEntry>> LoadTrace(const std::string &path);

} // namespace plume::bench
