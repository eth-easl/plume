#include "trace.hpp"

#include <ctime>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace plume::bench {

namespace {

// Parse an ISO-8601-ish timestamp "YYYY-MM-DDTHH:MM:SS[.ffffff]" into whole
// seconds since epoch plus the fractional microseconds.
Result<int64_t> ParseTimestamp(const std::string &ts, int64_t &out_micros) {
    std::tm tm = {};
    std::istringstream iss(ts);
    iss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (iss.fail()) {
        return Error("Failed to parse timestamp '" + ts + "'.", ErrorKind::InvalidInput);
    }
    out_micros = 0;
    char dot;
    if (iss >> dot && dot == '.') {
        iss >> out_micros;
    }
    return static_cast<int64_t>(std::mktime(&tm));
}

std::vector<std::string> SplitCsv(const std::string &line) {
    std::vector<std::string> out;
    std::stringstream ss(line);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        out.push_back(tok);
    }
    return out;
}

} // namespace

Result<std::vector<TraceEntry>> LoadTrace(const std::string &path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        return Error("Could not open trace file '" + path + "'.", ErrorKind::InvalidInput);
    }

    std::string line;
    if (!std::getline(file, line)) {
        return Error("Trace file '" + path + "' is empty.", ErrorKind::InvalidInput);
    }

    size_t ts_col = SIZE_MAX, query_col = SIZE_MAX, sf_col = SIZE_MAX;
    {
        auto header = SplitCsv(line);
        for (size_t i = 0; i < header.size(); i++) {
            if (header[i] == "arrival_timestamp") {
                ts_col = i;
            } else if (header[i] == "query") {
                query_col = i;
            } else if (header[i] == "scale_factor") {
                sf_col = i;
            }
        }
    }
    if (ts_col == SIZE_MAX || query_col == SIZE_MAX || sf_col == SIZE_MAX) {
        return Error("Trace header must contain 'arrival_timestamp', 'query' and 'scale_factor'.",
                     ErrorKind::InvalidInput);
    }

    std::vector<TraceEntry> entries;
    bool have_start = false;
    int64_t start_s = 0, start_micros = 0;
    while (std::getline(file, line)) {
        if (line.empty()) {
            continue;
        }
        auto cols = SplitCsv(line);
        if (cols.size() <= std::max({ts_col, query_col, sf_col})) {
            continue;
        }
        int64_t micros = 0;
        TRY(auto secs, ParseTimestamp(cols[ts_col], micros));
        if (!have_start) {
            start_s = secs;
            start_micros = micros;
            have_start = true;
        }
        int64_t offset_ms = (secs - start_s) * 1000 + (micros - start_micros) / 1000;
        if (offset_ms < 0) {
            offset_ms = 0;
        }
        entries.push_back(TraceEntry{static_cast<uint64_t>(offset_ms), cols[query_col], cols[sf_col]});
    }
    return entries;
}

} // namespace plume::bench
