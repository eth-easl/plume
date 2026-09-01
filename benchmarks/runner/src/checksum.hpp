#pragma once

#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

namespace plume::bench {

struct ChecksumEntry {
    uint64_t row_count = 0;
    uint64_t hash_sum = 0;

    bool operator==(const ChecksumEntry &other) const {
        return row_count == other.row_count && hash_sum == other.hash_sum;
    }
};

// Maps query name -> scale-factor label -> expected checksum. 
// Loaded once from a JSON file shaped like:
//   { "q1": { "sf1": { "rowCount": 4, "hashSum": "0x..." } } }
class ChecksumFile {
  public:
    static Result<ChecksumFile> FromJsonFile(const std::string &path);

    // The expected checksum for (query, scale_factor), if the file has one.
    std::optional<ChecksumEntry> Find(const std::string &query, const std::string &scale_factor) const;

  private:
    std::unordered_map<std::string, std::unordered_map<std::string, ChecksumEntry>> entries_;
};

Result<ChecksumEntry> ComputeChecksum(const dandelion::DataSetVec &sets);

std::string ToString(const ChecksumEntry &entry);

} // namespace plume::bench
