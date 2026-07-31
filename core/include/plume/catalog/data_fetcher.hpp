#pragma once

#include "plume/common/result.hpp"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace plume::catalog {
    
struct DataFetcher {
    std::function<Result<std::vector<uint8_t>>(const std::string &url, int64_t offset, int64_t length)> range;
    std::function<Result<uint64_t>(const std::string &url)> size;
};

// cpr (libcurl) based fetcher for remote data.
DataFetcher RemoteFetcher();

// Local filesystem fetcher. Expands `~` against `$HOME`.
DataFetcher LocalFileFetcher();

// Chooses RemoteFetcher or LocalFileFetcher based on input path.
DataFetcher AutoFetcher();

} // namespace plume::catalog
