#pragma once

#include "plume/common/result.hpp"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace plume::ubench {

// Resolves byte-range requests against local files. `rewrites` is a list of
// (from, to) url-prefix substitutions applied in order; a `file://` scheme is
// always stripped. Thread-safe: the file cache is mutex-guarded and buffers are
// handed out as const views.
class LocalFetcher {
public:
    explicit LocalFetcher(std::vector<std::pair<std::string, std::string>> rewrites = {})
        : rewrites_(std::move(rewrites)) {}

    // Resolve one fetch request (the raw request bytes the exporter produced) into
    // the requested byte range's contents.
    Result<std::vector<uint8_t>> Resolve(const uint8_t *request, size_t size);

    // Map a url to the local path the runner reads (prefix rewrites + file:// strip).
    std::string UrlToLocalPath(const std::string &url) const;

private:
    const std::vector<uint8_t> &LoadFile(const std::string &local_path);

    std::vector<std::pair<std::string, std::string>> rewrites_;
    std::mutex mutex_;
    std::unordered_map<std::string, std::vector<uint8_t>> file_cache_;
};

} // namespace plume::ubench
