#include "fetch.hpp"

#include <algorithm>
#include <fstream>

namespace plume::ubench {

namespace {

// Parse "Range: bytes=START-END" (S3 half-open convention: [START, END+1)).
Result<std::pair<uint64_t, uint64_t>> ParseRange(const std::string &s) {
    auto pos = s.find("bytes=");
    if (pos == std::string::npos) {
        return Error("ubench fetch: request has no Range header", ErrorKind::InvalidInput);
    }
    pos += 6;
    auto dash = s.find('-', pos);
    if (dash == std::string::npos) {
        return Error("ubench fetch: malformed Range header", ErrorKind::InvalidInput);
    }
    uint64_t start = std::stoull(s.substr(pos, dash - pos));
    uint64_t end_inclusive = std::stoull(s.substr(dash + 1));
    return std::pair<uint64_t, uint64_t>{start, end_inclusive + 1};
}

// Extract the url from the request line "GET <url> …".
Result<std::string> ParseUrl(const std::string &s) {
    auto begin = s.find(' ');
    if (begin == std::string::npos) {
        return Error("ubench fetch: request has no url", ErrorKind::InvalidInput);
    }
    begin += 1;
    auto end = s.find(' ', begin);
    if (end == std::string::npos) {
        return Error("ubench fetch: malformed request line", ErrorKind::InvalidInput);
    }
    return s.substr(begin, end - begin);
}

} // namespace

std::string LocalFetcher::UrlToLocalPath(const std::string &url) const {
    std::string p = url;
    for (const auto &rw : rewrites_) {
        if (p.rfind(rw.first, 0) == 0) {
            p = rw.second + p.substr(rw.first.size());
            break;
        }
    }
    if (p.rfind("file://", 0) == 0) {
        p = p.substr(7);
    }
    return p;
}

const std::vector<uint8_t> &LocalFetcher::LoadFile(const std::string &local_path) {
    // Fast path: a cache hit only needs a shared read of the map. Holding the mutex
    // across the (potentially multi-GB) file read serialized every concurrent
    // invocation behind the first reader — and istreambuf_iterator reads byte by byte.
    // Read the file outside the lock with a bulk read, and only lock to look up/insert.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = file_cache_.find(local_path);
        if (it != file_cache_.end()) {
            return it->second;
        }
    }
    std::ifstream f(local_path, std::ios::binary | std::ios::ate);
    std::vector<uint8_t> bytes;
    if (f) {
        const std::streamsize sz = f.tellg();
        f.seekg(0);
        bytes.resize(static_cast<size_t>(sz));
        f.read(reinterpret_cast<char *>(bytes.data()), sz);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    // Another thread may have inserted the same file while we read it; keep theirs.
    auto res = file_cache_.emplace(local_path, std::move(bytes));
    return res.first->second;
}

Result<std::vector<uint8_t>> LocalFetcher::Resolve(const uint8_t *request, size_t size) {
    std::string s(reinterpret_cast<const char *>(request), size);
    TRY(std::string url, ParseUrl(s));
    TRY(auto range, ParseRange(s));
    std::string local = UrlToLocalPath(url);

    const std::vector<uint8_t> &file = LoadFile(local);
    if (file.empty()) {
        return Error("ubench fetch: cannot read local file '" + local + "' (from url '" + url + "')",
                     ErrorKind::InvalidInput);
    }
    uint64_t start = std::min<uint64_t>(range.first, file.size());
    uint64_t end = std::min<uint64_t>(range.second, file.size());
    return std::vector<uint8_t>(file.begin() + start, file.begin() + end);
}

} // namespace plume::ubench
