#include "plume/catalog/data_fetcher.hpp"

#include "plume/catalog/catalog.hpp"

#include <cpr/cpr.h>

namespace plume::catalog {

DataFetcher RemoteFetcher() {
    DataFetcher f;
    f.range = [](const std::string &url, int64_t offset, int64_t length) -> Result<std::vector<uint8_t>> {
        std::string spec = offset < 0 ? ("bytes=-" + std::to_string(length))
                                      : ("bytes=" + std::to_string(offset) + "-" + std::to_string(offset + length - 1));
        cpr::Response r = cpr::Get(cpr::Url{url}, cpr::Header{{"Range", spec}});
        if (r.error) {
            return Error("Fetching '" + url + "' failed: " + r.error.message);
        }
        if (r.status_code != 200 && r.status_code != 206) {
            return Error("Fetching '" + url + "' returned HTTP " + std::to_string(r.status_code));
        }
        return std::vector<uint8_t>(r.text.begin(), r.text.end());
    };

    f.size = [](const std::string &url) -> Result<uint64_t> {
        cpr::Response r = cpr::Head(cpr::Url{url});
        if (r.error) {
            return Error("Fetching HEAD of '" + url + "' failed: " + r.error.message);
        }
        auto it = r.header.find("Content-Length");
        if (it == r.header.end()) {
            return Error("Fetching source '" + url + "' did not report a Content-Length");
        }
        return static_cast<uint64_t>(std::stoull(it->second));
    };

    return f;
}

DataFetcher LocalFileFetcher() {
    auto local = [](const std::string &url) {
        std::string path = url.rfind("file://", 0) == 0 ? url.substr(7) : url;
        if (path == "~" || path.rfind("~/", 0) == 0) {
            if (const char *home = std::getenv("HOME")) {
                path = std::string(home) + path.substr(1);
            }
        }
        return path;
    };

    DataFetcher f;
    f.range = [local](const std::string &url, int64_t offset, int64_t length) -> Result<std::vector<uint8_t>> {
        std::ifstream s(local(url), std::ios::binary | std::ios::ate);
        if (!s) {
            return Error("Cannot open '" + url + "'", ErrorKind::InvalidInput);
        }
        int64_t fsize = static_cast<int64_t>(s.tellg());
        int64_t start = offset < 0 ? std::max<int64_t>(0, fsize - length) : std::min(offset, fsize);
        int64_t end = offset < 0 ? fsize : std::min<int64_t>(fsize, offset + length);
        s.seekg(start);
        std::vector<uint8_t> buf(static_cast<size_t>(end - start));
        s.read(reinterpret_cast<char *>(buf.data()), static_cast<std::streamsize>(buf.size()));
        return buf;
    };

    f.size = [local](const std::string &url) -> Result<uint64_t> {
        std::ifstream s(local(url), std::ios::binary | std::ios::ate);
        if (!s) {
            return Error("Cannot open '" + url + "'", ErrorKind::InvalidInput);
        }
        return static_cast<uint64_t>(s.tellg());
    };

    return f;
}

DataFetcher AutoFetcher() {
    auto is_http = [](const std::string &url) {
        return url.rfind("http://", 0) == 0 || url.rfind("https://", 0) == 0;
    };

    DataFetcher f;
    auto cpr = std::make_shared<DataFetcher>(RemoteFetcher());
    auto local = std::make_shared<DataFetcher>(LocalFileFetcher());
    f.range = [cpr, local, is_http](const std::string &url, int64_t offset, int64_t length) {
        return (is_http(url) ? *cpr : *local).range(url, offset, length);
    };
    f.size = [cpr, local, is_http](const std::string &url) { return (is_http(url) ? *cpr : *local).size(url); };
    return f;
}

} // namespace plume::catalog
