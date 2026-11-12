#include "dandelion_trace.hpp"

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <zlib.h>

#include <fstream>
#include <limits>

namespace plume::bench {

using json = nlohmann::json;

namespace {

constexpr uint8_t kFormatVersion = 2;
constexpr uint8_t kTagQuery = 0x01;
constexpr uint8_t kTagRun = 0x02;

// Little-endian binary writer over a byte buffer.
class Writer {
  public:
    void U8(uint8_t v) { buf_.push_back(static_cast<char>(v)); }
    void U16(uint16_t v) { Le(v, 2); }
    void U32(uint32_t v) { Le(v, 4); }
    void U64(uint64_t v) { Le(v, 8); }
    void Raw(const char *data, size_t n) { buf_.append(data, n); }
    void Str(const std::string &s) {
        // Lengths are validated when the strings are collected.
        U16(static_cast<uint16_t>(s.size()));
        buf_.append(s);
    }
    const std::string &Data() const { return buf_; }

  private:
    void Le(uint64_t v, int bytes) {
        for (int i = 0; i < bytes; i++) {
            buf_.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
        }
    }
    std::string buf_;
};

// Compresses `in` into a gzip stream.
Result<std::string> GzipCompress(const std::string &in) {
    z_stream zs{};
    // 15 window bits + 16 -> gzip header/trailer instead of a raw zlib stream.
    if (deflateInit2(&zs, Z_BEST_COMPRESSION, Z_DEFLATED, 15 + 16, 9, Z_DEFAULT_STRATEGY) != Z_OK) {
        return Error("Could not initialize gzip compression.", ErrorKind::RuntimeError);
    }
    std::string out(deflateBound(&zs, static_cast<uLong>(in.size())), '\0');
    zs.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(in.data()));
    zs.avail_in = static_cast<uInt>(in.size());
    zs.next_out = reinterpret_cast<Bytef *>(out.data());
    zs.avail_out = static_cast<uInt>(out.size());
    int ret = deflate(&zs, Z_FINISH);
    out.resize(zs.total_out);
    deflateEnd(&zs);
    if (ret != Z_STREAM_END) {
        return Error("gzip compression failed (" + std::to_string(ret) + ").", ErrorKind::RuntimeError);
    }
    return out;
}

Result<void> CheckStr(const std::string &s, const char *what) {
    if (s.size() > std::numeric_limits<uint16_t>::max()) {
        return Error(std::string(what) + " '" + s.substr(0, 32) + "...' exceeds the maximum string length.",
                     ErrorKind::OutOfRange);
    }
    return Ok();
}

template <typename T>
Result<T> GetUnsigned(const json &obj, const char *field) {
    auto it = obj.find(field);
    if (it == obj.end() || !it->is_number_unsigned()) {
        return Error(std::string("Trace field '") + field + "' is missing or not an unsigned integer.",
                     ErrorKind::InvalidInput);
    }
    uint64_t v = it->get<uint64_t>();
    if (v > std::numeric_limits<T>::max()) {
        return Error(std::string("Trace field '") + field + "' value " + std::to_string(v) + " is out of range.",
                     ErrorKind::OutOfRange);
    }
    return static_cast<T>(v);
}

} // namespace

Result<DandelionTraceCollector> DandelionTraceCollector::Init(const std::string &base_url, int timeout_ms) {
    std::string url = base_url;
    if (!url.empty() && url.back() == '/') {
        url.pop_back();
    }
    url += "/trace/timestamps";

    cpr::Response resp = cpr::Get(cpr::Url{url}, cpr::Timeout{timeout_ms});
    if (resp.error && resp.error.code != cpr::ErrorCode::OK) {
        return Error("Request to '" + url + "' failed: " + resp.error.message);
    }
    if (resp.status_code < 200 || resp.status_code >= 300) {
        return Error("Request to '" + url + "' returned HTTP " + std::to_string(resp.status_code) + ": " + resp.text);
    }

    std::vector<std::string> names;
    try {
        json j = json::parse(resp.text);
        if (!j.is_array()) {
            return Error("Timestamp names from '" + url + "' are not a json array.", ErrorKind::InvalidInput);
        }
        for (const auto &n : j) {
            if (!n.is_string()) {
                return Error("Timestamp names from '" + url + "' contain a non-string entry.",
                             ErrorKind::InvalidInput);
            }
            names.push_back(n.get<std::string>());
            TRYV(CheckStr(names.back(), "Timestamp name"));
        }
    } catch (const json::exception &e) {
        return Error("Could not parse timestamp names from '" + url + "': " + e.what(), ErrorKind::InvalidInput);
    }
    if (names.size() > std::numeric_limits<uint8_t>::max()) {
        return Error("Dandelion reports " + std::to_string(names.size()) + " timestamps, at most 255 are supported.",
                     ErrorKind::OutOfRange);
    }
    return DandelionTraceCollector(std::move(names));
}

DandelionTraceCollector::DandelionTraceCollector(std::vector<std::string> ts_names)
    : ts_names_(std::move(ts_names)) {}

Result<void> DandelionTraceCollector::AddTrace(const std::string &query, const std::string &trace) {
    if (trace.empty()) {
        return Ok();
    }
    TRYV(CheckStr(query, "Query name"));

    json root;
    try {
        root = json::parse(trace);
    } catch (const json::exception &e) {
        return Error(std::string("Could not parse trace: ") + e.what(), ErrorKind::InvalidInput);
    }
    if (!root.is_object()) {
        return Error("Trace is not a json object.", ErrorKind::InvalidInput);
    }

    Run run;
    if (root.contains("ts")) {
        run.flags |= kHasTs;
    }
    if (root.contains("data")) {
        run.flags |= kHasData;
    }
    if (run.flags == 0) {
        return Error("Trace contains neither 'ts' nor 'data'.", ErrorKind::InvalidInput);
    }
    if ((run.flags & kHasTs) && ts_names_.empty()) {
        return Error("Trace contains timestamps, but dandelion reported no timestamp names.",
                     ErrorKind::InvalidInput);
    }
    const bool has_ts = run.flags & kHasTs;
    const bool has_data = run.flags & kHasData;
    const size_t n_ts = ts_names_.size();

    std::unordered_map<std::string, uint16_t> id_idx;

    // Pre-order traversal: a record is appended before any of its children.
    auto visit = [&](auto &&self, const json &inv, uint32_t parent, uint16_t group) -> Result<void> {
        if (!inv.is_object()) {
            return Error("Trace invocation is not a json object.", ErrorKind::InvalidInput);
        }
        if (run.records.size() >= kNoParent) {
            return Error("Trace contains too many invocations.", ErrorKind::OutOfRange);
        }

        Record rec;
        rec.parent = parent;
        rec.group = group;

        auto id_it = inv.find("id");
        if (id_it == inv.end() || !id_it->is_string()) {
            return Error("Trace field 'id' is missing or not a string.", ErrorKind::InvalidInput);
        }
        const std::string &id = id_it->get_ref<const std::string &>();
        auto [it, inserted] = id_idx.try_emplace(id, static_cast<uint16_t>(run.ids.size()));
        if (inserted) {
            if (run.ids.size() >= std::numeric_limits<uint16_t>::max()) {
                return Error("Trace contains too many distinct ids.", ErrorKind::OutOfRange);
            }
            TRYV(CheckStr(id, "Invocation id"));
            run.ids.push_back(id);
        }
        rec.id = it->second;

        TRY(rec.node, GetUnsigned<uint32_t>(inv, "nodeId"));

        auto ts_it = inv.find("ts");
        if (has_ts != (ts_it != inv.end())) {
            return Error("Trace invocations disagree on the presence of 'ts'.", ErrorKind::InvalidInput);
        }
        if (has_ts) {
            if (!ts_it->is_array() || ts_it->size() != n_ts) {
                return Error("Trace field 'ts' is not an array of " + std::to_string(n_ts) + " timestamps.",
                             ErrorKind::InvalidInput);
            }
            for (const auto &t : *ts_it) {
                if (!t.is_number_unsigned()) {
                    return Error("Trace field 'ts' contains a non-integer timestamp.", ErrorKind::InvalidInput);
                }
                run.ts.push_back(t.get<uint64_t>());
            }
        }

        auto data_it = inv.find("data");
        if (has_data != (data_it != inv.end())) {
            return Error("Trace invocations disagree on the presence of 'data'.", ErrorKind::InvalidInput);
        }
        if (has_data) {
            if (!data_it->is_array() || data_it->size() > std::numeric_limits<uint16_t>::max()) {
                return Error("Trace field 'data' is not an array or too large.", ErrorKind::InvalidInput);
            }
            rec.data_begin = static_cast<uint32_t>(run.data.size());
            rec.n_data = static_cast<uint16_t>(data_it->size());
            for (const auto &d : *data_it) {
                if (!d.is_object()) {
                    return Error("Trace data entry is not a json object.", ErrorKind::InvalidInput);
                }
                DataEntry entry;
                TRY(entry.items, GetUnsigned<uint32_t>(d, "items"));
                TRY(entry.size, GetUnsigned<uint64_t>(d, "size"));
                run.data.push_back(entry);
            }
        }

        const uint32_t self_idx = static_cast<uint32_t>(run.records.size());
        run.records.push_back(rec);

        auto children_it = inv.find("children");
        if (children_it == inv.end()) {
            return Ok();
        }
        if (!children_it->is_array() || children_it->size() > std::numeric_limits<uint16_t>::max()) {
            return Error("Trace field 'children' is not an array or too large.", ErrorKind::InvalidInput);
        }
        for (size_t g = 0; g < children_it->size(); g++) {
            const json &grp = (*children_it)[g];
            if (!grp.is_array()) {
                return Error("Trace child group is not an array.", ErrorKind::InvalidInput);
            }
            for (const auto &child : grp) {
                TRYV(self(self, child, self_idx, static_cast<uint16_t>(g)));
            }
        }
        return Ok();
    };
    TRYV(visit(visit, root, kNoParent, 0));

    auto [qit, inserted] = query_idx_.try_emplace(query, queries_.size());
    if (inserted) {
        queries_.push_back(Query{query, {}});
    }
    queries_[qit->second].runs.push_back(std::move(run));
    num_runs_++;
    return Ok();
}

Result<void> DandelionTraceCollector::ExportToFile(const std::string &path) const {
    const size_t n_ts = ts_names_.size();
    Writer w;

    // header
    w.Raw("PLMT", 4);
    w.U8(kFormatVersion);
    w.U8(static_cast<uint8_t>(n_ts));
    for (const auto &name : ts_names_) {
        w.Str(name);
    }

    // blocks
    for (const auto &q : queries_) {
        if (q.runs.empty()) {
            continue;
        }
        w.U8(kTagQuery);
        w.Str(q.name);
        for (const auto &run : q.runs) {
            const bool has_ts = run.flags & kHasTs;
            const bool has_data = run.flags & kHasData;
            w.U8(kTagRun);
            w.U8(run.flags);
            w.U16(static_cast<uint16_t>(run.ids.size()));
            for (const auto &id : run.ids) {
                w.Str(id);
            }
            w.U32(static_cast<uint32_t>(run.records.size()));
            for (size_t r = 0; r < run.records.size(); r++) {
                const Record &rec = run.records[r];
                w.U32(rec.parent);
                w.U16(rec.group);
                w.U16(rec.id);
                w.U32(rec.node);
                if (has_ts) {
                    for (size_t t = 0; t < n_ts; t++) {
                        w.U64(run.ts[r * n_ts + t]);
                    }
                }
                if (has_data) {
                    w.U16(rec.n_data);
                    for (size_t d = 0; d < rec.n_data; d++) {
                        const DataEntry &e = run.data[rec.data_begin + d];
                        w.U32(e.items);
                        w.U64(e.size);
                    }
                }
            }
        }
    }

    TRY(auto out, GzipCompress(w.Data()));

    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f.is_open()) {
        return Error("Could not open '" + path + "' for writing.");
    }
    f.write(out.data(), static_cast<std::streamsize>(out.size()));
    if (!f) {
        return Error("Failed to write traces to '" + path + "'.");
    }
    return Ok();
}

} // namespace plume::bench
