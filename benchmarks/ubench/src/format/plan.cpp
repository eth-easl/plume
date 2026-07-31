#include "plan.hpp"

#include <cstring>

namespace plume::ubench {

namespace {

constexpr uint32_t kMagic = 0x42554C50; // "PLUB" (little-endian)
constexpr uint32_t kVersion = 1;

// --- writer ------------------------------------------------------------------

struct Writer {
    std::vector<uint8_t> out;

    void U8(uint8_t v) { out.push_back(v); }

    // LEB128 varint for lengths / small unsigned values.
    void Var(uint64_t v) {
        while (v >= 0x80) {
            out.push_back(static_cast<uint8_t>(v) | 0x80);
            v >>= 7;
        }
        out.push_back(static_cast<uint8_t>(v));
    }

    void U32(uint32_t v) { Var(v); }
    void I32(int32_t v) { Var(static_cast<uint64_t>(static_cast<uint32_t>(v))); }
    void U64(uint64_t v) { Var(v); }
    void Bool(bool v) { out.push_back(v ? 1 : 0); }

    void Bytes(const uint8_t *p, size_t n) {
        Var(n);
        out.insert(out.end(), p, p + n);
    }
    void Str(const std::string &s) { Bytes(reinterpret_cast<const uint8_t *>(s.data()), s.size()); }
    void Blob(const std::vector<uint8_t> &b) { Bytes(b.data(), b.size()); }

    void Item(const UbItem &it) {
        U64(it.key);
        Str(it.ident);
        Blob(it.data);
    }
    void Items(const std::vector<UbItem> &v) {
        Var(v.size());
        for (auto &it : v) {
            Item(it);
        }
    }
};

// --- reader ------------------------------------------------------------------

struct Reader {
    const uint8_t *p;
    const uint8_t *end;
    bool ok = true;

    Reader(const uint8_t *data, size_t size) : p(data), end(data + size) {}

    uint64_t Var() {
        uint64_t v = 0;
        int shift = 0;
        while (p < end) {
            uint8_t b = *p++;
            v |= static_cast<uint64_t>(b & 0x7F) << shift;
            if (!(b & 0x80)) {
                return v;
            }
            shift += 7;
            if (shift >= 64) {
                break;
            }
        }
        ok = false;
        return 0;
    }

    uint32_t U32() { return static_cast<uint32_t>(Var()); }
    int32_t I32() { return static_cast<int32_t>(static_cast<uint32_t>(Var())); }
    uint64_t U64() { return Var(); }
    uint8_t U8() {
        if (p >= end) {
            ok = false;
            return 0;
        }
        return *p++;
    }
    bool Bool() { return U8() != 0; }

    std::vector<uint8_t> Bytes() {
        uint64_t n = Var();
        if (!ok || static_cast<uint64_t>(end - p) < n) {
            ok = false;
            return {};
        }
        std::vector<uint8_t> b(p, p + n);
        p += n;
        return b;
    }
    std::string Str() {
        auto b = Bytes();
        return std::string(b.begin(), b.end());
    }

    UbItem Item() {
        UbItem it;
        it.key = U64();
        it.ident = Str();
        it.data = Bytes();
        return it;
    }
    std::vector<UbItem> Items() {
        uint64_t n = Var();
        std::vector<UbItem> v;
        if (!ok) {
            return v;
        }
        v.reserve(n);
        for (uint64_t i = 0; i < n && ok; i++) {
            v.push_back(Item());
        }
        return v;
    }
};

} // namespace

std::vector<uint8_t> WriteUbPlan(const UbPlan &plan) {
    Writer w;
    w.out.reserve(4096);
    // Header: magic + version written as raw fixed-width so a reader can validate
    // before trusting the varint stream.
    uint32_t magic = kMagic, version = kVersion;
    w.out.insert(w.out.end(), reinterpret_cast<uint8_t *>(&magic), reinterpret_cast<uint8_t *>(&magic) + 4);
    w.out.insert(w.out.end(), reinterpret_cast<uint8_t *>(&version), reinterpret_cast<uint8_t *>(&version) + 4);

    w.Str(plan.name);
    w.I32(plan.root_stage);
    w.Var(plan.stages.size());
    for (const auto &s : plan.stages) {
        w.I32(s.id);
        w.U8(static_cast<uint8_t>(s.source));
        w.U32(s.partitions);
        w.Bool(s.leads_with_join);
        w.Var(s.input_stages.size());
        for (int32_t up : s.input_stages) {
            w.I32(up);
        }
        w.Blob(s.pipeline_blob);
        w.Items(s.table_blocks);
        w.Items(s.source_info);
        w.Items(s.source_reqs);
    }
    return std::move(w.out);
}

Result<UbPlan> ReadUbPlan(const uint8_t *data, size_t size) {
    if (size < 8) {
        return Error("ubench: .ub file too small for header", ErrorKind::InvalidInput);
    }
    uint32_t magic = 0, version = 0;
    std::memcpy(&magic, data, 4);
    std::memcpy(&version, data + 4, 4);
    if (magic != kMagic) {
        return Error("ubench: bad .ub magic", ErrorKind::InvalidInput);
    }
    if (version != kVersion) {
        return Error("ubench: unsupported .ub version " + std::to_string(version), ErrorKind::InvalidInput);
    }

    Reader r(data + 8, size - 8);
    UbPlan plan;
    plan.name = r.Str();
    plan.root_stage = r.I32();
    uint64_t nstages = r.Var();
    if (!r.ok) {
        return Error("ubench: truncated .ub header", ErrorKind::InvalidInput);
    }
    plan.stages.reserve(nstages);
    for (uint64_t i = 0; i < nstages && r.ok; i++) {
        UbStage s;
        s.id = r.I32();
        s.source = static_cast<UbSourceKind>(r.U8());
        s.partitions = r.U32();
        s.leads_with_join = r.Bool();
        uint64_t nin = r.Var();
        for (uint64_t k = 0; k < nin && r.ok; k++) {
            s.input_stages.push_back(r.I32());
        }
        s.pipeline_blob = r.Bytes();
        s.table_blocks = r.Items();
        s.source_info = r.Items();
        s.source_reqs = r.Items();
        plan.stages.push_back(std::move(s));
    }
    if (!r.ok) {
        return Error("ubench: truncated or malformed .ub body", ErrorKind::InvalidInput);
    }
    return plan;
}

} // namespace plume::ubench
