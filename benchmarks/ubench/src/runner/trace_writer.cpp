#include "trace_writer.hpp"

#include "plume/common/trace.hpp"
#include "plume/execution/operator.hpp"
#include "plume/execution/pipeline.hpp"

#include <cstring>
#include <fstream>
#include <vector>

namespace plume::ubench {

namespace {

constexpr uint32_t kMagic = 0x52544C50; // "PLTR"
constexpr uint32_t kVersion = 1;

// Operator-type byte for the streaming output/terminal operator (past the fused
// ops); the fused ops use their exec::OpType value directly.
constexpr uint8_t kOpTypeOutput = 0xFF;

void PutU32(std::vector<uint8_t> &b, uint32_t v) {
    b.insert(b.end(), reinterpret_cast<uint8_t *>(&v), reinterpret_cast<uint8_t *>(&v) + 4);
}
void PutU64(std::vector<uint8_t> &b, uint64_t v) {
    b.insert(b.end(), reinterpret_cast<uint8_t *>(&v), reinterpret_cast<uint8_t *>(&v) + 8);
}
void PutStr(std::vector<uint8_t> &b, const std::string &s) {
    PutU32(b, static_cast<uint32_t>(s.size()));
    b.insert(b.end(), s.begin(), s.end());
}

} // namespace

Result<void> WriteTrace(const UbPlan &plan, const std::string &path) {
    std::vector<uint8_t> buf;
    PutU32(buf, kMagic);
    PutU32(buf, kVersion);

    // --- Plan header: map (stage, op index) -> operator type. ---
    PutStr(buf, plan.name);
    PutU32(buf, static_cast<uint32_t>(plan.stages.size()));
    for (const auto &s : plan.stages) {
        PutU32(buf, static_cast<uint32_t>(s.id));
        buf.push_back(static_cast<uint8_t>(s.source));
        PutU32(buf, s.partitions);

        // Enumerate the operator types (fused ops in order, then the terminal).
        std::vector<uint8_t> op_types;
        auto templ = DeserializePipeline(s.pipeline_blob);
        if (templ.is_ok()) {
            for (const auto &op : templ.unwrap().operators) {
                op_types.push_back(static_cast<uint8_t>(op->type));
            }
        }
        op_types.push_back(kOpTypeOutput); // op_index == operators.size()
        PutU32(buf, static_cast<uint32_t>(op_types.size()));
        buf.insert(buf.end(), op_types.begin(), op_types.end());
    }

    // --- Events. ---
    auto events = trace::CollectAll();
    PutU64(buf, static_cast<uint64_t>(events.size()));
    if (!events.empty()) {
        const auto *p = reinterpret_cast<const uint8_t *>(events.data());
        buf.insert(buf.end(), p, p + events.size() * sizeof(trace::Event));
    }

    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) {
        return Error("ubench: cannot open trace output '" + path + "'", ErrorKind::InvalidInput);
    }
    f.write(reinterpret_cast<const char *>(buf.data()), static_cast<std::streamsize>(buf.size()));
    return Ok();
}

} // namespace plume::ubench
