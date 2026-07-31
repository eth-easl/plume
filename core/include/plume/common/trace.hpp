#pragma once

#include <cstdint>
#include <vector>

namespace plume::trace {

enum class Phase : uint8_t {
    SETUP = 0,       // build the executor / operator chain
    DESERIALIZE = 1, // deserialize the pipeline template
    PUSH = 2,        // an operator consuming a chunk
    FINISH = 3,      // an operator flushing its buffered output
};

inline constexpr int kStageOp = -1;

// One aggregated timing record, emitted per (stage-invocation, operator, phase).
// Packed to 24 bytes, exactly the on-disk event layout (see benchmarks/ubench/docs/trace-format.md).
#pragma pack(push, 1)
struct Event {
    uint16_t stage_id = 0;
    uint16_t partition = 0;
    uint32_t invocation = 0;
    int16_t op_index = kStageOp; // -1 for stage phases
    uint8_t phase = 0;           // Phase
    uint8_t _pad = 0;
    uint32_t hits = 0;           // how many scope entries were aggregated
    uint64_t total_ns = 0;       // summed wall time
};
#pragma pack(pop)
static_assert(sizeof(Event) == 24, "trace Event must stay 24 bytes (on-disk layout)");

void BeginInvocation(uint16_t stage_id, uint32_t invocation, uint16_t partition, uint16_t num_ops);

void EndInvocation();

void Record(int op_index, Phase phase, uint64_t dur_ns);

std::vector<Event> CollectAll();

void Clear();

} // namespace plume::trace

#ifdef PLUME_UBENCH_TRACE

#include <chrono>

namespace plume::trace {

// Records time from object initialization to destruction.
class ScopeTimer {
public:
    ScopeTimer(int op_index, Phase phase)
        : op_index_(op_index), phase_(phase), start_(std::chrono::steady_clock::now()) {}
    ~ScopeTimer() {
        auto end = std::chrono::steady_clock::now();
        Record(op_index_, phase_,
               static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_).count()));
    }
    ScopeTimer(const ScopeTimer &) = delete;
    ScopeTimer &operator=(const ScopeTimer &) = delete;

private:
    int op_index_;
    Phase phase_;
    std::chrono::steady_clock::time_point start_;
};

} // namespace plume::trace

// Time the enclosing block as operator `this->trace_op_index_` (an Operator method).
#define PLUME_TRACE_OP(phase) ::plume::trace::ScopeTimer _plume_ts_(this->trace_op_index_, (phase))
// Time the enclosing block as a stage-level phase (a fn::Run* entrypoint).
#define PLUME_TRACE_STAGE(phase) ::plume::trace::ScopeTimer _plume_ts_(::plume::trace::kStageOp, (phase))

#else // !PLUME_UBENCH_TRACE

#define PLUME_TRACE_OP(phase) ((void)0)
#define PLUME_TRACE_STAGE(phase) ((void)0)

#endif // PLUME_UBENCH_TRACE
