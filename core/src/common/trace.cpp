#include "plume/common/trace.hpp"

// The header always declars the API:
// - if `PLUME_UBENCH_TRACE` is not defined we insert no-ops (the default case)
// - if `PLUME_UBENCH_TRACE` is defined we accumulate the traces
#ifndef PLUME_UBENCH_TRACE

namespace plume::trace {

void BeginInvocation(uint16_t, uint32_t, uint16_t, uint16_t) {}
void EndInvocation() {}
void Record(int, Phase, uint64_t) {}
std::vector<Event> CollectAll() { return {}; }
void Clear() {}

} // namespace plume::trace

#else // PLUME_UBENCH_TRACE

#include <mutex>
#include <vector>

namespace plume::trace {

namespace {

// The accumulator for the invocation currently open on this thread. Keyed by a
// dense slot: stage phases (op_index = -1) map to slot 0, operator `i` phase `p`
// to a slot past that. We keep it simple with a flat vector indexed by
// (op_index+1) * kNumPhases + phase.
constexpr int kNumPhases = 4;

struct Slot {
    uint64_t total_ns = 0;
    uint32_t hits = 0;
};

struct ThreadTrace {
    // Current invocation identity.
    uint16_t stage_id = 0;
    uint32_t invocation = 0;
    uint16_t partition = 0;
    bool open = false;

    // First kNumPhases are stage phases, rest operator phases ((op_index+1)*kNumPhases + phase).
    std::vector<Slot> slots;

    // Finalized events across all invocations this thread ran.
    std::vector<Event> events;
};

// Each worker thread accumulates into its own ThreadTrace.
thread_local ThreadTrace *t_trace = nullptr;

std::mutex g_registry_mutex;
std::vector<ThreadTrace *> g_registry;

ThreadTrace &Local() {
    if (!t_trace) {
        t_trace = new ThreadTrace();
        std::lock_guard<std::mutex> lock(g_registry_mutex);
        g_registry.push_back(t_trace);
    }
    return *t_trace;
}

size_t SlotIndex(int op_index, Phase phase) {
    return static_cast<size_t>(op_index + 1) * kNumPhases + static_cast<size_t>(phase);
}

// Flush the current invocation's non-empty slots into the event log.
void FlushOpen(ThreadTrace &tt) {
    for (size_t i = 0; i < tt.slots.size(); i++) {
        const Slot &s = tt.slots[i];
        if (s.hits == 0) {
            continue;
        }
        int op_index = static_cast<int>(i / kNumPhases) - 1;
        auto phase = static_cast<uint8_t>(i % kNumPhases);
        Event e;
        e.stage_id = tt.stage_id;
        e.partition = tt.partition;
        e.invocation = tt.invocation;
        e.op_index = static_cast<int16_t>(op_index);
        e.phase = phase;
        e.hits = s.hits;
        e.total_ns = s.total_ns;
        tt.events.push_back(e);
    }
}

} // namespace

void BeginInvocation(uint16_t stage_id, uint32_t invocation, uint16_t partition, uint16_t num_ops) {
    ThreadTrace &tt = Local();
    tt.stage_id = stage_id;
    tt.invocation = invocation;
    tt.partition = partition;
    tt.open = true;
    // +1 stage row, +1 for the streaming output operator past the fused ops.
    size_t needed = static_cast<size_t>(num_ops + 2) * kNumPhases;
    tt.slots.assign(needed, Slot{});
}

void EndInvocation() {
    ThreadTrace &tt = Local();
    if (!tt.open) {
        return;
    }
    FlushOpen(tt);
    tt.slots.clear();
    tt.open = false;
}

void Record(int op_index, Phase phase, uint64_t dur_ns) {
    ThreadTrace &tt = Local();
    if (!tt.open) {
        return; // a scope outside any invocation (e.g. setup before Begin) is ignored
    }
    size_t idx = SlotIndex(op_index, phase);
    if (idx >= tt.slots.size()) {
        tt.slots.resize(idx + 1, Slot{});
    }
    tt.slots[idx].total_ns += dur_ns;
    tt.slots[idx].hits += 1;
}

std::vector<Event> CollectAll() {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    std::vector<Event> all;
    for (ThreadTrace *tt : g_registry) {
        all.insert(all.end(), tt->events.begin(), tt->events.end());
    }
    return all;
}

void Clear() {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    for (ThreadTrace *tt : g_registry) {
        tt->events.clear();
        tt->slots.clear();
        tt->open = false;
    }
}

} // namespace plume::trace

#endif // PLUME_UBENCH_TRACE
