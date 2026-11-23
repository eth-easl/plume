#pragma once

#include <cstdint>

// Process-wide heap accounting for ubench_run (opt-in via the `track_memory` config key). Every
// malloc-family call of the process is counted by its usable size, so std:: containers and
// DuckDB/Plume allocations alike show up. The counters are global: per-invocation peaks are only
// meaningful when invocations don't overlap (run with `"threads": 1`).
namespace plume::ubench::mem {

void SetEnabled(bool enabled);
bool Enabled();

// Restarts peak tracking at the current live byte count and returns that count.
int64_t ResetPeak();

// Highest live byte count since the last ResetPeak.
int64_t Peak();

} // namespace plume::ubench::mem
