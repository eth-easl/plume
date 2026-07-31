#pragma once

#include "plume/abi/abi.hpp"
#include "plume/common/buffer.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace plume::ubench::rt {

// One captured AddOutput call on the calling thread.
struct CapturedOutput {
    std::string ident;
    size_t set_idx = 0;
    size_t key = 0;
    DataBuffer buffer;
};

// Clear the calling thread's input and output sets. Call before configuring a
// new invocation.
void Reset();

// Populate an input set on the calling thread. Buffers are stored as non-owning
// views, so the underlying bytes must outlive the fn:: call that reads them.
void AddInput(size_t set_idx, const uint8_t *data, size_t size, std::string ident = {}, size_t key = 0);

// Register a set index as present, even with zero items. fn::RunStage and friends
// unconditionally call abi::GetInputSet on some set indices (e.g. the join right
// side whenever the pipeline leads with a join) — a legitimately empty partition
// still needs the set to resolve as empty rather than "out of range".
void EnsureInputSet(size_t set_idx);

// The outputs the fn:: call wrote via abi::AddOutput on the calling thread.
std::vector<CapturedOutput> &Outputs();

} // namespace plume::ubench::rt
