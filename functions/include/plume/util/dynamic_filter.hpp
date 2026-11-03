#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"

#include <cstddef>
#include <cstdint>
#include <map>

namespace plume::exec {
class Executor;
} // namespace plume::exec

namespace plume::util {

// Reads and merges any dynamic filters from the (optional) input set.
Result<std::map<uint32_t, exec::DynamicFilter>> ReadDynamicFilters(size_t set_idx);

// Emits the computed dynamic filters from the executor's pipeline in the given output set index.
// Each filter becomes an output item identified by its filter id.
void EmitDynamicFilters(const exec::Executor &executor, size_t set_idx);

} // namespace plume::util
