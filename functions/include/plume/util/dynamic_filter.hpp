#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/pipeline.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <vector>

namespace plume::exec {
class Executor;
} // namespace plume::exec

namespace plume::util {

struct BoundDynamicFilter {
    uint32_t column;
    exec::DynamicFilter filter;
};

// Reads the dynamic filters of the (optional) input set combining instances with the same id. 
// An absent set yields an empty map.
Result<std::map<uint32_t, exec::DynamicFilter>> ReadDynamicFilters(size_t set_idx);

// Reads the dynamic filters of the (optional) input set and binds each spec to its filter.
// Returns nullopt if a bound filter has no bounds (i.e. has no non-NULL key, so no probe row can match).
Result<std::optional<std::vector<BoundDynamicFilter>>> BindDynamicFilters(
    size_t set_idx, const std::vector<exec::DynamicFilterProbeSpec> &specs);

// Emits the computed dynamic filters from the executor's pipeline in the given output set index.
// Each filter becomes an output item identified by its filter id.
void EmitDynamicFilters(const exec::Executor &executor, size_t set_idx);

} // namespace plume::util
