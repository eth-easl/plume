#include "plume/functions/dynamic_filter.hpp"

#include "plume/abi/abi.hpp"
#include "plume/common/serial.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/util/dynamic_filter.hpp"

// The first input set (index 0) holds (potentially several) dynamic filters computed by upstream stages.
#define SET_IDX_IN_DYNAMIC_FILTER 0

// The first output set (index 0) holds the combined dynamic filters.
#define SET_IDX_OUT_DYNAMIC_FILTER 0

namespace plume::fn {

Result<void> RunDynamicFilterCombine() {
    TRY(auto filters, util::ReadDynamicFilters(SET_IDX_IN_DYNAMIC_FILTER));
    for (const auto &entry : filters) {
        DataBuffer buffer;
        TRYV(TryCatch([&] { buffer = SerializeToBuffer(entry.second); }));
        abi::AddOutput(exec::DynamicFilterIdent(entry.first), SET_IDX_OUT_DYNAMIC_FILTER, std::move(buffer));
    }
    return Ok();
}

} // namespace plume::fn
