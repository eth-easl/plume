#include "plume/util/dynamic_filter.hpp"

#include "plume/abi/abi.hpp"
#include "plume/common/serial.hpp"
#include "plume/execution/executor.hpp"

namespace plume::util {

Result<std::map<uint32_t, exec::DynamicFilter>> ReadDynamicFilters(size_t set_idx) {
    std::map<uint32_t, exec::DynamicFilter> filters;
    auto itms = abi::GetOptionalInputSet(set_idx);
    if (!itms) {
        return filters;
    }

    for (auto &itm : *itms) {
        TRY(auto id, exec::ParseDynamicFilterIdent(itm.ident));
        
        exec::DynamicFilter filter;
        TRYV(TryCatch([&] {
            filter = DeserializeFromBytes<exec::DynamicFilter>(itm.buffer.data(), itm.buffer.size());
        }));
        
        auto it = filters.find(id);
        if (it == filters.end()) {
            filters.emplace(id, std::move(filter));
        } else {
            TRYV(TryCatch([&] { it->second.Merge(filter); }));
        }
    }
    return filters;
}

void EmitDynamicFilters(const exec::Executor &executor, size_t set_idx) {
    for (auto *build : executor.DynamicFilterBuilds()) {
        abi::AddOutput(exec::DynamicFilterIdent(build->FilterId()), set_idx, SerializeToBuffer(build->Filter()));
    }
}

} // namespace plume::util
