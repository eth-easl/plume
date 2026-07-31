#pragma once

#include "plume/common/result.hpp"

#include "../format/plan.hpp"

#include <string>

namespace plume::ubench {

// Collect the current process's trace events (trace::CollectAll) and write them,
// with a plan header derived from `plan`, to `path`.
Result<void> WriteTrace(const UbPlan &plan, const std::string &path);

} // namespace plume::ubench
