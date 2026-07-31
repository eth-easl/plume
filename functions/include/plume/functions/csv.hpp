#pragma once

#include "plume/common/result.hpp"

namespace plume::fn {

Result<void> RunCSVPrepare();

Result<void> RunCSVStage();

} // namespace plume::fn
