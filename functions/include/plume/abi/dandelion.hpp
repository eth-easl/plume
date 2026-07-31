#pragma once

// Dandelion host-ABI backend. Unlike the linux backend there is no Init/Close:
// the Dandelion runtime hands the function its input/output sets directly, so the
// entry points only need the plume::abi interface (implemented in src/abi/dandelion.cpp).
#include "plume/abi/abi.hpp"

namespace plume::dandelion {

// Reserved for future Dandelion-specific entry points. The function binaries
// include this header under `#ifdef __DANDELION__`; today they only call
// plume::fn::Run* + the plume::abi interface, so nothing is declared here yet.

} // namespace plume::dandelion
