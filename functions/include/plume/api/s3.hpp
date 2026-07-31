#pragma once

#include "plume/common/buffer.hpp"

#include <string_view>

namespace plume::s3 {

DataBuffer CreateGetRequest(std::string_view url, int64_t bytes_size = 0, int64_t bytes_offset = 0);

} // namespace plume::s3
