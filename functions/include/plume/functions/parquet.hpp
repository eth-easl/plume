#pragma once

#include "plume/common/result.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume::fn {

Result<void> RunParquetPrepare();

Result<void> RunParquetStage();

} // namespace plume::fn
