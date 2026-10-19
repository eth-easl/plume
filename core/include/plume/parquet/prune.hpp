#pragma once

#include "plume/common/types.hpp"
#include "plume/expression/expression.hpp"
#include "plume/parquet/metadata.hpp"

#include <optional>
#include <vector>

namespace plume::parquet {

bool RowGroupMayMatch(const expr::ExprNode &pred, const RowGroupMeta &rgm, const Schema &file_schema);

// The mean fraction of the column's overall [min, max] that a row group's [min, max] spans.
// nullopt if a row group lacks decodable min/max statistics for the column.
std::optional<double> MeanRowGroupCoverage(const std::vector<FileMeta> &files, uint32_t column);

} // namespace plume::parquet
