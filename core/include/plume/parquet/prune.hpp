#pragma once

#include "plume/common/types.hpp"
#include "plume/expression/expression.hpp"
#include "plume/parquet/metadata.hpp"

#include "duckdb/common/types/value.hpp"

#include <optional>
#include <vector>

namespace plume::parquet {

bool RowGroupMayMatch(const expr::ExprNode &pred, const RowGroupMeta &rgm, const Schema &file_schema);

// The values cast to the column type and sorted, as RowGroupMayContainAny expects them. nullopt if
// a value doesn't cast.
std::optional<std::vector<duckdb::Value>> SortStatValues(const std::vector<duckdb::Value> &values,
                                                         const ColumnType &type);

// Whether any of the sorted values lies within the row group's [min, max] of the column (true if
// the column lacks decodable min/max statistics).
bool RowGroupMayContainAny(const RowGroupMeta &rgm, const Schema &file_schema, uint32_t column,
                           const std::vector<duckdb::Value> &sorted_values);

// The mean fraction of the column's overall [min, max] that a row group's [min, max] spans.
// nullopt if a row group lacks decodable min/max statistics for the column.
std::optional<double> MeanRowGroupCoverage(const std::vector<FileMeta> &files, uint32_t column);

} // namespace plume::parquet
