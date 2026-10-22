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

// Rows [begin, end) of a row group.
struct RowRange {
    uint64_t begin;
    uint64_t end;

    bool operator==(const RowRange &o) const { return begin == o.begin && end == o.end; }
};

// The rows of a row group (of rg_rows rows) whose pages of file column `column` may hold a key
// that passes `pred` (over file columns, only `column` is known) and, if given, is one of the
// sorted values, by the column's page index. Pages of only NULLs never do. nullopt if the page
// index doesn't fit.
std::optional<std::vector<RowRange>> SelectPageRows(const expr::ExprNode *pred,
                                                    const std::vector<duckdb::Value> *sorted_values,
                                                    uint32_t column, const Schema &file_schema,
                                                    const std::vector<PageLocation> &pages, const ColumnIndex &index,
                                                    uint64_t rg_rows);

// The mean fraction of the column's overall [min, max] that a row group's [min, max] spans.
// nullopt if a row group lacks decodable min/max statistics for the column.
std::optional<double> MeanRowGroupCoverage(const std::vector<FileMeta> &files, uint32_t column);

} // namespace plume::parquet
