//===----------------------------------------------------------------------===//
// plume — parquet row-group pruning from column statistics (filter pushdown).
//
// Given a pushed-down predicate (whose column references are file/storage column
// indices) and a row group's per-column min/max statistics, decide whether the
// row group can be skipped. Conservative by construction: it returns "keep" for
// anything it cannot prove empty (missing stats, unsupported types, predicate
// shapes it doesn't model), so pruning never changes query results.
//===----------------------------------------------------------------------===//

#pragma once

#include "plume/common/types.hpp"
#include "plume/expression/expression.hpp"
#include "plume/parquet/metadata.hpp"

namespace plume::parquet {

// Returns true if `rgm` MIGHT contain a row satisfying `pred` (so it must be kept),
// and false only when the statistics PROVE no row can match (safe to skip). `pred`
// references columns by their file (storage) column index into `file_schema`.
bool RowGroupMayMatch(const expr::ExprNode &pred, const RowGroupMeta &rgm, const Schema &file_schema);

} // namespace plume::parquet
