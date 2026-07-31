#pragma once

#include "plume/common/types.hpp"
#include "plume/expression/expression.hpp"
#include "plume/parquet/metadata.hpp"

namespace plume::parquet {

bool RowGroupMayMatch(const expr::ExprNode &pred, const RowGroupMeta &rgm, const Schema &file_schema);

} // namespace plume::parquet
