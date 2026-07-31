#pragma once

#include "plume/common/result.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <string>
#include <vector>

namespace plume::expr {

// Resolves a scalar function by name and concrete arguemtn types, returning a function with an
// executable kernel.
// Fails if the name is unknown, no overload matches, or the kernel cannot be selected without an instance.
Result<duckdb::ScalarFunction> ResolveScalarFunction(const std::string &name, 
    const std::vector<duckdb::LogicalType> &arguments, const duckdb::LogicalType &return_type);

bool HasScalarFunction(const std::string &name);

// Resolves an aggregate function by name and concrete argument types.
Result<duckdb::AggregateFunction> ResolveAggregateFunction(
    const std::string &name, const std::vector<duckdb::LogicalType> &arguments);

} // namespace plume::expr
