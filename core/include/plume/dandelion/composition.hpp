#pragma once

#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/parser/physical_plan.hpp"
#include "plume/parser/converter.hpp"

#include <string>
#include <vector>

namespace duckdb {
class Connection;
} // namespace duckdb

namespace plume::dandelion {

// Everything needed to register + invoke a query as a dandelion composition.
struct DandelionComposition {
    std::string name;
    std::string dsl;
    DataSetVec in_sets;
};

Result<DandelionComposition> BuildDandelionComposition(duckdb::Connection &con,
    const parser::PhysicalPlan &plan, const std::string &name, const parser::ConverterConfig &config);

Result<std::vector<std::string>> ParseCompositionInputNames(const DandelionComposition &comp);

BinaryData RegistrationBody(const DandelionComposition& comp);

Result<BinaryData> InvocationBody(const DandelionComposition& comp, bool is_registered = false);

Result<DataSetVec> ParseResponseBody(const BinaryData& body,
    std::string* timestamps = nullptr);

Result<std::string> ParseAndRenderResponseBody(const dandelion::BinaryData& data, 
    const Schema &schema, std::string* timestamps = nullptr);

} // namespace plume::dandelion
