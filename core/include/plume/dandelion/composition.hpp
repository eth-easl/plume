#pragma once

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/parser/physical_plan.hpp"

#include <memory>
#include <string>
#include <vector>

namespace plume::dandelion {

struct StageTemplate {
    std::string var; // composition input identifier (e.g. "opt_0")
    BinaryData buf;  // serialized PipelineTemplate
};

struct TableInput {
    std::string var;                    // composition input identifier (e.g. "in_0")
    std::shared_ptr<catalog::DataSource> source; // the base table to materialize as blocks
    std::shared_ptr<std::vector<uint32_t>> projection;
    std::shared_ptr<expr::ExprNode> pushed_filter;
    uint32_t target_splits = 1;
};

struct RemoteInput {
    std::string var_info;               // composition input for the region/chunk infos (e.g. "info_0")
    std::string var_req;                // composition input for the fetch requests (e.g. "req_0")
    std::shared_ptr<catalog::DataSource> source; // the base table to materialize as blocks
    std::shared_ptr<std::vector<uint32_t>> projection;
    std::shared_ptr<expr::ExprNode> pushed_filter;
    uint32_t target_splits = 1;
};

// Everything needed to register + invoke a query as a dandelion composition.
struct DandelionComposition {
    std::string name;
    std::string dsl;                            // the textual composition
    std::vector<TableInput> table_inputs;       // table block inputs
    std::vector<RemoteInput> remote_inputs;     // remote file inputs
    std::vector<StageTemplate> stage_templates; // per-stage pipeline templates 
};

Result<DandelionComposition> BuildDandelionComposition(const parser::PhysicalPlan &plan, const std::string &name);

BinaryData RegistrationBody(const DandelionComposition& comp);

Result<BinaryData> InvocationBody(const DandelionComposition& comp, const DataSetVec &table_blocks,
    const DataSetVec &remote_infos, const DataSetVec &remote_reqs, bool is_registered = false);

Result<DataSetVec> ParseResponseBody(const BinaryData& body,
    std::string* timestamps = nullptr);

} // namespace plume::dandelion
