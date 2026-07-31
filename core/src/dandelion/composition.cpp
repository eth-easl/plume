#include "plume/dandelion/composition.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/parser/physical_plan.hpp"

#include <nlohmann/json.hpp>

#include <memory>
#include <sstream>

// TODO: move to cmake config
#define ENABLE_STDIO // uncomment to use function with stdio set registered (debugging)
#ifdef ENABLE_STDIO
# define EXTRA_OUT_SET ", stdio"
#else
# define EXTRA_OUT_SET
#endif

namespace plume::dandelion {

using catalog::DataSourceType;
using parser::LeafStage;
using parser::PhysicalPlan;

//===----------------------------------------------------------------------===//
// Composition building
//===----------------------------------------------------------------------===//

namespace {

std::string StageTemplVar(int stage) { return "st_" + std::to_string(stage); }
std::string TableInVar(int stage) { return "tin_" + std::to_string(stage); }
std::string RemoteInfoVar(int stage) { return "info_" + std::to_string(stage); }
std::string RemoteReqVar(int stage) { return "req_" + std::to_string(stage); }
std::string OutVar(int stage) { return "out_" + std::to_string(stage); }

struct DeclarationTracker {
    bool using_http = false;  // -> the runtime-provided HTTP function (file fetches)
    bool using_stage = false; // -> plume_stage (BLOCKS / join / compute stages)
    bool using_csv = false;   // -> plume_csv_stage
    bool using_pq = false;    // -> plume_pq_stage

    void AppendDeclarations(std::ostringstream &out) const {
        if (using_http) {
            out << "function HTTP (requests) => (headers, bodies);\n";
        }
        if (using_stage) {
            out << "function plume_stage (template, inData, inData2) => (outData" EXTRA_OUT_SET ");\n";
        }
        if (using_csv) {
            out << "function plume_csv_stage (template, chunkInfo, inBuffers) => (outData" EXTRA_OUT_SET ");\n";
        }
        if (using_pq) {
            out << "function plume_pq_stage (template, regionInfo, inBuffers) => (outData" EXTRA_OUT_SET ");\n";
        }
    }
};

} // namespace

Result<DandelionComposition> BuildDandelionComposition(const PhysicalPlan &plan, const std::string &name) {
    DandelionComposition comp;
    comp.name = name;

    // Iterate over stages adding the function application(s) and inputs of each.
    DeclarationTracker dt;
    std::ostringstream fappls;
    for (const auto &stage : plan.stages) {
        std::string st_var = StageTemplVar(stage->idx);

        StageTemplate templ;
        templ.var = st_var;
        templ.buf = plume::SerializePipeline(stage->pipeline);
        comp.stage_templates.push_back(std::move(templ));

        if (stage->is_leaf()) {
            LeafStage *leaf_stage = static_cast<LeafStage*>(stage.get());
            const auto &data_source = leaf_stage->data_source;
            switch (data_source->type) {
            case DataSourceType::LOCAL_TABLE: {
                dt.using_stage = true;
                std::string data_var = TableInVar(stage->idx);
                comp.table_inputs.push_back({
                    data_var, data_source, 
                    leaf_stage->projection, leaf_stage->pushed_filter
                });
                fappls << "  plume_stage (template = all " << st_var << ", inData = keyed "
                       << data_var << ") => (" << OutVar(stage->idx) << " = outData);\n";
                break;
            }
            case DataSourceType::REMOTE_PARQUET:
            case DataSourceType::REMOTE_CSV:
            {
                dt.using_http = true;
                std::string info_var = RemoteInfoVar(stage->idx);
                std::string req_var = RemoteReqVar(stage->idx);
                const std::string data = "data_" + std::to_string(stage->idx);

                if (data_source->type == DataSourceType::REMOTE_PARQUET) {
                    dt.using_pq = true;
                    fappls << "  HTTP (requests = each " << req_var << ") => (" << data << " = bodies);\n";
                    fappls << "  plume_pq_stage (template = all " << st_var << ", regionInfo = keyed " << info_var
                           << ", inBuffers = keyed " << data << ") => (" << OutVar(stage->idx) 
                           << " = outData) by regionInfo inner inBuffers;\n";
                } else {
                    dt.using_csv = true;
                    fappls << "  HTTP (requests = each " << req_var << ") => (" << data << " = bodies);\n";
                    fappls << "  plume_csv_stage (template = all " << st_var << ", chunkInfo = keyed " << info_var
                           << ", inBuffers = keyed " << data << ") => (" << OutVar(stage->idx) 
                           << " = outData) by chunkInfo inner inBuffers;\n";
                }

                comp.remote_inputs.push_back({
                    std::move(info_var), std::move(req_var), data_source,
                    leaf_stage->projection, leaf_stage->pushed_filter, leaf_stage->source_splits
                });
                break;
            }
            }
        } else {
            dt.using_stage = true;
            if (stage->input_stages.size() == 0) {
                return Error("Non leaf stage has no input stages.", ErrorKind::RuntimeError);
            }

            bool in_parallel = plan.stages[stage->input_stages[0]]->pipeline.output_split.partitions > 1;
            fappls << "  plume_stage (template = all " << st_var << ", inData = "
                   << (in_parallel ? "keyed" : "all") << " " << OutVar(stage->input_stages[0]);
            
            bool in2_parallel = false;
            if (stage->leads_with_join()) {
                in2_parallel = plan.stages[stage->input_stages[1]]->pipeline.output_split.partitions > 1;
                fappls << ", inData2 = " << (in2_parallel ? "keyed" : "all") << " "
                       << OutVar(stage->input_stages[1]);
            }
    
            fappls << ") => (" << OutVar(stage->idx) << " = outData)";
            if (stage->leads_with_join() && in_parallel && in2_parallel) {
                auto join_template = std::static_pointer_cast<exec::JoinTemplate>(stage->pipeline.operators[0]);
                const char *join_strategy;
                switch (join_template->kind) {
                case exec::JoinKind::INNER:
                    join_strategy = "inner";
                    break;
                case exec::JoinKind::LEFT:
                case exec::JoinKind::SEMI:
                case exec::JoinKind::ANTI:
                case exec::JoinKind::SINGLE:
                case exec::JoinKind::MARK:
                    join_strategy = "left";
                    break;
                case exec::JoinKind::RIGHT:
                case exec::JoinKind::RIGHT_SEMI:
                case exec::JoinKind::RIGHT_ANTI:
                    join_strategy = "right";
                    break;
                case exec::JoinKind::OUTER:
                    join_strategy = "full";
                    break;
                }
                fappls << " by inData " << join_strategy << " inData2";
            }
            fappls << ";\n";
        }
    }

    // Write final composition dsl.
    std::ostringstream out;
    dt.AppendDeclarations(out);
    out << "\ncomposition " << name << " (";
    bool first = true;
    auto param = [&](const std::string &p) {
        out << (first ? "" : ", ") << p;
        first = false;
    };
    for (const auto &ti : comp.table_inputs) {
        param(ti.var);
    }
    for (const auto &ri : comp.remote_inputs) {
        param(ri.var_info);
        param(ri.var_req);
    }
    for (const auto &st : comp.stage_templates) {
        param(st.var);
    }
    out << ") => (" << OutVar(plan.root_idx) << ") {\n";
    out << fappls.str();
    out << "}\n";
    comp.dsl = out.str();

    return comp;
}

//===----------------------------------------------------------------------===//
// Composition execution
//===----------------------------------------------------------------------===//

using json = nlohmann::json;

namespace {

// Build a json set (identifier + items) from precomputed keyed source items.
json SourceItemSet(const std::string& var, const DataItemVec& items) {
    json json_items;
    for (const auto& itm : items) {
        json_items.push_back(json{
            {"identifier", itm.identifier},
            {"key", itm.key},
            {"data", json::binary(itm.data)}
        });
    }
    return json{{"identifier", var}, {"items", json_items}};
}

} // namespace

BinaryData RegistrationBody(const DandelionComposition& comp) {
    json body;
    body["composition"] = comp.dsl;
    return json::to_bson(body);
}

Result<BinaryData> InvocationBody(const DandelionComposition& comp,
        const DataSetVec &table_blocks, const DataSetVec &remote_infos, 
        const DataSetVec &remote_reqs, bool is_registered) {
    if (table_blocks.size() != comp.table_inputs.size()) {
        return Error("table_blocks size does not match composition table inputs size.");
    }
    if (remote_infos.size() != comp.remote_inputs.size()) {
        return Error("remote_infos size does not match composition remote inputs size.");
    }
    if (remote_reqs.size() != comp.remote_inputs.size()) {
        return Error("remote_infos size does not match composition remote inputs size.");
    }

    json sets;

    // First sets correspond to table input data.
    for (size_t i = 0; i < comp.table_inputs.size(); i++) {
        sets.push_back(SourceItemSet(comp.table_inputs[i].var, table_blocks[i]));
    }

    // Next sets correspond to remote input data.
    for (size_t i = 0; i < comp.remote_inputs.size(); i++) {
        sets.push_back(SourceItemSet(comp.remote_inputs[i].var_info, remote_infos[i]));
        sets.push_back(SourceItemSet(comp.remote_inputs[i].var_req, remote_reqs[i]));
    }

    // Remaining sets are stage pipeline templates.
    for (const auto& st : comp.stage_templates) {
        json items;
        items.push_back(json{
            {"identifier", ""},
            {"key", 0},
            {"data", json::binary(st.buf)}
        });
        sets.push_back(json{
            {"identifier", ""},
            {"items", items}
        });
    }

    json body;
    if (is_registered) {
        body["name"] = comp.name;
    } else {
        body["composition"] = comp.dsl;
    }
    body["sets"] = sets;
    return json::to_bson(body);
}

Result<DataSetVec> ParseResponseBody(const BinaryData& body, std::string* timestamps) {
    json body_json;
    try {
        body_json= json::from_bson(body);
    } catch (const json::parse_error& e) {
        return Error("Parse error: " + std::string(e.what()));
    } catch (const json::exception& e) {
       return Error("JSON exception: " + std::string(e.what()));
    } catch (const std::exception& e) {
        return Error("Standard exception: " + std::string(e.what()));
    } catch (...) {
        return Error("Unexpected error while parsing bson");
    }

    DataSetVec out_sets;
    if (body_json.contains("sets") && body_json["sets"].is_array()) {
        out_sets.reserve(body_json["sets"].size());
        for (const auto& set : body_json["sets"]) {
            DataItemVec out_items;
            if (set.contains("items") && set["items"].is_array()) {
                out_items.reserve(set["items"].size());
                for (const auto& item : set["items"]) {
                    if (!item.contains("data") || !item["data"].is_binary()) {
                        return Error("Item is missing required binary 'data' field.");
                    }
                    out_items.push_back({
                        (item.contains("key") && item["key"].is_number_integer()) ? static_cast<uint64_t>(item["key"]) : 0,
                        (item.contains("identifier") && item["identifier"].is_string()) ? item["identifier"].get<std::string>() : "",
                        item["data"].get<nlohmann::json::binary_t>()
                    });
                }
                out_sets.push_back(out_items);
            } else {
                return Error("Set does not contain array field called 'items'.");
            }
        }
    } else {
        return Error("Body does not contain array field called 'sets'.");
    }

    if (timestamps) {
        if (body_json.contains("timestamps")) {
            *timestamps = body_json["timestamps"].get<std::string>();
        } else {
            *timestamps = "";
        }
    }

    return out_sets;
}


} // namespace plume::dandelion
