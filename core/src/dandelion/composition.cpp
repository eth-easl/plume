#include "plume/dandelion/composition.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/csv.hpp"
#include "plume/catalog/local.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/parser/converter.hpp"
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

using catalog::BuildCSVStageInputs;
using catalog::BuildParquetStageInputs;
using catalog::BuildParquetPrepareInputs;
using catalog::DataSourceType;
using catalog::LocalTableDataSource;
using catalog::MaterializeTable;
using catalog::RemoteCSVDataSource;
using catalog::RemoteParquetDataSource;
using parser::ConverterConfig;
using parser::LeafStage;
using parser::PhysicalPlan;

//===----------------------------------------------------------------------===//
// Composition building
//===----------------------------------------------------------------------===//

namespace {

std::string StageTemplVar(int stage) { return "st_" + std::to_string(stage); }
std::string TableInVar(int stage) { return "tin_" + std::to_string(stage); }
std::string RemoteVar(int stage) { return "info_" + std::to_string(stage); }
std::string RemoteInfoVar(int stage) { return "info_" + std::to_string(stage); }
std::string RemoteReqVar(int stage) { return "req_" + std::to_string(stage); }
std::string OutVar(int stage) { return "out_" + std::to_string(stage); }
std::string DynFilterVar(int stage) { return "dfltr_" + std::to_string(stage); }

struct DeclarationTracker {
    bool using_http = false;    // -> the runtime-provided HTTP function (file fetches)
    bool using_stage = false;   // -> plume_stage (BLOCKS / join / compute stages)
    bool using_csv = false;     // -> plume_csv_stage
    bool using_pq_prep = false; // -> plume_pq_prepare
    bool using_pq = false;      // -> plume_pq_stage

    void AppendDeclarations(std::ostringstream &out) const {
        if (using_http) {
            out << "function HTTP (requests) => (headers, bodies);\n";
        }
        if (using_stage) {
            out << "function plume_stage (template, inData, inData2) => (outData, dynFilter" EXTRA_OUT_SET ");\n";
        }
        if (using_csv) {
            out << "function plume_csv_stage (template, chunkInfo, inBuffers) => (outData, dynFilter" EXTRA_OUT_SET ");\n";
        }
        if (using_pq_prep) {
            out << "function plume_pq_prepare (config, footer, url, dynFilter) => (region, chunkReq" EXTRA_OUT_SET ");\n";
        }
        if (using_pq) {
            out << "function plume_pq_stage (template, regionInfo, inBuffers) => (outData, dynFilter" EXTRA_OUT_SET ");\n";
        }
    }
};

} // namespace

Result<DandelionComposition> BuildDandelionComposition(duckdb::Connection &con, 
        const PhysicalPlan &plan, const std::string &name, const ConverterConfig &config) {
    DandelionComposition comp;
    comp.name = name;

    DeclarationTracker dt;
    std::ostringstream fappls;
    std::ostringstream comp_input;
    auto add_input = [&comp, &comp_input](DataItemVec &&set, const std::string &in_name) {
        comp.in_sets.push_back(set);
        comp_input << in_name << ", ";
    };
    auto dyn_filter_output = [](const parser::Stage &s) -> std::string {
        return s.ProducesDynFilter() ? ", " + DynFilterVar(static_cast<int>(s.idx)) + " = dynFilter" : "";
    };
    for (const auto &stage : plan.stages) {
        std::string st_var = StageTemplVar(stage->idx);
        comp_input << st_var << ", ";
        comp.in_sets.push_back({DataItem{0, "", plume::SerializePipeline(stage->pipeline)}});

        if (stage->IsLeaf()) {
            LeafStage *leaf_stage = static_cast<LeafStage*>(stage.get());
            const auto &data_source = leaf_stage->data_source;
            const auto &projection = leaf_stage->projection ? *leaf_stage->projection : std::vector<uint32_t>{};
            const expr::ExprNode *filter = leaf_stage->pushed_filter.get();

            switch (data_source->type) {
            case DataSourceType::LOCAL_TABLE: {
                dt.using_stage = true;
                std::string data_var = TableInVar(stage->idx);

                auto table_source = std::static_pointer_cast<LocalTableDataSource>(data_source);
                TRY(auto materialized, MaterializeTable(con, *table_source, projection, filter));
                add_input(std::move(materialized), data_var);

                fappls << "  plume_stage (template = all " << st_var << ", inData = keyed "
                       << data_var << ") => (" << OutVar(stage->idx) << " = outData"
                       << dyn_filter_output(*stage) << ");\n";
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

                    auto pq_src = std::static_pointer_cast<RemoteParquetDataSource>(leaf_stage->data_source);
                    if (leaf_stage->HasDynFilter()) {
                        dt.using_pq_prep = true;
                        std::string cfg_var = "cfg_" + std::to_string(stage->idx);
                        std::string footer_var = "ftr_" + std::to_string(stage->idx);
                        std::string url_var = "url_" + std::to_string(stage->idx);
                        auto pq_prepare_inputs = BuildParquetPrepareInputs(*pq_src, projection, filter, 
                            leaf_stage->dynamic_filter_column, leaf_stage->source_splits, 
                            config.coalesce_distance, config.max_region_bytes);
                        add_input(std::move(pq_prepare_inputs.cfg), cfg_var);
                        add_input(std::move(pq_prepare_inputs.footers), footer_var);
                        add_input(std::move(pq_prepare_inputs.urls), url_var);

                        fappls << "  plume_pq_prepare (config = all " << cfg_var
                              << ", footer = anyKeyed " << footer_var
                              << ", url = anyKeyed " << url_var << ", dynFilter = all " 
                              << DynFilterVar(leaf_stage->dynamic_filter_source_stage) << ") => (" << info_var << " = region, " 
                              << req_var << " = chunkReq) by footer inner url;\n";
                    } else {
                        TRY(auto pq_stage_inputs, BuildParquetStageInputs(*pq_src, projection, filter,
                            leaf_stage->source_splits, config.coalesce_distance, config.max_region_bytes));
                        add_input(std::move(pq_stage_inputs.region_info), info_var);
                        add_input(std::move(pq_stage_inputs.chunk_reqs), req_var);
                    }

                    fappls << "  HTTP (requests = each " << req_var << ") => (" << data << " = bodies);\n";
                    fappls << "  plume_pq_stage (template = all " << st_var << ", regionInfo = keyed " << info_var
                           << ", inBuffers = keyed " << data << ") => (" << OutVar(stage->idx)
                           << " = outData" << dyn_filter_output(*stage) << ") by regionInfo inner inBuffers;\n";
                } else {
                    dt.using_csv = true;

                    auto csv_src = std::static_pointer_cast<RemoteCSVDataSource>(leaf_stage->data_source);
                    TRY(auto csv_stage_inputs,
                        BuildCSVStageInputs(*csv_src, projection, leaf_stage->source_splits, config.max_region_bytes));
                    add_input(std::move(csv_stage_inputs.chunk_info), info_var);
                    add_input(std::move(csv_stage_inputs.chunk_reqs), req_var);

                    fappls << "  HTTP (requests = each " << req_var << ") => (" << data << " = bodies);\n";
                    fappls << "  plume_csv_stage (template = all " << st_var << ", chunkInfo = keyed " << info_var
                           << ", inBuffers = keyed " << data << ") => (" << OutVar(stage->idx)
                           << " = outData" << dyn_filter_output(*stage) << ") by chunkInfo inner inBuffers;\n";
                }
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
            if (stage->LeadsWithJoin()) {
                in2_parallel = plan.stages[stage->input_stages[1]]->pipeline.output_split.partitions > 1;
                fappls << ", inData2 = " << (in2_parallel ? "keyed" : "all") << " "
                       << OutVar(stage->input_stages[1]);
            }
    
            fappls << ") => (" << OutVar(stage->idx) << " = outData" << dyn_filter_output(*stage) << ")";
            if (stage->LeadsWithJoin() && in_parallel && in2_parallel) {
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
    std::string comp_input_str = comp_input.str();
    if (comp_input_str.size() >= 2) {
        comp_input_str.resize(comp_input_str.size() - 2); // remove trailing ", "
    }

    // Write final composition dsl.
    std::ostringstream out;
    dt.AppendDeclarations(out);
    out << "\ncomposition " << name << " (" << comp_input_str << ") => (" << OutVar(plan.root_idx) << ") {\n";
    out << fappls.str();
    out << "}\n";
    
    comp.dsl = out.str();
    return comp;
}

Result<std::vector<std::string>> ParseCompositionInputNames(const DandelionComposition &comp) {
    const std::string marker = "composition " + comp.name + " (";
    size_t start = comp.dsl.find(marker);
    if (start == std::string::npos) {
        return Error("dsl does not contain a 'composition " + comp.name + " (' header", ErrorKind::InvalidInput);
    }
    start += marker.size();
    size_t end = comp.dsl.find(')', start);
    if (end == std::string::npos) {
        return Error("dsl composition header is missing its closing ')'", ErrorKind::InvalidInput);
    }

    std::vector<std::string> names;
    const std::string params = comp.dsl.substr(start, end - start);
    size_t pos = 0;
    while (pos < params.size()) {
        size_t comma = params.find(',', pos);
        std::string tok = params.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
        size_t a = tok.find_first_not_of(" \t");
        if (a != std::string::npos) {
            size_t b = tok.find_last_not_of(" \t");
            names.push_back(tok.substr(a, b - a + 1));
        }
        if (comma == std::string::npos) {
            break;
        }
        pos = comma + 1;
    }
    return names;
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

Result<BinaryData> InvocationBody(const DandelionComposition& comp, bool is_registered) {
    json sets;
    for (const auto &set : comp.in_sets) {
        sets.push_back(SourceItemSet("", set));
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

namespace {

Result<std::string> RenderChunks(const std::vector<std::unique_ptr<duckdb::DataChunk>> &chunks,
                                 const Schema &schema, bool write_header = true) {
    std::ostringstream out;
    if (write_header) {
        std::string header;
        for (size_t c = 0; c < schema.columns.size(); c++) {
            if (c) out << "\t";
            out << schema.columns[c].name;
        }
        out << "\n";
    }

    for (const auto &chunk : chunks) {
        for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
            std::string row;
            for (duckdb::idx_t c = 0; c < chunk->ColumnCount(); c++) {
                if (c) out << "\t";
                out << chunk->GetValue(c, r).ToString();
            }
            out << "\n";
        }
    }
    return out.str();
}

} // namespace

Result<std::string> ParseAndRenderResponseBody(const dandelion::BinaryData& data, const Schema &schema, 
        std::string* timestamps) {
    TRY(auto sets, dandelion::ParseResponseBody(data, timestamps));
    if (sets.size() == 0) {
        return "";
    }

    std::string out;
    bool write_header = true;
    for (auto &block : sets[0]) {
        plume::Schema block_schema;
        TRY(auto chunks, memory::ImportBlockChunks(block.data.data(), block.data.size(), block_schema));
        if (block_schema.columns.size() != schema.columns.size()) {
            return Error("Block schema does not match given schema.");
        }
        TRY(auto chunk_str, RenderChunks(chunks, schema, write_header));
        out += chunk_str;
        write_header = false;
    }
    return out;
}

} // namespace plume::dandelion
