#include "plume/catalog/catalog.hpp"
#include "plume/catalog/plume_remote.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/parser/compile.hpp"
#include "plume/parser/physical_plan.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include "format/plan.hpp"

#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

using namespace plume;
using namespace plume::catalog;
using namespace plume::parser;

namespace {

ubench::UbSourceKind MapKind(const Stage &stage) {
    if (!stage.is_leaf()) {
        return ubench::UbSourceKind::STAGE_OUTPUT;
    }
    switch (static_cast<const LeafStage &>(stage).data_source->type) {
    case DataSourceType::LOCAL_TABLE:
        return ubench::UbSourceKind::TABLE_BLOCKS;
    case DataSourceType::REMOTE_CSV:
        return ubench::UbSourceKind::CSV;
    case DataSourceType::REMOTE_PARQUET:
        return ubench::UbSourceKind::PARQUET;
    }
    return ubench::UbSourceKind::STAGE_OUTPUT;
}

ubench::UbItem FromDataItem(const dandelion::DataItem &d) {
    return ubench::UbItem{d.key, d.identifier, d.data};
}

std::string ReadAll(std::istream &in) {
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

} // namespace

int main(int argc, char **argv) {
    std::string sql;
    std::string out_path = "query.ub";
    std::string name = "Query";
    ConverterConfig config;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "-o" || a == "--output") {
            if (++i >= argc) {
                std::cerr << "ubench_export: " << a << " needs an argument\n";
                return 2;
            }
            out_path = argv[i];
        } else if (a == "--name") {
            if (++i >= argc) {
                std::cerr << "ubench_export: --name needs an argument\n";
                return 2;
            }
            name = argv[i];
        } else if (a == "--no-pre-aggregate") {
            config.early_aggregation = false;
        } else if (a == "--no-projection-pushdown") {
            config.projection_pushdown = false;
        } else if (a == "--no-filter-pushdown") {
            config.filter_pushdown = false;
        } else if (a == "--max-splits") {
            if (++i >= argc) {
                std::cerr << "ubench_export: --max-splits needs an argument\n";
                return 2;
            }
            config.max_splits = static_cast<uint32_t>(std::stoul(argv[i]));
        } else if (a == "--target-rows-per-split") {
            if (++i >= argc) {
                std::cerr << "ubench_export: --target-rows-per-split needs an argument\n";
                return 2;
            }
            config.target_rows_per_split = std::stoull(argv[i]);
        } else if (a == "--max-region-size") {
            if (++i >= argc) {
                std::cerr << "ubench_export: --max-region-size needs an argument\n";
                return 2;
            }
            config.max_region_bytes = std::stoull(argv[i]);
        } else if (!a.empty() && a[0] == '-') {
            std::cerr << "ubench_export: unknown flag " << a << "\n";
            return 2;
        } else {
            sql = a;
        }
    }

    if (sql.empty()) {
        sql = ReadAll(std::cin);
    }
    if (sql.empty()) {
        std::cerr << "ubench_export: no SQL provided (argument or stdin)\n";
        return 2;
    }

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    auto catalog = std::make_shared<SourceCatalog>();
    auto registry = duckdb::make_shared_ptr<PlumeRemoteInfo>();
    registry->catalog = catalog;
    RegisterPlumeRemote(con, std::move(registry));

    auto compiled = CompileQuery(con, *catalog, sql, name, config);
    if (compiled.is_error()) {
        std::cerr << "ubench_export: compile failed: " << compiled.error().message() << "\n";
        return 1;
    }
    CompiledComposition cq = std::move(compiled).unwrap();

    ubench::UbPlan plan;
    plan.name = name;
    plan.root_stage = static_cast<int32_t>(cq.plan.root_idx);
    plan.stages.reserve(cq.plan.stages.size());

    // composition.{table,remote}_inputs, and CompileQuery's parallel materialized
    // {table_blocks,remote_info,remote_requests}, are built by walking plan.stages
    // in order and appending one entry per LOCAL_TABLE / REMOTE_* leaf stage
    // encountered (see BuildDandelionComposition / CompileQuery in core) — replay
    // that exact walk here to match each leaf stage back to its materialized data.
    size_t table_idx = 0, remote_idx = 0;
    for (const auto &stage : cq.plan.stages) {
        ubench::UbStage us;
        us.id = static_cast<int32_t>(stage->idx);
        us.source = MapKind(*stage);
        us.partitions = stage->pipeline.output_split.partitions == 0 ? 1 : stage->pipeline.output_split.partitions;
        us.leads_with_join = stage->leads_with_join();
        for (size_t up : stage->input_stages) {
            us.input_stages.push_back(static_cast<int32_t>(up));
        }
        us.pipeline_blob = SerializePipeline(stage->pipeline);

        if (us.source == ubench::UbSourceKind::TABLE_BLOCKS) {
            if (table_idx >= cq.table_blocks.size()) {
                std::cerr << "ubench_export: no materialized blocks for stage " << us.id << "\n";
                return 1;
            }
            for (const auto &blk : cq.table_blocks[table_idx]) {
                us.table_blocks.push_back(FromDataItem(blk));
            }
            table_idx++;
        } else if (us.source == ubench::UbSourceKind::CSV || us.source == ubench::UbSourceKind::PARQUET) {
            if (remote_idx >= cq.remote_info.size() || remote_idx >= cq.remote_requests.size()) {
                std::cerr << "ubench_export: no file inputs for source stage " << us.id << "\n";
                return 1;
            }
            for (const auto &info : cq.remote_info[remote_idx]) {
                us.source_info.push_back(FromDataItem(info));
            }
            for (const auto &req : cq.remote_requests[remote_idx]) {
                us.source_reqs.push_back(FromDataItem(req));
            }
            remote_idx++;
        }

        plan.stages.push_back(std::move(us));
    }

    auto bytes = ubench::WriteUbPlan(plan);
    std::ofstream f(out_path, std::ios::binary | std::ios::trunc);
    if (!f) {
        std::cerr << "ubench_export: cannot open output '" << out_path << "'\n";
        return 1;
    }
    f.write(reinterpret_cast<const char *>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    f.close();

    std::cout << "Wrote " << out_path << " (" << bytes.size() << " bytes, " << plan.stages.size()
              << " stages, root " << plan.root_stage << ")\n";
    return 0;
}
