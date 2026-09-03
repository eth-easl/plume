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
    if (!stage.IsLeaf()) {
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
    size_t fetcher_threads = 10;

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
        } else if (a == "--no-optimize-remote-fetching") {
            config.optimize_remote_fetching = false;
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
        } else if (a == "--fetcher-threads") {
            if (++i >= argc) {
                std::cerr << "ubench_export: --fetcher-threads needs an argument\n";
                return 2;
            }
            fetcher_threads = std::stoull(argv[i]);
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

    auto compiled = CompileQuery(con, *catalog, sql, name, config, fetcher_threads);
    if (compiled.is_error()) {
        std::cerr << "ubench_export: compile failed: " << compiled.error().message() << "\n";
        return 1;
    }
    CompiledComposition cq = std::move(compiled).unwrap();

    ubench::UbPlan plan;
    plan.name = name;
    plan.root_stage = static_cast<int32_t>(cq.plan->root_idx);
    plan.stages.reserve(cq.plan->stages.size());

    // CompileQuery no longer hands back per-stage materialized data separately
    // (table_blocks/remote_info/remote_requests) -- BuildDandelionComposition now
    // materializes everything itself, interleaved into one flat comp.in_sets, in the
    // exact order it walks plan.stages: one set per stage for its serialized pipeline
    // template, then (for a leaf stage only) the sets for its materialized/precomputed
    // source data. Replay that same walk here to pull each stage's data back out.
    const auto &in_sets = cq.composition.in_sets;
    size_t set_idx = 0;
    // Pulls the next set off in_sets in walk order, or prints an error and returns
    // nullptr if the composition ran out (a bug in this replay, or a mismatched core).
    auto next_set = [&](const char *what, int32_t stage_id) -> const dandelion::DataItemVec * {
        if (set_idx >= in_sets.size()) {
            std::cerr << "ubench_export: missing " << what << " input set for stage " << stage_id << "\n";
            return nullptr;
        }
        return &in_sets[set_idx++];
    };

    for (const auto &stage : cq.plan->stages) {
        ubench::UbStage us;
        us.id = static_cast<int32_t>(stage->idx);
        us.source = MapKind(*stage);
        us.partitions = stage->pipeline.output_split.partitions == 0 ? 1 : stage->pipeline.output_split.partitions;
        us.leads_with_join = stage->LeadsWithJoin();
        for (size_t up : stage->input_stages) {
            us.input_stages.push_back(static_cast<int32_t>(up));
        }
        us.pipeline_blob = SerializePipeline(stage->pipeline);

        // The stage's own pipeline template set (BuildDandelionComposition pushes this
        // for every stage, leaf or not).
        if (!next_set("template", us.id)) {
            return 1;
        }

        if (stage->IsLeaf()) {
            const auto &leaf = static_cast<const LeafStage &>(*stage);
            if (leaf.data_source->type == DataSourceType::LOCAL_TABLE) {
                const auto *blocks = next_set("table blocks", us.id);
                if (!blocks) {
                    return 1;
                }
                for (const auto &blk : *blocks) {
                    us.table_blocks.push_back(FromDataItem(blk));
                }
            } else if (leaf.data_source->type == DataSourceType::REMOTE_PARQUET && leaf.HasDynFilter()) {
                // Runtime-dynamic-filter parquet leaves consume 3 sets (cfg/footer/url)
                // and are prepared server-side via plume_pq_prepare at invocation time --
                // there's no precomputed region/chunk-info for ubench to replay yet.
                std::cerr << "ubench_export: stage " << us.id
                          << " is a dynamic-filter parquet leaf, which ubench does not yet support\n";
                return 1;
            } else {
                // REMOTE_PARQUET (no dynamic filter) or REMOTE_CSV: region/chunk-info,
                // then the byte-range fetch requests.
                const auto *info = next_set("source info", us.id);
                if (!info) {
                    return 1;
                }
                for (const auto &item : *info) {
                    us.source_info.push_back(FromDataItem(item));
                }
                const auto *reqs = next_set("source requests", us.id);
                if (!reqs) {
                    return 1;
                }
                for (const auto &item : *reqs) {
                    us.source_reqs.push_back(FromDataItem(item));
                }
            }
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
