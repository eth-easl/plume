#include "plume/client/client.hpp"

#include <nlohmann/json.hpp>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

struct Args {
    std::string sql;
    std::string output_prefix = "."; // where the composition + buffers are written
    plume::parser::ConverterConfig config;
    size_t fetcher_threads = 10;
};

plume::Result<Args> ParseArgs(int argc, char **argv) {
    Args a;
    bool have_sql = false;
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        auto next = [&](const char *flag) -> plume::Result<std::string> {
            if (i + 1 >= argc) {
                return plume::Error(std::string("missing value for ") + flag, plume::ErrorKind::InvalidInput);
            }
            return std::string(argv[++i]);
        };
        if (arg == "--output") {
            TRY(a.output_prefix, next("--output"));
        } else if (arg == "--no-pre-aggregate") {
            a.config.early_aggregation = false;
        } else if (arg == "--no-projection-pushdown") {
            a.config.projection_pushdown = false;
        } else if (arg == "--no-filter-pushdown") {
            a.config.filter_pushdown = false;
        } else if (arg == "--no-optimize-remote-fetching") {
            a.config.optimize_remote_fetching = false;
        } else if (arg == "--max-splits") {
            TRY(auto v, next("--max-splits"));
            a.config.max_splits = static_cast<uint32_t>(std::stoul(v));
        } else if (arg == "--target-rows-per-split") {
            TRY(auto v, next("--target-rows-per-split"));
            a.config.target_rows_per_split = std::stoull(v);
        } else if (arg == "--max-region-size") {
            TRY(auto v, next("--max-region-size"));
            a.config.max_region_bytes = std::stoull(v);
        } else if (arg == "--fetcher-threads") {
            TRY(auto v, next("--fetcher-threads"));
            a.fetcher_threads = std::stoull(v);
        } else if (!have_sql && !arg.empty() && arg.rfind("--", 0) != 0) {
            a.sql = arg;
            have_sql = true;
        }
    }
    if (!have_sql || a.sql.empty()) {
        a.sql = std::string(std::istreambuf_iterator<char>(std::cin), std::istreambuf_iterator<char>());
    }
    return a;
}

void WriteBytes(const fs::path &path, const uint8_t *data, size_t size) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char *>(data), static_cast<std::streamsize>(size));
}

void WriteText(const fs::path &path, const std::string &text) {
    std::ofstream f(path, std::ios::binary);
    f << text;
}

// Dumps one keyed set of items (block/region/request buffers) to files named
// `var_K.ext`, returning the manifest entries describing them.
json DumpItems(const fs::path &prefix, const std::string &var, const plume::dandelion::DataItemVec &items,
               const char *ext) {
    json files = json::array();
    for (size_t k = 0; k < items.size(); k++) {
        const std::string file = var + "_" + std::to_string(k) + "." + ext;
        WriteBytes(prefix / file, items[k].data.data(), items[k].data.size());
        files.push_back({{"file", file}, {"key", items[k].key}, {"identifier", items[k].identifier}});
    }
    return files;
}

plume::Result<int> RunMain(int argc, char **argv) {
    TRY(auto args, ParseArgs(argc, argv));
    if (args.sql.empty()) {
        fprintf(stderr,
                "usage: %s [sql] [--output PREFIX] [--no-pre-aggregate] [--no-projection-pushdown] "
                "[--no-filter-pushdown] [--no-optimize-remote-fetching] [--max-splits N] "
                "[--target-rows-per-split N] [--max-region-size N] [--fetcher-threads N]\n"
                "       (sql read from stdin if omitted)\n",
                argv[0]);
        return 2;
    }
    const fs::path prefix = fs::path(args.output_prefix);
    std::error_code ec;
    fs::create_directories(prefix, ec);

    plume::client::Client client(args.config, args.fetcher_threads);
    TRY(auto compiled, client.Resolve(args.sql, "Query"));
    const auto &comp = compiled.composition;

    json manifest;
    manifest["name"] = comp.name;

    // Composition DSL.
    WriteText(prefix / "composition.dwf", comp.dsl);
    manifest["composition"] = "composition.dwf";

    // Per-stage pipeline templates.
    for (const auto &st : comp.stage_templates) {
        const std::string file = st.var + ".tmpl";
        WriteBytes(prefix / file, st.buf.data(), st.buf.size());
        manifest["stage_templates"].push_back({{"var", st.var}, {"file", file}});
    }

    // Base-table block inputs (materialized data).
    for (size_t i = 0; i < comp.table_inputs.size(); i++) {
        const auto &ti = comp.table_inputs[i];
        manifest["table_inputs"].push_back({{"var", ti.var},
                                             {"source_table", ti.source->name},
                                             {"blocks", DumpItems(prefix, ti.var, compiled.table_blocks[i], "blk")}});
    }

    // Remote (CSV/parquet) inputs: the client-precomputed region/chunk infos and the
    // byte-range fetch requests the composition feeds to the stage + HTTP functions.
    for (size_t i = 0; i < comp.remote_inputs.size(); i++) {
        const auto &ri = comp.remote_inputs[i];
        manifest["remote_inputs"].push_back(
            {{"var_info", ri.var_info},
             {"var_req", ri.var_req},
             {"format", ri.source->type == plume::catalog::DataSourceType::REMOTE_PARQUET ? "PARQUET" : "CSV"},
             {"paths", ri.source->paths},
             {"info_items", DumpItems(prefix, ri.var_info, compiled.remote_info[i], "bin")},
             {"req_items", DumpItems(prefix, ri.var_req, compiled.remote_requests[i], "bin")}});
    }

    WriteText(prefix / "manifest.json", manifest.dump(2));

    printf("wrote composition '%s' (%zu stage(s), %zu table input(s), %zu remote input(s)) to %s\n",
           comp.name.c_str(), comp.stage_templates.size(), comp.table_inputs.size(), comp.remote_inputs.size(),
           prefix.string().c_str());
    return 0;
}

} // namespace

int main(int argc, char **argv) {
    try {
        auto res = RunMain(argc, argv);
        if (res.is_error()) {
            fprintf(stderr, "error: %s\n", res.error().message().c_str());
            return 1;
        }
        return res.unwrap();
    } catch (const std::exception &e) {
        fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
