#include "plume/client/client.hpp"
#include "plume/common/serial.hpp"

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

json DumpItems(const fs::path &prefix, const std::string &name, const plume::dandelion::DataItemVec &items) {
    json files = json::array();
    for (size_t i = 0; i < items.size(); i++) {
        const std::string file = name + "_" + std::to_string(i) + ".bin";
        WriteBytes(prefix / file, items[i].data.data(), items[i].data.size());
        files.push_back({{"file", file}, {"key", items[i].key}, {"identifier", items[i].identifier}});
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

    // composition DSL
    WriteText(prefix / "composition.dwf", comp.dsl);
    manifest["composition"] = "composition.dwf";

    // output schema
    auto schema_buf = plume::SerializeToBuffer(compiled.output_schema);
    WriteBytes(prefix / "schema.bin", schema_buf.data(), schema_buf.size());
    manifest["schema"] = "schema.bin";

    // every composition input
    TRY(auto names, plume::dandelion::ParseCompositionInputNames(comp));
    if (names.size() != comp.in_sets.size()) {
        return plume::Error("composition has " + std::to_string(comp.in_sets.size()) +
                            " input set(s) but the dsl names " + std::to_string(names.size()),
                            plume::ErrorKind::RuntimeError);
    }
    for (size_t i = 0; i < comp.in_sets.size(); i++) {
        manifest["inputs"].push_back({{"name", names[i]}, {"items", DumpItems(prefix, names[i], comp.in_sets[i])}});
    }

    WriteText(prefix / "manifest.json", manifest.dump(2));

    printf("wrote composition '%s' (%zu input set(s)) to %s\n", comp.name.c_str(), comp.in_sets.size(),
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
