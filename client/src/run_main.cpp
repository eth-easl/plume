#include "plume/client/client.hpp"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

namespace fs = std::filesystem;

namespace {

struct Args {
    std::string sql;
    std::string url = "http://localhost:8080";
    std::string results_prefix; // empty -> print to console
    uint32_t timeout_secs = 60;
    plume::parser::ConverterConfig config;
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
        if (arg == "--url") {
            TRY(a.url, next("--url"));
        } else if (arg == "--results") {
            TRY(a.results_prefix, next("--results"));
        } else if (arg == "--timeout") {
            TRY(auto v, next("--timeout"));
            a.timeout_secs = static_cast<uint32_t>(std::stoul(v));
        } else if (arg == "--no-pre-aggregate") {
            a.config.early_aggregation = false;
        } else if (arg == "--no-projection-pushdown") {
            a.config.projection_pushdown = false;
        } else if (arg == "--no-filter-pushdown") {
            a.config.filter_pushdown = false;
        } else if (arg == "--max-splits") {
            TRY(auto v, next("--max-splits"));
            a.config.max_splits = static_cast<uint32_t>(std::stoul(v));
        } else if (arg == "--target-rows-per-split") {
            TRY(auto v, next("--target-rows-per-split"));
            a.config.target_rows_per_split = std::stoull(v);
        } else if (arg == "--max-region-size") {
            TRY(auto v, next("--max-region-size"));
            a.config.max_region_bytes = std::stoull(v);
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

plume::Result<int> RunMain(int argc, char **argv) {
    TRY(auto args, ParseArgs(argc, argv));
    if (args.sql.empty()) {
        fprintf(stderr,
                "usage: %s [sql] [--url URL] [--results PREFIX] [--timeout SECONDS]\n"
                "       [--no-pre-aggregate] [--no-projection-pushdown] [--no-filter-pushdown]\n"
                "       [--max-splits N] [--target-rows-per-split N] [--max-region-size N]\n"
                "       (sql read from stdin if omitted)\n",
                argv[0]);
        return 2;
    }

    plume::client::Client client(args.config);
    plume::client::ExecutionConfig exec_cfg{args.url, args.timeout_secs};
    TRY(auto response, client.Execute(args.sql, exec_cfg));

    std::string rendered = response.ToString();

    if (args.results_prefix.empty()) {
        std::cout << rendered;
    } else {
        std::error_code ec;
        fs::create_directories(fs::path(args.results_prefix).parent_path(), ec);
        std::ofstream f(args.results_prefix, std::ios::binary);
        f << rendered;
        printf("wrote results to %s\n", args.results_prefix.c_str());
    }
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
