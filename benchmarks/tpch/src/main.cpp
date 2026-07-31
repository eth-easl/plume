#include "config.hpp"
#include "runner.hpp"

#include <cstdio>
#include <exception>

using namespace plume::bench;

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <config.json> [<config.json> ...]\n", argv[0]);
        return 2;
    }

    int rc = 0;
    for (int i = 1; i < argc; i++) {
        printf("=== running config '%s' ===\n", argv[i]);
        try {
            auto cfg = BenchmarkConfig::FromJsonFile(argv[i]);
            if (cfg.is_error()) {
                fprintf(stderr, "error: %s\n", cfg.error().message().c_str());
                rc = 1;
                continue;
            }
            Runner runner(std::move(cfg).unwrap());
            auto status = runner.Run();
            if (status.is_error()) {
                fprintf(stderr, "error: %s\n", status.error().message().c_str());
                rc = 1;
            }
        } catch (const std::exception &e) {
            fprintf(stderr, "error: %s\n", e.what());
            rc = 1;
        }
    }
    return rc;
}
