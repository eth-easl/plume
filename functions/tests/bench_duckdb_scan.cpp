#include "duckdb.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1] : "/users/tstocker/lineitem.parquet";
    const int reps = argc > 2 ? std::atoi(argv[2]) : 5;
    const std::string where = argc > 3 ? std::string(" WHERE ") + argv[3] : "";

    duckdb::DBConfig config;
    config.options.maximum_threads = 1; // single core
    duckdb::DuckDB db(nullptr, &config);
    duckdb::Connection con(db);
    con.Query("SET threads TO 1;");
    con.Query("PRAGMA disable_progress_bar;");

    const std::string sql = "SELECT * FROM read_parquet('" + std::string(path) + "')" + where;

    double best_ms = 1e18;
    uint64_t checksum = 0;
    idx_t total_rows = 0;
    for (int rep = 0; rep < reps; rep++) {
        auto t0 = std::chrono::steady_clock::now();
        auto result = con.SendQuery(sql); // streaming
        if (result->HasError()) {
            std::fprintf(stderr, "duckdb error: %s\n", result->GetError().c_str());
            return 1;
        }
        uint64_t acc = 0;
        idx_t rows = 0;
        for (;;) {
            auto chunk = result->Fetch();
            if (!chunk || chunk->size() == 0) {
                break;
            }
            rows += chunk->size();
            // Touch the first column's first value each chunk to defeat any laziness.
            acc += chunk->GetValue(0, 0).IsNull() ? 0 : 1;
        }
        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        best_ms = std::min(best_ms, ms);
        checksum ^= acc;
        total_rows = rows;
        std::fprintf(stderr, "  rep %d: %.1f ms (%llu rows)\n", rep, ms,
                     static_cast<unsigned long long>(rows));
    }
    std::fprintf(stderr, "best: %.1f ms  (%.1f M rows/s)  rows=%llu chk=%llu\n", best_ms,
                 total_rows / (best_ms / 1e3) / 1e6, static_cast<unsigned long long>(total_rows),
                 static_cast<unsigned long long>(checksum));
    return 0;
}
