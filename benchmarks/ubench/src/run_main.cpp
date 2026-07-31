#include "plume/common/trace.hpp"
#include "plume/execution/pipeline.hpp"

#include "format/plan.hpp"
#include "runner/config.hpp"
#include "runner/fetch.hpp"
#include "runner/stage_runner.hpp"
#include "runner/thread_pool.hpp"
#include "runner/trace_writer.hpp"

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace plume;
using namespace plume::ubench;

namespace {

std::vector<uint8_t> ReadFileBytes(const std::string &path, bool &ok) {
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        ok = false;
        return {};
    }
    ok = true;
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

// Rescale every shuffle stage (partitions > 1) to `n`, rewriting both the stage
// record and the OutputSplit inside its serialized pipeline template so the
// output operator actually splits into `n` partitions. Gather stages stay at 1.
Result<void> Repartition(UbPlan &plan, uint32_t n) {
    for (auto &s : plan.stages) {
        if (s.partitions <= 1) {
            continue;
        }
        s.partitions = n;
        TRY(auto templ, DeserializePipeline(s.pipeline_blob));
        templ.output_split.partitions = n;
        s.pipeline_blob = SerializePipeline(templ);
    }
    return Ok();
}

size_t TotalBytes(const BlockSet &blocks) {
    size_t t = 0;
    for (auto &b : blocks) {
        t += b.size();
    }
    return t;
}

} // namespace

int main(int argc, char **argv) {
    if (argc != 2 || (argv[1][0] == '-')) {
        std::cerr << "usage: ubench_run CONFIG.json\n";
        return 2;
    }

    auto cfg_res = RunConfig::FromJsonFile(argv[1]);
    if (cfg_res.is_error()) {
        std::cerr << "ubench_run: " << cfg_res.error().message() << "\n";
        return 2;
    }
    RunConfig cfg = std::move(cfg_res).unwrap();

    const std::string &plan_path = cfg.plan_path;
    const std::string &trace_path = cfg.trace_path;
    size_t threads = cfg.threads != 0 ? cfg.threads : std::thread::hardware_concurrency();
    size_t core_offset = cfg.core_offset;
    uint32_t repartition = cfg.repartition; // 0 = leave the plan's fan-out unchanged
    uint32_t reps = cfg.reps;
    bool quiet = cfg.quiet;
    const std::vector<std::pair<std::string, std::string>> &rewrites = cfg.rewrites;

    bool ok = false;
    auto bytes = ReadFileBytes(plan_path, ok);
    if (!ok) {
        std::cerr << "ubench_run: cannot read '" << plan_path << "'\n";
        return 1;
    }
    auto plan_res = ReadUbPlan(bytes);
    if (plan_res.is_error()) {
        std::cerr << "ubench_run: " << plan_res.error().message() << "\n";
        return 1;
    }
    UbPlan plan = std::move(plan_res).unwrap();

    if (repartition > 0) {
        auto r = Repartition(plan, repartition);
        if (r.is_error()) {
            std::cerr << "ubench_run: repartition failed: " << r.error().message() << "\n";
            return 1;
        }
    }

    if (threads == 0) {
        threads = 1;
    }
    LocalFetcher fetch(rewrites);
    ThreadPool pool(threads, core_offset);

    if (!quiet) {
        std::cerr << "ubench_run: '" << plan.name << "' — " << plan.stages.size() << " stages, root "
                  << plan.root_stage << ", " << pool.size() << " threads\n";
    }

    BlockSet result;
    for (uint32_t rep = 0; rep < reps; rep++) {
        trace::Clear(); // keep only the final rep's events
        StageGraphRunner runner(plan, fetch, pool);
        auto t0 = std::chrono::steady_clock::now();
        auto res = runner.Run();
        auto t1 = std::chrono::steady_clock::now();
        if (res.is_error()) {
            std::cerr << "ubench_run: " << res.error().message() << "\n";
            return 1;
        }
        result = std::move(res).unwrap();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        if (!quiet) {
            std::cerr << "  rep " << rep << ": " << ms << " ms, " << result.size() << " result blocks, "
                      << TotalBytes(result) << " bytes\n";
        }
    }

    if (!trace_path.empty()) {
        auto w = WriteTrace(plan, trace_path);
        if (w.is_error()) {
            std::cerr << "ubench_run: " << w.error().message() << "\n";
            return 1;
        }
        if (!quiet) {
            std::cerr << "ubench_run: wrote trace to " << trace_path << "\n";
        }
    }
    return 0;
}
