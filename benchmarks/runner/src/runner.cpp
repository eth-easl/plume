#include "runner.hpp"
#include "trace.hpp"

#include "plume/catalog/plume_remote.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/dandelion/composition.hpp"
#include "plume/parser/compile.hpp"

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <numeric>
#include <thread>

namespace fs = std::filesystem;

namespace plume::bench {

namespace {

std::string UrlPath(const std::string &base_url, const std::string &path) {
    if (base_url.back() == '/') {
        return base_url + path;
    }
    return base_url + "/" + path;
}

std::string DandelionUrl(const std::string &base_url) {
    return UrlPath(base_url, "hot/matmul");
}

std::string AsyncSubmitUrl(const std::string &base_url) {
    return UrlPath(base_url, "async/warm");
}

std::string AsyncResultUrl(const std::string &base_url, const std::string &invocation_id) {
    return UrlPath(base_url, "async/invocation/" + invocation_id + "/result?wait=true");
}

bool IsIdent(const std::string &s) {
    if (s.empty() || (!std::isalpha(static_cast<unsigned char>(s[0])) && s[0] != '_')) {
        return false;
    }
    return std::all_of(s.begin(), s.end(),
                       [](unsigned char c) { return std::isalnum(c) || c == '_'; });
}

std::string Trim(const std::string &s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    size_t e = s.find_last_not_of(" \t\r\n");
    return b == std::string::npos ? std::string() : s.substr(b, e - b + 1);
}

std::string SourceExpr(const std::string &table, const DataSource &source) {
    auto it = source.num_files.find(table);
    const std::string base = source.path_prefix + "/" + table + "/" + table;
    if (it == source.num_files.end() || it->second <= 1) {
        if (it != source.num_files.end() && it->second == 1) {
            return "['" + base + ".1" + source.path_suffix + "']";
        }
        return "'" + base + source.path_suffix + "'";
    }
    std::string expr = "[";
    for (uint32_t i = 0; i < it->second; i++) {
        expr += (i ? ", " : "");
        expr += "'" + base + "." + std::to_string(i + 1) + source.path_suffix + "'";
    }
    expr += "]";
    return expr;
}

std::string ResolvePlaceholders(const std::string &sql, const DataSource &source) {
    std::string out;
    out.reserve(sql.size());
    for (size_t i = 0; i < sql.size();) {
        if (sql[i] == '[') {
            size_t close = sql.find(']', i);
            if (close != std::string::npos) {
                std::string inner = Trim(sql.substr(i + 1, close - i - 1));
                if (IsIdent(inner)) {
                    out += SourceExpr(inner, source);
                    i = close + 1;
                    continue;
                }
            }
        }
        out += sql[i++];
    }
    return out;
}

std::string QueryStem(const std::string &query_file) {
    return fs::path(query_file).stem().string();
}

} // namespace

Runner::Runner(BenchmarkConfig config) : config_(std::move(config)), db_(nullptr), con_(db_),
        catalog_(std::make_shared<catalog::SourceCatalog>()) {
    auto registry = duckdb::make_shared_ptr<catalog::PlumeRemoteInfo>();
    registry->catalog = catalog_;
    catalog::RegisterPlumeRemote(con_, std::move(registry));
}

Result<void> Runner::Run() {
    if (!config_.checksum_path.empty()) {
        TRY(auto cf, ChecksumFile::FromJsonFile(config_.checksum_path));
        checksum_file_ = std::move(cf);
        LogInfo("Loaded checksums from '", config_.checksum_path, "'");
    }

    Result<void> status = Ok();
    switch (config_.type) {
    case BenchmarkConfig::Type::kSingle:
        status = RunSingle(std::get<SingleConfig>(config_.bench));
        break;
    case BenchmarkConfig::Type::kThroughput:
        status = RunThroughput(std::get<ThroughputConfig>(config_.bench));
        break;
    case BenchmarkConfig::Type::kTrace:
        status = RunTrace(std::get<TraceConfig>(config_.bench));
        break;
    }

    TRYV(ExportResults());

    return status;
}

Result<size_t> Runner::BuildInvocation(const std::string &key, const std::string &query_file,
                                       const DataSource &source, const std::string &checksum_query,
                                       const std::string &checksum_scale_factor) {
    if (auto it = by_key_.find(key); it != by_key_.end()) {
        return it->second;
    }

    const std::string sql_path = config_.query_dir + "/" + query_file;
    std::ifstream file(sql_path);
    if (!file.is_open()) {
        return Error("Could not open query file '" + sql_path + "'.", ErrorKind::InvalidInput);
    }
    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    sql = ResolvePlaceholders(sql, source);
    LogDebug(" > compiling '", key, "' from '", sql_path, "'");

    auto start = std::chrono::steady_clock::now();
    TRY(auto compiled,
        parser::CompileQuery(con_, *catalog_, sql, key, config_.converter_config, config_.fetcher_threads));
    TRY(auto body, dandelion::InvocationBody(compiled.composition, compiled.table_blocks,
                                             compiled.remote_info, compiled.remote_requests,
                                             /*is_registered=*/false));

    auto now = std::chrono::steady_clock::now();
    int64_t planning_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();

    if (config_.debug_prints) {
        LogDebug("   compiled physical plan:\n===== PHYSICAL PLAN =====\n", 
                 compiled.plan->ToString(), "===== PHYSICAL PLAN =====");
    }

    Invocation inv;
    inv.name = key;
    inv.body = std::move(body);
    inv.planning_ms = planning_ms;
    if (checksum_file_) {
        inv.expected_checksum = checksum_file_->Find(checksum_query, checksum_scale_factor);
        if (!inv.expected_checksum) {
            LogDebug("   no checksum configured for '", checksum_query, "'/'", checksum_scale_factor, "'");
        }
    }
    inv.out_schema = compiled.plan->output_schema;
    size_t idx = invocations_.size();
    invocations_.push_back(std::move(inv));
    by_key_[key] = idx;
    LogDebug("   planned in ", planning_ms, " ms (body ", invocations_[idx].body.size(), " bytes)");
    return idx;
}

Result<void> Runner::HandleResponse(size_t idx, const cpr::Response &resp,
                                    std::string *resp_string,
                                    std::optional<int64_t> latency_ms) {
    if (resp.error && resp.error.code != cpr::ErrorCode::OK) {
        return Error("dandelion request failed: " + resp.error.message, ErrorKind::Generic);
    }
    if (resp.status_code < 200 || resp.status_code >= 300) {
        return Error("dandelion returned HTTP " + std::to_string(resp.status_code) + ": " + resp.text,
                     ErrorKind::Generic);
    }
    Invocation &inv = invocations_[idx];

    dandelion::BinaryData resp_body(resp.text.begin(), resp.text.end());
    std::string timestamps;
    if (resp_string) {
        TRY(*resp_string, dandelion::ParseAndRenderResponseBody(resp_body, inv.out_schema));
    }

    TRY(auto sets, dandelion::ParseResponseBody(resp_body, &timestamps));
    if (inv.expected_checksum) {
        TRY(auto actual, ComputeChecksum(sets));
        if (!(actual == *inv.expected_checksum)) {
            inv.checksum_failures++;
            return Error("checksum mismatch for '" + inv.name + "': expected " + ToString(*inv.expected_checksum) +
                         ", got " + ToString(actual), ErrorKind::Generic);
        }
    }

    inv.latencies_ms.push_back(latency_ms.value_or(static_cast<int64_t>(resp.elapsed * 1000.0)));
    inv.timestamps.push_back(std::move(timestamps));
    return Ok();
}

bool IsTransportError(const cpr::Response &resp) {
    return resp.error && resp.error.code != cpr::ErrorCode::OK;
}

cpr::Timeout RequestTimeout(int timeout_s) {
    return cpr::Timeout{std::chrono::milliseconds(timeout_s * 1000)};
}

std::chrono::steady_clock::time_point RecoverDeadline(int timeout_s) {
    const auto cap = timeout_s > 0 ? std::chrono::seconds(timeout_s) : std::chrono::minutes(10);
    return std::chrono::steady_clock::now() + cap;
}

void RecoverBackoff() {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

Result<void> Runner::Invoke(size_t idx, std::string *resp_string) {
    invocations_[idx].num_invocations++;
    TRY(auto response, PerformRequest(idx));
    return HandleResponse(idx, response.response, resp_string, response.latency_ms);
}

Result<Runner::InvocationResponse> Runner::PerformRequest(size_t idx) {
    const Invocation &inv = invocations_[idx];

    if (config_.invocation_mode == BenchmarkConfig::InvocationMode::kAsync) {
        const auto start = std::chrono::steady_clock::now();
        const auto deadline = RecoverDeadline(config_.request_timeout_s);
        const auto timeout = RequestTimeout(config_.request_timeout_s);

        cpr::Response accepted;
        for (;;) {
            accepted = cpr::Post(
                cpr::Url{AsyncSubmitUrl(config_.dandelion_url)},
                cpr::Body{reinterpret_cast<const char *>(inv.body.data()), inv.body.size()},
                cpr::Header{{"Content-Type", "application/octet-stream"}},
                timeout);
            if (!IsTransportError(accepted) && accepted.status_code == 202) {
                break;
            }
            if (!IsTransportError(accepted) || std::chrono::steady_clock::now() >= deadline) {
                if (IsTransportError(accepted)) {
                    return Error("async dandelion submission failed: " + accepted.error.message,
                                 ErrorKind::Generic);
                }
                return Error("async dandelion submission returned HTTP " +
                                 std::to_string(accepted.status_code) + ": " + accepted.text,
                             ErrorKind::Generic);
            }
            std::cerr << "   retry async submit after transport error: " << accepted.error.message
                      << std::endl;
            RecoverBackoff();
        }

        std::string invocation_id;
        try {
            const auto body = nlohmann::json::from_bson(accepted.text.begin(), accepted.text.end());
            invocation_id = body.at("invocation_id").get<std::string>();
        } catch (const std::exception &e) {
            return Error("failed to parse async submission response: " + std::string(e.what()),
                         ErrorKind::Generic);
        }

        cpr::Response result;
        for (;;) {
            result = cpr::Get(cpr::Url{AsyncResultUrl(config_.dandelion_url, invocation_id)}, timeout);
            if (!IsTransportError(result)) {
                break;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return Error("async result GET failed after reconnect retries: " + result.error.message,
                             ErrorKind::Generic);
            }
            std::cerr << "   retry GET invocation " << invocation_id
                      << " after transport error: " << result.error.message << std::endl;
            RecoverBackoff();
        }
        const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - start)
                                    .count();
        return InvocationResponse{std::move(result), elapsed_ms};
    }

    cpr::Response resp = cpr::Post(
        cpr::Url{DandelionUrl(config_.dandelion_url)},
        cpr::Body{reinterpret_cast<const char *>(inv.body.data()), inv.body.size()},
        cpr::Header{{"Content-Type", "application/octet-stream"}},
        cpr::Timeout{std::chrono::milliseconds(config_.request_timeout_s * 1000)});
    return InvocationResponse{std::move(resp), std::nullopt};
}

Result<void> Runner::RunSingle(const SingleConfig &sc) {
    LogInfo("Compiling queries...");
    for (const auto &qf : config_.queries) {
        const std::string stem = QueryStem(qf);
        auto idx = BuildInvocation(stem, qf, config_.source, stem, config_.scale_factor);
        if (idx.is_error()) {
            std::cerr << "error: failed to compile '" << qf << "': " << idx.error().message() << "\n";
        }
    }

    if (config_.dandelion_url.empty()) {
        LogInfo("No dandelion url set —> compiled only, skipping invocation.");
        return Ok();
    }
    for (size_t r = 0; r < sc.repetitions; r++) {
        LogInfo("Round ", (r + 1), "/", sc.repetitions);
        for (size_t idx = 0; idx < invocations_.size(); idx++) {
            std::cout << " > " << invocations_[idx].name << std::flush;
            std::string result_str;
            auto status = Invoke(idx, config_.debug_prints ? &result_str : nullptr);
            if (status.is_error()) {
                std::cout << " -> ERROR: " << status.error().message() << "\n";
            } else {
                std::cout << " -> " << invocations_[idx].latencies_ms.back() << " ms\n";
                LogDebug("   Result:\n", std::move(result_str));
            }
        }
    }
    return Ok();
}

Result<void> Runner::RunThroughput(const ThroughputConfig &tc) {
    LogInfo("Compiling queries...");
    const std::string stem = QueryStem(tc.query);
    TRY(size_t idx, BuildInvocation(stem, tc.query, config_.source, stem, config_.scale_factor));
    if (config_.dandelion_url.empty()) {
        LogInfo("No dandelion url set —> compiled only, skipping invocation.");
        return Ok();
    }

    Invocation& inv = invocations_[idx];
    for (size_t rps_idx=0; rps_idx<tc.rps_values.size(); rps_idx++) {
        if (rps_idx > 0) {
            StartNextRun();
        }

        const double rps = tc.rps_values[rps_idx];
        size_t interval_ms = (size_t)(1000 / rps);
        size_t repetitions = (size_t)((rps * (double)tc.duration_s));

        LogInfo("Throughput: Firing '", inv.name, "' every ", interval_ms, " ms for ", tc.duration_s, " s");

        struct TimedResponse {
            Result<InvocationResponse> response;
            double submitted_s;
            double completed_s;
        };
        struct PendingRequest {
            std::future<TimedResponse> future;
            size_t seq;
            double scheduled_s;
        };

        std::vector<PendingRequest> pending;
        pending.reserve(repetitions);
        auto start = std::chrono::steady_clock::now();
        auto next_interval = start;
        size_t dispatched = 0, completed = 0, ok = 0, failed = 0;
        while (completed < repetitions) {
            auto now = std::chrono::steady_clock::now();

            if (dispatched < repetitions && now >= next_interval) {
                LogDebug("   > Sending request (", (dispatched+1), "/", repetitions, ")...");

                // use standard futures so we can force immediate async execution of the request
                inv.num_invocations++;
                const size_t seq = dispatched;
                const double scheduled_s = std::chrono::duration<double>(next_interval - start).count();
                pending.push_back({std::async(std::launch::async, [this, idx, start]() {
                    const double submitted_s = std::chrono::duration<double>(
                        std::chrono::steady_clock::now() - start).count();
                    auto response = PerformRequest(idx);
                    const double completed_s = std::chrono::duration<double>(
                        std::chrono::steady_clock::now() - start).count();
                    return TimedResponse{std::move(response), submitted_s, completed_s};
                }), seq, scheduled_s});
                dispatched++;
                next_interval += std::chrono::milliseconds(interval_ms);
                if (dispatched == repetitions) {
                    LogInfo("   > Waiting for all responses...");
                }
            }

            for (auto &request : pending) {
                if (request.future.valid()) {
                    if (request.future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
                        auto timed = request.future.get();
                        Result<void> status = timed.response.is_error()
                            ? Result<void>(timed.response.error())
                            : HandleResponse(idx, timed.response.unwrap().response, nullptr,
                                             timed.response.unwrap().latency_ms);
                        throughput_events_.push_back({rps_idx, rps, request.seq, status.is_ok(),
                                                      request.scheduled_s, timed.submitted_s,
                                                      timed.completed_s});
                        if (status.is_ok()) {
                            ok++;
                        } else {
                            LogDebug("   request failed: ", status.error().message());
                            failed++;
                        }
                        completed++;
                    }
                }
            }
            
            // quick sleep
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        
        double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        LogInfo("   > completed ", ok, " ok, ", failed, " failed in ", secs, " s (", (ok / secs), " req/s successful)");
        if (ok == 0) {
            LogInfo("   > Found no successful invocation -> stopping sweep.");
            break;
        }
    }

    return Ok();
}

Result<void> Runner::RunTrace(const TraceConfig &trc) {
    LogInfo("Loading trace...");
    TRY(auto trace, LoadTrace(trc.path));
    LogInfo("Trace: ", trace.size(), " entries from '", trc.path, "'");

    LogInfo("Compiling queries...");
    std::vector<size_t> entry_idx(trace.size(), SIZE_MAX);
    for (size_t i = 0; i < trace.size(); i++) {
        const auto &e = trace[i];
        auto sf = trc.scale_factors.find(e.scale_factor);
        if (sf == trc.scale_factors.end()) {
            std::cerr << "error: trace scale factor '" << e.scale_factor << "' has no source config; skipping.\n";
            continue;
        }
        std::string key = e.query + "_" + e.scale_factor;
        auto idx = BuildInvocation(key, e.query + ".sql", sf->second, e.query, e.scale_factor);
        if (idx.is_error()) {
            std::cerr << "error: failed to compile '" << key << "': " << idx.error().message() << "\n";
            continue;
        }
        entry_idx[i] = idx.unwrap();
    }
    if (config_.dandelion_url.empty()) {
        LogInfo("No dandelion url set —> compiled only, skipping invocation.");
        return Ok();
    }

    std::string results_path = config_.results_prefix + "/trace_results.csv";
    std::ofstream results(results_path, std::ios::trunc);
    results << std::fixed << std::setprecision(3);
    results << "seq,query_id,status,start_s,end_s,rel_start_s,rel_end_s,latency_s" << std::endl;
    if (trc.closed_loop) {
        LogInfo("Running trace in closed loop...");
        auto start = std::chrono::steady_clock::now();
        size_t ok = 0, failed = 0;
        for (size_t i=0; i<trace.size(); i++) {
            if (entry_idx[i] == SIZE_MAX) {
                continue;
            }
            LogInfo(" > Sending request for ", invocations_[entry_idx[i]].name, "...");
            auto now = std::chrono::steady_clock::now();
            auto start_offset_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
            auto status = Invoke(entry_idx[i]);
            results << i << "," << trace[i].query << "_" << i;
            int64_t latency_ms = 0;
            if (status.is_ok()) {
                latency_ms = invocations_[entry_idx[i]].latencies_ms.back();
                ok++;
                results << ",success,";
            } else {
                std::cerr << "     > ERROR: Failed to execute query '" << trace[i].query
                            << "': " << status.error().message() << std::endl;
                failed++;
                results << ",error,"; 
            }
            double start_s = ((double) start_offset_ms) / 1000;
            double latency_s = ((double) latency_ms) / 1000;
            results << start_s << "," << start_s + latency_s << "," << start_s << ","
                    << start_s + latency_s << "," << latency_s << std::endl;
        }
    } else { // open loop
        LogInfo("Running trace in open loop...");
        struct TraceInvocation {
            std::future<Result<InvocationResponse>> future;
            size_t inv_idx;
            size_t trace_idx;
            int64_t latency = 0;
        };
        std::vector<TraceInvocation> pending;
        auto start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < trace.size(); i++) {
            if (entry_idx[i] == SIZE_MAX) {
                continue;
            }
            auto target = start + std::chrono::milliseconds(trace[i].offset_ms);
            std::this_thread::sleep_until(target);
            LogInfo(" > [", trace[i].offset_ms, " ms] ", invocations_[entry_idx[i]].name);
            const size_t inv_idx = entry_idx[i];
            invocations_[inv_idx].num_invocations++;
            pending.push_back({std::async(std::launch::async, [this, inv_idx]() {
                return PerformRequest(inv_idx);
            }),
            inv_idx, i});
        }
        LogInfo("Dispatched all ", pending.size(), " requests, awaiting completion...");

        size_t ok = 0, failed = 0;
        for (auto &p : pending) {
            auto response = p.future.get();
            Result<void> status = response.is_error()
                ? Result<void>(response.error())
                : HandleResponse(p.inv_idx, response.unwrap().response, nullptr,
                                 response.unwrap().latency_ms);
            if (status.is_ok()) {
                p.latency = response.unwrap().latency_ms.value_or(
                    static_cast<int64_t>(response.unwrap().response.elapsed * 1000.0));
                ok++;
                continue;
            }
            std::cerr << "     > ERROR: Failed to execute query '" << invocations_[p.inv_idx].name
                      << "': " << status.error().message() << std::endl;
            failed++;
        }
        LogInfo("Trace completed: ", ok, " ok, ", failed, " failed");

        // export trace result in useful format
        for (const auto &p : pending) {
            results << p.trace_idx << "," << trace[p.trace_idx].query << "_" << p.trace_idx << ",";
            results << (p.latency > 0 ? "success" : "error");
            double start_s = ((double) trace[p.trace_idx].offset_ms) / 1000;
            double latency_s = ((double) p.latency) / 1000;
            results << "," << start_s << "," << start_s + latency_s << "," << start_s << "," 
                      << start_s + latency_s << "," << latency_s << std::endl;
        }
    }

    results.close();
    LogInfo(" > Wrote '", results_path, "'");

    return Ok();
}

void Runner::StartNextRun() {
    for (auto &inv : invocations_) {
        inv.run_starts.push_back(inv.latencies_ms.size());
    }
}

Result<void> Runner::ExportResults() {
    // Console summary (always shown).
    LogInfo("\n=== summary ===");
    std::cout << "query\tplanning_ms\truns\tmin_ms\tmean_ms\tmax_ms";
    if (checksum_file_) {
        std::cout << "\tchecksum_ok";
    }
    std::cout << "\n";
    for (const auto &inv : invocations_) {
        std::cout << inv.name << "\t" << inv.planning_ms << "\t" << inv.latencies_ms.size();
        if (!inv.latencies_ms.empty()) {
            int64_t mn = *std::min_element(inv.latencies_ms.begin(), inv.latencies_ms.end());
            int64_t mx = *std::max_element(inv.latencies_ms.begin(), inv.latencies_ms.end());
            int64_t sum = std::accumulate(inv.latencies_ms.begin(), inv.latencies_ms.end(), int64_t{0});
            std::cout << "\t" << mn << "\t" << (sum / static_cast<int64_t>(inv.latencies_ms.size())) << "\t" << mx;
        } else {
            std::cout << "\t-\t-\t-";
        }
        if (checksum_file_) {
            if (inv.expected_checksum) {
                // A latency is recorded only after the HTTP response was parsed and
                // its checksum matched, so transport/HTTP/parse errors must not be
                // reported as successful checksum checks.
                std::cout << "\t" << inv.latencies_ms.size() << "/" << inv.num_invocations;
            } else {
                std::cout << "\tn/a";
            }
        }
        std::cout << "\n";

        // Multiple runs (e.g. a throughput rps sweep) -> break the min/mean/max
        // down per run instead of only reporting the pooled figures above.
        if (!inv.run_starts.empty()) {
            size_t start = 0;
            for (size_t r = 0; r <= inv.run_starts.size(); r++) {
                size_t end = (r < inv.run_starts.size()) ? inv.run_starts[r] : inv.latencies_ms.size();
                std::cout << "  run " << r << "\t\t" << (end - start);
                if (end > start) {
                    int64_t mn = *std::min_element(inv.latencies_ms.begin() + start, inv.latencies_ms.begin() + end);
                    int64_t mx = *std::max_element(inv.latencies_ms.begin() + start, inv.latencies_ms.begin() + end);
                    int64_t sum = std::accumulate(inv.latencies_ms.begin() + start, inv.latencies_ms.begin() + end, int64_t{0});
                    std::cout << "\t" << mn << "\t" << (sum / static_cast<int64_t>(end - start)) << "\t" << mx;
                } else {
                    std::cout << "\t-\t-\t-";
                }
                std::cout << "\n";
                start = end;
            }
        }
    }

    if (config_.results_prefix.empty()) {
        return Ok();
    }

    std::error_code ec;
    fs::create_directories(config_.results_prefix, ec);
    if (ec) {
        return Error("Could not create results dir '" + config_.results_prefix + "': " + ec.message());
    }

    // timings.txt
    {
        std::ofstream f(config_.results_prefix + "/timings.txt", std::ios::trunc);
        for (const auto &inv : invocations_) {
            if (inv.run_starts.empty()) {
                f << "Query " << inv.name << ": ";
                for (size_t i = 0; i < inv.latencies_ms.size(); i++) {
                    f << (i ? "," : "") << inv.latencies_ms[i];
                }
                f << "\n";
                continue;
            }
            f << "Query " << inv.name << ":\n";
            size_t start = 0;
            for (size_t r = 0; r <= inv.run_starts.size(); r++) {
                size_t end = (r < inv.run_starts.size()) ? inv.run_starts[r] : inv.latencies_ms.size();
                f << "  run " << r << ": ";
                for (size_t i = start; i < end; i++) {
                    f << (i > start ? "," : "") << inv.latencies_ms[i];
                }
                f << "\n";
                start = end;
            }
        }
    }

    // planning.txt
    {
        std::ofstream f(config_.results_prefix + "/planning.txt", std::ios::trunc);
        for (const auto &inv : invocations_) {
            f << "Query " << inv.name << ": " << inv.planning_ms << "\n";
        }
    }

    if (config_.type == BenchmarkConfig::Type::kThroughput) {
        const std::string path = config_.results_prefix + "/throughput_results.csv";
        std::ofstream f(path, std::ios::trunc);
        f << std::fixed << std::setprecision(6);
        f << "run,target_rps,seq,status,scheduled_s,submitted_s,completed_s,latency_s\n";
        for (const auto &event : throughput_events_) {
            f << event.run << "," << event.target_rps << "," << event.seq << ","
              << (event.success ? "success" : "error") << "," << event.scheduled_s << ","
              << event.submitted_s << "," << event.completed_s << ","
              << (event.completed_s - event.submitted_s) << "\n";
        }
        LogInfo("Wrote throughput_results.csv to ", config_.results_prefix);
    }

    // timestamps.txt
    {
        std::ofstream f(config_.results_prefix + "/timestamps.txt", std::ios::trunc);
        for (const auto &inv : invocations_) {
            f << "Query " << inv.name << ":\n[\n";
            for (const auto &ts : inv.timestamps) {
                f << ts << ",\n";
            }
            f.seekp(-2, f.cur); // remove last ',\n'
            f << "\n]\n";
        }
    }

    LogInfo("Wrote timings.txt, planning.txt, timestamps.txt to ", config_.results_prefix);
    return Ok();
}

} // namespace plume::bench
