#include "plume/client/client.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/plume_remote.hpp"
#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/dandelion/composition.hpp"
#include "plume/parser/compile.hpp"

#include <cpr/cpr.h>

#include <string>
#include <vector>

namespace plume::client {

namespace {

CompiledQuery ToCompiledQuery(parser::CompiledComposition &&cc) {
    CompiledQuery out;
    out.composition = std::move(cc.composition);
    out.output_schema = std::move(cc.plan->output_schema);
    return out;
}

} // namespace

//===----------------------------------------------------------------------===//
// QueryResponse
//===----------------------------------------------------------------------===//

std::string QueryResponse::ToString() {
    auto maybe_result_str = dandelion::ParseAndRenderResponseBody(data, schema);
    if (maybe_result_str.is_error()) {
        return "<Error: " + maybe_result_str.error_msg() + ">";
    }
    return std::move(maybe_result_str).unwrap();
}

//===----------------------------------------------------------------------===//
// CompiledQuery
//===----------------------------------------------------------------------===//

Result<dandelion::BinaryData> CompiledQuery::Request() {
    return dandelion::InvocationBody(composition);
}

std::string CompiledQuery::ResultToString(const dandelion::BinaryData& data) {
    auto maybe_result_str = dandelion::ParseAndRenderResponseBody(data, output_schema);
    if (maybe_result_str.is_error()) {
        return "<Error: " + maybe_result_str.error_msg() + ">";
    }
    return std::move(maybe_result_str).unwrap();
}

//===----------------------------------------------------------------------===//
// Client
//===----------------------------------------------------------------------===//

Client::Client(parser::ConverterConfig converter_cfg, size_t fetcher_threads)
    : db_(nullptr), con_(db_), catalog_(std::make_shared<catalog::SourceCatalog>()),
      converter_cfg_(std::move(converter_cfg)), fetcher_threads_(fetcher_threads) {
    auto registry = duckdb::make_shared_ptr<catalog::PlumeRemoteInfo>();
    registry->catalog = catalog_;
    catalog::RegisterPlumeRemote(con_, std::move(registry));
}

Result<CompiledQuery> Client::Resolve(const std::string &sql, const std::string &query_name) {
    TRY(auto cc, parser::CompileQuery(con_, *catalog_, sql, query_name, converter_cfg_, fetcher_threads_));
    return ToCompiledQuery(std::move(cc));
}

Result<QueryResponse> Client::Execute(const std::string &sql, const ExecutionConfig &exec_cfg,
        const std::string &query_name) {
    TRY(auto cc, parser::CompileQuery(con_, *catalog_, sql, query_name, converter_cfg_, fetcher_threads_));
    TRY(auto body, dandelion::InvocationBody(cc.composition, /*is_registered=*/false));

    cpr::Response resp =
        cpr::Post(cpr::Url{exec_cfg.dandelion_url}, cpr::Body{reinterpret_cast<const char *>(body.data()), body.size()},
                  cpr::Header{{"Content-Type", "application/octet-stream"}},
                  cpr::Timeout{std::chrono::milliseconds(exec_cfg.timeout_secs * 1000)});

    if (resp.error) {
        return plume::Error("dandelion request to '" + exec_cfg.dandelion_url + "' failed: " + resp.error.message,
                            plume::ErrorKind::Generic);
    }
    if (resp.status_code < 200 || resp.status_code >= 300) {
        return plume::Error("dandelion returned HTTP " + std::to_string(resp.status_code) + ": " + resp.text,
                            plume::ErrorKind::Generic);
    }

    QueryResponse response;
    response.data = dandelion::BinaryData(resp.text.begin(), resp.text.end());
    response.schema = cc.plan->output_schema;
    return response;
}

} // namespace plume::client
