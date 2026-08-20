#include "plume/client/client.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/plume_remote.hpp"
#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/dandelion/composition.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/parser/compile.hpp"

#include "duckdb/common/types/data_chunk.hpp"

#include <cpr/cpr.h>

#include <sstream>
#include <string>
#include <vector>

namespace plume::client {

using plume::memory::ImportBlockChunks;

namespace {

Result<std::string> RenderChunks(const std::vector<std::unique_ptr<duckdb::DataChunk>> &chunks,
                                 const Schema &schema, bool write_header = true) {
    std::ostringstream out;
    if (write_header) {
        std::string header;
        for (size_t c = 0; c < schema.columns.size(); c++) {
            if (c) out << "\t";
            out << schema.columns[c].name;
        }
        out << "\n";
    }

    for (const auto &chunk : chunks) {
        for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
            std::string row;
            for (duckdb::idx_t c = 0; c < chunk->ColumnCount(); c++) {
                if (c) out << "\t";
                out << chunk->GetValue(c, r).ToString();
            }
            out << "\n";
        }
    }
    return out.str();
}

std::string ParseAndRenderResponseBody(const dandelion::BinaryData& data, const Schema &schema) {
    auto maybe_sets = dandelion::ParseResponseBody(data);
    if (maybe_sets.is_error()) return "<Error: Failed to parse response body.>";
    auto sets = std::move(maybe_sets).unwrap();
    if (sets.size() < 1) return "<empty result>";

    std::string out;
    bool write_header = true;
    for (auto &block : sets[0]) {
        plume::Schema block_schema;
        auto maybe_chunks = ImportBlockChunks(block.data.data(), block.data.size(), block_schema);
        if (maybe_chunks.is_error()) return "<Error: Failed to import block chunks.>";
        if (block_schema.columns.size() != schema.columns.size()) {
            return "<Error: Block schema does not match given schema.>";
        }
        auto chunks = std::move(maybe_chunks).unwrap();
        auto maybe_chunk_str = RenderChunks(chunks, schema, write_header);
        if (maybe_chunk_str.is_error()) return "<Failed to render chunks.>";
        out += std::move(maybe_chunk_str).unwrap();
        write_header = false;
    }
    return out;
}

CompiledQuery ToCompiledQuery(parser::CompiledComposition &&cc) {
    CompiledQuery out;
    out.composition = std::move(cc.composition);
    out.table_blocks = std::move(cc.table_blocks);
    out.remote_info = std::move(cc.remote_info);
    out.remote_requests = std::move(cc.remote_requests);
    out.output_schema = std::move(cc.plan->output_schema);
    return out;
}

} // namespace

//===----------------------------------------------------------------------===//
// QueryResponse
//===----------------------------------------------------------------------===//

std::string QueryResponse::ToString() {
    return ParseAndRenderResponseBody(data, schema);
}

//===----------------------------------------------------------------------===//
// CompiledQuery
//===----------------------------------------------------------------------===//

Result<dandelion::BinaryData> CompiledQuery::Request() {
    return dandelion::InvocationBody(composition, table_blocks, remote_info, remote_requests);
}

std::string CompiledQuery::ResultToString(const dandelion::BinaryData& data) {
    return ParseAndRenderResponseBody(data, output_schema);
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
    TRY(auto body, dandelion::InvocationBody(cc.composition, cc.table_blocks, cc.remote_info, cc.remote_requests,
                                             /*is_registered=*/false));

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
