#include "plume/catalog/local.hpp"

#include "plume/common/result.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <vector>

namespace plume::catalog {

namespace {

std::string LowerExtension(const std::string &path) {
    auto ext = std::filesystem::path(path).extension().string();
    std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::tolower(c); });
    return ext;
}

} // namespace

std::shared_ptr<DataSource> CreateTableSource(std::string name, Schema schema) {
    auto pq_src = std::make_shared<LocalTableDataSource>();
    pq_src->name = std::move(name);
    pq_src->schema = std::move(schema);
    return pq_src;
}

Result<dandelion::DataItemVec> MaterializeTable(duckdb::Connection &con, const LocalTableDataSource &src, 
        const std::vector<uint32_t> &projection, const ExprNode *pushed_filter) {
    if (!src.schema) {
        return Error("Table has no schema defined.", ErrorKind::RuntimeError);
    }
    const Schema &schema = src.schema.value();
    if (schema.columns.empty()) {
        return Error("Source table '" + src.name + "' has no input columns.", ErrorKind::InvalidInput);
    }

    // TODO: add projection, pushed_filter
    std::string select = "SELECT ";
    for (size_t i = 0; i < schema.columns.size(); i++) {
        select += (i ? ", " : "") + duckdb::KeywordHelper::WriteQuoted(schema.columns[i].name, '"');
    }
    select += " FROM " + duckdb::KeywordHelper::WriteQuoted(src.name, '"');

    auto result = con.Query(select);
    if (result->HasError()) {
        return Error("Failed to read table '" + src.name + "': " + result->GetError(), ErrorKind::InvalidInput);
    }

    memory::Allocator alloc;
    dandelion::DataItemVec blocks;
    while (auto chunk = result->Fetch()) {
        if (chunk->size() == 0) {
            continue;
        }
        // TODO: eliminate the data copy here
        TRY(auto block, memory::ExportChunk(schema, *chunk, alloc));
        blocks.push_back(dandelion::DataItem{
            0, 
            "", 
            dandelion::BinaryData{block.data, block.data + block.size}
        });
        alloc.Free(block.data, block.size);
    }
    return blocks;
}

Result<void> LoadDataFile(duckdb::Connection &con, const std::string &table_name,
        const std::vector<std::string> &paths) {
    if (paths.empty()) {
        return Error("LoadDataFile: no paths given for table '" + table_name + "'", ErrorKind::InvalidInput);
    }
    const auto ext = LowerExtension(paths[0]);

    std::string reader;
    if (ext == ".csv" || ext == ".tsv") {
        reader = "read_csv_auto";
    } else if (ext == ".parquet") {
        // Only the client (and anything else that links duckdb_glue.cpp) registers
        // the parquet extension.
        reader = "read_parquet";
    } else {
        return Error("Unsupported data file extension '" + ext + "' (expected .csv/.tsv/.parquet)",
                     ErrorKind::InvalidInput);
    }

    // A single path reads as one file; DuckDB's readers also accept a bracket-list
    // of paths to read (and concatenate) several files as one table.
    std::string path_arg;
    if (paths.size() == 1) {
        path_arg = duckdb::KeywordHelper::WriteQuoted(paths[0], '\'');
    } else {
        path_arg = "[";
        for (size_t i = 0; i < paths.size(); i++) {
            path_arg += (i ? ", " : "") + duckdb::KeywordHelper::WriteQuoted(paths[i], '\'');
        }
        path_arg += "]";
    }

    // Materialize the file(s) into a real table so the leading scan is a base
    // table (LOCAL_TABLE), which the rest of the pipeline already handles.
    const std::string sql = "CREATE TABLE " + duckdb::KeywordHelper::WriteQuoted(table_name, '"') +
                            " AS SELECT * FROM " + reader + "(" + path_arg + ")";
    auto result = con.Query(sql);
    if (result->HasError()) {
        return Error("Failed to load '" + paths[0] + "': " + result->GetError(), ErrorKind::InvalidInput);
    }
    return Ok();
}

} // namespace plume::catalog
