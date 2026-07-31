#include "plume/catalog/plume_remote.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/csv.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/catalog/remote_resolver.hpp"
#include "plume/common/result.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <cstring>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace plume::catalog {

//===----------------------------------------------------------------------===//
// ResolveAndRewriteSources
//===----------------------------------------------------------------------===//

namespace {

inline bool IsIdentChar(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

inline void SkipWhitespace(const std::string &sql, size_t &i) {
    while (i < sql.size() && std::isspace(static_cast<unsigned char>(sql[i]))) {
        i++;
    }
}

inline std::string Extension(const std::string &tok) {
    std::string ext = std::filesystem::path(tok).extension().string();
    std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::tolower(c); });
    return ext;
}

std::string NameFromPath(const std::string &path) {
    std::string stem = std::filesystem::path(path).stem().string();
    std::string out;
    for (char c : stem) {
        out += IsIdentChar(c) ? c : '_';
    }
    if (out.empty() || std::isdigit(static_cast<unsigned char>(out[0]))) {
        out = "src_" + out;
    }
    return out;
}

} // namespace

Result<std::string> ResolveAndRewriteSources(const std::string &sql, SourceCatalog &catalog, RemoteResolver &resolver) {
    struct Replacement {
        size_t begin;
        size_t end;
        std::string text;
    };
    std::vector<Replacement> replacements;

    const size_t name_len = std::strlen(kPlumeRemoteName);
    size_t search_from = 0;
    while (true) {
        size_t match = sql.find(kPlumeRemoteName, search_from);
        if (match == std::string::npos) {
            break;
        }
        
        size_t after_name = match + name_len;
        bool boundary_before = match == 0 || !IsIdentChar(sql[match - 1]);
        bool boundary_after = after_name >= sql.size() || !IsIdentChar(sql[after_name]);
        if (!boundary_before || !boundary_after) {
            search_from = after_name;
            continue;
        }

        size_t i = after_name;
        SkipWhitespace(sql, i);
        if (i >= sql.size() || sql[i] != '(') {
            search_from = after_name;
            continue;
        }
        i++; // consume '('
        const size_t args_begin = i;
        SkipWhitespace(sql, i);

        std::vector<std::string> urls;
        if (i < sql.size() && sql[i] == '[') {
            // A list of urls, separated by ',', terminated by ']'.
            i++; // consume '['
            while (true) {
                SkipWhitespace(sql, i);
                if (i >= sql.size()) {
                    return Error("plume_remote: unterminated '[' in call", ErrorKind::InvalidInput);
                }
                if (sql[i] == ']') {
                    i++;
                    break;
                }
                if (sql[i] != '\'' && sql[i] != '"') {
                    return Error("plume_remote: expected a quoted url inside '[...]'", ErrorKind::InvalidInput);
                }
                char quote = sql[i++];
                std::string url;
                while (i < sql.size() && sql[i] != quote) {
                    url += sql[i++];
                }
                if (i >= sql.size()) {
                    return Error("plume_remote: unterminated string literal", ErrorKind::InvalidInput);
                }
                i++; // consume closing quote
                if (!url.empty()) {
                    urls.push_back(std::move(url));
                }
                SkipWhitespace(sql, i);
                if (i < sql.size() && sql[i] == ',') {
                    i++;
                }
            }
        } else if (i < sql.size() && (sql[i] == '\'' || sql[i] == '"')) {
            // A quoted url token.
            char quote = sql[i++];
            std::string url;
            while (i < sql.size() && sql[i] != quote) {
                url += sql[i++];
            }
            if (i >= sql.size()) {
                return Error("plume_remote: unterminated string literal", ErrorKind::InvalidInput);
            }
            i++; // consume closing quote
            urls.push_back(std::move(url));
        } else {
            // A bare, unquoted url token.
            size_t start = i;
            while (i < sql.size() && sql[i] != ')' && sql[i] != ',' && !std::isspace(static_cast<unsigned char>(sql[i]))) {
                i++;
            }
            if (i == start) {
                return Error("plume_remote: expected a url argument", ErrorKind::InvalidInput);
            }
            urls.push_back(sql.substr(start, i - start));
        }

        SkipWhitespace(sql, i);
        if (i >= sql.size() || sql[i] != ')') {
            return Error("plume_remote: expected ')' to close call", ErrorKind::InvalidInput);
        }
        const size_t end = i; // points at ')'
        i++; // consume ')'

        if (urls.empty()) {
            return Error("plume_remote: no urls given", ErrorKind::InvalidInput);
        }

        // TODO: ideally this should should create the alias based on all urls if multiple are used
        std::string alias = NameFromPath(urls[0]);

        std::shared_ptr<DataSource> data_source;
        auto maybe_data_source = catalog.Get(alias);
        if (maybe_data_source.is_ok()) {
            data_source = maybe_data_source.unwrap();
        } else {
            const std::string ext = Extension(urls[0]);
            if (ext == ".parquet") {
                data_source = CreateRemoteParquetSource(alias, std::move(urls));
            } else if (ext == ".csv" || ext == ".tsv") {
                data_source = CreateRemoteCSVSource(alias, std::move(urls));
            } else {
                return Error("Unknown input format: '"+ext+"'");
            }
            TRYV(catalog.Add(data_source));
        }
        TRYV(data_source->Resolve(resolver));
        replacements.push_back({args_begin, end, "'" + alias + "'"});
        search_from = i;
    }

    TRYV(resolver.Execute());

    std::ostringstream rewritten;
    size_t cursor = 0;
    for (auto &repl : replacements) {
        rewritten << sql.substr(cursor, repl.begin - cursor);
        rewritten << repl.text;
        cursor = repl.end;
    }
    rewritten << sql.substr(cursor);
    return rewritten.str();
}

//===----------------------------------------------------------------------===//
// plume_remote table function
//===----------------------------------------------------------------------===//

duckdb::unique_ptr<duckdb::FunctionData> PlumeRemoteBindData::Copy() const {
    auto copy = duckdb::make_uniq<PlumeRemoteBindData>();
    copy->info = info;
    return std::move(copy);
}

bool PlumeRemoteBindData::Equals(const duckdb::FunctionData &other) const {
    return info == other.Cast<PlumeRemoteBindData>().info;
}

namespace {

duckdb::unique_ptr<duckdb::FunctionData> PlumeRemoteBind(duckdb::ClientContext &,
        duckdb::TableFunctionBindInput &input, duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names) {
    auto *registry = static_cast<PlumeRemoteInfo *>(input.info.get());
    if (!registry) {
        throw duckdb::InvalidInputException("plume_remote: missing source registry");
    }
    auto &catalog = registry->catalog;

    std::string alias = input.inputs[0].ToString();
    
    auto maybe_data_source = catalog->Get(alias);
    if (maybe_data_source.is_error()) {
        throw duckdb::InvalidInputException("plume_remote: unknown table '%s'", alias);
    }
    std::shared_ptr<DataSource> data_source = std::move(maybe_data_source).unwrap();

    if (!data_source->schema) {
        throw duckdb::InvalidInputException("plume_remote: unresolved table '%s'", alias);
    }
    auto bind_data = duckdb::make_uniq<PlumeRemoteBindData>();
    bind_data->info = data_source;
    for (const auto &col : bind_data->info->schema->columns) {
        names.push_back(col.name);
        return_types.push_back(ToLogicalType(col.type));
    }
    return std::move(bind_data);
}

duckdb::unique_ptr<duckdb::NodeStatistics> PlumeRemoteCardinality(duckdb::ClientContext &,
        const duckdb::FunctionData *bind_data) {
    auto &data_source = bind_data->Cast<PlumeRemoteBindData>().info;
    return duckdb::make_uniq<duckdb::NodeStatistics>(static_cast<duckdb::idx_t>(data_source->cardinality_total));
}

duckdb::unique_ptr<duckdb::BaseStatistics> PlumeRemoteStatistics(duckdb::ClientContext &,
        duckdb::TableFunctionGetStatisticsInput &input) {
    auto &data_source = input.bind_data->Cast<PlumeRemoteBindData>().info;
    auto col_idx = input.column_index.GetPrimaryIndex();
    if (col_idx >= data_source->col_stats.size() || data_source->schema || col_idx >= data_source->schema->columns.size()) {
        return nullptr;
    }

    const RemoteColumnStats &source_stats = data_source->col_stats[col_idx];
    auto base = duckdb::BaseStatistics::CreateUnknown(ToLogicalType(data_source->schema->columns[col_idx].type));
    if (source_stats.has_min) duckdb::NumericStats::SetMin(base, source_stats.min_value);
    if (source_stats.has_max) duckdb::NumericStats::SetMax(base, source_stats.max_value);
    if (source_stats.has_distinct) base.SetDistinctCount(static_cast<duckdb::idx_t>(source_stats.distinct_count));
    if (source_stats.not_null) base.Set(duckdb::StatsInfo::CANNOT_HAVE_NULL_VALUES);
    return base.ToUnique();
}

} // namespace


void RegisterPlumeRemote(duckdb::Connection &con, duckdb::shared_ptr<PlumeRemoteInfo> registry) {
    duckdb::TableFunction tf(kPlumeRemoteName, {duckdb::LogicalType::VARCHAR}, nullptr, PlumeRemoteBind);
    tf.cardinality = PlumeRemoteCardinality;
    tf.statistics_extended = PlumeRemoteStatistics;
    tf.filter_pushdown = true;
    tf.projection_pushdown = true;
    tf.filter_prune = true;
    tf.function_info = std::move(registry);

    duckdb::CreateTableFunctionInfo info(tf);
    con.BeginTransaction();
    auto &sys_catalog = duckdb::Catalog::GetSystemCatalog(*con.context);
    sys_catalog.CreateTableFunction(*con.context, info);
    con.Commit();
}

} // namespace plume::catalog
