#include "plume/catalog/source_scan.hpp"

#include "plume/catalog/local.hpp"
#include "plume/common/result.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
#include <unordered_set>
#include <vector>

namespace plume::catalog {

namespace {

// One file/URL reference found in a FROM/JOIN position.
struct FileRef {
    std::string alias;              // generated identifier (local sources only)
    std::vector<std::string> paths; // all paths (size > 1 for a bracket-list source)
    bool is_remote = false;
};

std::string Lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

std::string Extension(const std::string &path) {
    // std::filesystem on a URL still yields the trailing ".ext", which is what we want.
    return Lower(std::filesystem::path(path).extension().string());
}

bool LooksLikeUrl(const std::string &tok) { return tok.find("://") != std::string::npos; }

bool LooksLikeFile(const std::string &tok) {
    const auto ext = Extension(tok);
    return ext == ".csv" || ext == ".tsv" || ext == ".parquet";
}

bool IsIdentChar(char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; }

// A SQL-safe identifier derived from a path's stem (e.g. ".../orders.csv" -> "orders").
std::string AliasFromPath(const std::string &path) {
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

// Case-insensitive whole-word match of `kw` at position `i` in `sql`.
bool KeywordAt(const std::string &sql, size_t i, const char *kw) {
    size_t n = std::strlen(kw);
    if (i + n > sql.size()) {
        return false;
    }
    for (size_t k = 0; k < n; k++) {
        if (std::tolower(static_cast<unsigned char>(sql[i + k])) != kw[k]) {
            return false;
        }
    }
    bool left = (i == 0) || !IsIdentChar(sql[i - 1]);
    bool right = (i + n == sql.size()) || !IsIdentChar(sql[i + n]);
    return left && right;
}

// Keywords that terminate a FROM-clause entry/list (so the table-ref scan stops
// rather than treating them as an alias).
bool IsFromStopKeyword(const std::string &w) {
    static const std::unordered_set<std::string> kw = {
        "where", "group",  "order", "having", "limit", "offset", "union", "except", "intersect", "join",
        "inner", "left",   "right", "full",   "cross", "natural", "on",   "using",  "window",    "qualify"};
    return kw.count(w) > 0;
}

// Rewrites the file/url table references in `sql`, returning the rewritten SQL and
// the (deduplicated, local-only) set of sources still needing LoadDataFile. Remote
// references are rewritten inline to plume_remote(...) calls and need no further
// action from the caller. Handles single-table FROM/JOIN as well as comma-separated
// FROM lists (`FROM a.csv x, https://h/b y`); subquery entries (derived tables) are
// recursed into so sources nested inside them are rewritten too. Quoted-string
// literals are skipped so a keyword/path inside one is never matched.
std::pair<std::string, std::vector<FileRef>> RewriteSources(const std::string &sql) {
    std::map<std::string, size_t> local_by_path; // raw primary path -> index into `locals`
    std::vector<FileRef> locals;
    std::unordered_set<std::string> used_aliases;

    struct Replacement {
        size_t begin;
        size_t end;
        std::string text;
    };
    std::vector<Replacement> repls;

    auto skip_ws = [&](size_t &i) {
        while (i < sql.size() && std::isspace(static_cast<unsigned char>(sql[i]))) {
            i++;
        }
    };
    // The lowercase identifier word at `i`; `end` is set just past it (== i if none).
    auto peek_word = [&](size_t i, size_t &end) {
        std::string w;
        size_t j = i;
        while (j < sql.size() && IsIdentChar(sql[j])) {
            w += static_cast<char>(std::tolower(static_cast<unsigned char>(sql[j])));
            j++;
        }
        end = j;
        return w;
    };
    // Returns the index just past the ')' matching the '(' at `open`.
    auto find_paren_end = [&](size_t open) -> size_t {
        size_t i = open;
        int depth = 0;
        while (i < sql.size()) {
            char c = sql[i];
            if (c == '\'' || c == '"') {
                char q = c;
                i++;
                while (i < sql.size() && sql[i] != q) {
                    i++;
                }
                if (i < sql.size()) {
                    i++;
                }
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                i++;
                if (depth == 0) {
                    return i;
                }
                continue;
            }
            i++;
        }
        return sql.size();
    };
    // Records a resolved reference (local or remote) and queues its text replacement.
    auto record_source = [&](std::vector<std::string> paths, size_t b, size_t e) {
        const std::string &primary = paths[0];
        if (LooksLikeUrl(primary)) {
            std::string text = "plume_remote(";
            if (paths.size() == 1) {
                text += duckdb::KeywordHelper::WriteQuoted(primary, '\'');
            } else {
                text += "[";
                for (size_t i = 0; i < paths.size(); i++) {
                    text += (i ? ", " : "") + duckdb::KeywordHelper::WriteQuoted(paths[i], '\'');
                }
                text += "]";
            }
            text += ")";
            repls.push_back({b, e, std::move(text)});
            return;
        }
        auto it = local_by_path.find(primary);
        if (it == local_by_path.end()) {
            std::string alias = AliasFromPath(primary);
            std::string unique = alias;
            for (int n = 1; used_aliases.count(unique); n++) {
                unique = alias + "_" + std::to_string(n);
            }
            used_aliases.insert(unique);
            it = local_by_path.emplace(primary, locals.size()).first;
            locals.push_back(FileRef{unique, std::move(paths), /*is_remote=*/false});
        }
        repls.push_back({b, e, duckdb::KeywordHelper::WriteQuoted(locals[it->second].alias, '"')});
    };
    // Try to read a bracket-list of quoted paths/urls: ['url1', 'url2', ...].
    // If found at `i` (pointing at '['), records the multi-file source and returns
    // true (with `i` advanced past the closing ']'). Returns false otherwise.
    auto read_array_ref = [&](size_t &i) -> bool {
        if (i >= sql.size() || sql[i] != '[') {
            return false;
        }
        const size_t saved = i; // restore position if this turns out not to be a source list
        size_t b = i++;
        std::vector<std::string> urls;
        while (i < sql.size() && sql[i] != ']') {
            skip_ws(i);
            if (i < sql.size() && (sql[i] == '\'' || sql[i] == '"')) {
                char q = sql[i++];
                std::string raw;
                while (i < sql.size() && sql[i] != q) {
                    raw += sql[i++];
                }
                if (i < sql.size()) {
                    i++; // closing quote
                }
                if (!raw.empty()) {
                    urls.push_back(std::move(raw));
                }
            }
            skip_ws(i);
            if (i < sql.size() && sql[i] == ',') {
                i++;
            }
        }
        if (i < sql.size()) {
            i++; // consume ']'
        }
        if (urls.empty()) {
            i = saved;
            return false;
        }
        if (!LooksLikeUrl(urls[0]) && !LooksLikeFile(urls[0])) {
            i = saved;
            return false;
        }
        record_source(std::move(urls), b, i);
        return true;
    };
    // Read one table-reference token (quoted or bare) at `i`, recording it if it is
    // a file/url source. No-op when positioned at a subquery '(' or end.
    auto read_table_ref = [&](size_t &i) {
        skip_ws(i);
        if (i >= sql.size() || sql[i] == '(' || sql[i] == ',' || sql[i] == ')' || sql[i] == ';') {
            return;
        }
        size_t b = i;
        std::string raw;
        if (sql[i] == '\'' || sql[i] == '"') {
            char q = sql[i++];
            while (i < sql.size() && sql[i] != q) {
                raw += sql[i++];
            }
            if (i < sql.size()) {
                i++; // closing quote
            }
        } else {
            while (i < sql.size() && !std::isspace(static_cast<unsigned char>(sql[i])) && sql[i] != ',' &&
                   sql[i] != '(' && sql[i] != ')' && sql[i] != ';') {
                raw += sql[i++];
            }
        }
        if (!raw.empty() && (LooksLikeUrl(raw) || LooksLikeFile(raw))) {
            record_source({raw}, b, i);
        }
    };
    // Skip an optional table alias (`[AS] name`) following a table ref; stop without
    // consuming a clause/join keyword.
    auto skip_alias = [&](size_t &i) {
        skip_ws(i);
        size_t end = 0;
        std::string w = peek_word(i, end);
        if (w.empty() || IsFromStopKeyword(w)) {
            return;
        }
        if (w == "as") {
            i = end;
            skip_ws(i);
            std::string a = peek_word(i, end);
            if (!a.empty()) {
                i = end; // consume the alias name
            }
            return;
        }
        i = end; // a bare alias
    };

    // Scans sql[begin, end) for FROM/JOIN table references. A subquery
    // (derived table) found in a table-ref position is recursed into via
    // scan_range on its inner span, so sources nested inside it get rewritten
    // the same as top-level ones; `i` then resumes right after its ')'.
    std::function<void(size_t, size_t)> scan_range = [&](size_t begin, size_t end) {
        for (size_t i = begin; i < end;) {
            char c = sql[i];
            if (c == '\'' || c == '"') {
                char q = c;
                i++;
                while (i < end && sql[i] != q) {
                    i++;
                }
                if (i < end) {
                    i++;
                }
                continue;
            }
            if (i + 4 <= end && KeywordAt(sql, i, "join")) {
                i += 4;
                skip_ws(i);
                if (i < end && sql[i] == '(') {
                    size_t close = find_paren_end(i);
                    scan_range(i + 1, close - 1);
                    i = close;
                } else if (!read_array_ref(i)) {
                    read_table_ref(i);
                }
                continue;
            }
            if (i + 4 <= end && KeywordAt(sql, i, "from")) {
                i += 4;
                // A comma-separated list of table references, each optionally aliased.
                while (true) {
                    skip_ws(i);
                    if (i < end && sql[i] == '(') {
                        size_t close = find_paren_end(i);
                        scan_range(i + 1, close - 1);
                        i = close;
                    } else if (!read_array_ref(i)) {
                        read_table_ref(i);
                    }
                    skip_alias(i);
                    skip_ws(i);
                    if (i < end && sql[i] == ',') {
                        i++;
                        continue;
                    }
                    break;
                }
                continue;
            }
            i++;
        }
    };
    scan_range(0, sql.size());

    // Apply replacements right-to-left so earlier offsets stay valid.
    std::string out = sql;
    std::sort(repls.begin(), repls.end(), [](const Replacement &a, const Replacement &b) { return a.begin > b.begin; });
    for (const auto &r : repls) {
        out.replace(r.begin, r.end - r.begin, r.text);
    }

    return {out, std::move(locals)};
}

} // namespace

Result<std::string> DetectFileSources(duckdb::Connection &con, const std::string &sql) {
    auto [rewritten, locals] = RewriteSources(sql);
    for (const auto &ref : locals) {
        TRYV(LoadDataFile(con, ref.alias, ref.paths));
    }
    return rewritten;
}

} // namespace plume::catalog
