#include "plume/csv/csv.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/value.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>

namespace plume::csv {

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;

//===----------------------------------------------------------------------===//
// Serialization
//===----------------------------------------------------------------------===//

void CSVOptions::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "delimiter", delimiter);
    s.WriteProperty(101, "has_header", has_header);
}

CSVOptions CSVOptions::Deserialize(duckdb::Deserializer &d) {
    CSVOptions o;
    o.delimiter = d.ReadProperty<uint8_t>(100, "delimiter");
    o.has_header = d.ReadProperty<bool>(101, "has_header");
    return o;
}

void CSVRegionInfo::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "schema", schema);
    s.WriteProperty(101, "options", options);
    s.WriteProperty(102, "end", logical_end);
    s.WriteList(103, "projection", projection.size(),
            [&](duckdb::Serializer::List &list, duckdb::idx_t i) { list.WriteElement(projection[i]); });
}

CSVRegionInfo CSVRegionInfo::Deserialize(duckdb::Deserializer &d) {
    CSVRegionInfo info;
    info.schema = d.ReadProperty<Schema>(100, "schema");
    info.options = d.ReadProperty<CSVOptions>(101, "options");
    info.logical_end = d.ReadProperty<uint64_t>(102, "end");
    d.ReadList(103, "projection", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
        info.projection.push_back(list.ReadElement<uint32_t>());
    });
    return info;
}

void CSVConfig::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "num_splits", num_splits);
    s.WriteProperty(101, "total_size", total_size);
    s.WriteProperty(102, "line_margin", line_margin);
    s.WriteProperty(103, "resolve_names_from_header", resolve_names_from_header);
    s.WriteProperty(104, "options", options);
    s.WriteProperty(105, "schema", schema);
    s.WriteList(106, "projection", projection.size(),
            [&](duckdb::Serializer::List &list, duckdb::idx_t i) { list.WriteElement(projection[i]); });
}

CSVConfig CSVConfig::Deserialize(duckdb::Deserializer &d) {
    CSVConfig c;
    c.num_splits = d.ReadProperty<uint32_t>(100, "num_splits");
    c.total_size = d.ReadProperty<uint64_t>(101, "total_size");
    c.line_margin = d.ReadProperty<uint64_t>(102, "line_margin");
    c.resolve_names_from_header = d.ReadProperty<bool>(103, "resolve_names_from_header");
    c.options = d.ReadProperty<CSVOptions>(104, "options");
    c.schema = d.ReadProperty<Schema>(105, "schema");
    d.ReadList(106, "projection", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
        c.projection.push_back(list.ReadElement<uint32_t>());
    });
    return c;
}

//===----------------------------------------------------------------------===//
// Schema sniffing
//===----------------------------------------------------------------------===//

namespace {

bool ParsesAsInt(const std::string &s) {
    if (s.empty()) {
        return false;
    }
    errno = 0;
    char *end = nullptr;
    std::strtoll(s.c_str(), &end, 10);
    return errno == 0 && end == s.c_str() + s.size();
}

bool ParsesAsDouble(const std::string &s) {
    if (s.empty()) {
        return false;
    }
    char *end = nullptr;
    std::strtod(s.c_str(), &end);
    return end == s.c_str() + s.size();
}

bool ParsesAsBool(const std::string &s) {
    auto ieq = [&](const char *lit) {
        size_t n = std::strlen(lit);
        if (s.size() != n) {
            return false;
        }
        for (size_t i = 0; i < n; i++) {
            if (std::tolower(static_cast<unsigned char>(s[i])) != lit[i]) {
                return false;
            }
        }
        return true;
    };
    return ieq("true") || ieq("false");
}

} // namespace

std::vector<std::string> SplitCSVLine(const uint8_t *data, size_t size, char delimiter, size_t &line_end) {
    // TODO: no quoted field handling
    const char *b = reinterpret_cast<const char *>(data);
    size_t nl = 0;
    while (nl < size && b[nl] != '\n') {
        nl++;
    }
    size_t field_end = nl;
    if (field_end > 0 && b[field_end - 1] == '\r') {
        field_end--;
    }
    std::vector<std::string> fields;
    size_t start = 0;
    for (size_t i = 0; i < field_end; i++) {
        if (b[i] == delimiter) {
            fields.emplace_back(b + start, i - start);
            start = i + 1;
        }
    }
    fields.emplace_back(b + start, field_end - start);
    line_end = (nl < size) ? nl + 1 : size;
    return fields;
}

Result<Schema> SniffSchema(const uint8_t *data, size_t size, const CSVOptions &options, size_t max_sample_rows) {
    if (size == 0) {
        return Error("CSV sniff: empty input", ErrorKind::InvalidInput);
    }
    const char delim = static_cast<char>(options.delimiter);
    size_t pos = 0;

    std::vector<std::string> names;
    if (options.has_header) {
        size_t line_end = 0;
        names = SplitCSVLine(data + pos, size - pos, delim, line_end);
        pos += line_end;
    }

    struct ColFlags {
        bool any = false;       // saw at least one non-empty value
        bool all_int = true;
        bool all_double = true;
        bool all_bool = true;
    };
    std::vector<ColFlags> flags;
    size_t ncols = names.size();
    size_t rows = 0;
    while (pos < size && rows < max_sample_rows) {
        size_t line_end = 0;
        auto fields = SplitCSVLine(data + pos, size - pos, delim, line_end);
        pos += line_end;
        if (fields.size() == 1 && fields[0].empty()) {
            continue; // blank line (e.g. a trailing newline)
        }
        ncols = std::max(ncols, fields.size());
        if (flags.size() < ncols) {
            flags.resize(ncols);
        }
        for (size_t c = 0; c < fields.size(); c++) {
            const std::string &v = fields[c];
            if (v.empty()) {
                continue; // empty field -> NULL, doesn't constrain the type
            }
            ColFlags &f = flags[c];
            f.any = true;
            f.all_int &= ParsesAsInt(v);
            f.all_double &= ParsesAsDouble(v);
            f.all_bool &= ParsesAsBool(v);
        }
        rows++;
    }
    if (ncols == 0) {
        return Error("CSV sniff: no columns found", ErrorKind::InvalidInput);
    }
    if (flags.size() < ncols) {
        flags.resize(ncols);
    }

    Schema schema;
    for (size_t c = 0; c < ncols; c++) {
        Column col;
        col.name = (c < names.size() && !names[c].empty()) ? names[c] : ("col" + std::to_string(c));
        const ColFlags &f = flags[c];
        if (!f.any) {
            col.type.id = TypeId::VARCHAR; // no data sampled — safest default
        } else if (f.all_int) {
            col.type.id = TypeId::INT64;
        } else if (f.all_double) {
            col.type.id = TypeId::DOUBLE;
        } else if (f.all_bool) {
            col.type.id = TypeId::BOOLEAN;
        } else {
            col.type.id = TypeId::VARCHAR;
        }
        col.nullable = true;
        schema.columns.push_back(std::move(col));
    }
    return schema;
}

//===----------------------------------------------------------------------===//
// Parsing
//===----------------------------------------------------------------------===//

namespace {

size_t NextNewline(const char *b, size_t size, size_t from) {
    while (from < size && b[from] != '\n') {
        from++;
    }
    return from;
}

} // namespace

Result<void> ParseCSVChunk(const uint8_t *data, size_t size, const CSVRegionInfo &reg_info, 
                           memory::Allocator &alloc, const ChunkSink &emit) {
    // TODO: no quoted field handling
    const auto &schema = reg_info.schema;
    const char delim = static_cast<char>(reg_info.options.delimiter);
    const char *b = reinterpret_cast<const char *>(data);

    duckdb::vector<LogicalType> types;
    types.reserve(schema.size());
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }

    // Setup projection pushdown.
    constexpr uint32_t kNoOut = std::numeric_limits<uint32_t>::max();
    std::vector<uint32_t> src_to_out;
    if (!reg_info.projection.empty()) {
        uint32_t max_src = 0;
        for (uint32_t s : reg_info.projection) {
            max_src = std::max(max_src, s);
        }
        src_to_out.assign(max_src + 1, kNoOut);
        for (uint32_t out = 0; out < reg_info.projection.size(); out++) {
            src_to_out[reg_info.projection[out]] = out;
        }
    }
    auto out_col = [&](uint32_t sfi) -> uint32_t {
        if (src_to_out.empty()) {
            return sfi < schema.size() ? sfi : kNoOut; // no projection: identity
        }
        return sfi < src_to_out.size() ? src_to_out[sfi] : kNoOut;
    };

    std::unique_ptr<DataChunk> chunk;
    idx_t cur = 0;
    size_t nl = NextNewline(b, size, 0);
    size_t pos = (nl < size) ? nl + 1 : size;
    while (pos < size) {
        // This region owns records with a start byte that is within the logical range.
        if (pos > reg_info.logical_end) {
            break;
        }
        const size_t nl = NextNewline(b, size, pos);
        size_t line_end = nl;
        if (line_end > pos && b[line_end - 1] == '\r') {
            line_end--; // strip CR of CRLF
        }

        // Emit the current chunk once full and start a fresh one.
        if (chunk && cur == STANDARD_VECTOR_SIZE) {
            chunk->SetCardinality(cur);
            TRYV(emit(std::move(chunk)));
            chunk.reset();
            cur = 0;
        }
        if (!chunk) {
            chunk = std::make_unique<DataChunk>();
            chunk->Initialize(alloc.Get(), types);
        }

        auto &dst = *chunk;
        // Default every output column to NULL; present + projected fields overwrite
        // below (handles short rows and unprojected source fields uniformly).
        for (uint32_t c = 0; c < schema.size(); c++) {
            dst.SetValue(c, cur, Value(types[c]));
        }
        uint32_t sfi = 0; // physical source field index
        auto set_field = [&](size_t a, size_t e) {
            const uint32_t col = out_col(sfi);
            if (col != kNoOut) {
                std::string field(b + a, e - a);
                Value v;
                if (field.empty()) {
                    v = Value(types[col]); // NULL
                } else {
                    try {
                        v = Value(field).DefaultCastAs(types[col]);
                    } catch (...) {
                        v = Value(types[col]);
                    }
                }
                dst.SetValue(col, cur, v);
            }
            sfi++;
        };
        size_t fstart = pos;
        for (size_t i = pos; i < line_end; i++) {
            if (b[i] == delim) {
                set_field(fstart, i);
                fstart = i + 1;
            }
        }
        set_field(fstart, line_end);
        cur++;

        pos = (nl < size) ? nl + 1 : size;
    }

    // Emit the final partial chunk.
    if (chunk && cur > 0) {
        chunk->SetCardinality(cur);
        TRYV(emit(std::move(chunk)));
    }
    return Ok();
}

//===----------------------------------------------------------------------===//
// Writing
//===----------------------------------------------------------------------===//

namespace {

void AppendField(std::string &out, const std::string &field, char delim) {
    bool needs_quote = false;
    for (char c : field) {
        if (c == delim || c == '"' || c == '\n' || c == '\r') {
            needs_quote = true;
            break;
        }
    }
    if (!needs_quote) {
        out += field;
        return;
    }
    out += '"';
    for (char c : field) {
        if (c == '"') {
            out += '"'; // double the embedded quote
        }
        out += c;
    }
    out += '"';
}

} // namespace

Result<DataBuffer> WriteCSV(const Schema &schema, const std::vector<DataChunk *> &chunks, const CSVOptions &options) {
    const char delim = static_cast<char>(options.delimiter);
    std::string out;

    if (options.has_header) {
        for (size_t c = 0; c < schema.size(); c++) {
            if (c > 0) {
                out += delim;
            }
            AppendField(out, schema.columns[c].name, delim);
        }
        out += '\n';
    }

    for (auto *chunk : chunks) {
        if (chunk == nullptr) {
            continue;
        }
        const idx_t rows = chunk->size();
        const idx_t cols = chunk->ColumnCount();
        for (idx_t r = 0; r < rows; r++) {
            for (idx_t c = 0; c < cols; c++) {
                if (c > 0) {
                    out += delim;
                }
                Value v = chunk->GetValue(c, r);
                if (!v.IsNull()) {
                    AppendField(out, v.ToString(), delim); // NULL -> empty field
                }
            }
            out += '\n';
        }
    }

    DataBuffer buf(out.size());
    if (!out.empty()) {
        std::memcpy(buf.mutable_data(), out.data(), out.size());
    }
    return buf;
}

} // namespace plume::csv
