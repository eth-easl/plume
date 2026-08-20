#pragma once

#include "plume/catalog/remote_resolver.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"

#include "duckdb/common/types/value.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace plume::catalog {

enum class DataSourceType : uint8_t {
    LOCAL_TABLE = 0,
    REMOTE_PARQUET = 1,
    REMOTE_CSV = 2,
};

struct RemoteColumnStats {
    bool has_min = false;
    bool has_max = false;
    duckdb::Value min_value;
    duckdb::Value max_value;

    bool has_distinct = false;
    uint64_t distinct_count = 0;

    bool not_null = false; // the source proves the column has no nulls
};

// Parent struct.
struct DataSource {
    DataSourceType type;

    // The base-table name.
    std::string name;
    // Backing file paths/urls.
    std::vector<std::string> paths;

    // The schema of the data source (optional to allow for lazy resolving).
    std::optional<Schema> schema = std::nullopt;
    // Cardinality per path.
    std::vector<uint64_t> cardinalities = {};
    // Total table cardinailty (estimated if no accurate cardinalities are available).
    uint64_t cardinality_total = 0;

    // Per-column statistics for the query optimizer (empty if not available).
    std::vector<RemoteColumnStats> col_stats = {};

    DataSource(DataSourceType type) : type(type) {}
    virtual ~DataSource() = default;
    virtual Result<void> Resolve(const RemoteResolver &resolver) = 0;
    bool IsRemote() const { return type == DataSourceType::REMOTE_PARQUET || type == DataSourceType::REMOTE_CSV; }
};

class SourceCatalog {
public:
    Result<size_t> Add(std::shared_ptr<DataSource> source);

    Result<const std::shared_ptr<DataSource>> Get(size_t source_idx) const;
    Result<const std::shared_ptr<DataSource>> Get(const std::string &source_name) const;

    size_t size() const { return sources_.size(); }
    bool empty() const { return sources_.empty(); }
    bool contains(const std::string &name) { return name_map_.find(name) != name_map_.end(); }

private:
    std::vector<std::shared_ptr<DataSource>> sources_;
    std::map<std::string, size_t> name_map_;
};

} // namespace plume::catalog
