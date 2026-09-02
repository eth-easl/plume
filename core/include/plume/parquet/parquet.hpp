#pragma once

#include "plume/common/buffer.hpp"
#include "plume/expression/expression.hpp"

#include <cstdint>
#include <vector>

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume::parquet {

// Parquet reading configuration. The information used by csv_prepare.
struct ParquetConfig {
    uint32_t num_splits = 1;
    uint64_t coalesce_distance = 1 << 20; // merge column chunks within this gap into one request
    uint64_t max_region_size = 0;         // cap on the compressed bytes each region processes (0 ⇒ unlimited)

    // Projection pushdown: the column indices to be read, empty -> no projection.
    std::vector<uint32_t> projection;

    // Filter pushdown: a predicate whose column references are file column indices.
    bool has_pushed_filter = false;
    expr::ExprNode pushed_filter;

    // Dynamic filter: file column index a runtime-computed DynamicFilterBounds input applies to.
    // -1 means this scan doesn't take a dynamic filter.
    int32_t dynamic_filter_column = -1;

    void Serialize(duckdb::Serializer &s) const;
    static ParquetConfig Deserialize(duckdb::Deserializer &d);
};

// Represents a Parquet ColumnChunk.
struct DataChunk {
    // The row group index.
    uint64_t row_group_idx;
    // The column index.
    uint64_t column_idx;
    // The offset in the original file.
    int64_t offset;
    // The size of the data chunk in bytes.
    int64_t size;
    // Index of the request loading this chunk in the Region requests vector.
    uint64_t req_idx;
    // Offset in the request loading this chunk in the Region requests vector.
    uint64_t req_offset;
    // The parquet CompressionCodec.
    int32_t codec = 0;

    inline uint64_t end() const {
        return offset + size;
    }
};

// Bundles one or more DataChunks into a single data fetching request.
struct ChunkRequest {
    // The offset in the original file.
    int64_t offset;
    // The size of the request in bytes.
    int64_t size;

    inline uint64_t end() const {
        return offset + size;
    }
};

struct Region {
    // All data chunks belonging to this region grouped by their row group.
    std::vector<std::vector<DataChunk>> row_groups;
    // The data fetching requests.
    std::vector<ChunkRequest> requests;

    inline bool empty() const {
        return row_groups.empty();
    }
    inline size_t total_chunks() const {
        if (empty()) { return 0; }
        return row_groups.size() * row_groups[0].size();
    }

    DataBuffer Serialize() const;
    static Region Deserialize(DataBuffer& buf);
};

} // namespace plume::parquet
