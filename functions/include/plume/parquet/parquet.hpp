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

/// plume_pq_prepare configuration.
struct ParquetConfig {
    uint32_t num_splits = 1;
    uint64_t coalesce_distance = 1 << 20; // merge column chunks within this gap into one request
    uint64_t max_region_size = 0;         // cap on the compressed bytes each region processes (0 ⇒ unlimited)

    /// Projection pushdown: the file (storage) column indices to read, in output
    /// order. Empty ⇒ read every column. When non-empty, each region's row groups
    /// carry only these columns (in this order) and pq_stage's input schema is the
    /// matching projected schema.
    std::vector<uint32_t> projection;

    /// Filter pushdown: a predicate whose column references are file (storage)
    /// column indices. pq_prepare uses it to skip row groups whose statistics prove
    /// no row can match. Only set when `has_pushed_filter`.
    bool has_pushed_filter = false;
    expr::ExprNode pushed_filter;

    void Serialize(duckdb::Serializer &s) const;
    static ParquetConfig Deserialize(duckdb::Deserializer &d);
};

/// Represents a parquet ColumnChunk.
struct DataChunk {
    /// The row group index.
    uint64_t row_group_idx;
    /// The column index.
    uint64_t column_idx;
    /// The offset in the original file.
    int64_t offset;
    /// The size of the data chunk in bytes.
    int64_t size;
    /// Index of the request loading this chunk in the Region requests vector.
    uint64_t req_idx;
    /// Offset in the request loading this chunk in the Region requests vector.
    uint64_t req_offset;
    /// parquet CompressionCodec (0=UNCOMPRESSED, 1=SNAPPY, ...).
    int32_t codec = 0;

    inline uint64_t end() const {
        return offset + size;
    }
};

/// Bundles one or more DataChunks into a single data fetching request.
struct ChunkRequest {
    /// The offset in the original file.
    int64_t offset;
    /// The size of the request in bytes.
    int64_t size;

    inline uint64_t end() const {
        return offset + size;
    }
};

struct Region {
    /// All data chunks belonging to this region grouped by their row group.
    std::vector<std::vector<DataChunk>> row_groups;
    /// The (optimized) data fetching requests.
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
