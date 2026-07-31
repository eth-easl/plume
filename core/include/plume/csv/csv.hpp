#pragma once

#include "plume/common/buffer.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume::csv {

struct CSVOptions {
    uint8_t delimiter = ',';
    bool has_header = true;

    void Serialize(duckdb::Serializer &s) const;
    static CSVOptions Deserialize(duckdb::Deserializer &d);
};

// A region of a CSV file. The information handed from csv_prepare to the csv_stage.
struct CSVRegionInfo {
    Schema schema; // the schema after reading and (optional) projection
    CSVOptions options;
    uint64_t logical_end;

    // Projection pushdown: the column indices to be read, empty -> no projection.
    // If a projection is pushed down we expect `schema` size = `projection` size.
    std::vector<uint32_t> projection;

    void Serialize(duckdb::Serializer &s) const;
    static CSVRegionInfo Deserialize(duckdb::Deserializer &d);
};

// CSV reading configuration. The information used by csv_prepare.
// Splitting cuts records at byte boundaries, so each split fetches a small over-read margin 
// (`line_margin`) past its logical end.
struct CSVConfig {
    // In how many regions the CSV is split into.
    uint32_t num_splits = 1;
    // The total CSV size.
    uint64_t total_size = 0;
    
    // Schema (column names + types) unless resolved automatically from the header.
    Schema schema;
    // General CSV options.
    CSVOptions options;
    // Over-read margin (per region) to finish a record.
    uint64_t line_margin = 1 << 20;
    // Whether to resolve names from the first row of the CSV.
    bool resolve_names_from_header = false;
    
    // Projection pushdown: the column indices to be read, empty -> no projection.
    // If a projection is pushed down we expect `schema` size = `projection` size.
    std::vector<uint32_t> projection;

    void Serialize(duckdb::Serializer &s) const;
    static CSVConfig Deserialize(duckdb::Deserializer &d);
};

std::vector<std::string> SplitCSVLine(const uint8_t *data, size_t size, char delimiter, size_t &line_end);

Result<Schema> SniffSchema(const uint8_t *data, size_t size, const CSVOptions &options,
                           size_t max_sample_rows = 100);

using ChunkSink = std::function<Result<void>(std::unique_ptr<duckdb::DataChunk>)>;
Result<void> ParseCSVChunk(const uint8_t *data, size_t size, const CSVRegionInfo &reg_info,
                           memory::Allocator &alloc, const ChunkSink &emit);

Result<DataBuffer> WriteCSV(const Schema &schema, const std::vector<duckdb::DataChunk *> &chunks,
                            const CSVOptions &options);

} // namespace plume::csv
