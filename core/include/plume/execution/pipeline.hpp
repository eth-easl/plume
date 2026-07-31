#pragma once

#include "plume/csv/csv.hpp"
#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"

#include <cstdint>
#include <vector>

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume::exec {

// How a stage partitions its output rows for downstream data parallelism.
struct OutputSplit {
    std::vector<uint32_t> key_columns; // column indices in the output schema
    uint32_t partitions = 1;           // N

    void Serialize(duckdb::Serializer &s) const;
    static OutputSplit Deserialize(duckdb::Deserializer &d);
};

// How a stage materializes its output.
enum class OutputFormat : uint8_t {
    PLUME_BLOCKS = 0, // (default) serialized Plume block format
    CSV = 1,
    PARQUET = 2
};

struct OutputSink {
    OutputFormat format = OutputFormat::PLUME_BLOCKS;
    csv::CSVOptions csv;        // CSV only
    int32_t parquet_codec = 0;  // PARQUET only (0 = UNCOMPRESSED)

    void Serialize(duckdb::Serializer &s) const;
    static OutputSink Deserialize(duckdb::Deserializer &d);
};

struct PipelineTemplate {
    Schema input_schema;
    std::vector<std::shared_ptr<OperatorTemplate>> operators;
    OutputSplit output_split;
    OutputSink output_sink;

    void Serialize(duckdb::Serializer &s) const;
    static PipelineTemplate Deserialize(duckdb::Deserializer &d);
};

} // namespace plume::exec

namespace plume {

std::vector<uint8_t> SerializePipeline(const exec::PipelineTemplate &pipeline);
Result<exec::PipelineTemplate> DeserializePipeline(const uint8_t *data, size_t size);

inline Result<exec::PipelineTemplate> DeserializePipeline(const std::vector<uint8_t> &bytes) {
    return DeserializePipeline(bytes.data(), bytes.size());
}

} // namespace plume
