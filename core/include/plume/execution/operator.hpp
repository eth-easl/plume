#pragma once

#include "plume/common/result.hpp"
#include "plume/common/trace.hpp"
#include "plume/common/types.hpp"

#include "duckdb/common/types/data_chunk.hpp"

#include <memory>
#include <vector>

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume::exec {

using ChunkList = std::vector<std::unique_ptr<duckdb::DataChunk>>;

enum class OpType : uint8_t {
    INVALID = 0,
    PROJECTION = 1,
    FILTER = 2,
    LIMIT = 3,
    AGGREGATE = 4,
    ORDER_BY = 5,
    JOIN = 6,
    TOP_N = 7, // fused ORDER BY + LIMIT
};

// Parent class, inherited by all specific operator templates.
struct OperatorTemplate {
    OpType type = OpType::INVALID;

    explicit OperatorTemplate(OpType t) : type(t) {}
    virtual ~OperatorTemplate() = default;

    virtual bool Equals(const OperatorTemplate &other) const = 0;
    virtual void Serialize(duckdb::Serializer &s) const = 0;
    static std::shared_ptr<OperatorTemplate> DeserializeOperator(OpType type, duckdb::Deserializer &d);
};

// A pipeline operator that emits its results to the next downstream operator.
// An operator ingests chunks using the Push() method, and for each chunk may forward ownership of a
// derived slice, accumulate it into its state, or retain it until output is produced.
class Operator {
public:
    explicit Operator(duckdb::vector<duckdb::LogicalType> output_types) : output_types_(std::move(output_types)) {}
    virtual ~Operator() = default;

    // Consumes one chunk (taking ownership of the chunk).
    virtual Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) = 0;
    
    // Flushes any buffered results downstream.
    virtual Result<void> Finish() = 0;

    void SetNext(Operator *next) { next_ = next; }
    const duckdb::vector<duckdb::LogicalType> &OutputTypes() const { return output_types_; }

    // Sets the operator's index within the stage pipeline. Used only for tracing, useless otherwise.
    void SetTraceOpIndex(int idx) { trace_op_index_ = idx; }

protected:
    Operator *next_ = nullptr; // downstream operator; set by the executor at build time
    duckdb::vector<duckdb::LogicalType> output_types_;
    int trace_op_index_ = ::plume::trace::kStageOp;
};

} // namespace plume::exec
