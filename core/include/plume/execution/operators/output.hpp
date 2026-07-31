#pragma once

#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/operator.hpp"
#include "plume/execution/sink.hpp"

#include "duckdb/common/types/data_chunk.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace plume::exec {

// Terminal operator base that owns the sink and flushes it on Finish().
// Push(...) is provided by the concrete Single/Split subclasses.
class OutputOperatorBase : public Operator {
public:
    OutputOperatorBase(duckdb::vector<duckdb::LogicalType> output_types, std::unique_ptr<PartitionSink> sink)
        : Operator(std::move(output_types)), sink_(std::move(sink)) {}

    Result<void> Finish() override {
        PLUME_TRACE_OP(trace::Phase::FINISH);
        return sink_->FlushAll();
    }

protected:
    std::unique_ptr<PartitionSink> sink_;
};

// Sink that does not split the output.
class SingleOutputOperator : public OutputOperatorBase {
public:
    using OutputOperatorBase::OutputOperatorBase;
    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;
};

// Sink that splits the output, routing every row to hash(key_columns) % partitions.
class SplitOutputOperator : public OutputOperatorBase {
public:
    SplitOutputOperator(duckdb::vector<duckdb::LogicalType> output_types, std::unique_ptr<PartitionSink> sink,
                        std::vector<uint32_t> key_columns, uint32_t partitions);
    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;

private:
    std::vector<uint32_t> key_columns_;
    uint32_t partitions_;
};

std::unique_ptr<Operator> MakeOutputOperator(const Schema &schema, std::vector<uint32_t> key_columns,
                                             uint32_t partitions, std::unique_ptr<PartitionSink> sink);

} // namespace plume::exec
