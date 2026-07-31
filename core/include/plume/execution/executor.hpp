#pragma once

#include "plume/execution/operator.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/execution/sink.hpp"
#include "plume/memory/allocator.hpp"

#include <memory>
#include <vector>

namespace plume::exec {

class FilterOperator;

class Executor {
public:
    Executor(memory::Allocator &alloc);

    // Builds the operator chain from the pipeline template and attaches `sink` as the streaming 
    // output terminal.
    Result<void> Build(const PipelineTemplate &pipeline, std::unique_ptr<PartitionSink> sink);

    // Builds the operator chain from the pipeline template and attaches a BlockSink using `emit` 
    // to emit blocks.
    Result<void> Build(const PipelineTemplate &pipeline, OutputEmit emit) {
        return Build(pipeline, std::make_unique<BlockSink>(std::move(emit)));
    }

    // Pushes a chunk to the leading join operator build side (fails if first operator is not a join).
    Result<void> PushJoinBuild(std::unique_ptr<duckdb::DataChunk> chunk);
    // Imports each host block and pushes it to the leading join operator build side (fails if 
    // first operator is not a join).
    Result<void> PushJoinBuildBlocks(const std::vector<memory::InputBlock> &blocks);

    // Finishes building the join.
    Result<void> FinishJoinBuild();

    // Pushes a chunk through the pipeline (join probe side) transfering `chunk` ownership.
    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk);
    // Imports each host block (one or more chunks) and pushes it through the pipeline. 
    // The chunks reference data from `blocks`, thus they must outlive execution
    Result<void> PushBlocks(const std::vector<memory::InputBlock> &blocks);

    // Flushes any blocking operators in the pipeline and makes the sink emit the final block(s).
    Result<void> Finish();

    // Whether the pipeline begins with a (two-input) join.
    bool HasJoin() const { return join_ != nullptr; }

    // Returns a pointer to the leading FilterOperator if the first operator is a filter and a 
    // nullptr otherwise (ownership remains with the Executor).
    // Fails if the execution has started already.
    Result<FilterOperator *> TakeLeadingScanFilter();

    // Schema of the produced output chunks.
    const Schema &OutputSchema() const { return output_schema_; }

private:
    Operator *ChainHead() {
        return operators_.empty() ? terminal_.get() : operators_.front().get();
    }

    memory::Allocator &alloc_;
    Schema output_schema_;
    std::vector<std::unique_ptr<Operator>> operators_;
    std::unique_ptr<JoinOperator> join_;    // null unless the pipeline leads with a join
    std::unique_ptr<Operator> scan_filter_; // leading filter taken out for pushdown (owns it)

    Operator *chain_head_; // the first (non-join) operator or the terminal if the pipeline is empty
    std::unique_ptr<Operator> terminal_; // streaming output operator; always the tail of the chain
    bool started_ = false;               // set on first push; guards TakeLeadingScanFilter()
};

} // namespace plume::exec
