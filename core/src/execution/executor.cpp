#include "plume/execution/executor.hpp"

#include "plume/execution/operator.hpp"
#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/execution/operators/limit.hpp"
#include "plume/execution/operators/output.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/execution/operators/sort.hpp"
#include "plume/execution/operators/top_n.hpp"
#include "plume/memory/adapter.hpp"

#include "duckdb/common/exception.hpp"

#include <memory>

namespace plume::exec {

using memory::Allocator;
using memory::InputBlock;
using memory::ImportBlockChunks;

using duckdb::DataChunk;
using duckdb::LogicalType;

namespace {

duckdb::vector<LogicalType> SchemaTypes(const Schema &schema) {
    duckdb::vector<LogicalType> types;
    types.reserve(schema.size());
    for (auto &col : schema.columns) {
        types.push_back(ToLogicalType(col.type));
    }
    return types;
}

} // namespace

Executor::Executor(Allocator &alloc) : alloc_(alloc) {
    // Route DuckDB kernel scratch (context-less ExpressionExecutor) through Plume.
    alloc_.InstallAsDuckDBDefault();
}

Result<void> Executor::Build(const PipelineTemplate &pipeline, std::unique_ptr<PartitionSink> sink) {
    Schema cur_schema = pipeline.input_schema;

    // A join may only lead the pipeline -> pull it out as the two-input source.
    size_t op_idx = 0;
    Operator *prev_op = nullptr;
    if (!pipeline.operators.empty() && pipeline.operators[0]->type == OpType::JOIN) {
        auto join_templ = std::dynamic_pointer_cast<JoinTemplate>(pipeline.operators[0]);
        TRY(join_, BuildJoinTemplate(join_templ, SchemaTypes(cur_schema), alloc_));
        cur_schema = JoinSchema(*join_templ, cur_schema);
        prev_op = static_cast<Operator *>(join_.get());
        join_->SetTraceOpIndex(0);
        op_idx = 1;
    }

    for (; op_idx < pipeline.operators.size(); op_idx++) {
        auto &op_templ = pipeline.operators[op_idx];
        if (op_templ->type == OpType::JOIN) {
            return Error("Plume: a join may only be the leading operator", ErrorKind::InvalidInput);
        }
        auto cur_types = SchemaTypes(cur_schema);

        std::unique_ptr<plume::exec::Operator> op;
        switch (op_templ->type) {
        case OpType::PROJECTION: {
            auto projection_templ = std::dynamic_pointer_cast<ProjectionTemplate>(op_templ);
            TRY(op, BuildProjectionTemplate(projection_templ, cur_types, alloc_));
            cur_schema = ProjectionSchema(projection_templ->projections, cur_schema);
            break;
        }
        case OpType::FILTER: {
            auto filter_templ = std::dynamic_pointer_cast<FilterTemplate>(op_templ);
            TRY(op, BuildFilterTemplate(filter_templ, cur_types));
            // schema unchanged
            break;
        }
        case OpType::LIMIT: {
            auto limit_templ = std::dynamic_pointer_cast<LimitTemplate>(op_templ);
            TRY(op, BuildLimitTemplate(limit_templ, cur_types));
            // schema unchanged
            break;
        }
        case OpType::AGGREGATE: {
            auto aggregate_templ = std::dynamic_pointer_cast<AggregateTemplate>(op_templ);
            TRY(op, BuildAggregateTemplate(aggregate_templ, cur_types, alloc_));
            cur_schema = AggregateSchema(*aggregate_templ, cur_schema);
            break;
        }
        case OpType::ORDER_BY: {
            auto sort_templ = std::dynamic_pointer_cast<SortTemplate>(op_templ);
            TRY(op, BuildSortTemplate(sort_templ, cur_types, alloc_));
            // schema unchanged
            break;
        }
        case OpType::TOP_N: {
            auto top_n_templ = std::dynamic_pointer_cast<TopNTemplate>(op_templ);
            TRY(op, BuildTopNTemplate(top_n_templ, cur_types, alloc_));
            // schema unchanged
            break;
        }
        default:
            return Error("Plume: unknown operator type");
        }

        if(prev_op) {
            prev_op->SetNext(op.get());
        }
        prev_op = op.get();
        op->SetTraceOpIndex(static_cast<int>(op_idx));
        operators_.push_back(std::move(op));
    }

    output_schema_ = std::move(cur_schema);
    const uint32_t num_splits =
        pipeline.output_split.partitions == 0 ? 1 : pipeline.output_split.partitions;
    terminal_ = MakeOutputOperator(
        output_schema_, pipeline.output_split.key_columns, num_splits, std::move(sink));
    terminal_->SetTraceOpIndex(static_cast<int>(pipeline.operators.size()));

    if (prev_op) {
        prev_op->SetNext(terminal_.get());
    }

    chain_head_ = ChainHead();
    return Ok();
}

Result<void> Executor::PushJoinBuild(std::unique_ptr<DataChunk> chunk) {
    if (!join_) {
        return Error("PushJoinBuild requires a leading join", ErrorKind::InvalidInput);
    }
    join_->PushBuild(std::move(chunk));
    return Ok();
}

Result<void> Executor::PushJoinBuildBlocks(const std::vector<InputBlock> &blocks) {
    for (const auto &block : blocks) {
        Schema in_schema;
        TRY(auto chunks, ImportBlockChunks(block.data, block.size, in_schema));
        for (auto &chunk : chunks) {
            TRYV(PushJoinBuild(std::move(chunk)));
        }
    }
    return Ok();
}

Result<void> Executor::FinishJoinBuild() {
    if (!join_) {
        return Error("FinishJoinBuild requires a leading join", ErrorKind::InvalidInput);
    }
    join_->FinishBuild();
    return Ok();
}

Result<void> Executor::Push(std::unique_ptr<DataChunk> chunk) {
    started_ = true;
    if (join_) {
        return join_->Push(std::move(chunk));
    }
    return chain_head_->Push(std::move(chunk));
}

Result<void> Executor::PushBlocks(const std::vector<InputBlock> &blocks) {
    for (const auto &block : blocks) {
        Schema in_schema;
        TRY(auto chunks, ImportBlockChunks(block.data, block.size, in_schema));
        for (auto &chunk : chunks) {
            TRYV(Push(std::move(chunk)));
        }
    }
    return Ok();
}

Result<void> Executor::Finish() {
    if (!terminal_) {
        return Error("Executor::Finish requires a terminal (call Build with a sink)", ErrorKind::InvalidInput);
    }
    if (join_) {
        return join_->Finish();
    }
    return chain_head_->Finish();
}

Result<FilterOperator *> Executor::TakeLeadingScanFilter() {
    if (join_ || operators_.empty()) {
        return nullptr;
    }
    auto *filter = dynamic_cast<FilterOperator *>(operators_.front().get());
    if (!filter) {
        return nullptr;
    }
    
    if (started_) {
        return Error("TakeLeadingScanFilter must be called before pushing the first block.");
    }
    scan_filter_ = std::move(operators_.front());
    operators_.erase(operators_.begin());
    chain_head_ = ChainHead();
    return filter;
}

} // namespace plume::exec
