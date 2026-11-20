#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/group_table.hpp"
#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"
#include "plume/expression/function_registry.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/storage/arena_allocator.hpp"

#include <string>
#include <vector>

namespace plume::exec {

struct AggregateSpec {
    std::string func_name;          // e.g. "sum", "count", "min", "max", "avg"
    ColumnType return_type;
    std::vector<expr::ExprNode> arguments; // input expressions
    bool distinct = false;          // whether this aggregate is distinct (dedup before aggregation)

    bool Equals(const AggregateSpec &other) const;
    void Serialize(duckdb::Serializer &s) const;
    static AggregateSpec Deserialize(duckdb::Deserializer &d);
};

struct AggregateTemplate : OperatorTemplate {
    std::vector<AggregateSpec> aggregates;
    std::vector<expr::ExprNode> group_keys;

    AggregateTemplate() : OperatorTemplate(OpType::AGGREGATE) {}
    AggregateTemplate(std::vector<AggregateSpec> aggregates, std::vector<expr::ExprNode> group_keys = {})
        : OperatorTemplate(OpType::AGGREGATE)
        , aggregates(std::move(aggregates)), group_keys(std::move(group_keys)) {}

    bool Equals(const OperatorTemplate &other) const override;
    void Serialize(duckdb::Serializer &s) const override;
};

struct AggregateBuild {
    AggregateBuild(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> arguments_p,
                   duckdb::LogicalType return_type_p, expr::ResolvedAggregate aggregate_p, bool distinct_p = false)
        : arguments(std::move(arguments_p)), return_type(std::move(return_type_p)),
          aggregate(std::move(aggregate_p)), distinct(distinct_p) {}

    // Move-only (arguments hold unique_ptrs)
    AggregateBuild(AggregateBuild &&) = default;
    AggregateBuild &operator=(AggregateBuild &&) = default;
    AggregateBuild(const AggregateBuild &) = delete;
    AggregateBuild &operator=(const AggregateBuild &) = delete;

    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> arguments;
    duckdb::LogicalType return_type;
    expr::ResolvedAggregate aggregate; // bound kernel + bind data
    bool distinct = false;
};

class AggregateOperator : public Operator {
public:
    AggregateOperator(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> group_exprs,
                      std::vector<AggregateBuild> aggregates, duckdb::vector<duckdb::LogicalType> output_types,
                      memory::Allocator &alloc);
    ~AggregateOperator();

    // Accumulates the chunk's contribution into the group states and drops it.
    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;

    // Emits the grouped result once the source is drained.
    Result<void> Finish() override;

private:
    struct AggImpl;

    duckdb::data_ptr_t GroupPayload(duckdb::idx_t group) const;
    void InitializeStates(const duckdb::data_ptr_t *payloads, duckdb::idx_t count);
    void EnsureUngroupedState();

    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> group_exprs_;
    std::unique_ptr<duckdb::ExpressionExecutor> group_executor_;
    duckdb::DataChunk key_chunk_;
    duckdb::vector<duckdb::LogicalType> key_types_;

    std::vector<std::unique_ptr<AggImpl>> aggs_;
    duckdb::idx_t payload_size_ = 0; // all aggregates' states of one group

    memory::Allocator alloc_;
    duckdb::ArenaAllocator arena_; // backs AggregateInputData + the ungrouped payload

    std::unique_ptr<GroupTable> table_;              // grouped: key -> payload
    duckdb::data_ptr_t ungrouped_payload_ = nullptr; // ungrouped: the single payload
    bool ungrouped_;

    // Per-chunk scratch (STANDARD_VECTOR_SIZE).
    std::vector<duckdb::data_ptr_t> payloads_;     // each row's group payload
    std::vector<duckdb::data_ptr_t> new_payloads_; // payloads of the groups a chunk created
    std::vector<duckdb::data_ptr_t> state_ptrs_;
    duckdb::SelectionVector new_groups_;
};

Result<std::unique_ptr<Operator>> BuildAggregateTemplate(std::shared_ptr<AggregateTemplate> templ,
                                                         duckdb::vector<duckdb::LogicalType> col_types,
                                                         memory::Allocator &alloc);

Schema AggregateSchema(const AggregateTemplate &templ, const Schema &input_schema);

} // namespace plume::exec
