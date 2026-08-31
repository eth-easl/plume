#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/storage/arena_allocator.hpp"

#include <cstdint>
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
    AggregateBuild(std::string func_name_p, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> arguments_p,
                   duckdb::LogicalType return_type_p,
                   std::unique_ptr<duckdb::AggregateFunction> function_p = nullptr, bool distinct_p = false)
        : func_name(std::move(func_name_p)), arguments(std::move(arguments_p)),
          return_type(std::move(return_type_p)), function(std::move(function_p)), distinct(distinct_p) {}

    // Move-only (arguments hold unique_ptrs); deleting copy forces std::vector to use the move 
    // path on reallocation.
    AggregateBuild(AggregateBuild &&) = default;
    AggregateBuild &operator=(AggregateBuild &&) = default;
    AggregateBuild(const AggregateBuild &) = delete;
    AggregateBuild &operator=(const AggregateBuild &) = delete;

    std::string func_name;
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> arguments;
    duckdb::LogicalType return_type;
    std::unique_ptr<duckdb::AggregateFunction> function; // resolved kernel; null for min/max
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
    // Precomputed per-chunk accessor for one group-key column (built once per
    // chunk, not re-derived per row — the old EncodeCell hot path). Public so the
    // key-encode helper in the .cpp can name it.
    struct KeyCol {
        duckdb::Vector *vec = nullptr;
        const uint8_t *raw = nullptr; // FlatVector::GetData(*vec)
        uint32_t width = 0;           // physical width; 0 for VARCHAR
        bool is_varchar = false;
    };

    struct AggImpl;

    void EncodeCell(std::string &key, const KeyCol &col, duckdb::idx_t row);
    void RehashGroups();
    void AddGroupSlots();

    duckdb::idx_t ProbeOrInsert(duckdb::DataChunk &keys, const std::vector<KeyCol> &cols, duckdb::idx_t row);

    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> group_exprs_;
    std::unique_ptr<duckdb::ExpressionExecutor> group_executor_;
    duckdb::DataChunk key_chunk_;
    duckdb::vector<duckdb::LogicalType> key_types_;

    std::vector<std::unique_ptr<AggImpl>> aggs_;

    memory::Allocator alloc_;
    duckdb::ArenaAllocator arena_; // backs aggregate states + AggregateInputData

    std::vector<int32_t> group_slots_;        // slot -> group index, or -1 if empty
    uint64_t group_mask_ = 0;                 // group_slots_.size() - 1 (a power of two)
    std::vector<char> group_key_store_;       // all groups' encoded key bytes, concatenated
    std::vector<uint32_t> group_key_off_;     // size ngroups+1: group g spans [off[g], off[g+1])
    std::vector<uint64_t> group_hashes_;      // per-group key hash
    std::string key_scratch_;                 // reused per-row key-encode buffer
    std::vector<duckdb::idx_t> group_of_row_; // reused per-chunk row->group mapping

    std::vector<std::vector<duckdb::Value>> group_key_values_; // representative keys per group
    bool ungrouped_;
};

Result<std::unique_ptr<Operator>> BuildAggregateTemplate(std::shared_ptr<AggregateTemplate> templ,
                                                         duckdb::vector<duckdb::LogicalType> col_types,
                                                         memory::Allocator &alloc);

Schema AggregateSchema(const AggregateTemplate &templ, const Schema &input_schema);

} // namespace plume::exec
