#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/execution/expression_executor.hpp"

namespace plume::exec {

enum class SortOrder : uint8_t { ASCENDING = 0, DESCENDING = 1 };
enum class NullOrder : uint8_t { NULLS_FIRST = 0, NULLS_LAST = 1 };

struct SortKey {
    expr::ExprNode expr;
    SortOrder order = SortOrder::ASCENDING;
    NullOrder null_order = NullOrder::NULLS_LAST;

    bool Equals(const SortKey &other) const;
    void Serialize(duckdb::Serializer &s) const;
    static SortKey Deserialize(duckdb::Deserializer &d);
};

struct SortTemplate : OperatorTemplate {
    std::vector<SortKey> sort_keys;

    SortTemplate() : OperatorTemplate(OpType::ORDER_BY) {}
    SortTemplate(std::vector<SortKey> sort_keys)
        : OperatorTemplate(OpType::ORDER_BY), sort_keys(std::move(sort_keys)) {}

    bool Equals(const OperatorTemplate &other) const override;
    void Serialize(duckdb::Serializer &s) const override;
};

class SortOperator : public Operator {
public:
    SortOperator(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> key_exprs, 
        std::vector<SortOrder> orders, std::vector<NullOrder> null_orders, 
        duckdb::vector<duckdb::LogicalType> output_types, memory::Allocator &alloc);

    // Takes ownership of the chunk and retains it. 
    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;
    // Orders the retained chunks, outputs the sorted rows and frees the buffered chunks.
    Result<void> Finish() override;

private:
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> key_exprs_;
    std::unique_ptr<duckdb::ExpressionExecutor> key_executor_;
    duckdb::vector<duckdb::LogicalType> key_types_;
    std::vector<SortOrder> orders_;
    std::vector<NullOrder> null_orders_;
    memory::Allocator alloc_;
    ChunkList buffered_; // all pushed chunks, retained until Finish orders + emits them
};

Result<std::unique_ptr<Operator>> BuildSortTemplate(std::shared_ptr<SortTemplate> templ,
    duckdb::vector<duckdb::LogicalType> col_types, memory::Allocator &alloc);

} // namespace plume::exec
