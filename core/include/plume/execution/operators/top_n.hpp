#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operator.hpp"
#include "plume/execution/operators/sort.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/execution/expression_executor.hpp"

namespace plume::exec {

struct TopNTemplate : OperatorTemplate {
    std::vector<SortKey> sort_keys;
    uint64_t limit = 0;
    uint64_t offset = 0;

    TopNTemplate() : OperatorTemplate(OpType::TOP_N) {}
    TopNTemplate(std::vector<SortKey> sort_keys, uint64_t limit, uint64_t offset = 0)
        : OperatorTemplate(OpType::TOP_N), sort_keys(std::move(sort_keys)), limit(limit), offset(offset) {}

    void Serialize(duckdb::Serializer &s) const override;
};

class TopNOperator : public Operator {
public:
    TopNOperator(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> key_exprs,
        std::vector<SortOrder> orders, std::vector<NullOrder> null_orders, uint64_t limit, uint64_t offset,
        duckdb::vector<duckdb::LogicalType> output_types, memory::Allocator &alloc);

    // Evaluates each row's sort key and keeps it only if it belongs in the curren top.
    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;
    Result<void> Finish() override;

private:
    // One retained row with its sort keys (for comparison), its full column values (for output), 
    // and the input order it arrived in (stable tie-break).
    struct HeapRow {
        std::vector<duckdb::Value> keys;
        std::vector<duckdb::Value> row;
        uint64_t seq;
    };

    // True if `a` sorts strictly before `b` (ties broken by arrival order).
    bool RowBefore(const std::vector<duckdb::Value> &a_keys, uint64_t a_seq,
        const std::vector<duckdb::Value> &b_keys, uint64_t b_seq) const;
    // Heap comparator: max-heap by this ordering keeps the worst kept row at the front.
    bool HeapLess(const HeapRow &a, const HeapRow &b) const { return RowBefore(a.keys, a.seq, b.keys, b.seq); }

    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> key_exprs_;
    std::unique_ptr<duckdb::ExpressionExecutor> key_executor_;
    duckdb::vector<duckdb::LogicalType> key_types_;
    std::vector<SortOrder> orders_;
    std::vector<NullOrder> null_orders_;
    uint64_t limit_;
    uint64_t offset_;
    uint64_t capacity_; // limit_ + offset_, or 0 if limit_ == 0
    memory::Allocator alloc_;

    std::vector<HeapRow> heap_;
    uint64_t next_seq_ = 0;
};

Result<std::unique_ptr<Operator>> BuildTopNTemplate(std::shared_ptr<TopNTemplate> templ,
    duckdb::vector<duckdb::LogicalType> col_types, memory::Allocator &alloc);

} // namespace plume::exec
