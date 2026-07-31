#include "plume/execution/operators/top_n.hpp"

#include "plume/expression/expression_builder.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/vector.hpp"

#include <algorithm>

namespace plume::exec {

using expr::BuildExpression;

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::unique_ptr;
using duckdb::Value;

//===----------------------------------------------------------------------===//
// TopNTemplate
//===----------------------------------------------------------------------===//

void TopNTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteList(101, "sort_keys", sort_keys.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteElement(sort_keys[i]);
    });
    s.WriteProperty(102, "limit", limit);
    s.WriteProperty(103, "offset", offset);
}

//===----------------------------------------------------------------------===//
// TopNOperator
//===----------------------------------------------------------------------===//

namespace {

// TODO: same as in sorting -> move to shared function
int CompareCell(const Value &a, const Value &b, SortOrder order, NullOrder null_order) {
    const bool an = a.IsNull();
    const bool bn = b.IsNull();
    if (an && bn) {
        return 0;
    }
    if (an) {
        return null_order == NullOrder::NULLS_FIRST ? -1 : 1;
    }
    if (bn) {
        return null_order == NullOrder::NULLS_FIRST ? 1 : -1;
    }
    if (a < b) {
        return order == SortOrder::ASCENDING ? -1 : 1;
    }
    if (b < a) {
        return order == SortOrder::ASCENDING ? 1 : -1;
    }
    return 0;
}

} // namespace

TopNOperator::TopNOperator(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> key_exprs,
    std::vector<SortOrder> orders, std::vector<NullOrder> null_orders, uint64_t limit, uint64_t offset,
    duckdb::vector<LogicalType> output_types, memory::Allocator &alloc)
    : Operator(std::move(output_types)), key_exprs_(std::move(key_exprs)), orders_(std::move(orders)),
      null_orders_(std::move(null_orders)), limit_(limit), offset_(offset), alloc_(alloc) {
    capacity_ = limit_ == 0 ? 0 : limit_ + offset_;
    key_executor_ = std::make_unique<duckdb::ExpressionExecutor>();
    for (auto &e : key_exprs_) {
        key_executor_->AddExpression(*e);
        key_types_.push_back(e->return_type);
    }
    heap_.reserve(capacity_);
}

bool TopNOperator::RowBefore(const std::vector<Value> &a_keys, uint64_t a_seq, const std::vector<Value> &b_keys,
    uint64_t b_seq) const {
    for (size_t c = 0; c < a_keys.size(); c++) {
        int cmp = CompareCell(a_keys[c], b_keys[c], orders_[c], null_orders_[c]);
        if (cmp != 0) {
            return cmp < 0;
        }
    }
    return a_seq < b_seq; // stable: earlier input row wins ties
}

Result<void> TopNOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    const idx_t n = chunk->size();
    if (n == 0 || capacity_ == 0) {
        return Ok();
    }

    const idx_t n_keys = key_exprs_.size();
    const idx_t n_cols = output_types_.size();

    DataChunk key_chunk;
    key_chunk.Initialize(alloc_.Get(), key_types_);
    key_executor_->Execute(*chunk, key_chunk);
    key_chunk.Flatten();

    auto heap_comp = [&](const HeapRow &a, const HeapRow &b) { return HeapLess(a, b); };

    for (idx_t r = 0; r < n; r++) {
        std::vector<Value> keys;
        keys.reserve(n_keys);
        for (idx_t c = 0; c < n_keys; c++) {
            keys.push_back(key_chunk.GetValue(c, r));
        }
        const uint64_t seq = next_seq_++;

        // Heap has space -> insert the row.
        if (heap_.size() < capacity_) {
            std::vector<Value> row;
            row.reserve(n_cols);
            for (idx_t c = 0; c < n_cols; c++) {
                row.push_back(chunk->GetValue(c, r));
            }
            heap_.push_back(HeapRow{std::move(keys), std::move(row), seq});
            std::push_heap(heap_.begin(), heap_.end(), heap_comp);
            continue;
        }

        // Heap is full -> only materialize + insert the row if it beats the worst kept row.
        const HeapRow &worst = heap_.front();
        if (!RowBefore(keys, seq, worst.keys, worst.seq)) {
            continue; // not in the top `capacity_` rows
        }
        std::pop_heap(heap_.begin(), heap_.end(), heap_comp); // worst row is now at the back
        HeapRow &slot = heap_.back();
        slot.keys = std::move(keys);
        slot.row.clear();
        for (idx_t c = 0; c < n_cols; c++) {
            slot.row.push_back(chunk->GetValue(c, r));
        }
        slot.seq = seq;
        std::push_heap(heap_.begin(), heap_.end(), heap_comp);
    }
    return Ok();
}

Result<void> TopNOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    std::sort(heap_.begin(), heap_.end(),
        [&](const HeapRow &a, const HeapRow &b) { return RowBefore(a.keys, a.seq, b.keys, b.seq); });

    const idx_t n_cols = output_types_.size();
    const idx_t total = heap_.size();
    const idx_t start = std::min<idx_t>(offset_, total);
    for (idx_t base = start; base < total; base += STANDARD_VECTOR_SIZE) {
        const idx_t batch = std::min<idx_t>(STANDARD_VECTOR_SIZE, total - base);
        auto result = std::make_unique<DataChunk>();
        result->Initialize(alloc_.Get(), output_types_);
        for (idx_t i = 0; i < batch; i++) {
            const HeapRow &r = heap_[base + i];
            for (idx_t c = 0; c < n_cols; c++) {
                result->SetValue(c, i, r.row[c]);
            }
        }
        result->SetCardinality(batch);
        TRYV(next_->Push(std::move(result)));
    }

    heap_.clear();
    return next_->Finish();
}

//===----------------------------------------------------------------------===//
// Build
//===----------------------------------------------------------------------===//

Result<std::unique_ptr<Operator>> BuildTopNTemplate(std::shared_ptr<TopNTemplate> templ,
    duckdb::vector<LogicalType> col_types, memory::Allocator &alloc) {
    duckdb::vector<unique_ptr<duckdb::Expression>> key_exprs;
    std::vector<SortOrder> orders;
    std::vector<NullOrder> null_orders;
    for (auto &k : templ->sort_keys) {
        TRY(auto key, BuildExpression(k.expr));
        key_exprs.push_back(std::move(key));
        orders.push_back(k.order);
        null_orders.push_back(k.null_order);
    }
    return std::unique_ptr<Operator>(std::make_unique<TopNOperator>(std::move(key_exprs), std::move(orders),
        std::move(null_orders), templ->limit, templ->offset, std::move(col_types), alloc));
}

} // namespace plume::exec
