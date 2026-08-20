#include "plume/execution/operators/sort.hpp"

#include "plume/expression/expression_builder.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/vector.hpp"

#include <algorithm>
#include <numeric>

namespace plume::exec {

using expr::BuildExpression;

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::unique_ptr;
using duckdb::Value;
using duckdb::Vector;

//===----------------------------------------------------------------------===//
// SortKey
//===----------------------------------------------------------------------===//

bool SortKey::Equals(const SortKey &other) const {
    return order == other.order && null_order == other.null_order && expr.Equals(other.expr);
}

void SortKey::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "expr", expr);
    s.WriteProperty(101, "order", static_cast<uint8_t>(order));
    s.WriteProperty(102, "null_order", static_cast<uint8_t>(null_order));
}

SortKey SortKey::Deserialize(duckdb::Deserializer &d) {
    SortKey k;
    k.expr = d.ReadProperty<expr::ExprNode>(100, "expr");
    k.order = static_cast<SortOrder>(d.ReadProperty<uint8_t>(101, "order"));
    k.null_order = static_cast<NullOrder>(d.ReadProperty<uint8_t>(102, "null_order"));
    return k;
}

//===----------------------------------------------------------------------===//
// SortTemplate
//===----------------------------------------------------------------------===//

bool SortTemplate::Equals(const OperatorTemplate &other) const {
    if (other.type != type) return false;
    auto &o = static_cast<const SortTemplate &>(other);
    if (sort_keys.size() != o.sort_keys.size()) return false;
    for (size_t i = 0; i < sort_keys.size(); i++) {
        if (!sort_keys[i].Equals(o.sort_keys[i])) return false;
    }
    return true;
}

void SortTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteList(101, "sort_keys", sort_keys.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteElement(sort_keys[i]);
    });
}

//===----------------------------------------------------------------------===//
// SortOperator
//===----------------------------------------------------------------------===//

namespace {

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

SortOperator::SortOperator(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> key_exprs,
        std::vector<SortOrder> orders, std::vector<NullOrder> null_orders, 
        duckdb::vector<LogicalType> output_types, memory::Allocator &alloc)
    : Operator(std::move(output_types)), key_exprs_(std::move(key_exprs)), orders_(std::move(orders)), 
      null_orders_(std::move(null_orders)), alloc_(alloc) {
    key_executor_ = std::make_unique<duckdb::ExpressionExecutor>();
    for (auto &e : key_exprs_) {
        key_executor_->AddExpression(*e);
        key_types_.push_back(e->return_type);
    }
}

Result<void> SortOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    if (chunk->size() > 0) {
        buffered_.push_back(std::move(chunk));
    }
    return Ok();
}

Result<void> SortOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    const idx_t n_keys = key_exprs_.size();

    // Materialize sort keys per row and a reference back to the source row.
    struct RowRef {
        DataChunk *chunk;
        idx_t row;
    };
    std::vector<RowRef> rows;
    std::vector<std::vector<Value>> keys; // [row][key]

    DataChunk key_chunk;
    key_chunk.Initialize(alloc_.Get(), key_types_);
    for (auto &chunk : buffered_) {
        const idx_t n = chunk->size();
        if (n == 0) {
            continue;
        }
        key_chunk.Reset();
        key_executor_->Execute(*chunk, key_chunk);
        key_chunk.Flatten();
        for (idx_t r = 0; r < n; r++) {
            std::vector<Value> k;
            k.reserve(n_keys);
            for (idx_t c = 0; c < n_keys; c++) {
                k.push_back(key_chunk.GetValue(c, r));
            }
            keys.push_back(std::move(k));
            rows.push_back({chunk.get(), r});
        }
    }

    const idx_t total = rows.size();
    std::vector<idx_t> perm(total);
    std::iota(perm.begin(), perm.end(), idx_t(0));

    std::stable_sort(perm.begin(), perm.end(), [&](idx_t i, idx_t j) {
        for (idx_t c = 0; c < n_keys; c++) {
            int cmp = CompareCell(keys[i][c], keys[j][c], orders_[c], null_orders_[c]);
            if (cmp != 0) {
                return cmp < 0;
            }
        }
        return false; // equal -> stable_sort preserves input order
    });

    const idx_t n_cols = output_types_.size();
    for (idx_t base = 0; base < total; base += STANDARD_VECTOR_SIZE) {
        const idx_t batch = std::min<idx_t>(STANDARD_VECTOR_SIZE, total - base);
        auto result = std::make_unique<DataChunk>();
        result->Initialize(alloc_.Get(), output_types_);
        for (idx_t i = 0; i < batch; i++) {
            const RowRef &ref = rows[perm[base + i]];
            for (idx_t c = 0; c < n_cols; c++) {
                result->SetValue(c, i, ref.chunk->GetValue(c, ref.row));
            }
        }
        result->SetCardinality(batch);
        TRYV(next_->Push(std::move(result)));
    }

    buffered_.clear(); // drop the source chunks
    return next_->Finish();
}

//===----------------------------------------------------------------------===//
// Build
//===----------------------------------------------------------------------===//

Result<std::unique_ptr<Operator>> BuildSortTemplate(std::shared_ptr<SortTemplate> templ,
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
    return std::unique_ptr<Operator>(std::make_unique<SortOperator>(
        std::move(key_exprs), std::move(orders), std::move(null_orders), std::move(col_types), alloc));
}

} // namespace plume::exec
