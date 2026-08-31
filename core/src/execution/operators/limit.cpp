#include "plume/execution/operators/limit.hpp"

#include "duckdb/common/serializer/serializer.hpp"

#include <algorithm>

namespace plume::exec {

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::SelectionVector;

//===----------------------------------------------------------------------===//
// LimitTemplate
//===----------------------------------------------------------------------===//

bool LimitTemplate::Equals(const OperatorTemplate &other) const {
    if (other.type != type) return false;
    auto &o = static_cast<const LimitTemplate &>(other);
    return has_limit == o.has_limit && limit == o.limit && offset == o.offset;
}

void LimitTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(101, "has_limit", has_limit);
    s.WriteProperty(102, "limit", limit);
    s.WriteProperty(103, "offset", offset);
}

//===----------------------------------------------------------------------===//
// LimitOperator
//===----------------------------------------------------------------------===//

LimitOperator::LimitOperator(bool has_limit, uint64_t limit, uint64_t offset,
                             duckdb::vector<LogicalType> output_types)
    : Operator(std::move(output_types)), has_limit_(has_limit), limit_(limit), offset_(offset) {}

Result<void> LimitOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    const idx_t n = chunk->size();
    if (n == 0 || (has_limit_ && emitted_ >= limit_)) {
        return Ok(); // done: drop any further input
    }
    const uint64_t remaining_offset = offset_ > skipped_ ? offset_ - skipped_ : 0;
    const idx_t start = static_cast<idx_t>(std::min<uint64_t>(remaining_offset, n));
    skipped_ += start;

    idx_t available = n - start;
    idx_t to_emit = available;
    if (has_limit_) {
        const uint64_t limit_remaining = limit_ > emitted_ ? limit_ - emitted_ : 0;
        to_emit = static_cast<idx_t>(std::min<uint64_t>(available, limit_remaining));
    }
    if (to_emit == 0) {
        return Ok();
    }
    emitted_ += to_emit;

    if (start != 0 || to_emit != n) {
        chunk->Slice(start, to_emit);
    }
    return next_->Push(std::move(chunk));
}

Result<void> LimitOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    // not a blocking operator -> just forward the signal to the next operator
    return next_->Finish();
}

//===----------------------------------------------------------------------===//
// Build
//===----------------------------------------------------------------------===//

Result<std::unique_ptr<Operator>> BuildLimitTemplate(std::shared_ptr<LimitTemplate> templ,
        duckdb::vector<LogicalType> col_types) {
    return std::unique_ptr<Operator>(
        std::make_unique<LimitOperator>(templ->has_limit, templ->limit, templ->offset, std::move(col_types)));
}

} // namespace plume::exec
