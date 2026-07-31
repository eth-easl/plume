#include "plume/execution/sink.hpp"

#include "plume/memory/adapter.hpp"
#include "plume/memory/block.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/string_type.hpp"

#include <memory>
#include <string>

namespace plume::exec {

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::SelectionVector;
using duckdb::UnifiedVectorFormat;

//===----------------------------------------------------------------------===//
// Size estimation
//===----------------------------------------------------------------------===//

SizeFactors ComputeSizeFactors(const Schema &schema) {
    SizeFactors f;
    size_t names = 0;
    for (uint32_t c = 0; c < schema.size(); c++) {
        const auto &col = schema.columns[c];
        names += col.name.size();
        f.per_row_fixed += PhysicalWidth(col.type);
        if (col.nullable) {
            f.num_nullable++;
        }
        if (col.type.id == TypeId::VARCHAR) {
            f.varchar_cols.push_back(c);
        }
    }
    f.base_overhead = sizeof(memory::BlockHeader) + schema.size() * sizeof(memory::ColumnDescriptor) + names;
    return f;
}

size_t EstimateBlockBytes(const SizeFactors &f, size_t rows, size_t heap) {
    return f.base_overhead + rows * f.per_row_fixed + f.num_nullable * ((rows + 7) / 8) + heap;
}

//===----------------------------------------------------------------------===//
// PartitionSink
//===----------------------------------------------------------------------===//

void PartitionSink::Bind(Schema schema, uint32_t partitions) {
    schema_ = std::move(schema);
    partitions_ = partitions == 0 ? 1 : partitions;
    factors_ = ComputeSizeFactors(schema_);
    batches_.resize(partitions_);
    for (uint32_t p = 0; p < partitions_; p++) {
        batches_[p].key = p;
    }
}

size_t PartitionSink::RunHeap(DataChunk &chunk, const SelectionVector *sel, idx_t count) const {
    // Approximate: sums every selected string's size (skipping the inline/null checks a
    // byte-exact heap would do). Overestimates by at most the inlined bytes, so blocks
    // flush a touch early — never past the cap by the estimate's error.
    size_t heap = 0;
    for (uint32_t c : factors_.varchar_cols) {
        UnifiedVectorFormat u;
        chunk.data[c].ToUnifiedFormat(chunk.size(), u);
        auto *strs = UnifiedVectorFormat::GetData<duckdb::string_t>(u);
        for (idx_t i = 0; i < count; i++) {
            const idx_t phys = u.sel->get_index(sel ? sel->get_index(i) : i);
            heap += strs[phys].GetSize();
        }
    }
    return heap;
}

Result<void> PartitionSink::Accept(uint32_t p, std::shared_ptr<DataChunk> chunk,
                                   std::unique_ptr<SelectionVector> sel, idx_t count,
                                   std::shared_ptr<SelectionVector> sel_owner) {
    if (count == 0) {
        return Ok();
    }
    PartitionBatch &b = batches_[p];
    const size_t run_heap = RunHeap(*chunk, sel.get(), count);
    if (!b.parts.empty() && WouldExceed(b, count, run_heap)) {
        TRYV(Flush(b));
    }
    b.parts.push_back({chunk.get(), sel.get(), count});
    b.sels.push_back(std::move(sel));
    b.sel_owners.push_back(std::move(sel_owner));
    b.chunks.push_back(std::move(chunk));
    b.rows += count;
    b.heap += run_heap;
    return Ok();
}

Result<void> PartitionSink::Flush(PartitionBatch &b) {
    if (b.parts.empty()) {
        return Ok();
    }
    TRYV(FlushBatch(b));
    b.parts.clear();
    b.sels.clear();
    b.sel_owners.clear();
    b.chunks.clear();
    b.rows = 0;
    b.heap = 0;
    b.seq++;
    return Ok();
}

Result<void> PartitionSink::FlushAll() {
    for (auto &b : batches_) {
        TRYV(Flush(b));
    }
    return Finalize();
}

//===----------------------------------------------------------------------===//
// BlockSink
//===----------------------------------------------------------------------===//

bool BlockSink::WouldExceed(const PartitionBatch &b, size_t add_rows, size_t add_heap) const {
    return EstimateBlockBytes(factors_, b.rows + add_rows, b.heap + add_heap) > max_bytes_;
}

Result<void> BlockSink::FlushBatch(PartitionBatch &b) {
    TRY(auto buf, memory::ExportSelection(schema_, b.parts));
    emit_(std::to_string(b.seq), 0, std::move(buf), b.key);
    return Ok();
}

} // namespace plume::exec
