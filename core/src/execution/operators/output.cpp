#include "plume/execution/operators/output.hpp"

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <cstdint>
#include <memory>

namespace plume::exec {

using duckdb::DataChunk;
using duckdb::hash_t;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Vector;
using duckdb::VectorOperations;

namespace {

duckdb::vector<LogicalType> SchemaTypes(const Schema &schema) {
    duckdb::vector<LogicalType> types;
    types.reserve(schema.size());
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }
    return types;
}

} // namespace

OutputOperator::OutputOperator(const Schema &schema, std::unique_ptr<Sink> sink,
                               std::vector<uint32_t> key_columns, uint32_t partitions)
    : Operator(SchemaTypes(schema)), sink_(std::move(sink)), key_columns_(std::move(key_columns)),
      partitions_(partitions == 0 ? 1 : partitions), formats_(schema.size()) {
    sink_->Bind(schema, partitions_);
}

Result<void> OutputOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    if (!chunk || chunk->size() == 0) {
        return Ok();
    }

    for (idx_t c = 0; c < formats_.size(); c++) {
        chunk->data[c].ToUnifiedFormat(formats_[c]);
    }

    if (partitions_ <= 1 || key_columns_.empty()) {
        return sink_->Append(0, {*chunk, formats_.data(), nullptr, chunk->size()});
    }
    return Route(*chunk);
}

Result<void> OutputOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    return sink_->Finish();
}

Result<void> OutputOperator::Route(const DataChunk &chunk) {
    const idx_t count = chunk.size();

    // Hash key columns.
    Vector hashes(LogicalType::HASH, count);
    VectorOperations::Hash(chunk.data[key_columns_[0]], hashes, count);
    for (size_t k = 1; k < key_columns_.size(); k++) {
        VectorOperations::CombineHash(hashes, chunk.data[key_columns_[k]], count);
    }
    if (hashes.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
        return sink_->Append(
            PartitionOf(*duckdb::ConstantVector::GetData<hash_t>(hashes), partitions_), 
            {chunk, formats_.data(), nullptr, count}
        );
    }
    hashes.Flatten();
    const auto *hash_data = duckdb::FlatVector::GetData<hash_t>(hashes);

    // Assign partitions and compute prefix-sum offsets to scatter row indices into a per-partition 
    // selection vector.
    partition_ids_.resize(count);
    offsets_.assign(partitions_ + 1, 0);
    for (idx_t i = 0; i < count; i++) {
        const uint32_t p = PartitionOf(hash_data[i], partitions_);
        partition_ids_[i] = p;
        offsets_[p + 1]++;
    }
    if (offsets_[partition_ids_[0] + 1] == count) { // a single partition: no selection needed
        return sink_->Append(partition_ids_[0], {chunk, formats_.data(), nullptr, count});
    }
    for (uint32_t p = 0; p < partitions_; p++) {
        offsets_[p + 1] += offsets_[p];
    }
    fill_.assign(offsets_.begin(), offsets_.end() - 1);
    sel_.resize(count);
    for (idx_t i = 0; i < count; i++) {
        sel_[fill_[partition_ids_[i]]++] = static_cast<duckdb::sel_t>(i);
    }

    for (uint32_t p = 0; p < partitions_; p++) {
        const idx_t n = offsets_[p + 1] - offsets_[p];
        if (n != 0) {
            const duckdb::SelectionVector run(sel_.data() + offsets_[p], n);
            TRYV(sink_->Append(p, {chunk, formats_.data(), &run, n}));
        }
    }
    return Ok();
}

std::unique_ptr<Operator> MakeOutputOperator(const Schema &schema, std::vector<uint32_t> key_columns,
                                             uint32_t partitions, std::unique_ptr<Sink> sink) {
    return std::make_unique<OutputOperator>(schema, std::move(sink), std::move(key_columns), partitions);
}

} // namespace plume::exec
