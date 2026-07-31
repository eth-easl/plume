#include "plume/execution/operators/output.hpp"

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <cstdint>
#include <memory>

namespace plume::exec {

using duckdb::DataChunk;
using duckdb::hash_t;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::SelectionVector;
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

std::vector<uint32_t> RoutePartitionIds(DataChunk &chunk, const std::vector<uint32_t> &key_columns, 
                                        uint32_t n, idx_t count) {
    std::vector<uint32_t> pid(count, 0);
    if (n <= 1 || key_columns.empty()) {
        return pid;
    }
    Vector hashes(LogicalType::HASH);
    VectorOperations::Hash(chunk.data[key_columns[0]], hashes, count);
    for (size_t k = 1; k < key_columns.size(); k++) {
        VectorOperations::CombineHash(hashes, chunk.data[key_columns[k]], count);
    }
    hashes.Flatten(count);
    auto *hd = duckdb::FlatVector::GetData<hash_t>(hashes);
    for (idx_t r = 0; r < count; r++) {
        pid[r] = static_cast<uint32_t>(hd[r] % n);
    }
    return pid;
}

} // namespace

//===----------------------------------------------------------------------===//
// SingleOutputOperator
//===----------------------------------------------------------------------===//

Result<void> SingleOutputOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    if (!chunk || chunk->size() == 0) {
        return Ok();
    }
    std::shared_ptr<DataChunk> sc = std::move(chunk);
    const idx_t count = sc->size();
    return sink_->Accept(0, std::move(sc), nullptr, count);
}

//===----------------------------------------------------------------------===//
// SplitOutputOperator
//===----------------------------------------------------------------------===//

SplitOutputOperator::SplitOutputOperator(duckdb::vector<LogicalType> output_types, std::unique_ptr<PartitionSink> sink,
                                         std::vector<uint32_t> key_columns, uint32_t partitions)
    : OutputOperatorBase(std::move(output_types), std::move(sink)), key_columns_(std::move(key_columns)),
      partitions_(partitions == 0 ? 1 : partitions) {}

Result<void> SplitOutputOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    if (!chunk || chunk->size() == 0) {
        return Ok();
    }

    const idx_t chunk_size = chunk->size();
    std::shared_ptr<DataChunk> shared_chunk = std::move(chunk);
    shared_chunk->Flatten(); // TODO: check if this flatten is needed
    
    auto selection = std::make_shared<SelectionVector>(chunk_size);
    if (partitions_ <= 1 || key_columns_.empty()) {
        for (idx_t i = 0; i < chunk_size; i++) {
            selection->set_index(i, i);
        }
        auto view = std::make_unique<SelectionVector>(selection->data());
        TRYV(sink_->Accept(0, shared_chunk, std::move(view), chunk_size, selection));
    }

    Vector hashes(LogicalType::HASH);
    VectorOperations::Hash(shared_chunk->data[key_columns_[0]], hashes, chunk_size);
    for (size_t k = 1; k < key_columns_.size(); k++) {
        VectorOperations::CombineHash(hashes, shared_chunk->data[key_columns_[k]], chunk_size);
    }
    hashes.Flatten(chunk_size);
    
    std::vector<uint32_t> partition_id(chunk_size, 0);
    std::vector<idx_t> offsets(partitions_, 0);
    auto *hd = duckdb::FlatVector::GetData<hash_t>(hashes);
    for (idx_t i = 0; i < chunk_size; i++) {
        partition_id[i] = static_cast<uint32_t>(hd[i] % partitions_);
        offsets[partition_id[i]]++;
    }
    idx_t acc = 0;
    for (uint32_t i = 0; i < partitions_; i++) {
        const idx_t c = offsets[i];
        offsets[i] = acc;
        acc += c;
    }
    std::vector<idx_t> fill(offsets.begin(), offsets.end());
    for (idx_t r = 0; r < chunk_size; r++) {
        selection->set_index(fill[partition_id[r]]++, r);
    }
    for (uint32_t i = 0; i < partitions_; i++) {
        const idx_t count = fill[i] - offsets[i];
        if (count == 0) {
            continue;
        }
        auto view = std::make_unique<SelectionVector>(selection->data() + offsets[i]);
        TRYV(sink_->Accept(i, shared_chunk, std::move(view), count, selection));
    }
    return Ok();
}

//===----------------------------------------------------------------------===//
// Build
//===----------------------------------------------------------------------===//

std::unique_ptr<Operator> MakeOutputOperator(const Schema &schema, std::vector<uint32_t> key_columns,
                                             uint32_t partitions, std::unique_ptr<PartitionSink> sink) {
    const uint32_t n = partitions == 0 ? 1 : partitions;
    sink->Bind(schema, n);
    auto types = SchemaTypes(schema);
    if (n <= 1) {
        return std::make_unique<SingleOutputOperator>(std::move(types), std::move(sink));
    }
    return std::make_unique<SplitOutputOperator>(std::move(types), std::move(sink), std::move(key_columns), n);
}

} // namespace plume::exec
