#include "plume/functions/output_split.hpp"

#include "plume/csv/csv.hpp"
#include "plume/execution/operators/output.hpp"
#include "plume/parquet/encoder.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <memory>
#include <string>

namespace plume::fn {

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;

namespace {

duckdb::vector<LogicalType> SchemaTypes(const Schema &schema) {
    duckdb::vector<LogicalType> types;
    types.reserve(schema.size());
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }
    return types;
}

// CSV/PARQUET sink: each flushed partition batch is materialized into DataChunks
// (a zero-copy slice of the retained source, flattened once) and encoded into one
// complete host file, emitted via the ABI. Empty partitions get one empty file so
// every partition produces at least one output file.
class EncodedSink : public exec::PartitionSink {
public:
    EncodedSink(exec::OutputEmit emit, exec::OutputSink format, size_t max_bytes, size_t row_target)
        : PartitionSink(std::move(emit)), format_(std::move(format)), max_bytes_(max_bytes), row_target_(row_target) {}

protected:
    bool WouldExceed(const exec::PartitionBatch &b, size_t add_rows, size_t add_heap) const override {
        if (format_.format == exec::OutputFormat::PARQUET) {
            return b.rows + add_rows > row_target_;
        }
        return exec::EstimateBlockBytes(factors_, b.rows + add_rows, b.heap + add_heap) > max_bytes_;
    }

    Result<void> FlushBatch(exec::PartitionBatch &b) override {
        const auto types = SchemaTypes(schema_);
        // Coalesce the partition's runs into a handful of full (STANDARD_VECTOR_SIZE)
        // flat chunks with a single gather pass, rather than one DataChunk per run.
        // Repartitioning fragments each input chunk into up to N tiny runs; a
        // per-run Slice+Flatten made the parquet path scale with the run count
        // (thousands of tiny runs per flush), so the gather is bounded by the row
        // count (== the number of output chunks) instead.
        std::vector<std::unique_ptr<DataChunk>> mats;
        std::vector<DataChunk *> ptrs;
        mats.reserve((b.rows / STANDARD_VECTOR_SIZE) + 1);
        auto &allocator = duckdb::Allocator::DefaultAllocator();

        std::unique_ptr<DataChunk> cur;
        idx_t fill = 0;
        auto new_chunk = [&]() {
            cur = std::make_unique<DataChunk>();
            cur->Initialize(allocator, types);
            fill = 0;
        };
        for (auto &part : b.parts) {
            idx_t done = 0;
            while (done < part.count) {
                if (!cur || fill == STANDARD_VECTOR_SIZE) {
                    if (cur) {
                        cur->SetCardinality(fill);
                        ptrs.push_back(cur.get());
                        mats.push_back(std::move(cur));
                    }
                    new_chunk();
                }
                const idx_t n = std::min<idx_t>(part.count - done, STANDARD_VECTOR_SIZE - fill);
                for (idx_t c = 0; c < types.size(); c++) {
                    if (part.sel) {
                        duckdb::VectorOperations::Copy(part.chunk->data[c], cur->data[c], *part.sel, done + n, done,
                                                       fill);
                    } else {
                        duckdb::VectorOperations::Copy(part.chunk->data[c], cur->data[c], done + n, done, fill);
                    }
                }
                fill += n;
                done += n;
            }
        }
        if (cur && fill > 0) {
            cur->SetCardinality(fill);
            ptrs.push_back(cur.get());
            mats.push_back(std::move(cur));
        }
        return Emit(b.key, b.seq, ptrs);
    }

    Result<void> Finalize() override {
        // Emit one empty file for any partition that never produced output, so each
        // partition yields at least one file.
        for (uint32_t p = 0; p < partitions_; p++) {
            if (batches_[p].seq == 0) {
                std::vector<DataChunk *> empty;
                TRYV(Emit(p, 0, empty));
            }
        }
        return Ok();
    }

private:
    Result<void> Emit(uint32_t key, uint64_t seq, const std::vector<DataChunk *> &ptrs) {
        const bool csv = format_.format == exec::OutputFormat::CSV;
        DataBuffer buf;
        if (csv) {
            TRY(buf, csv::WriteCSV(schema_, ptrs, format_.csv));
        } else {
            TRY(buf, parquet::WriteParquet(schema_, ptrs, format_.parquet_codec));
        }
        const char *ext = csv ? ".csv" : ".parquet";
        emit_("part-" + std::to_string(key) + "-" + std::to_string(seq) + ext, 0, std::move(buf), key);
        return Ok();
    }

    exec::OutputSink format_;
    size_t max_bytes_;
    size_t row_target_;
};

} // namespace

std::unique_ptr<exec::PartitionSink> MakeSink(const exec::OutputSink &sink, exec::OutputEmit emit) {
    if (sink.format == exec::OutputFormat::PLUME_BLOCKS) {
        return std::make_unique<exec::BlockSink>(std::move(emit), kSplitMaxBytes);
    }
    return std::make_unique<EncodedSink>(std::move(emit), sink, kSplitMaxBytes, kParquetRowGroupRows);
}

Result<std::vector<SplitBlock>> SplitOutput(const exec::OutputSplit &split, const Schema &schema,
                                            const exec::ChunkList &chunks, size_t max_bytes) {
    std::vector<SplitBlock> out;
    exec::OutputEmit emit = [&](const std::string &, size_t, DataBuffer buffer, size_t key) {
        out.push_back({static_cast<uint32_t>(key), std::move(buffer)});
    };
    const uint32_t n = split.partitions == 0 ? 1 : split.partitions;
    auto op = exec::MakeOutputOperator(schema, split.key_columns, n,
                                       std::make_unique<exec::BlockSink>(emit, max_bytes));

    // Push a zero-copy reference of each borrowed chunk so `chunks` is not consumed
    // (the operator retains the reference until the partition flushes).
    const auto types = SchemaTypes(schema);
    for (auto &c : chunks) {
        if (!c || c->size() == 0) {
            continue;
        }
        auto ref = std::make_unique<DataChunk>();
        ref->InitializeEmpty(types); // allocate the vector slots Reference() writes into
        ref->Reference(*c);
        TRYV(op->Push(std::move(ref)));
    }
    TRYV(op->Finish());
    return out;
}

} // namespace plume::fn
