#include "plume/util/output_split.hpp"

#include "plume/csv/csv.hpp"
#include "plume/parquet/encoder.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <memory>
#include <string>

namespace plume::util {

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

} // namespace plume::util
