#include "plume/util/output_split.hpp"

#include "plume/csv/csv.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/block.hpp"
#include "plume/parquet/encoder.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

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

// Encodes each partition's rows as complete CSV/Parquet files. Rows are copied into the partition's
// pending chunks as they arrive; once the next run would push the pending rows past the cap (bytes
// they would take as a block for CSV, rows for Parquet), they are encoded and emitted as one file.
// A partition without rows still gets one empty file.
class EncodedSink final : public exec::Sink {
public:
    EncodedSink(exec::OutputEmit emit, exec::OutputConfig config, size_t max_bytes, size_t max_rows)
        : emit_(std::move(emit)), config_(std::move(config)), max_bytes_(max_bytes), max_rows_(max_rows) {}

    void Bind(const Schema &schema, uint32_t partitions) override {
        schema_ = schema;
        types_ = SchemaTypes(schema);
        pending_.clear();
        pending_.resize(partitions);
        row_bytes_ = 0;
        nullable_ = 0;
        varchar_cols_.clear();
        size_t names = 0;
        for (uint32_t c = 0; c < schema.size(); c++) {
            const auto &col = schema.columns[c];
            names += col.name.size();
            row_bytes_ += PhysicalWidth(col.type);
            nullable_ += col.nullable ? 1 : 0;
            if (col.type.id == TypeId::VARCHAR) {
                varchar_cols_.push_back(c);
            }
        }
        block_overhead_ = sizeof(memory::BlockHeader) + schema.size() * sizeof(memory::ColumnDescriptor) + names;
    }

    Result<void> Append(uint32_t partition, const exec::OutputRows &rows) override {
        Pending &part = pending_[partition];
        size_t heap = 0;
        const duckdb::sel_t *sel = rows.sel ? rows.sel->data() : nullptr;
        for (uint32_t c : varchar_cols_) {
            heap += memory::VarcharHeapBytes(rows.formats[c], sel, rows.count);
        }
        if (part.rows > 0 && WouldExceed(part, rows.count, heap)) {
            TRYV(Flush(partition));
        }
        CopyRows(part, rows);
        part.rows += rows.count;
        part.heap += heap;
        return Ok();
    }

    Result<void> Finish() override {
        for (uint32_t p = 0; p < pending_.size(); p++) {
            if (pending_[p].rows > 0) {
                TRYV(Flush(p));
            }
        }
        for (uint32_t p = 0; p < pending_.size(); p++) {
            if (pending_[p].seq == 0) {
                TRYV(Encode(p, 0, {}));
            }
        }
        return Ok();
    }

private:
    struct Pending {
        std::vector<std::unique_ptr<DataChunk>> chunks; // full but the last
        idx_t fill = 0;                                 // rows in the last chunk
        size_t rows = 0;
        size_t heap = 0;  // varchar heap bytes of the rows as a block (memory::VarcharHeapBytes)
        uint64_t seq = 0; // files emitted (ident)
    };

    bool WouldExceed(const Pending &part, size_t add_rows, size_t add_heap) const {
        const size_t rows = part.rows + add_rows;
        if (config_.format == exec::OutputFormat::PARQUET) {
            return rows > max_rows_;
        }
        return block_overhead_ + rows * row_bytes_ + nullable_ * ((rows + 7) / 8) + part.heap + add_heap > max_bytes_;
    }

    void CopyRows(Pending &part, const exec::OutputRows &rows) {
        for (idx_t done = 0; done < rows.count;) {
            if (part.chunks.empty() || part.fill == STANDARD_VECTOR_SIZE) {
                if (!part.chunks.empty()) {
                    part.chunks.back()->SetChildCardinality(part.fill);
                }
                part.chunks.push_back(std::make_unique<DataChunk>());
                part.chunks.back()->Initialize(duckdb::Allocator::DefaultAllocator(), types_);
                part.fill = 0;
            }
            DataChunk &dst = *part.chunks.back();
            const idx_t n = std::min<idx_t>(rows.count - done, STANDARD_VECTOR_SIZE - part.fill);
            for (idx_t c = 0; c < types_.size(); c++) {
                if (rows.sel) {
                    duckdb::VectorOperations::Copy(rows.chunk.data[c], dst.data[c], *rows.sel, done + n, done,
                                                   part.fill);
                } else {
                    duckdb::VectorOperations::Copy(rows.chunk.data[c], dst.data[c], done + n, done, part.fill);
                }
            }
            part.fill += n;
            done += n;
        }
    }

    Result<void> Flush(uint32_t partition) {
        Pending &part = pending_[partition];
        part.chunks.back()->SetChildCardinality(part.fill);
        std::vector<DataChunk *> chunks;
        chunks.reserve(part.chunks.size());
        for (auto &chunk : part.chunks) {
            chunks.push_back(chunk.get());
        }
        TRYV(Encode(partition, part.seq++, chunks));
        part.chunks.clear();
        part.fill = 0;
        part.rows = 0;
        part.heap = 0;
        return Ok();
    }

    Result<void> Encode(uint32_t partition, uint64_t seq, const std::vector<DataChunk *> &chunks) {
        const bool csv = config_.format == exec::OutputFormat::CSV;
        DataBuffer buf;
        if (csv) {
            TRY(buf, csv::WriteCSV(schema_, chunks, config_.csv));
        } else {
            TRY(buf, parquet::WriteParquet(schema_, chunks, config_.parquet_codec));
        }
        const char *ext = csv ? ".csv" : ".parquet";
        emit_("part-" + std::to_string(partition) + "-" + std::to_string(seq) + ext, 0, std::move(buf), partition);
        return Ok();
    }

    exec::OutputEmit emit_;
    exec::OutputConfig config_;
    size_t max_bytes_;
    size_t max_rows_;

    Schema schema_;
    duckdb::vector<LogicalType> types_;
    std::vector<Pending> pending_; // per partition
    
    // Block size estimate of the pending rows (the CSV cap).
    size_t block_overhead_ = 0; // header + descriptors + names
    size_t row_bytes_ = 0;      // sum of physical column widths
    size_t nullable_ = 0;       // columns carrying a validity mask
    std::vector<uint32_t> varchar_cols_;
};

} // namespace

std::unique_ptr<exec::Sink> MakeSink(const exec::OutputConfig &config, exec::OutputEmit emit) {
    if (config.format == exec::OutputFormat::PLUME_BLOCKS) {
        return std::make_unique<exec::BlockSink>(std::move(emit), kSplitMaxBytes);
    }
    return std::make_unique<EncodedSink>(std::move(emit), config, kSplitMaxBytes, kParquetRowGroupRows);
}

} // namespace plume::util
