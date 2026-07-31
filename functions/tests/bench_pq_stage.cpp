#include "plume/common/buffer.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/operator.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/output_split.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/parquet/decoder.hpp"
#include "plume/parquet/metadata.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace plume;
using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;

namespace {

struct MappedFile {
    const uint8_t *data = nullptr;
    size_t size = 0;
    int fd = -1;
    explicit MappedFile(const char *path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) {
            return;
        }
        struct stat st {};
        ::fstat(fd, &st);
        size = static_cast<size_t>(st.st_size);
        data = static_cast<const uint8_t *>(::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
    }
    ~MappedFile() {
        if (data) {
            ::munmap(const_cast<uint8_t *>(data), size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

duckdb::vector<LogicalType> SchemaTypes(const Schema &schema) {
    duckdb::vector<LogicalType> types;
    for (auto &c : schema.columns) {
        types.push_back(ToLogicalType(c.type));
    }
    return types;
}

double MsSince(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
}

// ---- OLD split algorithm (pre-change), for the A/B baseline -----------------
// Per-cell GetValue/SetValue into per-partition accumulator chunks, ExportChunks
// once a partition reaches the target, then memcpy the block into a DataBuffer.
struct OldBuilder {
    const duckdb::vector<LogicalType> *types = nullptr;
    const Schema *schema = nullptr;
    memory::Allocator *alloc = nullptr;
    size_t target = fn::kSplitMaxBytes;
    size_t base_overhead = 0, per_row_fixed = 0, num_nullable = 0;

    std::vector<std::unique_ptr<DataChunk>> pending;
    idx_t cur = 0;
    size_t pending_rows = 0, pending_heap = 0;
    size_t out_bytes = 0, out_blocks = 0;

    size_t Estimate() const {
        return base_overhead + pending_rows * per_row_fixed + num_nullable * ((pending_rows + 7) / 8) + pending_heap;
    }
    void Append(DataChunk &src, idx_t row) {
        if (pending.empty() || cur == STANDARD_VECTOR_SIZE) {
            auto c = std::make_unique<DataChunk>();
            c->Initialize(alloc->Get(), *types);
            pending.push_back(std::move(c));
            cur = 0;
        }
        auto &dst = *pending.back();
        for (idx_t col = 0; col < dst.ColumnCount(); col++) {
            duckdb::Value v = src.GetValue(col, row);
            if (!v.IsNull() && schema->columns[col].type.id == TypeId::VARCHAR) {
                const auto &s = duckdb::StringValue::Get(v);
                if (s.size() > duckdb::string_t::INLINE_LENGTH) {
                    pending_heap += s.size();
                }
            }
            dst.SetValue(col, cur, std::move(v));
        }
        dst.SetCardinality(++cur);
        pending_rows++;
        if (Estimate() >= target) {
            Flush();
        }
    }
    void Flush() {
        if (pending.empty()) {
            return;
        }
        std::vector<DataChunk *> ptrs;
        for (auto &c : pending) {
            if (c->size() > 0) {
                ptrs.push_back(c.get());
            }
        }
        if (!ptrs.empty()) {
            auto block = memory::ExportChunks(*schema, ptrs, *alloc).unwrap();
            DataBuffer buf(block.size); // malloc + memcpy (the old ToBuffer copy)
            std::memcpy(buf.mutable_data(), block.data, block.size);
            out_bytes += block.size;
            out_blocks++;
            alloc->Free(block.data, block.size);
        }
        pending.clear();
        cur = 0;
        pending_rows = 0;
        pending_heap = 0;
    }
};

void SplitOld(const exec::OutputSplit &split, const Schema &schema, const exec::ChunkList &chunks,
              memory::Allocator &alloc, size_t &out_bytes, size_t &out_blocks) {
    const uint32_t n = split.partitions == 0 ? 1 : split.partitions;
    const auto types = SchemaTypes(schema);
    size_t base = sizeof(memory::BlockHeader) + schema.size() * sizeof(memory::ColumnDescriptor);
    size_t per_row = 0, nullable = 0;
    for (auto &c : schema.columns) {
        base += c.name.size();
        per_row += PhysicalWidth(c.type);
        if (c.nullable) {
            nullable++;
        }
    }
    std::vector<OldBuilder> b(n);
    for (uint32_t p = 0; p < n; p++) {
        b[p] = {&types, &schema, &alloc, fn::kSplitMaxBytes, base, per_row, nullable, {}, 0, 0, 0, 0, 0};
    }
    for (auto &cptr : chunks) {
        if (!cptr) {
            continue;
        }
        DataChunk &chunk = *cptr;
        for (idx_t r = 0; r < chunk.size(); r++) {
            uint64_t h = 0;
            for (uint32_t kc : split.key_columns) {
                h = h * 31 + chunk.GetValue(kc, r).Hash();
            }
            b[h % n].Append(chunk, r);
        }
    }
    out_bytes = 0;
    out_blocks = 0;
    for (uint32_t p = 0; p < n; p++) {
        b[p].Flush();
        out_bytes += b[p].out_bytes;
        out_blocks += b[p].out_blocks;
    }
}

} // namespace

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1] : "/users/tstocker/data/tpch10/lineitem/lineitem.1.parquet";
    const int reps = argc > 2 ? std::atoi(argv[2]) : 5;
    const uint64_t max_rows = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 5'000'000;
    const uint32_t partitions = argc > 4 ? static_cast<uint32_t>(std::atoi(argv[4])) : 8;
    const uint32_t key_col = argc > 5 ? static_cast<uint32_t>(std::atoi(argv[5])) : 0;

    MappedFile f(path);
    if (!f.data) {
        std::fprintf(stderr, "cannot open %s\n", path);
        return 1;
    }
    if (f.size < 8) {
        std::fprintf(stderr, "not a parquet file\n");
        return 1;
    }
    uint32_t meta_len;
    std::memcpy(&meta_len, f.data + f.size - 8, 4);
    const size_t tail_len = static_cast<size_t>(meta_len) + 8;
    auto meta_res = parquet::ParseFooter(f.data + f.size - tail_len, tail_len);
    if (meta_res.is_error()) {
        std::fprintf(stderr, "footer parse failed\n");
        return 1;
    }
    const parquet::FileMeta meta = std::move(meta_res).unwrap();
    const Schema &schema = meta.schema;
    const size_t ncol = schema.columns.size();
    const auto types = SchemaTypes(schema);

    // Decode into an operator-chain ChunkList (all columns, up to max_rows).
    memory::Allocator alloc;
    alloc.InstallAsDuckDBDefault();
    exec::ChunkList chunks;
    uint64_t rows = 0;
    auto t_dec = std::chrono::steady_clock::now();
    for (const auto &rg : meta.row_groups) {
        if (rows >= max_rows) {
            break;
        }
        std::vector<parquet::ColumnChunkReader> readers;
        readers.reserve(ncol);
        for (size_t c = 0; c < ncol && c < rg.columns.size(); c++) {
            readers.emplace_back(f.data + rg.columns[c].offset, static_cast<size_t>(rg.columns[c].size),
                                 schema.columns[c].type, schema.columns[c].nullable, rg.columns[c].codec);
        }
        if (readers.size() < ncol) {
            break;
        }
        while (!readers[0].Done() && rows < max_rows) {
            const size_t n = std::min<size_t>(STANDARD_VECTOR_SIZE, readers[0].RowsRemaining());
            auto chunk = std::make_unique<DataChunk>();
            chunk->Initialize(alloc.Get(), types);
            for (size_t c = 0; c < ncol; c++) {
                readers[c].ReadInto(chunk->data[c], n);
            }
            chunk->SetCardinality(n);
            rows += n;
            chunks.push_back(std::move(chunk));
        }
    }
    const double dec_ms = MsSince(t_dec);

    exec::OutputSplit split;
    split.key_columns = {key_col};
    split.partitions = partitions;

    std::fprintf(stderr, "file: %s\n  decoded %llu rows, %zu cols, %zu chunks in %.0f ms\n  split: %u partitions, key col %u (%s)\n",
                 path, (unsigned long long)rows, ncol, chunks.size(), dec_ms, partitions, key_col,
                 schema.columns[key_col].name.c_str());

    // NEW split.
    double new_best = 1e18;
    size_t new_bytes = 0, new_blocks = 0;
    for (int rep = 0; rep < reps; rep++) {
        auto t0 = std::chrono::steady_clock::now();
        auto parts = fn::SplitOutput(split, schema, chunks).unwrap();
        double ms = MsSince(t0);
        new_best = std::min(new_best, ms);
        new_bytes = 0;
        new_blocks = parts.size();
        for (auto &sb : parts) {
            new_bytes += sb.buffer.size();
        }
    }

    // OLD split.
    double old_best = 1e18;
    size_t old_bytes = 0, old_blocks = 0;
    for (int rep = 0; rep < reps; rep++) {
        auto t0 = std::chrono::steady_clock::now();
        size_t ob = 0, obl = 0;
        SplitOld(split, schema, chunks, alloc, ob, obl);
        double ms = MsSince(t0);
        old_best = std::min(old_best, ms);
        old_bytes = ob;
        old_blocks = obl;
    }

    auto mrows = [&](double ms) { return rows / (ms / 1e3) / 1e6; };
    std::fprintf(stderr, "\n  OLD: %8.1f ms  (%.1f M rows/s)  %zu blocks, %.1f MB out\n", old_best, mrows(old_best),
                 old_blocks, old_bytes / 1e6);
    std::fprintf(stderr, "  NEW: %8.1f ms  (%.1f M rows/s)  %zu blocks, %.1f MB out\n", new_best, mrows(new_best),
                 new_blocks, new_bytes / 1e6);
    std::fprintf(stderr, "  speedup: %.2fx\n", old_best / new_best);
    return 0;
}
