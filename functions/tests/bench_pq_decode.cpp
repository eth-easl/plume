#include "plume/common/types.hpp"
#include "plume/parquet/decoder.hpp"
#include "plume/parquet/metadata.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"

#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <vector>

using namespace plume;
using duckdb::LogicalType;
using duckdb::UnifiedVectorFormat;
using duckdb::Vector;

namespace {

// Read a whole file into memory (mmap).
struct MappedFile {
    const uint8_t *data = nullptr;
    size_t size = 0;
    int fd = -1;
    explicit MappedFile(const char *path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { return; }
        struct stat st{};
        ::fstat(fd, &st);
        size = static_cast<size_t>(st.st_size);
        data = static_cast<const uint8_t *>(::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
    }
    ~MappedFile() {
        if (data) { ::munmap(const_cast<uint8_t *>(data), size); }
        if (fd >= 0) { ::close(fd); }
    }
};

// Consume a decoded vector the way an aggregate kernel would: through the unified
// format (no forced materialization). Returns a checksum to defeat DCE.
uint64_t Consume(Vector &vec, size_t n, const ColumnType &type) {
    UnifiedVectorFormat uf;
    vec.ToUnifiedFormat(n, uf);
    uint64_t acc = 0;
    if (type.id == TypeId::VARCHAR) {
        auto *strs = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(uf);
        for (size_t i = 0; i < n; i++) {
            auto idx = uf.sel->get_index(i);
            if (!uf.validity.RowIsValid(idx)) { continue; }
            acc += strs[idx].GetSize();
            if (strs[idx].GetSize() > 0) { acc += static_cast<uint8_t>(strs[idx].GetData()[0]); }
        }
    } else {
        const size_t stride = parquet::ColumnPhysicalWidth(type);
        auto *base = uf.data;
        for (size_t i = 0; i < n; i++) {
            auto idx = uf.sel->get_index(i);
            if (!uf.validity.RowIsValid(idx)) { continue; }
            acc += base[idx * stride]; // touch the first byte of each value
        }
    }
    return acc;
}

} // namespace

// Deterministic pseudo-random keep bit at the given selectivity (percent).
inline bool KeepRow(uint64_t global_row, int keep_pct) {
    uint64_t h = global_row * 2654435761u;
    h ^= h >> 15;
    return static_cast<int>(h % 100) < keep_pct;
}

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1] : "/users/tstocker/lineitem.parquet";
    const int reps = argc > 2 ? std::atoi(argv[2]) : 5;
    // Optional: simulate filter pushdown at this selectivity (percent kept). 100 =
    // no filter (decode everything). <100 = late materialization: decode only the
    // kept rows' values, then Slice.
    const int keep_pct = argc > 3 ? std::atoi(argv[3]) : 100;
    // "corr": keep is correlated with storage order (keep the first keep_pct% of each
    // column) so rejected windows are contiguous — exercises the whole-window Skip
    // path. Otherwise keep is uniform pseudo-random (exercises late materialization).
    // argv[4]: "corr" = per-window Skip over a storage-correlated reject suffix;
    // "page" = coalesce the reject suffix into one Skip (page-level skip prototype).
    const std::string mode = argc > 4 ? argv[4] : "";
    const bool page_mode = mode == "page";
    const bool correlated = mode == "corr" || page_mode;

    MappedFile f(path);
    if (!f.data) { std::fprintf(stderr, "cannot open %s\n", path); return 1; }

    // Locate the footer: [ ... metadata ... ][i32 metadata_len]["PAR1"].
    if (f.size < 8) { std::fprintf(stderr, "not a parquet file\n"); return 1; }
    uint32_t meta_len;
    std::memcpy(&meta_len, f.data + f.size - 8, 4);
    const size_t tail_len = static_cast<size_t>(meta_len) + 8;
    const uint8_t *tail = f.data + f.size - tail_len;
    auto meta_res = parquet::ParseFooter(tail, tail_len);
    if (meta_res.is_error()) { std::fprintf(stderr, "footer parse failed\n"); return 1; }
    const parquet::FileMeta meta = std::move(meta_res).unwrap();

    const auto &schema = meta.schema;
    const size_t ncol = schema.columns.size();
    std::fprintf(stderr, "file: %s\n  size=%.1f MB  rows=%lld  row_groups=%zu  cols=%zu\n", path,
                 f.size / 1e6, static_cast<long long>(meta.num_rows), meta.row_groups.size(), ncol);

    double best_ms = 1e18;
    uint64_t checksum = 0;
    for (int rep = 0; rep < reps; rep++) {
        auto t0 = std::chrono::steady_clock::now();
        uint64_t acc = 0;
        // page mode models a storage-correlated filter globally: keep the first
        // global_keep rows across the whole file, so trailing row groups are rejected
        // in full and can be pruned (what pq_prepare's RowGroupMayMatch does).
        const uint64_t global_keep = meta.num_rows * static_cast<uint64_t>(keep_pct) / 100;
        uint64_t g_row = 0;
        for (const auto &rg : meta.row_groups) {
            // Row count of this group (cheap header-only prescan of column 0).
            uint64_t rg_rows = 0;
            if (!rg.columns.empty()) {
                parquet::ColumnChunkReader r0(f.data + rg.columns[0].offset,
                                              static_cast<size_t>(rg.columns[0].size), schema.columns[0].type,
                                              schema.columns[0].nullable, rg.columns[0].codec);
                rg_rows = r0.TotalRows();
            }
            // Row-group pruning: skip the whole group without constructing readers.
            if (page_mode && g_row >= global_keep) {
                g_row += rg_rows;
                continue;
            }
            for (size_t c = 0; c < ncol && c < rg.columns.size(); c++) {
                const auto &col = schema.columns[c];
                const auto &cc = rg.columns[c];
                parquet::ColumnChunkReader r(f.data + cc.offset, static_cast<size_t>(cc.size), col.type,
                                             col.nullable, cc.codec);
                const LogicalType lt = ToLogicalType(col.type);
                const uint64_t col_rows = r.TotalRows();
                // page mode: local boundary from the global keep; other modes: per-rg.
                const uint64_t keep_before =
                    page_mode ? (global_keep > g_row ? std::min<uint64_t>(col_rows, global_keep - g_row) : 0)
                              : col_rows * static_cast<uint64_t>(keep_pct) / 100;
                uint64_t row0 = 0;
                while (!r.Done()) {
                    const size_t n = std::min<size_t>(STANDARD_VECTOR_SIZE, r.RowsRemaining());
                    // Page-skip prototype: a filter correlated with storage order rejects a
                    // contiguous suffix. Coalesce it into ONE big Skip so whole pages are
                    // dropped by header only (no decompression), instead of per-window Skips
                    // that re-enter (and decompress) each large page. This is the ceiling a
                    // page-level pushdown would reach.
                    if (page_mode && row0 >= keep_before) {
                        r.Skip(col_rows - row0);
                        break;
                    }
                    if (keep_pct >= 100 || (page_mode && row0 < keep_before)) {
                        Vector vec(lt);
                        const size_t got = r.ReadInto(vec, n);
                        acc += Consume(vec, got, col.type);
                    } else {
                        // Late materialization at the given selectivity: build the keep
                        // mask + survivor selection, decode kept rows only, Slice, consume.
                        std::vector<uint8_t> keep(n);
                        duckdb::SelectionVector sel(n);
                        size_t sc = 0;
                        for (size_t i = 0; i < n; i++) {
                            const bool k = correlated ? (row0 + i) < keep_before : KeepRow(row0 + i, keep_pct);
                            keep[i] = k ? 1 : 0;
                            if (k) { sel.set_index(sc++, i); }
                        }
                        if (sc == 0) {
                            r.Skip(n);
                        } else {
                            Vector vec(lt);
                            r.ReadInto(vec, n, keep.data());
                            vec.Slice(sel, sc);
                            acc += Consume(vec, sc, col.type);
                        }
                    }
                    row0 += n;
                }
            }
            g_row += rg_rows;
        }
        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        best_ms = std::min(best_ms, ms);
        checksum ^= acc;
        std::fprintf(stderr, "  rep %d: %.1f ms\n", rep, ms);
    }

    const double rows_per_s = meta.num_rows / (best_ms / 1e3);
    std::fprintf(stderr, "best: %.1f ms  (%.1f M rows/s, %.0f MB/s compressed)  checksum=%llu\n", best_ms,
                 rows_per_s / 1e6, f.size / 1e6 / (best_ms / 1e3), static_cast<unsigned long long>(checksum));
    return 0;
}
