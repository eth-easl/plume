#pragma once

#include "plume/common/result.hpp"
#include "plume/common/types.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace duckdb {
class Vector;
} // namespace duckdb

namespace plume::parquet {

// Allocator that default-initializes (leaves bytes untouched) instead of
// value-initializing on resize. The decoder always fully overwrites the bytes it
// grows into (decompressed pages, scattered fixed-width values), so the zero-fill
// std::vector would otherwise do is pure wasted memory traffic.
template <typename T, typename A = std::allocator<T>>
class default_init_allocator : public A {
    using traits = std::allocator_traits<A>;

public:
    template <typename U>
    struct rebind {
        using other = default_init_allocator<U, typename traits::template rebind_alloc<U>>;
    };

    using A::A;

    template <typename U>
    void construct(U *ptr) noexcept(std::is_nothrow_default_constructible<U>::value) {
        ::new (static_cast<void *>(ptr)) U; // default-init: no zero-fill for trivial types
    }
    template <typename U, typename... Args>
    void construct(U *ptr, Args &&...args) {
        traits::construct(static_cast<A &>(*this), ptr, std::forward<Args>(args)...);
    }
};

// Byte buffer whose grow-into bytes are left uninitialized (see above).
using ByteVec = std::vector<uint8_t, default_init_allocator<uint8_t>>;

// Returns the DuckDB physical byte width for a column type.
// VARCHAR and unsupported types return 0.
// DECIMAL width 1-4 -> 2 (int16_t), 5-9 -> 4 (int32_t), 10-18 -> 8 (int64_t).
size_t ColumnPhysicalWidth(const ColumnType &type);

// A borrowed view of one VARCHAR value. `ptr` points into either the source
// column-chunk buffer (uncompressed pages) or a decompressed buffer owned by the
// DecodedColumn's `str_backing` — both stay alive for the DecodedColumn's
// lifetime, so no per-value heap allocation or copy is needed while decoding.
struct StrView {
    const char *ptr = nullptr;
    uint32_t len = 0;
};

// Decoded column: holds count rows in the DuckDB physical layout.
//
// Fixed-width types (all except VARCHAR):
//   raw.size() == count * ColumnPhysicalWidth(type)
//   raw[i*stride..(i+1)*stride) = DuckDB-native bytes for row i
//
// VARCHAR:
//   str_views.size() == count, one borrowed (ptr,len) per row pointing into the
//   source buffer or a buffer kept alive in `str_backing`. No per-row copy.
//
// valid:
//   empty  => all rows non-null
//   otherwise: valid[i] == 1 iff row i is non-null
struct DecodedColumn {
    size_t count = 0;
    ByteVec raw;
    std::vector<StrView> str_views;
    std::vector<ByteVec> str_backing; // owns decompressed bytes str_views may point into
    std::vector<uint8_t> valid;

    size_t size() const { return count; }
    bool IsNull(size_t i) const { return !valid.empty() && !valid[i]; }

    std::string_view Str(size_t i) const { return {str_views[i].ptr, str_views[i].len}; }

    template <class T>
    T Get(size_t i) const {
        T v;
        std::memcpy(&v, raw.data() + i * sizeof(T), sizeof(T));
        return v;
    }
};

// Decode one column chunk into a DecodedColumn. `codec` is the parquet
// CompressionCodec (0=UNCOMPRESSED, 1=SNAPPY). Returns an Error on
// unsupported encodings/types/codecs.
//
// This is the reference (unit-tested) path: it materializes the whole column.
// The streaming production path is ColumnChunkReader below, which decodes
// page-by-page straight into a DataChunk vector without the intermediate
// whole-column buffer. Both share the same low-level decode primitives.
Result<DecodedColumn> DecodeColumnChunk(const uint8_t *data, size_t size, const ColumnType &type,
                                        bool nullable, int codec = 0);

// Streaming column-chunk cursor: decodes one column chunk page-by-page directly
// into a caller-provided DataChunk vector, STANDARD_VECTOR_SIZE rows at a time.
//
// Unlike DecodeColumnChunk this never builds a whole-column buffer — the decode
// of each page writes its values straight into the target FlatVector, so the
// just-decoded page stays hot in cache while the operators consume it (the
// DuckDB ColumnReader model). Fixed-width non-null values are decoded directly
// into the vector; VARCHAR values are copied into the vector's string heap
// (StringVector::AddString) so no page buffer needs to outlive a chunk.
//
// The cursor borrows `data` (the column-chunk bytes); the caller keeps it alive
// for the reader's lifetime. Methods throw on malformed input (wrapped by the
// caller's TryCatch/host boundary, matching DecodeColumnChunk).
class ColumnChunkReader {
public:
    ColumnChunkReader(const uint8_t *data, size_t size, const ColumnType &type, bool nullable, int codec = 0);
    ~ColumnChunkReader(); // out-of-line: dict_vec_ holds an incomplete duckdb::Vector
    // Movable (held in a std::vector); the user-declared dtor otherwise suppresses
    // the implicit move. Non-copyable via the unique_ptr member.
    ColumnChunkReader(ColumnChunkReader &&) noexcept;
    ColumnChunkReader &operator=(ColumnChunkReader &&) noexcept;

    // Total rows in the column chunk (sum of every data page's num_values),
    // determined by a header-only prescan in the constructor.
    size_t TotalRows() const { return total_rows_; }
    size_t RowsRemaining() const { return total_rows_ - emitted_; }
    bool Done() const { return emitted_ >= total_rows_; }

    // Emit up to `n` rows into `vec` at positions [0, returned). `vec` must be a
    // freshly initialized flat vector of the reader's type (validity all-valid);
    // nulls are marked via the validity mask. Returns the number of rows written
    // (< n only when the chunk is exhausted).
    //
    // `keep` (optional, length `n`) drives late materialization for filter pushdown:
    // when non-null, only rows with keep[i] != 0 have their value materialized; the
    // rest are advanced over (parquet is sequential) but their expensive
    // materialization (PLAIN VARCHAR string copy) is skipped, leaving a default at
    // that slot. The caller must then Slice `vec` down to the kept rows — the skipped
    // slots are never read. Cheap-to-materialize columns (dictionary, bulk fixed-
    // width) ignore `keep` and decode fully.
    size_t ReadInto(duckdb::Vector &vec, size_t n, const uint8_t *keep = nullptr);

    // Advance `n` rows without materializing any values (filter pushdown: a window
    // the predicate rejected entirely). Only walks page headers, levels, and (for
    // PLAIN VARCHAR) length prefixes.
    void Skip(size_t n);

private:
    bool EnterNextDataPage();
    void SkipInPage(size_t m); // advance value cursors over m rows, no materialization
    // Dictionary-vector fast path: build the shared dictionary vector once (lazily),
    // then emit a whole window as a DICTIONARY_VECTOR referencing it — no per-row
    // gather / string copy. Falls through to EmitFromPage when a window straddles
    // pages. See the DuckDB DictionaryDecoder::Read model.
    void EnsureDictVector(const duckdb::Vector &like);
    void EmitDictVector(duckdb::Vector &vec, size_t m);
    void SetupDictionaryPage(const uint8_t *page, size_t comp_size, size_t uncomp_size, size_t num_values);
    void SetupDataPage(const uint8_t *page, size_t comp_size, size_t uncomp_size, size_t num_values, bool is_v2,
                       size_t def_levels_len, size_t rep_levels_len, bool page_compressed, int encoding);
    void EmitFromPage(duckdb::Vector &vec, size_t dst_off, size_t m, const uint8_t *keep);
    void EmitFixedNonNull(uint8_t *dst, size_t k);
    StrView NextVarcharView();

    const uint8_t *data_;
    size_t size_;
    ColumnType type_;
    bool nullable_;
    int codec_;
    size_t stride_;
    bool is_varchar_;
    bool bulk_; // parquet PLAIN layout == DuckDB physical layout (bulk memcpy)

    size_t pos_ = 0;        // byte cursor: start of the next page header in `data_`
    size_t total_rows_ = 0; // from the header prescan
    size_t emitted_ = 0;    // rows emitted so far

    // Dictionary — persists across the column chunk's data pages.
    ByteVec dict_raw_;
    std::vector<StrView> dict_views_;
    std::vector<ByteVec> dict_backing_; // owns decompressed dict bytes views point into
    size_t dict_size_ = 0;
    bool has_dict_ = false; // a dictionary page was seen for this chunk

    // Shared dictionary vector for the fast path, built lazily from dict_raw_/
    // dict_views_. Holds dict_size_ values plus a trailing invalid null sentinel at
    // index dict_size_, which null rows select. Referenced (zero-copy) by emitted
    // dictionary vectors, so it must outlive every chunk that references it — it
    // does, as the reader outlives pq_stage's per-window Executor::Push calls.
    std::unique_ptr<duckdb::Vector> dict_vec_;
    bool dict_vec_built_ = false;

    // Current data page state.
    ByteVec page_owned_;             // decompressed page bytes (PLAIN values borrow this)
    std::vector<uint64_t> page_def_; // definition levels; empty ⇒ no nulls in page
    bool page_has_nulls_ = false;
    int page_encoding_ = 0;
    const uint8_t *page_vals_ = nullptr; // start of the value region (borrows page_owned_ or data_)
    size_t page_vals_size_ = 0;
    size_t page_rows_ = 0;              // total rows in the page (incl. nulls)
    size_t page_non_null_ = 0;          // non-null values in the page
    size_t page_cursor_ = 0;            // rows emitted from the current page
    size_t page_val_cursor_ = 0;        // non-null values consumed (index into PLAIN stream / indices)
    size_t page_plain_pos_ = 0;         // byte cursor into page_vals_ (PLAIN VARCHAR sequential read)
    std::vector<uint32_t> page_indices_; // whole-page dictionary indices (dict encodings only)

    ByteVec scatter_tmp_; // reused per-chunk scratch for the nullable fixed-width scatter
};

} // namespace plume::parquet
