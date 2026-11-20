#pragma once

#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/arena_allocator.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace plume::exec {

// Hash table over composite keys of fixed-width or VARCHAR columns.
// The caller owns the payload. Strings longer than string_t's inline size are copied once into an 
// arena. 
// Lookup is vectorized: keys are first hashed column by column, then probed against a table of 
// (16-bit salt | row pointer) entries, and salt-matching candidates are compred column by column.
// Row layout: [8B hash | NULL flag per key | key0 | key1 | ... | payload] where keys and payload 
// are min(type size, 8B) aligned
class GroupTable {
public:
    GroupTable(const duckdb::vector<duckdb::LogicalType> &key_types, duckdb::idx_t payload_size,
               memory::Allocator &alloc);
    ~GroupTable();

    GroupTable(const GroupTable &) = delete;
    GroupTable &operator=(const GroupTable &) = delete;

    // Whether keys of this type are supported (fixed-width physical types and VARCHAR).
    static bool SupportsType(const duckdb::LogicalType &type);

    // Hashes the first `count` rows of `keys` into `hashes` (a HASH vector).
    void Hash(duckdb::DataChunk &keys, duckdb::Vector &hashes, duckdb::idx_t count);

    // For each of the first `count` rows of `keys`, stores the payload of its group in `payloads`,
    // creating the group if it is new. `new_groups` receives the rows that created a group (one row
    // per new key); returns their count. The new groups' payloads are left uninitialized.
    duckdb::idx_t FindOrCreate(duckdb::DataChunk &keys, duckdb::Vector &hashes, duckdb::idx_t count,
                               duckdb::data_ptr_t *payloads, duckdb::SelectionVector &new_groups);

    // Like FindOrCreate without creating: `payloads[i]` is null for a key the table does not hold.
    void Find(duckdb::DataChunk &keys, duckdb::Vector &hashes, duckdb::idx_t count,
              duckdb::data_ptr_t *payloads);

    // Number of groups, in creation order.
    duckdb::idx_t Count() const { return rows_.size(); }

    duckdb::data_ptr_t Payload(duckdb::idx_t group) const { return rows_[group] + payload_offset_; }

    // Writes the keys of groups [offset, offset + count) into `out`'s first key columns (rows
    // 0..count-1) and their payloads into `payloads`. VARCHAR results reference the table's string
    // arena, which the output vectors keep alive.
    void ScanKeys(duckdb::idx_t offset, duckdb::idx_t count, duckdb::DataChunk &out,
                  duckdb::data_ptr_t *payloads) const;

private:
    struct KeyColumn;
    struct StringArena;

    using MatchFn = duckdb::idx_t (*)(const KeyColumn &col, const duckdb::UnifiedVectorFormat &format,
                                      duckdb::idx_t count, duckdb::idx_t *rows, duckdb::data_ptr_t *group_rows,
                                      duckdb::idx_t *failed, duckdb::idx_t &failed_count);
    template <class T>
    static duckdb::idx_t MatchColumn(const KeyColumn &col, const duckdb::UnifiedVectorFormat &format,
                                     duckdb::idx_t count, duckdb::idx_t *rows, duckdb::data_ptr_t *group_rows,
                                     duckdb::idx_t *failed, duckdb::idx_t &failed_count);
    static MatchFn GetMatchFunction(duckdb::PhysicalType type);

    void Grow(duckdb::idx_t needed);
    void InsertEntry(uint64_t entry, uint64_t hash);
    duckdb::data_ptr_t CreateRow(uint64_t hash, duckdb::idx_t row);
    template <bool CREATE>
    duckdb::idx_t Probe(duckdb::DataChunk &keys, duckdb::Vector &hashes, duckdb::idx_t count,
                        duckdb::data_ptr_t *payloads, duckdb::SelectionVector *new_groups);

    std::vector<KeyColumn> columns_;
    duckdb::idx_t row_width_ = 0;
    duckdb::idx_t payload_offset_ = 0;

    memory::Allocator alloc_;
    duckdb::ArenaAllocator row_arena_;
    std::shared_ptr<StringArena> strings_; // referenced by ScanKeys' VARCHAR output

    std::vector<uint64_t> slots_; // 0 = empty, else (salt << 48) | row pointer
    uint64_t mask_ = 0;
    std::vector<duckdb::data_ptr_t> rows_; // creation order

    // Per-call scratch (sized to STANDARD_VECTOR_SIZE).
    std::vector<duckdb::UnifiedVectorFormat> formats_;
    std::vector<uint64_t> positions_;
    std::vector<duckdb::idx_t> remaining_, next_remaining_, candidates_, failed_;
    std::vector<duckdb::data_ptr_t> candidate_rows_;
};

} // namespace plume::exec
