#include "plume/execution/group_table.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <cstring>

namespace plume::exec {

using duckdb::data_ptr_t;
using duckdb::DataChunk;
using duckdb::FlatVector;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::PhysicalType;
using duckdb::SelectionVector;
using duckdb::string_t;
using duckdb::UnifiedVectorFormat;
using duckdb::Vector;

namespace {

constexpr idx_t kHashOffset = 0;                 // the row's full hash
constexpr idx_t kFlagsOffset = sizeof(uint64_t); // one NULL flag byte per key follows
constexpr idx_t kInitialSlots = 1024;
constexpr uint64_t kPointerMask = (uint64_t(1) << 48) - 1;

inline uint64_t Salt(uint64_t hash) {
    return hash >> 48;
}

inline data_ptr_t EntryRow(uint64_t entry) {
    return reinterpret_cast<data_ptr_t>(static_cast<uintptr_t>(entry & kPointerMask));
}

struct StringArenaHolder : duckdb::AuxiliaryDataHolder {
    explicit StringArenaHolder(std::shared_ptr<void> arena_p) : arena(std::move(arena_p)) {}
    std::shared_ptr<void> arena;
};

template <idx_t WIDTH>
void GatherFixed(const data_ptr_t *rows, idx_t count, idx_t offset, data_ptr_t out) {
    for (idx_t i = 0; i < count; i++) {
        std::memcpy(out + i * WIDTH, rows[i] + offset, WIDTH);
    }
}

void Gather(const data_ptr_t *rows, idx_t count, idx_t offset, idx_t width, data_ptr_t out) {
    switch (width) {
    case 1:  return GatherFixed<1>(rows, count, offset, out);
    case 2:  return GatherFixed<2>(rows, count, offset, out);
    case 4:  return GatherFixed<4>(rows, count, offset, out);
    case 8:  return GatherFixed<8>(rows, count, offset, out);
    case 16: return GatherFixed<16>(rows, count, offset, out);
    default:
        for (idx_t i = 0; i < count; i++) {
            std::memcpy(out + i * width, rows[i] + offset, width);
        }
    }
}

} // namespace

// Keeps the string arena alive for as long as an output vector references it.
struct GroupTable::StringArena {
    explicit StringArena(memory::Allocator &alloc_p) : alloc(alloc_p), arena(alloc.Get()) {}
    memory::Allocator alloc; // declared first: the arena allocates from it
    duckdb::ArenaAllocator arena;
};

struct GroupTable::KeyColumn {
    PhysicalType type;
    idx_t width = 0;       // bytes of the field in the row
    idx_t offset = 0;      // field offset in the row
    idx_t null_offset = 0; // NULL flag offset in the row
    MatchFn match = nullptr;
};

template <class T>
idx_t GroupTable::MatchColumn(const KeyColumn &col, const UnifiedVectorFormat &format, idx_t count, idx_t *rows,
                              data_ptr_t *group_rows, idx_t *failed, idx_t &failed_count) {
    const T *data = UnifiedVectorFormat::GetDataUnsafe<T>(format);
    idx_t keep = 0;
    for (idx_t k = 0; k < count; k++) {
        const idx_t r = rows[k];
        const data_ptr_t g = group_rows[k];
        const idx_t idx = format.sel->get_index(r);
        const bool input_valid = format.validity.RowIsValid(idx);
        const bool group_valid = g[col.null_offset] == 0;
        
        bool equal;
        if (input_valid && group_valid) {
            equal = duckdb::Equals::Operation<T>(data[idx], duckdb::Load<T>(g + col.offset));
        } else {
            equal = input_valid == group_valid;
        }
        if (equal) {
            rows[keep] = r;
            group_rows[keep] = g;
            keep++;
        } else {
            failed[failed_count++] = r;
        }
    }
    return keep;
}

GroupTable::MatchFn GroupTable::GetMatchFunction(PhysicalType type) {
    switch (type) {
    case PhysicalType::BOOL:     return MatchColumn<bool>;
    case PhysicalType::INT8:     return MatchColumn<int8_t>;
    case PhysicalType::INT16:    return MatchColumn<int16_t>;
    case PhysicalType::INT32:    return MatchColumn<int32_t>;
    case PhysicalType::INT64:    return MatchColumn<int64_t>;
    case PhysicalType::UINT8:    return MatchColumn<uint8_t>;
    case PhysicalType::UINT16:   return MatchColumn<uint16_t>;
    case PhysicalType::UINT32:   return MatchColumn<uint32_t>;
    case PhysicalType::UINT64:   return MatchColumn<uint64_t>;
    case PhysicalType::INT128:   return MatchColumn<duckdb::hugeint_t>;
    case PhysicalType::UINT128:  return MatchColumn<duckdb::uhugeint_t>;
    case PhysicalType::FLOAT:    return MatchColumn<float>;
    case PhysicalType::DOUBLE:   return MatchColumn<double>;
    case PhysicalType::INTERVAL: return MatchColumn<duckdb::interval_t>;
    case PhysicalType::VARCHAR:  return MatchColumn<string_t>;
    default:                     return nullptr;
    }
}

bool GroupTable::SupportsType(const LogicalType &type) {
    return GetMatchFunction(type.InternalType()) != nullptr;
}

GroupTable::GroupTable(const duckdb::vector<LogicalType> &key_types, idx_t payload_size, memory::Allocator &alloc)
    : alloc_(alloc), row_arena_(alloc_.Get()), strings_(std::make_shared<StringArena>(alloc_)) {
    idx_t offset = kFlagsOffset + key_types.size();
    columns_.resize(key_types.size());
    for (idx_t c = 0; c < key_types.size(); c++) {
        auto &col = columns_[c];
        col.type = key_types[c].InternalType();
        col.match = GetMatchFunction(col.type);
        if (!col.match) {
            throw duckdb::NotImplementedException("GroupTable: unsupported key type %s", key_types[c].ToString());
        }
        col.width = duckdb::GetTypeIdSize(col.type);
        col.null_offset = kFlagsOffset + c;
        offset = duckdb::AlignValue(offset, std::min<idx_t>(col.width, 8));
        col.offset = offset;
        offset += col.width;
    }
    payload_offset_ = duckdb::AlignValue<idx_t>(offset, 8);
    row_width_ = duckdb::AlignValue<idx_t>(payload_offset_ + payload_size, 8);

    formats_.resize(columns_.size());
    positions_.resize(STANDARD_VECTOR_SIZE);
    remaining_.resize(STANDARD_VECTOR_SIZE);
    next_remaining_.resize(STANDARD_VECTOR_SIZE);
    candidates_.resize(STANDARD_VECTOR_SIZE);
    failed_.resize(STANDARD_VECTOR_SIZE);
    candidate_rows_.resize(STANDARD_VECTOR_SIZE);
}

GroupTable::~GroupTable() = default;

void GroupTable::Hash(DataChunk &keys, Vector &hashes, idx_t count) {
    duckdb::VectorOperations::Hash(keys.data[0], hashes, count);
    for (idx_t c = 1; c < columns_.size(); c++) {
        duckdb::VectorOperations::CombineHash(hashes, keys.data[c], count);
    }
    hashes.Flatten();
}

void GroupTable::InsertEntry(uint64_t entry, uint64_t hash) {
    uint64_t pos = hash & mask_;
    while (slots_[pos] != 0) {
        pos = (pos + 1) & mask_;
    }
    slots_[pos] = entry;
}

void GroupTable::Grow(idx_t needed) {
    // Keep the load factor at or below 1/2 so probe sequences stay short.
    const idx_t target = (rows_.size() + needed) * 2;
    if (target <= slots_.size()) {
        return;
    }
    idx_t capacity = std::max<idx_t>(slots_.size(), kInitialSlots);
    while (capacity < target) {
        capacity *= 2;
    }
    slots_.assign(capacity, 0);
    mask_ = capacity - 1;
    for (auto row : rows_) {
        const uint64_t hash = duckdb::Load<uint64_t>(row + kHashOffset);
        InsertEntry((Salt(hash) << 48) | reinterpret_cast<uintptr_t>(row), hash);
    }
}

data_ptr_t GroupTable::CreateRow(uint64_t hash, idx_t r) {
    data_ptr_t row = row_arena_.AllocateAligned(row_width_);
    if ((reinterpret_cast<uintptr_t>(row) & ~kPointerMask) != 0) {
        throw duckdb::InternalException("GroupTable: row pointer does not fit in 48 bits");
    }
    duckdb::Store<uint64_t>(hash, row + kHashOffset);
    for (idx_t c = 0; c < columns_.size(); c++) {
        const auto &col = columns_[c];
        const auto &format = formats_[c];
        const idx_t idx = format.sel->get_index(r);
        if (!format.validity.RowIsValid(idx)) {
            row[col.null_offset] = 1;
            std::memset(row + col.offset, 0, col.width);
            continue;
        }
        row[col.null_offset] = 0;
        if (col.type == PhysicalType::VARCHAR) {
            string_t value = UnifiedVectorFormat::GetDataUnsafe<string_t>(format)[idx];
            if (!value.IsInlined()) {
                auto copy = strings_->arena.Allocate(value.GetSize());
                std::memcpy(copy, value.GetData(), value.GetSize());
                value = string_t(reinterpret_cast<const char *>(copy), static_cast<uint32_t>(value.GetSize()));
            }
            duckdb::Store<string_t>(value, row + col.offset);
        } else {
            std::memcpy(row + col.offset, format.data + idx * col.width, col.width);
        }
    }
    rows_.push_back(row);
    return row;
}

template <bool CREATE>
idx_t GroupTable::Probe(DataChunk &keys, Vector &hashes, idx_t count, data_ptr_t *payloads,
                        SelectionVector *new_groups) {
    D_ASSERT(count <= STANDARD_VECTOR_SIZE);
    if (CREATE) {
        Grow(count);
    } else if (rows_.empty()) {
        std::fill(payloads, payloads + count, nullptr);
        return 0;
    }
    for (idx_t c = 0; c < columns_.size(); c++) {
        keys.data[c].ToUnifiedFormat(formats_[c]);
    }
    const uint64_t *hash_data = FlatVector::GetData<uint64_t>(hashes);

    idx_t remaining_count = count;
    for (idx_t i = 0; i < count; i++) {
        remaining_[i] = i;
        positions_[i] = hash_data[i] & mask_;
    }

    idx_t new_count = 0;
    while (remaining_count > 0) {
        // Walk each row's probe sequence to the first empty slot (new key) or salt match (candidate).
        idx_t candidate_count = 0;
        for (idx_t k = 0; k < remaining_count; k++) {
            const idx_t r = remaining_[k];
            const uint64_t hash = hash_data[r];
            const uint64_t salt = Salt(hash);
            uint64_t pos = positions_[r];
            while (true) {
                const uint64_t entry = slots_[pos];
                if (entry == 0) {
                    if (CREATE) {
                        data_ptr_t row = CreateRow(hash, r);
                        slots_[pos] = (salt << 48) | reinterpret_cast<uintptr_t>(row);
                        payloads[r] = row + payload_offset_;
                        new_groups->set_index(new_count++, r);
                    } else {
                        payloads[r] = nullptr;
                    }
                    break;
                }
                if (Salt(entry) == salt) {
                    positions_[r] = pos;
                    candidates_[candidate_count] = r;
                    candidate_rows_[candidate_count] = EntryRow(entry);
                    candidate_count++;
                    break;
                }
                pos = (pos + 1) & mask_;
            }
        }

        // Compare the candidates' full hash, then their keys column by column.
        idx_t failed_count = 0;
        idx_t match_count = 0;
        for (idx_t k = 0; k < candidate_count; k++) {
            const idx_t r = candidates_[k];
            if (duckdb::Load<uint64_t>(candidate_rows_[k] + kHashOffset) == hash_data[r]) {
                candidates_[match_count] = r;
                candidate_rows_[match_count] = candidate_rows_[k];
                match_count++;
            } else {
                failed_[failed_count++] = r;
            }
        }
        for (idx_t c = 0; c < columns_.size() && match_count > 0; c++) {
            const auto &col = columns_[c];
            match_count = col.match(col, formats_[c], match_count, candidates_.data(), candidate_rows_.data(),
                                    failed_.data(), failed_count);
        }
        for (idx_t k = 0; k < match_count; k++) {
            payloads[candidates_[k]] = candidate_rows_[k] + payload_offset_;
        }

        // Rows whose candidate was a different key continue probing after it.
        for (idx_t k = 0; k < failed_count; k++) {
            const idx_t r = failed_[k];
            positions_[r] = (positions_[r] + 1) & mask_;
            next_remaining_[k] = r;
        }
        std::swap(remaining_, next_remaining_);
        remaining_count = failed_count;
    }
    return new_count;
}

idx_t GroupTable::FindOrCreate(DataChunk &keys, Vector &hashes, idx_t count, data_ptr_t *payloads,
                               SelectionVector &new_groups) {
    return Probe<true>(keys, hashes, count, payloads, &new_groups);
}

void GroupTable::Find(DataChunk &keys, Vector &hashes, idx_t count, data_ptr_t *payloads) {
    Probe<false>(keys, hashes, count, payloads, nullptr);
}

void GroupTable::ScanKeys(idx_t offset, idx_t count, DataChunk &out, data_ptr_t *payloads) const {
    D_ASSERT(offset + count <= rows_.size());
    const data_ptr_t *rows = rows_.data() + offset;
    for (idx_t c = 0; c < columns_.size(); c++) {
        const auto &col = columns_[c];
        Vector &vec = out.data[c];
        Gather(rows, count, col.offset, col.width, FlatVector::GetDataMutable(vec));
        for (idx_t i = 0; i < count; i++) {
            if (rows[i][col.null_offset] != 0) {
                FlatVector::SetNull(vec, i, true);
            }
        }
        if (col.type == PhysicalType::VARCHAR) {
            duckdb::StringVector::AddAuxiliaryData(vec, duckdb::make_uniq<StringArenaHolder>(strings_));
        }
    }
    for (idx_t i = 0; i < count; i++) {
        payloads[i] = rows[i] + payload_offset_;
    }
}

} // namespace plume::exec
