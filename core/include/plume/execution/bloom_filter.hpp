#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume::exec {

// Split-block Bloom filter (SBBF) over 64-bit key hashes.
// The filter is an array of 256-bit blocks. The upper 32 hash bits select the block and the lower
// 32 bits set one salted bit in each of the block's eight 32-bit words. Compared to standard Bloom
// filters, on a lookup the this approach only touches a single cache line.
// Filters of equal size built from the same hash function merge by OR-ing their words.
class BloomFilter {
public:
    static constexpr size_t kWordsPerBlock = 8;
    static constexpr size_t kBitsPerBlock = kWordsPerBlock * 32;

    BloomFilter() = default;
    // Allocates an empty filter of at least `num_bits` bits (rounded up to whole blocks).
    explicit BloomFilter(uint64_t num_bits);

    bool Empty() const { return words_.empty(); }
    uint64_t NumBlocks() const { return words_.size() / kWordsPerBlock; }

    void Insert(uint64_t hash);
    bool MayContain(uint64_t hash) const;

    bool Merge(const BloomFilter &other);
    void Clear() { words_.clear(); }

    void Serialize(duckdb::Serializer &s) const;
    static BloomFilter Deserialize(duckdb::Deserializer &d);

private:
    size_t BlockOffset(uint64_t hash) const;

    std::vector<uint32_t> words_;
};

} // namespace plume::exec
