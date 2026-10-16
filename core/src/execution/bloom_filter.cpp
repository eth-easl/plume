#include "plume/execution/bloom_filter.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace plume::exec {

namespace {

// The salts of parquet's split-block Bloom filter.
constexpr uint32_t kSalt[BloomFilter::kWordsPerBlock] = {
    0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U,
};

inline uint32_t BitInWord(uint32_t key, size_t i) {
    return 1u << ((key * kSalt[i]) >> 27);
}

} // namespace

BloomFilter::BloomFilter(uint64_t num_bits) {
    const uint64_t num_blocks = num_bits == 0 ? 1 : (num_bits + kBitsPerBlock - 1) / kBitsPerBlock;
    words_.assign(num_blocks * kWordsPerBlock, 0);
}

size_t BloomFilter::BlockOffset(uint64_t hash) const {
    // Multiply-shift maps the upper 32 hash bits onto [0, NumBlocks()) without a modulo.
    const uint64_t block = ((hash >> 32) * NumBlocks()) >> 32;
    return static_cast<size_t>(block) * kWordsPerBlock;
}

void BloomFilter::Insert(uint64_t hash) {
    uint32_t *block = words_.data() + BlockOffset(hash);
    const uint32_t key = static_cast<uint32_t>(hash);
    for (size_t i = 0; i < kWordsPerBlock; i++) {
        block[i] |= BitInWord(key, i);
    }
}

bool BloomFilter::MayContain(uint64_t hash) const {
    const uint32_t *block = words_.data() + BlockOffset(hash);
    const uint32_t key = static_cast<uint32_t>(hash);
    for (size_t i = 0; i < kWordsPerBlock; i++) {
        if (!(block[i] & BitInWord(key, i))) {
            return false;
        }
    }
    return true;
}

bool BloomFilter::Merge(const BloomFilter &other) {
    if (words_.size() != other.words_.size()) {
        return false;
    }
    for (size_t i = 0; i < words_.size(); i++) {
        words_[i] |= other.words_[i];
    }
    return true;
}

void BloomFilter::Serialize(duckdb::Serializer &s) const {
    const uint64_t num_words = words_.size();
    s.WriteProperty(100, "num_words", num_words);
    s.WriteProperty(101, "words", reinterpret_cast<duckdb::const_data_ptr_t>(words_.data()),
                    num_words * sizeof(uint32_t));
}

BloomFilter BloomFilter::Deserialize(duckdb::Deserializer &d) {
    BloomFilter b;
    const auto num_words = d.ReadProperty<uint64_t>(100, "num_words");
    b.words_.resize(num_words);
    d.ReadProperty(101, "words", reinterpret_cast<duckdb::data_ptr_t>(b.words_.data()),
                   num_words * sizeof(uint32_t));
    return b;
}

} // namespace plume::exec
