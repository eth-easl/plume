#include "plume/memory/allocator.hpp"

#include <cstdlib>
#include <iostream>
#include <mutex>

#ifdef PLUME_RECYCLING_ALLOCATOR
# include <cstddef>
# include <unordered_map>
# include <vector>
#endif


namespace plume::memory {

using duckdb::data_ptr_t;
using duckdb::idx_t;
using duckdb::PrivateAllocatorData;

namespace {

#ifdef PLUME_RECYCLING_ALLOCATOR

class RecyclingPool {
public:
    // Only pool the small, recurring sizes (vectors / selection buffers). Larger one-off blocks 
    // bypass the pool so it never holds meaningful memory hostage.
    static constexpr size_t kMaxBlock = 256 * 1024;
    static constexpr size_t kMaxPerBucket = 64;
    static constexpr size_t kMaxHeldBytes = 64 * 1024 * 1024; // total idle memory cap / thread

    void *Take(size_t size) {
        if (size == 0 || size > kMaxBlock) {
            return malloc(size);
        }
        auto it = buckets_.find(size);
        if (it != buckets_.end() && !it->second.empty()) {
            void *p = it->second.back();
            it->second.pop_back();
            held_bytes_ -= size;
            return p;
        }
        return malloc(size);
    }

    void Give(void *p, size_t size) {
        if (size == 0 || size > kMaxBlock || held_bytes_ + size > kMaxHeldBytes) {
            free(p);
            return;
        }
        auto &b = buckets_[size];
        if (b.size() >= kMaxPerBucket) {
            free(p);
            return;
        }
        b.push_back(p);
        held_bytes_ += size;
    }

    ~RecyclingPool() {
        for (auto &kv : buckets_) {
            for (void *p : kv.second) {
                free(p);
            }
        }
    }

private:
    std::unordered_map<size_t, std::vector<void *>> buckets_;
    size_t held_bytes_ = 0;
};

// dandelion does not support thread_local
#ifdef __DANDELION__
static RecyclingPool t_pool;
#else
thread_local RecyclingPool t_pool;
#endif

#endif // PLUME_RECYCLING_ALLOCATOR

data_ptr_t PlumeAllocate(PrivateAllocatorData *priv, idx_t size) {
    auto &s = priv->Cast<AllocatorStats>();
    s.live_bytes += size;
    s.total_bytes += size;
    s.alloc_count++;
    s.max_live_bytes = s.live_bytes > s.max_live_bytes ? s.live_bytes : s.max_live_bytes;
#ifdef PLUME_RECYCLING_ALLOCATOR
    return reinterpret_cast<data_ptr_t>(t_pool.Take(size));
#else
    return reinterpret_cast<data_ptr_t>(malloc(size));
#endif
}

void PlumeFree(PrivateAllocatorData *priv, data_ptr_t pointer, idx_t size) {
    auto &s = priv->Cast<AllocatorStats>();
    s.live_bytes -= size;
    s.free_count++;
#ifdef PLUME_RECYCLING_ALLOCATOR
    t_pool.Give(pointer, size);
#else
    free(pointer);
#endif
}

data_ptr_t PlumeReallocate(PrivateAllocatorData *priv, data_ptr_t pointer, idx_t old_size, idx_t size) {
    auto &s = priv->Cast<AllocatorStats>();
    s.live_bytes += size - old_size;
    if (size > old_size) {
        s.total_bytes += size - old_size;
    }
    s.max_live_bytes = s.live_bytes > s.max_live_bytes ? s.live_bytes : s.max_live_bytes;
    return reinterpret_cast<data_ptr_t>(realloc(pointer, size));
}

} // namespace

Allocator::Allocator() {
    auto stats = duckdb::make_uniq<AllocatorStats>();
    stats_ = stats.get();
    allocator_ = duckdb::make_shared_ptr<duckdb::Allocator>(
        PlumeAllocate, PlumeFree, PlumeReallocate, std::move(stats));
}

void Allocator::InstallAsDuckDBDefault() {
    static std::once_flag once;
    std::call_once(once, [this] { duckdb::Allocator::DefaultAllocatorReference() = allocator_; });
}

void Allocator::PrintStats() {
    std::cout << "Allocator Stats:" << std::endl;
    std::cout << "  live_bytes:     " << stats_->live_bytes << "B" << std::endl;
    std::cout << "  max_live_bytes: " << stats_->max_live_bytes << "B" << std::endl;
    std::cout << "  total_bytes:    " << stats_->total_bytes << "B" << std::endl;
    std::cout << "  alloc_count:    " << stats_->alloc_count << std::endl;
    std::cout << "  free_count:     " << stats_->free_count << std::endl;
}

} // namespace plume::memory
