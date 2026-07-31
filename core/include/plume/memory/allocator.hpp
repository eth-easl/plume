#pragma once

#include "duckdb/common/allocator.hpp"

#include <cstddef>

namespace plume::memory {

struct AllocatorStats : public duckdb::PrivateAllocatorData {
    size_t live_bytes = 0;     // currently outstanding
    size_t max_live_bytes = 0; // maximum bytes allocated at one time
    size_t total_bytes = 0;    // cumulative allocated
    size_t alloc_count = 0;    // number of allocations
    size_t free_count = 0;     // number of deallocations/frees
};

// Plume specific memory allocator (duckdb::Allocator).
class Allocator {
public:
    Allocator();

    duckdb::Allocator &Get() { return *allocator_; }
    const AllocatorStats &Stats() const { return *stats_; }

    // Makes this the process-wide DuckDB DefaultAllocator.
    // Idempotent and thread-safe (safe to call on every invocation).
    void InstallAsDuckDBDefault();

    duckdb::data_ptr_t Allocate(size_t size) { return allocator_->AllocateData(size); }
    void Free(duckdb::data_ptr_t ptr, size_t size) { allocator_->FreeData(ptr, size); }

    void PrintStats();

private:
    duckdb::shared_ptr<duckdb::Allocator> allocator_;
    AllocatorStats *stats_; // owned by allocator_'s private data
};

} // namespace plume::memory
