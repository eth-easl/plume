#include "runner/mem_tracker.hpp"

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <malloc.h>

// glibc's internal entry points; the wrappers at the bottom interpose the public names.
extern "C" {
void *__libc_malloc(size_t size);
void __libc_free(void *ptr);
void *__libc_calloc(size_t n, size_t size);
void *__libc_realloc(void *ptr, size_t size);
void *__libc_memalign(size_t alignment, size_t size);
}

namespace plume::ubench::mem {

namespace {

std::atomic<bool> g_enabled {false};
std::atomic<int64_t> g_live {0};
std::atomic<int64_t> g_peak {0};

} // namespace

// Allocations made before tracking starts may be freed while it is on, so `live` is relative: only
// differences (peak - baseline) are meaningful.
inline void Add(void *ptr) {
    if (!ptr || !g_enabled.load(std::memory_order_relaxed)) {
        return;
    }
    const auto size = static_cast<int64_t>(malloc_usable_size(ptr));
    const int64_t live = g_live.fetch_add(size, std::memory_order_relaxed) + size;
    int64_t peak = g_peak.load(std::memory_order_relaxed);
    while (live > peak && !g_peak.compare_exchange_weak(peak, live, std::memory_order_relaxed)) {
    }
}

inline void Sub(void *ptr) {
    if (ptr && g_enabled.load(std::memory_order_relaxed)) {
        g_live.fetch_sub(static_cast<int64_t>(malloc_usable_size(ptr)), std::memory_order_relaxed);
    }
}

void SetEnabled(bool enabled) { g_enabled.store(enabled); }

bool Enabled() { return g_enabled.load(); }

int64_t ResetPeak() {
    const int64_t live = g_live.load();
    g_peak.store(live);
    return live;
}

int64_t Peak() { return g_peak.load(); }

} // namespace plume::ubench::mem

extern "C" {

void *malloc(size_t size) {
    void *p = __libc_malloc(size);
    plume::ubench::mem::Add(p);
    return p;
}

void free(void *ptr) {
    plume::ubench::mem::Sub(ptr);
    __libc_free(ptr);
}

void *calloc(size_t n, size_t size) {
    void *p = __libc_calloc(n, size);
    plume::ubench::mem::Add(p);
    return p;
}

void *realloc(void *ptr, size_t size) {
    plume::ubench::mem::Sub(ptr);
    void *p = __libc_realloc(ptr, size);
    plume::ubench::mem::Add(p ? p : (size == 0 ? nullptr : ptr)); // a failed realloc keeps ptr
    return p;
}

void *memalign(size_t alignment, size_t size) {
    void *p = __libc_memalign(alignment, size);
    plume::ubench::mem::Add(p);
    return p;
}

void *aligned_alloc(size_t alignment, size_t size) { return memalign(alignment, size); }

int posix_memalign(void **out, size_t alignment, size_t size) {
    void *p = memalign(alignment, size);
    if (!p) {
        return ENOMEM;
    }
    *out = p;
    return 0;
}

} // extern "C"
