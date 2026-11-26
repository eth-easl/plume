// Link/runtime shims for the Dandelion target. These satisfy symbols that the
// host toolchain provides on linux but the Dandelion runtime/libc do not. They
// are compiled only for the Dandelion build (see functions/CMakeLists.txt).

// ---------------------------------------------------------------------------
// __cxa_thread_atexit
//   DuckDB has thread_local state with non-trivial destructors (e.g. the
//   BlockAllocator thread-local cache). The C++ runtime registers those dtors
//   via __cxa_thread_atexit, which the Dandelion runtime does not provide.
//   Functions are single-shot and the sandbox reclaims all memory on teardown,
//   so skipping thread-local destruction is safe: a no-op shim is enough.
// ---------------------------------------------------------------------------
extern "C" int __cxa_thread_atexit(void (*func)(void *), void *obj, void *dso_symbol) {
    (void)func;
    (void)obj;
    (void)dso_symbol;
    return 0;
}
