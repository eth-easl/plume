#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace plume::ubench {

class ThreadPool {
public:
    // `num_threads` workers, worker i pinned to core (`core_offset` + i) when
    // pinning is available. 0 threads is treated as 1.
    explicit ThreadPool(size_t num_threads, size_t core_offset = 0);
    ~ThreadPool();

    ThreadPool(const ThreadPool &) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;

    size_t size() const { return workers_.size(); }

    // Run `fn(i)` for i in [0, n) across the pool and block until all complete.
    // Runs inline (no dispatch) when n <= 1 or the pool has a single worker.
    void ParallelFor(size_t n, const std::function<void(size_t)> &fn);

private:
    void WorkerLoop();

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable done_cv_;
    size_t pending_ = 0;
    bool stop_ = false;
};

} // namespace plume::ubench
