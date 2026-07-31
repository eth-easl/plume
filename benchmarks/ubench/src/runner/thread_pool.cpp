#include "thread_pool.hpp"

#include <atomic>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace plume::ubench {

namespace {

// Pin the calling thread to `core`. Best-effort: a failure (e.g. core out of
// range) leaves the thread unpinned rather than aborting.
void PinToCore(size_t core) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
    (void)core;
#endif
}

} // namespace

ThreadPool::ThreadPool(size_t num_threads, size_t core_offset) {
    if (num_threads == 0) {
        num_threads = 1;
    }
    for (size_t i = 0; i < num_threads; i++) {
        workers_.emplace_back([this, core = core_offset + i] {
            PinToCore(core);
            WorkerLoop();
        });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    for (auto &t : workers_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void ThreadPool::WorkerLoop() {
    for (;;) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
            if (stop_ && tasks_.empty()) {
                return;
            }
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        task();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (--pending_ == 0) {
                done_cv_.notify_all();
            }
        }
    }
}

void ThreadPool::ParallelFor(size_t n, const std::function<void(size_t)> &fn) {
    if (n == 0) {
        return;
    }
    // Run inline when there is nothing to gain from dispatch.
    if (n == 1 || workers_.size() <= 1) {
        for (size_t i = 0; i < n; i++) {
            fn(i);
        }
        return;
    }
    {
        std::unique_lock<std::mutex> lock(mutex_);
        pending_ = n;
        for (size_t i = 0; i < n; i++) {
            tasks_.push([&fn, i] { fn(i); });
        }
    }
    cv_.notify_all();
    std::unique_lock<std::mutex> lock(mutex_);
    done_cv_.wait(lock, [this] { return pending_ == 0; });
}

} // namespace plume::ubench
