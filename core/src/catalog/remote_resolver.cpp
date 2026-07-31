#include "plume/catalog/remote_resolver.hpp"

#include <algorithm>
#include <mutex>
#include <thread>

namespace plume::catalog {

void RemoteResolver::Add(RemoteResolvingJob job) const {
    std::lock_guard<std::mutex> lock(jobs_mu_);
    jobs_.push_back(std::move(job));
}

Result<void> RemoteResolver::Execute() {
    if (jobs_.empty()) {
        return Ok();
    }

    std::mutex mu;
    size_t next = 0;
    Result<void> status = Ok();

    auto worker = [&]() {
        while (true) {
            size_t job_idx;
            {
                std::lock_guard<std::mutex> lock(mu);
                if (next >= jobs_.size()) {
                    return;
                }
                job_idx = next++;
            }

            Result<void> result = jobs_[job_idx]();

            if (result.is_error()) {
                std::lock_guard<std::mutex> lock(mu);
                if (status.is_ok()) {
                    status = std::move(result);
                }
            }
        }
    };

    const size_t num_threads = std::max<size_t>(1, std::min(num_threads_, jobs_.size()));
    std::vector<std::thread> threads;
    threads.reserve(num_threads - 1);
    for (size_t t = 1; t < num_threads; t++) {
        threads.emplace_back(worker);
    }
    worker(); // this thread pulls jobs too, instead of just waiting
    for (auto &t : threads) {
        t.join();
    }

    return status;
}

} // namespace plume::catalog
