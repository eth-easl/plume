#pragma once

#include "plume/catalog/data_fetcher.hpp"
#include "plume/common/result.hpp"

#include <cstddef>
#include <functional>
#include <mutex>
#include <vector>

namespace plume::catalog {

// Job must only touch state it owns plus the resolver's shared, thread-safe DataFetcher.
using RemoteResolvingJob = std::function<Result<void>()>;

class RemoteResolver {
public:
    RemoteResolver(DataFetcher fetcher, size_t num_threads)
        : fetcher_(std::move(fetcher)), num_threads_(num_threads) {}

    void Add(RemoteResolvingJob job) const;
    Result<void> Execute();

    const DataFetcher &fetcher() const { return fetcher_; }

private:
    DataFetcher fetcher_;
    size_t num_threads_;
    mutable std::mutex jobs_mu_;
    mutable std::vector<RemoteResolvingJob> jobs_;
};

} // namespace plume::catalog
