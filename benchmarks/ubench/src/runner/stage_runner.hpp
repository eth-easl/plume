#pragma once

#include "../format/plan.hpp"
#include "fetch.hpp"
#include "thread_pool.hpp"

#include "plume/common/result.hpp"

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace plume::ubench {

using Block = std::vector<uint8_t>;
using BlockSet = std::vector<Block>;
// A stage output, split by the partition index each block was tagged with.
using PartitionedBlocks = std::map<uint32_t, BlockSet>;

// A single ABI input item (a non-owning view; the caller keeps the bytes alive
// for the duration of the invocation that reads them).
struct InItem {
    const uint8_t *data;
    size_t size;
    std::string ident;
    size_t key;
};

class StageGraphRunner {
public:
    StageGraphRunner(const UbPlan &plan, LocalFetcher &fetch, ThreadPool &pool);

    // Run the whole graph and return the root stage's output blocks (flattened
    // across partitions). Fails if any stage invocation errored.
    Result<BlockSet> Run();

private:
    // Run one stage entirely, returning its partitioned output.
    Result<PartitionedBlocks> RunStage(const UbStage &s);
    Result<PartitionedBlocks> RunTableStage(const UbStage &s);
    Result<PartitionedBlocks> RunBlockStage(const UbStage &s);
    Result<PartitionedBlocks> RunSourceStage(const UbStage &s);

    uint32_t ProducerPartitions(int stage_id) const;

    const UbPlan &plan_;
    LocalFetcher &fetch_;
    ThreadPool &pool_;

    std::unordered_map<int, const UbStage *> by_id_;
    std::unordered_map<int, uint16_t> num_ops_; // stage id -> operator count (for trace sizing)
    std::unordered_map<int, PartitionedBlocks> outputs_;
};

} // namespace plume::ubench
