// Runner: a thin, noexcept host-boundary entry point kept purely for tests —
// deserializes a pipeline and runs it over host input blocks, catching
// everything DuckDB/Plume might throw and returning a Status code instead.
// Outputs are exposed as Plume-owned blocks, freed when the Runner is
// destroyed or re-run. No production caller drives the host boundary this way
// (that lives in functions/); real callers use Executor directly.
#pragma once

#include "plume/common/buffer.hpp"
#include "plume/common/types.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/memory/block.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace plume::exec {

enum class Status : int {
    OK = 0,
    INVALID_INPUT = 1,   // malformed block/pipeline
    NOT_IMPLEMENTED = 2, // unsupported function/type/operator
    OUT_OF_RANGE = 3,    // something went out of range
    ERROR = 4,           // any other failure
};

const char *StatusName(Status s);

class Runner {
public:
    Runner();
    ~Runner();
    Runner(const Runner &) = delete;
    Runner &operator=(const Runner &) = delete;

    // Deserialize `pipeline` and run it over `inputs`. Never throws. On OK, the
    // results are available via Outputs()/OutputSchema(); on failure, Error()
    // holds a message. Each call replaces (and frees) the previous outputs.
    Status Run(const uint8_t *pipeline, size_t pipeline_size,
               const std::vector<memory::InputBlock> &inputs) noexcept;

    // One output block per emitted partition-block; valid until the next
    // Run()/destruction. Malloc-backed hand-offs (as a stage's AddOutput produces),
    // so their bytes are NOT counted in Stats().
    const std::vector<DataBuffer> &Outputs() const { return outputs_; }
    const Schema &OutputSchema() const { return output_schema_; }
    const std::string &Error() const { return error_; }

    // Byte accounting of the Plume allocator backing this run.
    const memory::AllocatorStats &Stats() const { return alloc_.Stats(); }

private:
    void FreeOutputs();

    memory::Allocator alloc_;
    std::vector<DataBuffer> outputs_;
    Schema output_schema_;
    std::string error_;
};

} // namespace plume::exec
