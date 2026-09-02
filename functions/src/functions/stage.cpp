#include "plume/functions/stage.hpp"

#include "plume/abi/abi.hpp"
#include "plume/common/serial.hpp"
#include "plume/execution/executor.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/output_split.hpp"
#include "plume/memory/allocator.hpp"

#include <cstdint>
#include <vector>

/// The first input set (index 0) should have one item containing the pipeline template.
#define SET_IDX_IN_PIPELINE_TEMPL 0
/// The second input set (index 1) should have the items containing the data chunks.
#define SET_IDX_IN_DATA_CHUNKS 1
/// The third input set (index 2) should have the items containing the data chunks for the right
/// side of the join.
#define SET_IDX_IN_DATA_CHUNKS_RIGHT 2

/// The first output set (index 0) contains the normal row output.
/// The second output set (index 1) optionally contains a single serialized DynamicFilterBounds item,
/// emitted only if the pipeline contains a DynamicFilterBuildTemplate operator.
#define SET_IDX_OUT_DYNAMIC_FILTER 1

namespace plume::fn {

Result<void> RunStage() {
    TRY(auto template_data, abi::GetInputSingleton(SET_IDX_IN_PIPELINE_TEMPL));
    exec::PipelineTemplate desc;
    {
        PLUME_TRACE_STAGE(trace::Phase::DESERIALIZE);
        TRY(desc, plume::DeserializePipeline(template_data.buffer.data(), template_data.buffer.size()));
    }
    memory::Allocator alloc;
    exec::Executor executor(alloc);
    {
        PLUME_TRACE_STAGE(trace::Phase::SETUP);
        TRYV(executor.Build(desc, MakeSink(desc.output_sink, abi::AddOutput)));
    }

    TRY(auto inputs, abi::GetInputSet(SET_IDX_IN_DATA_CHUNKS));
    std::vector<memory::InputBlock> in_blocks;
    in_blocks.reserve(inputs.size());
    for (auto &b : inputs) {
        in_blocks.push_back({const_cast<uint8_t *>(b.buffer.data()), b.buffer.size()});
    }

    std::vector<abi::InputItem> right;
    if (executor.HasJoin()) {
        TRY(right, abi::GetInputSet(SET_IDX_IN_DATA_CHUNKS_RIGHT));
        std::vector<memory::InputBlock> right_blocks;
        right_blocks.reserve(right.size());
        for (auto &b : right) {
            right_blocks.push_back({const_cast<uint8_t *>(b.buffer.data()), b.buffer.size()});
        }
        TRYV(executor.PushJoinBuildBlocks(right_blocks));
        TRYV(executor.FinishJoinBuild());
    }
    TRYV(executor.PushBlocks(in_blocks));
    TRYV(executor.Finish());

    if (auto *dyn_filter = executor.FindDynamicFilterBuild()) {
        abi::AddOutput("dynamic_filter", SET_IDX_OUT_DYNAMIC_FILTER, SerializeToBuffer(dyn_filter->Bounds()));
    }

#if defined(PLUME_VERBOSE) && PLUME_VERBOSE >= 1
    alloc.PrintStats();
#endif

    return Ok();
}

} // namespace plume::fn
