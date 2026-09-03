#include "plume/abi/abi.hpp"
#include "plume/common/serial.hpp"
#include "plume/csv/csv.hpp"
#include "plume/execution/executor.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/csv.hpp"
#include "plume/functions/output_split.hpp"
#include "plume/memory/allocator.hpp"

#include <map>
#include <vector>

// The first input set (index 0) should have one item containing the pipeline template.
#define SET_IDX_IN_PIPELINE_TEMPL 0
// The second input set (index 1) should have the items containing the csv chunk infos.
#define SET_IDX_IN_CHUNK_INFO 1
// The third input set (index 2) should have the items containing the resolved data buffers.
#define SET_IDX_IN_DATA_BUFFERS 2

namespace plume::fn {

Result<void> RunCSVStage() {
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

    TRY(auto chunk_info_itms, abi::GetInputSet(SET_IDX_IN_CHUNK_INFO));
    TRY(auto raw_buf_itms, abi::GetInputSet(SET_IDX_IN_DATA_BUFFERS));
    if (chunk_info_itms.size() != raw_buf_itms.size()) {
        return Error("Number of chunk info items does not match number of raw data buffer items.", ErrorKind::InvalidInput);
    }
    if (chunk_info_itms.empty()) {
        return Ok();
    }

    std::map<size_t, const DataBuffer *> buf_index;
    for (auto &itm : raw_buf_itms) {
        buf_index[itm.key] = &itm.buffer;
    }
    
    for (auto& chunk_info_itm : chunk_info_itms) {
        auto it = buf_index.find(chunk_info_itm.key);
        if (it == buf_index.end()) {
            return Error("Did not find data chunk item with matching key.");
        }
        auto& chunk_buf = *it->second;

        auto chunk_info = DeserializeFromBytes<csv::CSVRegionInfo>(
            chunk_info_itm.buffer.data(), chunk_info_itm.buffer.size());

        TRYV(csv::ParseCSVChunk(chunk_buf.data(), chunk_buf.size(), chunk_info, alloc,
                                [&](std::unique_ptr<duckdb::DataChunk> chunk) -> Result<void> {
            return executor.Push(std::move(chunk));
        }));
    }

    TRYV(executor.Finish());

#if defined(PLUME_VERBOSE) && PLUME_VERBOSE >= 1
    alloc.PrintStats();
#endif

    return Ok();
}

} // namespace plume::fn
