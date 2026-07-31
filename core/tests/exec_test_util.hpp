// Test helper: build an Executor with a block-collecting sink, drive it over host
// input blocks, and return the emitted blocks — the push API's equivalent of the
// old Execute(). Kept out of the stdlib-only test_util.hpp since it pulls in the executor.
#pragma once

#include "plume/execution/executor.hpp"
#include "plume/execution/operators/output.hpp"

#include <cstring>
#include <string>
#include <vector>

namespace plume_test {

// Build `exec` from `desc` with a single-partition block sink, push every block,
// finish, and return the emitted blocks. The streaming sink hands off malloc-backed
// buffers; we copy each into an alloc-backed OwnedBlock so callers Free() them via
// `alloc`, exactly as they handle input blocks. `exec` stays usable afterwards
// (e.g. for OutputSchema()). `blocks` must outlive the call.
inline std::vector<plume::memory::OwnedBlock> RunBlocks(plume::exec::Executor &exec,
                                                        const plume::exec::PipelineTemplate &desc,
                                                        const std::vector<plume::memory::InputBlock> &blocks,
                                                        plume::memory::Allocator &alloc) {
    using namespace plume;
    std::vector<memory::OwnedBlock> out;
    exec::OutputEmit emit = [&](const std::string &, size_t, DataBuffer buf, size_t) {
        memory::OwnedBlock ob;
        ob.size = buf.size();
        ob.data = alloc.Allocate(buf.size());
        std::memcpy(ob.data, buf.data(), buf.size());
        out.push_back(ob);
    };
    exec.Build(desc, emit).unwrap();
    exec.PushBlocks(blocks).unwrap();
    exec.Finish().unwrap();
    return out;
}

} // namespace plume_test
