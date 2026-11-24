#include "plume/execution/sink.hpp"

#include <string>

namespace plume::exec {

using duckdb::idx_t;

BlockSink::BlockSink(OutputEmit emit, size_t max_bytes) : emit_(std::move(emit)), max_bytes_(max_bytes) {}

BlockSink::~BlockSink() = default;

void BlockSink::Bind(const Schema &schema, uint32_t partitions) {
    schema_ = schema;
    writers_.clear();
    writers_.resize(partitions);
    seq_.assign(partitions, 0);
}

Result<void> BlockSink::Append(uint32_t partition, const OutputRows &rows) {
    auto &writer = writers_[partition];
    if (!writer) {
        writer = std::make_unique<memory::BlockWriter>(schema_, max_bytes_);
    }

    const duckdb::sel_t *sel = rows.sel ? rows.sel->data() : nullptr;

    idx_t done = writer->Append(rows.formats, sel, 0, rows.count);
    while (done < rows.count) {
        Emit(partition); // full
        done += writer->Append(rows.formats, sel, done, rows.count - done);
    }
    return Ok();
}

void BlockSink::Emit(uint32_t partition) {
    emit_(std::to_string(seq_[partition]++), 0, writers_[partition]->Finish(), partition);
}

Result<void> BlockSink::Finish() {
    for (uint32_t p = 0; p < writers_.size(); p++) {
        if (writers_[p] && writers_[p]->Rows() > 0) {
            Emit(p);
        }
        writers_[p].reset(); // frees its buffer
    }
    return Ok();
}

} // namespace plume::exec
