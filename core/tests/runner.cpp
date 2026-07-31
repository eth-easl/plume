#include "runner.hpp"

#include "plume/common/result.hpp"
#include "plume/execution/executor.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/execution/sink.hpp"

#include "duckdb/common/exception.hpp"

namespace plume::exec {

const char *StatusName(Status s) {
    switch (s) {
    case Status::OK:
        return "OK";
    case Status::INVALID_INPUT:
        return "INVALID_INPUT";
    case Status::NOT_IMPLEMENTED:
        return "NOT_IMPLEMENTED";
    case Status::OUT_OF_RANGE:
        return "OUT_OF_RANGE";
    case Status::ERROR:
        return "ERROR";
    }
    return "UNKNOWN";
}

Runner::Runner() = default;

Runner::~Runner() {
    FreeOutputs();
}

void Runner::FreeOutputs() {
    outputs_.clear(); // DataBuffer frees its malloc-backed block on destruction
}

Status Runner::Run(const uint8_t *pipeline_data, size_t pipeline_size,
                   const std::vector<memory::InputBlock> &inputs) noexcept {
    FreeOutputs();
    output_schema_ = Schema {};
    error_.clear();
    try {
        // unwrap() raises a ResultError on a Plume Error; the handler below maps its
        // kind to a status. DuckDB kernels may still throw at runtime (caught lower).
        PipelineTemplate desc = plume::DeserializePipeline(pipeline_data, pipeline_size).unwrap();
        Executor executor(alloc_);
        // Terminate with a streaming block sink that collects the emitted blocks.
        OutputEmit emit = [this](const std::string &, size_t, DataBuffer buffer, size_t) {
            outputs_.push_back(std::move(buffer));
        };
        executor.Build(desc, emit).unwrap();
        executor.PushBlocks(inputs).unwrap();
        executor.Finish().unwrap();
        output_schema_ = executor.OutputSchema();
        return Status::OK;
    } catch (const ResultError &e) {
        // Plume's own errors arrive here (via unwrap at the migration frontier).
        // Map the error kind to a status — no string-matching needed.
        error_ = e.error().to_string();
        switch (e.error().kind()) {
        case ErrorKind::NotImplemented:
            return Status::NOT_IMPLEMENTED;
        case ErrorKind::InvalidInput:
            return Status::INVALID_INPUT;
        case ErrorKind::OutOfRange:
            return Status::OUT_OF_RANGE;
        case ErrorKind::Generic:
            return Status::ERROR;
        }
        return Status::ERROR;
    } catch (const duckdb::NotImplementedException &e) {
        error_ = e.what();
        return Status::NOT_IMPLEMENTED;
    } catch (const duckdb::InvalidInputException &e) {
        error_ = e.what();
        return Status::INVALID_INPUT;
    } catch (const duckdb::SerializationException &e) {
        error_ = e.what();
        return Status::INVALID_INPUT;
    } catch (const std::exception &e) {
        error_ = e.what();
        return Status::ERROR;
    } catch (...) {
        error_ = "unknown error";
        return Status::ERROR;
    }
}

} // namespace plume::exec
