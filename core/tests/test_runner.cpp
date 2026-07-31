// Milestone-6 host-boundary tests: the noexcept Runner returns status codes
// instead of letting DuckDB exceptions escape (R6/D16).

#include "runner.hpp"
#include "test_util.hpp"

#include "plume/execution/operators/projection.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

using namespace plume;
using namespace plume::exec;
using namespace plume::expr;
using namespace plume::memory;
using duckdb::Value;
using duckdb::DataChunk;
using duckdb::LogicalType;
using duckdb::idx_t;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema OneCol() {
    Schema s;
    s.columns = {{"a", I32(), false}};
    return s;
}

OwnedBlock MakeBlock(Allocator &alloc, const std::vector<int32_t> &a) {
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), {LogicalType::INTEGER});
    for (idx_t r = 0; r < a.size(); r++) {
        chunk.SetValue(0, r, Value::INTEGER(a[r]));
    }
    chunk.SetCardinality(a.size());
    return plume::memory::ExportChunk(OneCol(), chunk, alloc).unwrap();
}

} // namespace

TEST_CASE("runner returns OK and output blocks for a valid pipeline") {
    Allocator host;
    auto blk = MakeBlock(host, {3, 1, 2});

    PipelineTemplate desc;
    desc.input_schema = OneCol();
    auto proj = std::make_shared<ProjectionTemplate>();
    proj->type = OpType::PROJECTION;
    proj->projections = {ExprNode::Function(
        "+", I32(), {ExprNode::Reference(0, I32()), ExprNode::Constant(Value::INTEGER(10), I32())})};
    desc.operators = {proj};
    auto bytes = plume::SerializePipeline(desc);

    Runner runner;
    auto st = runner.Run(bytes.data(), bytes.size(), {{blk.data, blk.size}});
    CHECK_MSG(st == Status::OK, "expected OK");
    CHECK(runner.Outputs().size() == 1);

    DataChunk c;
    Schema s;
    plume::memory::ImportBlock(const_cast<uint8_t *>(runner.Outputs()[0].data()), runner.Outputs()[0].size(), c, s)
        .unwrap();
    CHECK(c.size() == 3);
    CHECK(c.GetValue(0, 0).GetValue<int32_t>() == 13);
    host.Free(blk.data, blk.size);
}

TEST_CASE("runner maps unknown function to NOT_IMPLEMENTED (no throw escapes)") {
    Allocator host;
    auto blk = MakeBlock(host, {1});

    PipelineTemplate desc;
    desc.input_schema = OneCol();
    auto proj = std::make_shared<ProjectionTemplate>();
    proj->type = OpType::PROJECTION;
    proj->projections = {ExprNode::Function("no_such_fn", I32(), {ExprNode::Reference(0, I32())})};
    desc.operators = {proj};
    auto bytes = plume::SerializePipeline(desc);

    Runner runner;
    auto st = runner.Run(bytes.data(), bytes.size(), {{blk.data, blk.size}});
    CHECK_MSG(st == Status::NOT_IMPLEMENTED, "expected NOT_IMPLEMENTED status");
    CHECK(!runner.Error().empty());
    CHECK(runner.Outputs().empty());
    host.Free(blk.data, blk.size);
}

TEST_CASE("runner maps a malformed pipeline blob to INVALID_INPUT") {
    Runner runner;
    uint8_t garbage[8] = {0, 1, 2, 3, 4, 5, 6, 7};
    auto st = runner.Run(garbage, sizeof(garbage), {});
    CHECK_MSG(st == Status::INVALID_INPUT, "expected INVALID_INPUT for bad magic");
}

int main() {
    return plume_test::RunAll();
}
