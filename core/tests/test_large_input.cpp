// Large-input test: a single host block may exceed one vector. ImportBlockChunks
// must split it into zero-copy <= STANDARD_VECTOR_SIZE chunks (validity slices
// stay word-aligned), and the pipeline must process them correctly across the
// 2048-row boundaries.

#include "exec_test_util.hpp"
#include "test_util.hpp"

#include "plume/execution/executor.hpp"
#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/memory/block.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/value.hpp"

#include <cstring>

using namespace plume;
using namespace plume::exec;
using namespace plume::expr;
using namespace plume::memory;
using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::Value;
using duckdb::validity_t;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

// Build a block with `rows` rows and two INT32 columns laid out exactly like
// ExportChunk would: a (non-nullable) = r, b (nullable) = r*10, null when r%7==0.
// Built by hand because a single DataChunk cannot hold > STANDARD_VECTOR_SIZE.
OwnedBlock BuildTwoIntBlock(Allocator &alloc, idx_t rows) {
    const uint32_t ncol = 2;
    const char name[2] = {'a', 'b'};
    const bool nullable[2] = {false, true};

    uint64_t cur = sizeof(BlockHeader);
    const uint64_t schema_off = cur;
    cur += uint64_t(ncol) * sizeof(ColumnDescriptor);
    const uint64_t names_off = cur;
    cur += ncol; // one char per name

    uint64_t data_off[2], valid_off[2] = {0, 0};
    for (uint32_t c = 0; c < ncol; c++) {
        cur = AlignUp(cur, kDataAlign);
        data_off[c] = cur;
        cur += uint64_t(rows) * sizeof(int32_t);
        if (nullable[c]) {
            cur = AlignUp(cur, kValidityAlign);
            valid_off[c] = cur;
            cur += ((uint64_t(rows) + 63) / 64) * sizeof(validity_t);
        }
    }
    const uint64_t total = AlignUp(cur, kDataAlign);

    OwnedBlock block;
    block.data = alloc.Allocate(total);
    block.size = total;
    std::memset(block.data, 0, total);
    auto *base = block.data;

    auto &h = *reinterpret_cast<BlockHeader *>(base);
    h.magic = kBlockMagic;
    h.format_version = kFormatVersion;
    h.column_count = ncol;
    h.row_count = rows;
    h.total_size = total;
    h.schema_offset = schema_off;
    h.names_offset = names_off;
    h.heap_offset = total;
    h.heap_size = 0;

    auto *descs = reinterpret_cast<ColumnDescriptor *>(base + schema_off);
    auto *names = reinterpret_cast<char *>(base + names_off);
    for (uint32_t c = 0; c < ncol; c++) {
        auto &d = descs[c];
        d = {};
        d.type_id = static_cast<uint8_t>(TypeId::INT32);
        d.flags = nullable[c] ? kColNullable : 0;
        d.name_offset = c;
        d.name_length = 1;
        d.data_offset = data_off[c];
        d.validity_offset = valid_off[c];
        names[c] = name[c];
    }

    auto *a = reinterpret_cast<int32_t *>(base + data_off[0]);
    auto *b = reinterpret_cast<int32_t *>(base + data_off[1]);
    for (idx_t r = 0; r < rows; r++) {
        a[r] = static_cast<int32_t>(r);
        b[r] = static_cast<int32_t>(r * 10);
    }
    duckdb::ValidityMask bm(reinterpret_cast<validity_t *>(base + valid_off[1]), rows);
    bm.SetAllValid(rows);
    for (idx_t r = 0; r < rows; r++) {
        if (r % 7 == 0) {
            bm.SetInvalid(r);
        }
    }
    return block;
}

} // namespace

TEST_CASE("large block imports to multiple zero-copy chunks, values + nulls intact") {
    Allocator alloc;
    const idx_t rows = 5000; // 3 chunks: 2048 + 2048 + 904
    auto block = BuildTwoIntBlock(alloc, rows);

    Schema schema;
    auto chunks = ImportBlockChunks(block.data, block.size, schema).unwrap();
    CHECK(chunks.size() == 3);
    CHECK(chunks[0]->size() == 2048);
    CHECK(chunks[1]->size() == 2048);
    CHECK(chunks[2]->size() == 904);

    // Walk all chunks in order, reconstructing the global row index.
    idx_t r = 0;
    for (auto &chunk : chunks) {
        auto *base = reinterpret_cast<const uint8_t *>(block.data);
        // zero-copy: each chunk's column data must lie inside the block buffer.
        auto *col_a = reinterpret_cast<const uint8_t *>(duckdb::FlatVector::GetData<int32_t>(chunk->data[0]));
        CHECK(col_a >= base && col_a < base + block.size);
        for (idx_t i = 0; i < chunk->size(); i++, r++) {
            CHECK(chunk->GetValue(0, i).GetValue<int32_t>() == static_cast<int32_t>(r));
            if (r % 7 == 0) {
                CHECK(chunk->GetValue(1, i).IsNull());
            } else {
                CHECK(chunk->GetValue(1, i).GetValue<int32_t>() == static_cast<int32_t>(r * 10));
            }
        }
    }
    CHECK(r == rows);

    alloc.Free(block.data, block.size);
}

TEST_CASE("pipeline over a large block: filter across boundary + aggregate") {
    Allocator alloc;
    const idx_t rows = 5000;
    auto block = BuildTwoIntBlock(alloc, rows);

    Schema schema;
    schema.columns = {{"a", I32(), false}, {"b", I32(), true}};

    // SELECT SUM(a), COUNT(*) WHERE a >= 2048  (threshold lands on a chunk edge).
    PipelineTemplate desc;
    desc.input_schema = schema;

    auto filter = std::make_shared<FilterTemplate>();
    filter->type = OpType::FILTER;
    filter->filter = ExprNode::Comparison(duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                                          ExprNode::Reference(0, I32()), ExprNode::Constant(Value::INTEGER(2048), I32()));

    auto agg = std::make_shared<AggregateTemplate>();
    agg->type = OpType::AGGREGATE;
    AggregateSpec sum_a;
    sum_a.func_name = "sum";
    sum_a.return_type = {TypeId::HUGEINT};
    sum_a.arguments = {ExprNode::Reference(0, I32())};
    AggregateSpec cnt;
    cnt.func_name = "count_star";
    cnt.return_type = {TypeId::INT64};
    agg->aggregates = {std::move(sum_a), std::move(cnt)};

    desc.operators = {filter, agg};

    int64_t expect_sum = 0, expect_count = 0;
    for (idx_t r = 2048; r < rows; r++) {
        expect_sum += static_cast<int64_t>(r);
        expect_count++;
    }

    Executor exec(alloc);

    auto outputs = plume_test::RunBlocks(exec, plume::DeserializePipeline(plume::SerializePipeline(desc)).unwrap(),
                                         {{block.data, block.size}}, alloc);
    CHECK(outputs.size() == 1);

    DataChunk out;
    Schema out_schema;
    ImportBlock(outputs[0].data, outputs[0].size, out, out_schema).unwrap();
    CHECK(out.size() == 1);
    CHECK(out.GetValue(0, 0).GetValue<int64_t>() == expect_sum);
    CHECK(out.GetValue(1, 0).GetValue<int64_t>() == expect_count);

    alloc.Free(outputs[0].data, outputs[0].size);
    alloc.Free(block.data, block.size);
}

int main() { return plume_test::RunAll(); }
