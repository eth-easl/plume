// plume_stage with a leading inner hash equi-join: left (set 1) joined to right
// (set 2) on a key column, output partitioned by a right column.

#include "abi_mock.hpp"
#include "test_util.hpp"

#include "plume/execution/operators/join.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/functions/stage.hpp"
#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <map>
#include <vector>

using namespace plume;
using namespace plume::exec;
using namespace plume::memory;
using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;

namespace {

ColumnType I32() { return {TypeId::INT32}; }

Schema TwoCol(const char *a, const char *b) {
    Schema s;
    s.columns = {{a, I32(), false}, {b, I32(), false}};
    return s;
}

DataBuffer Make2ColBlock(Allocator &alloc, const Schema &schema, const std::vector<int32_t> &c0,
                         const std::vector<int32_t> &c1) {
    DataChunk chunk;
    chunk.Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::INTEGER});
    for (idx_t r = 0; r < c0.size(); r++) {
        chunk.SetValue(0, r, Value::INTEGER(c0[r]));
        chunk.SetValue(1, r, Value::INTEGER(c1[r]));
    }
    chunk.SetCardinality(c0.size());
    auto block = ExportChunk(schema, chunk, alloc).unwrap();
    auto buf = mock::MakeBuffer(block.data, block.size);
    alloc.Free(block.data, block.size);
    return buf;
}

} // namespace

TEST_CASE("plume_stage: inner hash join + partitioned output") {
    mock::Reset();
    Allocator alloc;
    const uint32_t N = 2;

    auto orders = TwoCol("order_id", "cust_id");    // left
    auto customers = TwoCol("cust_id", "region");   // right (build)

    // JOIN orders.cust_id (col 1) == customers.cust_id (col 0); split by region.
    // Output schema: order_id, cust_id(left), cust_id(right), region (cols 0..3).
    PipelineTemplate desc;
    desc.input_schema = orders;
    auto join = std::make_shared<JoinTemplate>();
    join->type = OpType::JOIN;
    join->left_keys = {1};
    join->right_keys = {0};
    join->right_schema = customers;
    desc.operators = {join};
    desc.output_split.key_columns = {3}; // region
    desc.output_split.partitions = N;
    auto blob = plume::SerializePipeline(desc);

    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(blob.data(), blob.size()));
        return v;
    }());
    // orders: (order_id, cust_id) — cust 99 has no matching customer.
    mock::SetInput(1, [&] {
        std::vector<DataBuffer> v;
        v.push_back(Make2ColBlock(alloc, orders, {10, 11, 12, 13, 14}, {1, 2, 1, 3, 99}));
        return v;
    }());
    // customers: (cust_id, region).
    mock::SetInput(2, [&] {
        std::vector<DataBuffer> v;
        v.push_back(Make2ColBlock(alloc, customers, {1, 2, 3, 4}, {100, 200, 300, 400}));
        return v;
    }());

    CHECK(fn::RunStage().is_ok());

    // order_id -> region expected from the inner join (order 14 dropped).
    std::map<int32_t, int32_t> expected = {{10, 100}, {11, 200}, {12, 100}, {13, 300}};

    std::map<int32_t, int32_t> got;
    for (auto *out : mock::OutputsForSet(0)) {
        Schema schema;
        auto chunks = ImportBlockChunks(const_cast<uint8_t *>(out->buffer.data()), out->buffer.size(), schema).unwrap();
        for (auto &c : chunks) {
            for (idx_t r = 0; r < c->size(); r++) {
                int32_t order_id = c->GetValue(0, r).GetValue<int32_t>();
                int32_t lcust = c->GetValue(1, r).GetValue<int32_t>();
                int32_t rcust = c->GetValue(2, r).GetValue<int32_t>();
                int32_t region = c->GetValue(3, r).GetValue<int32_t>();
                CHECK(lcust == rcust);                                               // join key equality
                CHECK(out->key == static_cast<uint32_t>(Value::INTEGER(region).Hash() % N)); // split by region
                got[order_id] = region;
            }
        }
    }
    CHECK(got.size() == expected.size());
    for (auto &[oid, region] : expected) {
        CHECK(got.count(oid) == 1);
        CHECK(got[oid] == region);
    }
}

TEST_CASE("plume_stage: left hash join keeps unmatched probe rows") {
    mock::Reset();
    Allocator alloc;

    auto orders = TwoCol("order_id", "cust_id");  // left (probe)
    auto customers = TwoCol("cust_id", "region"); // right (build)

    // LEFT JOIN orders.cust_id == customers.cust_id; single partition (no split).
    // Output: order_id, cust_id(left), cust_id(right), region.
    PipelineTemplate desc;
    desc.input_schema = orders;
    auto join = std::make_shared<JoinTemplate>();
    join->type = OpType::JOIN;
    join->left_keys = {1};
    join->right_keys = {0};
    join->right_schema = customers;
    join->kind = JoinKind::LEFT;
    desc.operators = {join};
    desc.output_split.key_columns = {0}; // order_id (never NULL)
    desc.output_split.partitions = 1;
    auto blob = plume::SerializePipeline(desc);

    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(blob.data(), blob.size()));
        return v;
    }());
    // orders: cust 99 (order 14) has no matching customer.
    mock::SetInput(1, [&] {
        std::vector<DataBuffer> v;
        v.push_back(Make2ColBlock(alloc, orders, {10, 11, 12, 13, 14}, {1, 2, 1, 3, 99}));
        return v;
    }());
    mock::SetInput(2, [&] {
        std::vector<DataBuffer> v;
        v.push_back(Make2ColBlock(alloc, customers, {1, 2, 3, 4}, {100, 200, 300, 400}));
        return v;
    }());

    CHECK(fn::RunStage().is_ok());

    // All five orders survive; matched rows carry a region, order 14 is NULL.
    std::map<int32_t, int32_t> matched = {{10, 100}, {11, 200}, {12, 100}, {13, 300}};
    int rows = 0;
    bool saw_null = false;
    for (auto *out : mock::OutputsForSet(0)) {
        Schema schema;
        auto chunks = ImportBlockChunks(const_cast<uint8_t *>(out->buffer.data()), out->buffer.size(), schema).unwrap();
        for (auto &c : chunks) {
            for (idx_t r = 0; r < c->size(); r++) {
                rows++;
                int32_t order_id = c->GetValue(0, r).GetValue<int32_t>();
                Value rcust = c->GetValue(2, r);
                Value region = c->GetValue(3, r);
                if (order_id == 14) {
                    CHECK(rcust.IsNull());
                    CHECK(region.IsNull());
                    saw_null = true;
                } else {
                    CHECK(region.GetValue<int32_t>() == matched[order_id]);
                }
            }
        }
    }
    CHECK(rows == 5);
    CHECK(saw_null);
}

TEST_CASE("plume_stage: right hash join keeps unmatched build rows") {
    mock::Reset();
    Allocator alloc;

    auto orders = TwoCol("order_id", "cust_id");  // left (probe)
    auto customers = TwoCol("cust_id", "region"); // right (build)

    // RIGHT JOIN: every customer survives; orders columns NULL where no order matches.
    PipelineTemplate desc;
    desc.input_schema = orders;
    auto join = std::make_shared<JoinTemplate>();
    join->type = OpType::JOIN;
    join->left_keys = {1};
    join->right_keys = {0};
    join->right_schema = customers;
    join->kind = JoinKind::RIGHT;
    desc.operators = {join};
    desc.output_split.key_columns = {2}; // cust_id (right) — never NULL here
    desc.output_split.partitions = 1;
    auto blob = plume::SerializePipeline(desc);

    mock::SetInput(0, [&] {
        std::vector<DataBuffer> v;
        v.push_back(mock::MakeBuffer(blob.data(), blob.size()));
        return v;
    }());
    // orders: cust 99 (order 14) has no matching customer -> dropped by RIGHT join.
    mock::SetInput(1, [&] {
        std::vector<DataBuffer> v;
        v.push_back(Make2ColBlock(alloc, orders, {10, 11, 12, 13, 14}, {1, 2, 1, 3, 99}));
        return v;
    }());
    // customer 4 has no orders -> kept with NULL order columns.
    mock::SetInput(2, [&] {
        std::vector<DataBuffer> v;
        v.push_back(Make2ColBlock(alloc, customers, {1, 2, 3, 4}, {100, 200, 300, 400}));
        return v;
    }());

    CHECK(fn::RunStage().is_ok());

    int rows = 0;
    bool saw_unmatched_customer = false;
    std::map<int32_t, int32_t> matched; // order_id -> region
    for (auto *out : mock::OutputsForSet(0)) {
        Schema schema;
        auto chunks = ImportBlockChunks(const_cast<uint8_t *>(out->buffer.data()), out->buffer.size(), schema).unwrap();
        for (auto &c : chunks) {
            for (idx_t r = 0; r < c->size(); r++) {
                rows++;
                Value order_id = c->GetValue(0, r);
                int32_t rcust = c->GetValue(2, r).GetValue<int32_t>(); // right cust_id never NULL
                int32_t region = c->GetValue(3, r).GetValue<int32_t>();
                if (order_id.IsNull()) {
                    CHECK(rcust == 4); // the customer with no orders
                    CHECK(region == 400);
                    saw_unmatched_customer = true;
                } else {
                    matched[order_id.GetValue<int32_t>()] = region;
                }
            }
        }
    }
    // 4 matched orders (10,11,12,13) + 1 unmatched customer (4); order 14 dropped.
    CHECK(rows == 5);
    CHECK(saw_unmatched_customer);
    CHECK(matched.size() == 4);
    CHECK(matched[10] == 100);
    CHECK(matched[13] == 300);
}

int main() { return plume_test::RunAll(); }
