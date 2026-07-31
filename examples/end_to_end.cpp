//===----------------------------------------------------------------------===//
// plume — end-to-end example (TASK.md M6)
//
// Demonstrates the whole library with NO DuckDB instance:
//   1. The host builds DuckDB-native input blocks.
//   2. A pipeline is described and serialized to bytes (as a client would).
//   3. plume::exec::Runner (noexcept boundary) deserializes + executes it.
//   4. Output chunks come back as single (pointer, size) blocks; we import and
//      print them.
//
// Query (over a tiny "sales" table region INT32, amount DOUBLE):
//   SELECT region, SUM(amount), COUNT(*)
//   WHERE amount > 0
//   GROUP BY region
//   ORDER BY region ASC
//===----------------------------------------------------------------------===//

#include "plume/memory/adapter.hpp"
#include "plume/memory/allocator.hpp"
#include "plume/execution/pipeline.hpp"
#include "plume/execution/runner.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/sort.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <cstdio>
#include <vector>

using namespace plume;
using namespace plume::exec;
using namespace plume::expr;
using namespace plume::memory;
using duckdb::Value;
using duckdb::DataChunk;
using duckdb::LogicalType;
using duckdb::ExpressionType;
using duckdb::idx_t;

namespace {

ColumnType I32() { return {TypeId::INT32}; }
ColumnType I64() { return {TypeId::INT64}; }
ColumnType F64() { return {TypeId::DOUBLE}; }

Schema SalesSchema() {
	Schema s;
	s.columns = {{"region", I32(), false}, {"amount", F64(), false}};
	return s;
}

// Host-built input block: region INT32, amount DOUBLE.
OwnedBlock MakeSalesBlock(Allocator &alloc, const std::vector<int32_t> &region,
                          const std::vector<double> &amount) {
	DataChunk chunk;
	chunk.Initialize(alloc.Get(), {LogicalType::INTEGER, LogicalType::DOUBLE});
	for (idx_t r = 0; r < region.size(); r++) {
		chunk.SetValue(0, r, Value::INTEGER(region[r]));
		chunk.SetValue(1, r, Value::DOUBLE(amount[r]));
	}
	chunk.SetCardinality(region.size());
	return ExportChunk(SalesSchema(), chunk, alloc).unwrap();
}

std::vector<uint8_t> BuildPipeline() {
	PipelineTemplate desc;
	desc.input_schema = SalesSchema();

	auto filter = std::make_shared<FilterTemplate>();
	filter->type = OpType::FILTER;
	filter->filter = ExprNode::Comparison(ExpressionType::COMPARE_GREATERTHAN,
	                                      ExprNode::Reference(1, F64()),
	                                      ExprNode::Constant(Value::DOUBLE(0.0), F64()));

	auto group = std::make_shared<AggregateTemplate>();
	group->type = OpType::AGGREGATE;
	group->group_keys = {ExprNode::Reference(0, I32())};
	AggregateSpec sum_amt;
	sum_amt.func_name = "sum";
	sum_amt.return_type = F64();
	sum_amt.arguments = {ExprNode::Reference(1, F64())};
	AggregateSpec cnt;
	cnt.func_name = "count_star";
	cnt.return_type = I64();
	group->aggregates = {std::move(sum_amt), std::move(cnt)};

	auto order = std::make_shared<SortTemplate>();
	order->type = OpType::ORDER_BY;
	SortKey k;
	k.expr = ExprNode::Reference(0, I32()); // region
	k.order = SortOrder::ASCENDING;
	k.null_order = NullOrder::NULLS_LAST;
	order->sort_keys = {std::move(k)};

	desc.operators = {filter, group, order};
	return plume::SerializePipeline(desc);
}

} // namespace

int main() {
	printf("=== plume end-to-end example (no DuckDB instance) ===\n\n");

	// 1. Host builds input blocks.
	Allocator host_alloc;
	auto b0 = MakeSalesBlock(host_alloc, {1, 2, 1, 3}, {100.0, 50.0, -5.0, 30.0});
	auto b1 = MakeSalesBlock(host_alloc, {2, 1, 3}, {25.0, 0.0, 70.0});
	printf("input: 2 blocks, 7 rows (region, amount); amount<=0 rows filtered out\n");

	// 2. Describe + serialize the pipeline.
	auto pipeline_bytes = BuildPipeline();
	printf("pipeline: %zu bytes  [FILTER amount>0; GROUP BY region -> sum,count; ORDER BY region]\n\n",
	       pipeline_bytes.size());

	// 3. Run across the noexcept boundary.
	Runner runner;
	std::vector<InputBlock> inputs = {{b0.data, b0.size}, {b1.data, b1.size}};
	Status status = runner.Run(pipeline_bytes.data(), pipeline_bytes.size(), inputs);
	if (status != Status::OK) {
		printf("run failed: %s — %s\n", StatusName(status), runner.Error().c_str());
		return 1;
	}

	// 4. Consume outputs as (pointer, size) blocks.
	const auto &schema = runner.OutputSchema();
	printf("output columns:");
	for (auto &c : schema.columns) {
		printf(" %s", c.name.c_str());
	}
	printf("\n");

	int total_rows = 0;
	double sum_region1 = -1;
	for (const auto &ob : runner.Outputs()) {
		printf("  output block: %zu bytes @ %p\n", ob.size(), (const void *)ob.data());
		DataChunk c;
		Schema s;
		ImportBlock(const_cast<uint8_t *>(ob.data()), ob.size(), c, s).unwrap();
		for (idx_t r = 0; r < c.size(); r++) {
			int32_t region = c.GetValue(0, r).GetValue<int32_t>();
			double sum = c.GetValue(1, r).GetValue<double>();
			int64_t count = c.GetValue(2, r).GetValue<int64_t>();
			printf("    region=%d  sum(amount)=%.1f  count=%lld\n", region, sum, (long long)count);
			if (region == 1) {
				sum_region1 = sum;
			}
			total_rows++;
		}
	}

	auto &st = runner.Stats();
	printf("\nplume allocator: %zu allocs, %zu bytes cumulative\n", st.alloc_count, st.total_bytes);

	// Expected: region 1 -> 100.0 (only the 100 row; -5 and 0 filtered), 2 -> 75.0, 3 -> 100.0
	bool ok = (total_rows == 3) && (sum_region1 == 100.0);
	printf("\n=== EXAMPLE %s ===\n", ok ? "PASSED" : "FAILED");

	host_alloc.Free(b0.data, b0.size);
	host_alloc.Free(b1.data, b1.size);
	return ok ? 0 : 1;
}
