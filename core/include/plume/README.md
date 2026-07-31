# plume block format & adapter (Milestone 2)

A plume **block** is one contiguous, self-describing, DuckDB-native columnar
chunk. The host hands plume input as blocks and receives each output chunk as a
single `(pointer, size)` (TASK.md B5/B8).

## Layout

```
offset 0   ┌─────────────────────────────┐
           │ BlockHeader (64B, POD)       │  magic/version, row/col counts,
           │                             │  schema/names/heap offsets, total_size
           ├─────────────────────────────┤  schema_offset
           │ ColumnDescriptor[ncol]       │  32B each: type, decimal w/s, flags,
           │                             │  name (offset,len), data_offset,
           │                             │  validity_offset (0 ⇒ all-valid)
           ├─────────────────────────────┤  names_offset
           │ names blob (UTF-8)           │
           ├─────────────────────────────┤  (16B-aligned)
           │ per-column regions:          │  fixed-width array (or string_t[]),
           │   data, then validity        │  then validity_t[] bitmask if nullable
           │   (each region aligned)      │
           ├─────────────────────────────┤  heap_offset
           │ varchar heap                 │  bytes of non-inlined strings
           └─────────────────────────────┘  total_size
```

- **Fixed-width** columns (bool/int32/int64/float/double/decimal/date/time): the
  in-block array is a plain DuckDB-native value array. Decimal/date/time use
  DuckDB's physical layout (int16/32/64/128, int32 days, int64 micros).
- **Validity** is the DuckDB validity bitmask (`uint64_t` words, bit set = valid).
  A non-nullable or all-valid column stores no mask (`validity_offset == 0`).
- **VARCHAR** is a DuckDB `string_t[]` array. Strings ≤ 12 bytes are inlined in
  the `string_t`. Longer strings live in the varchar heap; their `string_t`
  pointer slot holds an **in-block offset** (not a pointer) on disk.

## Adapter (`adapter.hpp`)

- `ImportBlock(base, size, chunk, schema)` — builds a `DataChunk` whose `Vector`s
  reference the block: **zero-copy** for fixed-width data and validity. VARCHAR
  `string_t` offsets are **swizzled in place** to absolute pointers. The chunk
  borrows the block; the block must outlive it and is mutated by the swizzle.
- `ExportChunk(schema, chunk, alloc)` — materializes a `DataChunk` into a fresh
  plume-owned contiguous block, de-swizzling VARCHAR back to in-block offsets.
  Returns `OwnedBlock{data, size}`.
- `ParseSchema(base, size)` — reads header + schema without copying data.

All block memory is owned by plume via `plume::Allocator` (TASK.md A4); there is
no DuckDB instance or BufferManager.

## Tested (`tests/test_block_roundtrip.cpp`)

All v1 types incl. nulls and short/long/empty VARCHAR: value-equality across
export→import, byte-stability of re-export vs. the pristine block, the zero-copy
property for fixed-width imports, and the no-mask-when-all-valid invariant.

---

# Pipeline & expression IR (Milestone 3)

## Expression representation (R2, refines C13)

DuckDB's own bound-expression deserialization re-resolves functions through the
catalog + `ClientContext`, which we cannot have. So plume uses its own IR:

- **`expression.hpp` (`ExprNode`)** — a compact tagged tree: reference, constant,
  function, comparison, conjunction, operator (cast/aggregate/between/case are
  reserved enum values for later milestones). The *client* (which has a
  DuckDB instance) binds SQL and translates the bound tree into this IR.
- **`expression_builder.hpp` (`BuildExpression`)** — rebuilds DuckDB bound
  `Expression` objects from the IR with **no instance**, feeding
  `ExpressionExecutor` directly.
- **`function_registry.hpp` (C14)** — a compile-time `name -> ScalarFunctionSet
  factory` map. Functions are resolved by calling DuckDB's `GetFunctions()`
  builders (e.g. `OperatorAddFun`) and matching argument types — **no catalog,
  no `ClientContext`**. Currently registers `+ - * / // %`. (Functions that need
  a bind callback, e.g. decimal arithmetic, are rejected for now — they need a
  context we don't have.)

## Pipeline description (`pipeline.hpp`, C11/C12)

`PipelineDesc` = input `Schema` + a linear `vector<OperatorDesc>`. Operators:
PROJECTION (expressions), FILTER (predicate), LIMIT (limit/offset), plus
AGGREGATE and ORDER_BY structures reserved for M5.

## Binary format (`serialization.hpp`, C10)

A compact, little-endian, length-prefixed format owned by plume (magic `PLP1`,
versioned). Self-contained — no DuckDB serializer dependency — so it stays
catalog/instance-free. Constant `Value`s are encoded per v1 type.

## Tested (`tests/test_pipeline_ir.cpp`)

Pipeline serialize→deserialize round-trip with byte-stable re-serialization, and
the **R2 end-to-end proof**: build IR (`col0+col1`, `col0<100 AND col1>0`),
reconstruct bound expressions via the registry, execute through a context-less
`ExpressionExecutor`, and verify results — all with no DuckDB instance.

---

# Operators & execution driver (Milestone 4)

Push-based, single-threaded, chunk-by-chunk (TASK.md §4). All streaming
(non-blocking) for now; blocking operators (aggregate, sort) come in M5.

## Operators (`operator.hpp`)

- **ProjectionOperator** — `ExpressionExecutor::Execute` over the projection
  expressions into an allocator-backed output chunk.
- **FilterOperator** — `ExpressionExecutor::SelectExpression` → `SelectionVector`
  → zero-copy `Slice` of the input chunk (no data copy).
- **LimitOperator** — offset/limit row windowing with state spanning chunks;
  reports `Finished()` once the limit is reached so the driver can stop early.

Each `Operator` exposes its output `LogicalType`s and an internal output chunk
returned by `Execute(input)` (valid until the next call).

## Driver + builder (`executor.hpp`, `PipelineExecutor`)

- Builds the operator chain from a `PipelineDesc`, resolving expressions via the
  R2 shim (no instance) and threading types/schema through to compute the output
  `Schema` (projection reuses a bare reference's input column name, else
  `exprN`).
- `Execute(input_blocks)`: for each block, `ImportBlock` (zero-copy) → push
  through the chain → `ExportChunk` non-empty results into owned output blocks.
  Installs the plume allocator as DuckDB's default so context-less kernel
  scratch is plume-owned (A4). Honors `LIMIT` early-stop across blocks.

## Tested (`tests/test_pipeline_execution.cpp`)

End-to-end with no instance: `WHERE a<100 AND b>0; SELECT a+b, a; LIMIT 3
OFFSET 1` over two input blocks (limit/offset state spans blocks), run through
serialize→deserialize→execute; plus a projection-arithmetic case. Output blocks
re-imported and values verified.

---

# Aggregation & sort (Milestone 5, R3)

Both are pipeline-breaking (blocking): they buffer all input then emit (D17).
The driver's staged chunk-list model handles this uniformly. R3 is resolved with
the TASK's sanctioned "simplified in-memory" approach — reuse DuckDB's compute
kernels where they're instance-free, and reimplement only the parts coupled to
the `BufferManager`.

## Aggregate (`aggregate.hpp`)

- **Grouping**: a plume-owned hash map (group-key bytes → group index), *not*
  `GroupedAggregateHashTable` (which needs the `BufferManager`). Group-key cells
  are encoded to bytes by physical width.
- **count / count_star / sum / avg**: reuse DuckDB `AggregateFunction` kernels
  directly (`initialize` / `simple_update` for ungrouped, `update` for grouped,
  `finalize`), states allocated in a plume-backed `ArenaAllocator`.
- **min / max**: plume-native value reductions. DuckDB's min/max are `{ANY}→ANY`
  with a bind callback that requires a `ClientContext` we don't have, so we
  compute them directly via `Value` comparison (correct, instance-free).
- Ungrouped aggregation always emits exactly one row (even over empty input).
- Aggregates whose result type isn't a v1 block type (e.g. `sum(int)→HUGEINT`)
  are out of scope for now — declare a supported result type.

## Sort (`sort.hpp`)

Plume-owned, **stable**, multi-key in-memory sort (avoids DuckDB's
buffer-manager-coupled sort). Materializes sort-key `Value`s per row, stable-sorts
an index permutation, then gathers full input rows into sorted output chunks.
Honors ASC/DESC and absolute NULLS FIRST/LAST. Output schema is unchanged; order
is the only added guarantee (D18).

## Tested (`tests/test_aggregate.cpp`, `tests/test_sort.cpp`)

No instance: ungrouped count*/min/max/sum/avg; grouped `GROUP BY g → sum, count`;
ORDER BY single/multi-key, ASC/DESC, NULLS LAST with stability — across multiple
input blocks.

---

# Host boundary & end-to-end (Milestone 6, D16/R6)

## `plume::Runner` (`runner.hpp`)

The thin **noexcept** entry point for the host. DuckDB kernels throw internally;
`Runner::Run(pipeline_bytes, inputs)` catches everything and returns a `Status`
(`OK` / `INVALID_INPUT` / `NOT_IMPLEMENTED` / `ERROR`) — no exception crosses the
boundary. Outputs are exposed as one plume-owned `(pointer, size)` block per
chunk (B8), freed on the next `Run()` or on destruction. The `Runner` owns the
plume `Allocator`, so it also owns all output memory.

## End-to-end example (`examples/end_to_end.cpp`, target `plume_example`)

Full library with no instance: the host builds DuckDB-native input blocks,
serializes a pipeline (`WHERE amount>0; GROUP BY region → sum,count; ORDER BY
region`), runs it through the `Runner`, and reads results back from the output
`(pointer, size)` blocks.

## Tested (`tests/test_runner.cpp`)

Valid pipeline → `OK` + correct output; unknown function → `NOT_IMPLEMENTED`
(no throw escapes); malformed blob → `INVALID_INPUT`.
