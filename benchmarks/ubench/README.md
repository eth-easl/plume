# ubench — per-operator compute analysis

A micro-benchmark tool that studies **where time goes** when executing a query's
stage DAG, per function invocation: setup (until the executor is built),
deserialization, per-operator compute (`Push`/`Finish`), and output
serialization. It runs the **real** v2 function engine (`fn::RunStage` /
`RunCSVStage` / `RunParquetStage`) in-process, data-parallel across pinned
threads, resolving file fetches from the local filesystem instead of HTTP.

It is split into two independently-linked pieces:

- **`ubench_export`** — compiles SQL with the client (a real DuckDB instance)
  into a self-contained `.ub` plan. The only piece that depends on the client /
  DuckDB; it precomputes everything the runner needs (plan, materialized table
  blocks, CSV/parquet chunk-info + fetch-request items).
- **`ubench_run`** — the standalone runner. Replays a `.ub` plan through a
  *traced* recompile of the core + function-stage sources (`ubench_engine`) on a
  core-pinned thread pool, and writes a per-operator trace. No client, no DuckDB
  instance.

## Building

Part of the umbrella build (needs the client + the function sources):

```bash
cmake -B build/plume
cmake --build build/plume -j --target ubench_export ubench_run
```

Disable with `-DPLUME_BUILD_UBENCH=OFF`. `ubench_run` and its `ubench_engine`
are always compiled with `-DPLUME_UBENCH_TRACE`; the normal `plume` library and
function binaries are **not**, so tracing adds zero overhead outside ubench.

## Usage

Reference data sources as table references in the SQL. A `file://…` (or
`http(s)://`) reference becomes a CSV/parquet **decode stage**; a bare path
(`orders.csv`) is materialized to blocks. Decode-stage urls are resolved by
scheme at export time: `http://`/`https://` are fetched over the network,
everything else (a bare path or `file://` url, with `~` expanded against
`$HOME`) is read from the local filesystem — so a query can freely mix local
and remote table references.

```bash
# 1. compile SQL -> plan
./build/plume/benchmarks/ubench/ubench_export \
    "SELECT k, sum(v) AS s FROM file:///data/big.csv GROUP BY k" -o q.ub

# 2. run it via a JSON config (8 core-pinned workers, emit a trace)
./build/plume/benchmarks/ubench/ubench_run run.json
```

### `ubench_export [sql]`

| flag | meaning |
|------|---------|
| `-o, --output PATH` | output `.ub` file (default `query.ub`) |
| `--name NAME` | plan label recorded in the trace |
| `--no-pre-aggregate` / `--no-projection-pushdown` / `--no-filter-pushdown` | client compile toggles |

SQL is read from the argument or stdin.

### `ubench_run CONFIG.json`

The runner is configured entirely by a JSON file (see
[`config/run.example.json`](config/run.example.json)):

| field | meaning |
|-------|---------|
| `plan` | the compiled `.ub` file to replay (**required**) |
| `threads` | worker threads, each pinned to a core (default/`0`: hw concurrency). An **execution-resource** knob — independent of the plan's fan-out. |
| `coreOffset` | first core to pin to (default 0) |
| `repartition` | override the fan-out of every data-parallel (shuffle) stage to this value; gather stages (already single-partition) stay 1. Rewrites both the stage record and its pipeline template's `OutputSplit`, so use it to sweep parallelism for a fixed query. `0` (default) leaves the plan unchanged. |
| `rewrites` | object of `FROM: TO` url-prefix substitutions for local fetch resolution |
| `reps` | repeat this many times, report each wall time (default 1) |
| `trace` | write the collected trace to this path (omit to skip) |
| `quiet` | suppress the progress lines (default false) |

## Execution model

Mirrors dandelion's data sharding (see `client/src/dandelion/composition.cpp`):

- **TABLE_BLOCKS** → one `all` invocation over the whole materialized table.
- **CSV / PARQUET** → one invocation per source key (`anyKeyed` chunk/region
  sharding): each csv chunk / parquet region decodes independently, in parallel.
- **STAGE_OUTPUT** → one invocation per producer output partition; a join reads
  partition `p` of each co-partitioned input side.

A stage's independent invocations run in parallel on the pool; stages run in
dependency order (the plan is topologically ordered). Each producer's output is
kept partitioned by the `OutputSplit` key each block was tagged with, so a
consumer reads the right partition.

## Trace output

A compact binary (no JSON) that connects every timing back to the concrete
operator in the DAG. Layout is documented in [`docs/trace-format.md`](docs/trace-format.md);
the `.ub` plan format lives in [`src/format/plan.hpp`](src/format/plan.hpp).

## Layout

```
benchmarks/ubench/
  src/
    export_main.cpp  ubench_export — client-linked SQL -> .ub
    run_main.cpp     ubench_run — JSON-configured replay + trace
    format/          the .ub plan model + binary (de)serializer (no client/DuckDB deps)
    runner/          thread pool, thread-local ABI backend, local fetch,
                     stage-graph runner, trace writer, run config
  config/            example ubench_run config
  docs/              trace format reference
```
