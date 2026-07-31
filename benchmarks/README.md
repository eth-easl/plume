# Benchmark harness (`plume_bench`)

Config-driven runner for TPC-H-style SQL queries against a Dandelion instance. Built directly on `core` (reuses `CompileQuery` + the dandelion API), so it measures both planning time (compile) and execution time (invocation) in one
binary.

## Running

```bash
# run from build directory
./benchmarks/tpch/plume_bench <path-to-config>
```

Multiple configs run in sequence. A config with no `dandelionUrl` compiles every query (planning time only) and skips invocation.

## Queries

Plain `.sql` files under `tpch/queries/`, referencing tables as `[table]` placeholders resolved at run time against the config's data source — a bracket-list of split files when `tableNumFiles[table] > 1`, else a single path.

## Benchmark Types

- **single** — each query in `queries`, repeated `repetitions` times.
- **throughput** — one `query` fired at each `rpsValues` rate for `durationSec`
  seconds, no backpressure; reports successful req/s per rate.
- **trace** — replay a redshift-style trace CSV (`arrival_timestamp,query,
  scale_factor`) at its recorded offsets; `scaleFactors` maps each label to a
  data source.

## Config schema

| Key | Description |
|-|-|
| `queries` / `queryStoragePrefix` | Query file names / directory (single mode). |
| `tablePathPrefix` / `tablePathSuffix` / `tableNumFiles` | How `[table]` resolves to file path(s). |
| `dandelionUrl` / `requestTimeout` | Instance to invoke / per-request timeout (empty URL => compile only). |
| `resultsPrefix` | Output directory for result files (empty => console only). |
| `preAggregate`, `projectionPushdown`, `filterPushdown` | `CompileQuery` converter toggles (default on). |
| `maxSplits`, `targetRowsPerSplit`, `maxRegionSize` | `CompileQuery` converter split tuning. |
| `scaleFactor` | Scale-factor label used to look up expected checksums (trace mode uses its own column). |
| `checksumFile` | Path to checksum file for correctness checking (empty => no checks, see Correctness Checking below). |
| `benchmarkConfig` | Type-specific settings (see Benchmark types). |

## Correctness Checking

With `checksumFile` set, every result is checked against a precomputed, order-independent checksum (a wraparound sum of DuckDB's per-row `hash()`).

Generate checksums offline, against the exact DuckDB version `external/duckdb` pins (`hash()` has no cross-version stability guarantee):

```bash
pip install -r tpch/scripts/requirements.txt
python3 tpch/scripts/generate_checksums.py tpch/config/local_single_sf10.json
```

## Output

A per-query summary table on the console. With `resultsPrefix` set, also writes `planning.txt`, `timings.txt` (latencies, or per-rps runs for a throughput sweep), and `timestamps.txt` under that directory. Trace benchmarks further output a `trace_results.txt` file.
