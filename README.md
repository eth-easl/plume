<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="logo/plume_logo.svg" height="150">
    <source media="(prefers-color-scheme: dark)" srcset="logo/plume_logo_dark.svg" height="150">
    <img alt="Plume logo" src="logo/plume_logo.svg" height="150">
  </picture>
</div>
<br>

## Plume

Plume executes data analytics queries on [Dandelion](https://github.com/eth-easl/dandelion) leveraging [DuckDB](https://github.com/duckdb/duckdb). It parses SQL queries using the standard DuckDB query parser and optimizer, then converts the resulting DuckDB LogicalPlan to a Plume PhysicalPlan (Dandelion composition DAG). The plan is executed using a set of operators built on top of DuckDB's internal compute kernels without using a full-fledged DuckDB instance to keep the functions lightweight.

## Layout

The repository is structured as follows:

- `core/` — the query engine + SQL compiler, built as the `plume` library shared by `functions/` and `client/`.
- `functions/` — the Dandelion-invoked function executables (`plume_stage`, `plume_{csv,pq}_{prepare,stage}`) and their host ABI.
- `client/` — the SQL -> Dandelion-composition compiler plus the `plume_export`/ `plume` binaries
- `benchmarks/` — the TPC-H harness + a simple micro-benchmark tool, built against `client`.
- `cmake/` — shared CMake modules (DuckDB, cpr, nlohmann_json) and the tracked DuckDB extraction patches.
- `dandelion/` — the Dandelion preload config listing the function binaries to deploy.
- `scripts/` — setup and build scripts, including the two below.

## Building the functions

`scripts/build_dandelion.sh` cross-compiles `functions/` with the Dandelion SDK compiler and builds DuckDB for the target platform. Output binaries land in `<build_dir>/functions/`.

```bash
# (optionally) install LLVM, Cmake, libssl-dev, ninja
./scripts/setup_cloudlab.sh

# fetches the dsdk compiler if not provided (all flags have defaults)
./scripts/build_dandelion.sh -p <platform> -b <build_dir> -c <dsdk_compiler_path> -t <cpu_arch>
```

## Building the client / benchmarks

`scripts/build_client.sh` builds `client` + `benchmarks` with the host compiler. Output binaries land in `<build_dir>/plume/client/` and `<build_dir>/plume/benchmarks/`.

```bash
# (optionally) install LLVM, Cmake, libssl-dev, ninja
./scripts/setup_cloudlab.sh

# builds
./scripts/build_client.sh -b <build_dir> -d <duckdb_dir>
```
