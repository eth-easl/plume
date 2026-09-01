#!/usr/bin/env python3
"""Generates the expected-checksum file plume_bench uses to validate query
correctness.

Runs each (query, scale factor) combination named by one or more
plume_bench-style config JSON files through a real DuckDB instance and
computes, per row, the same `hash(col1, col2, ...)` DuckDB uses internally for
joins/group-by (see benchmarks/runner/src/checksum.hpp) — summed mod 2**64
(order-independent) across all rows, paired with the row count. plume_bench
computes the identical checksum from a query's dandelion result and compares.

This must run against the *exact* DuckDB version plume vendors (see
requirements.txt) — hash() is an internal, non-cryptographic implementation
detail with no cross-version stability guarantee.

Usage:
    python3 generate_checksums.py <config.json> [<config.json> ...] [-o OUT]

Each config is the same JSON plume_bench itself reads (see ../README.md). Two
fields matter here that plume_bench also reads for correctness checking: a
top-level "scaleFactor" label for single/throughput configs (trace configs
already carry a label per scale factor), and "checksumFile" naming the output
path plume_bench should read from — used here too when -o/--output isn't
given, so a config and its checksums stay paired.

The output file is merged, not overwritten: existing (query, scale factor)
entries are updated in place and everything else is preserved, so this can be
run once per scale factor / config as they become available.
"""

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("error: the 'duckdb' package is required -- pip install -r requirements.txt")

MASK64 = (1 << 64) - 1
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PLACEHOLDER_RE = re.compile(r"\[([^\[\]]*)\]")


def expand_home(p):
    return os.path.expanduser(p) if p else p


# Mirrors SourceExpr in ../src/runner.cpp: a `[table]` placeholder resolves to
# a single quoted path, or a bracket-list of `.1..N` paths for a split table.
def read_fn(suffix):
    if suffix == ".parquet":
        return "read_parquet"
    if suffix == ".csv":
        return "read_csv"
    sys.exit(f"error: don't know how to read a multi-file '{suffix}' table (only .parquet/.csv are supported).")


def source_expr(table, prefix, suffix, num_files):
    n = num_files.get(table)
    base = f"{prefix}/{table}/{table}"
    if n is None or n <= 1:
        if n == 1:
            # DuckDB's replacement scan only accepts a bare string, not a
            # single-element list, as a table reference -- unlike ../src/runner.cpp's
            # SourceExpr (whose `['...']` text is resolved by the client's own
            # SQL scanner, not DuckDB's parser).
            return f"{read_fn(suffix)}('{base}.1{suffix}')"
        return f"'{base}{suffix}'"
    parts = ", ".join(f"'{base}.{i + 1}{suffix}'" for i in range(n))
    return f"{read_fn(suffix)}([{parts}])"


# Mirrors ResolvePlaceholders in ../src/runner.cpp.
def resolve_placeholders(sql, source):
    def repl(m):
        inner = m.group(1).strip()
        if IDENT_RE.match(inner):
            return source_expr(inner, source["prefix"], source["suffix"], source["numFiles"])
        return m.group(0)

    return PLACEHOLDER_RE.sub(repl, sql)


def parse_source(obj):
    return {
        "prefix": expand_home(obj.get("tablePathPrefix", "")),
        "suffix": obj.get("tablePathSuffix", ".parquet"),
        "numFiles": {k: int(v) for k, v in obj.get("tableNumFiles", {}).items()},
    }


# Independent execution engines (plume's pipeline vs. this reference DuckDB
# run) can produce bit-different DOUBLE/FLOAT values for the same aggregate --
# floating-point summation isn't associative, so the same numbers summed in a
# different order round differently in the last couple of bits. hash() is
# bit-exact, so an unrounded FLOAT/DOUBLE column would make the checksum
# spuriously fail. Round to 6 decimal places before hashing -- must match
# CanonicalizeDouble in ../src/checksum.cpp. TPC-H's only FLOAT/DOUBLE outputs
# are AVG() results of modest magnitude, where 1e-6 is many orders of
# magnitude looser than the ~1e-10..1e-15 noise floor, so this doesn't mask
# genuine correctness bugs.
def hash_expr(name, type_str):
    quoted = '"' + name.replace('"', '""') + '"'
    if type_str in ("DOUBLE", "FLOAT"):
        return f"round({quoted}, 6)"
    return quoted


def compute_checksum(con, sql):
    sql = sql.strip().rstrip(";").strip()
    desc = con.execute(f"SELECT * FROM ({sql}) __plume_q LIMIT 0").description
    if not desc:
        return {"rowCount": 0, "hashSum": 0}
    exprs = ", ".join(hash_expr(d[0], str(d[1])) for d in desc)
    rows = con.execute(f"SELECT hash({exprs}) FROM ({sql}) __plume_q").fetchall()
    hash_sum = 0
    for (h,) in rows:
        hash_sum = (hash_sum + h) & MASK64
    return {"rowCount": len(rows), "hashSum": hash_sum}


def load_trace_pairs(path):
    pairs = set()
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            pairs.add((row["query"], row["scale_factor"]))
    return sorted(pairs)


# Runs every (query, scale factor) pair named by `config_path`, adding entries
# to `results` (query -> scale_factor -> {rowCount, hashSum}). Returns the
# config's own "checksumFile", if it has one.
def process_config(con, config_path, results):
    with open(config_path) as f:
        cfg = json.load(f)

    query_dir = expand_home(cfg.get("queryStoragePrefix", "."))
    bench_type = cfg.get("benchmarkType", "single")
    bench_cfg = cfg.get("benchmarkConfig", {})

    pairs = []  # (query_stem, scale_factor_label, source)
    if bench_type == "trace":
        trace_path = expand_home(bench_cfg["path"])
        scale_factors = {label: parse_source(obj) for label, obj in bench_cfg.get("scaleFactors", {}).items()}
        for query, sf_label in load_trace_pairs(trace_path):
            if sf_label not in scale_factors:
                print(f"warning: trace scale factor '{sf_label}' has no source config in '{config_path}'; "
                      f"skipping '{query}'.", file=sys.stderr)
                continue
            pairs.append((query, sf_label, scale_factors[sf_label]))
    else:
        scale_factor = cfg.get("scaleFactor")
        if not scale_factor:
            sys.exit(f"error: '{config_path}' has no 'scaleFactor' -- add one so checksums can be looked up "
                      f"by (query, scale factor), matching what plume_bench will send.")
        source = parse_source(cfg)
        queries = cfg.get("queries", [])
        if bench_type == "throughput":
            q = bench_cfg.get("query") or (queries[0] if queries else None)
            queries = [q] if q else []
        for qf in queries:
            pairs.append((Path(qf).stem, scale_factor, source))

    for query, sf_label, source in pairs:
        sql_path = os.path.join(query_dir, query if query.endswith(".sql") else query + ".sql")
        with open(sql_path) as f:
            sql = resolve_placeholders(f.read(), source)
        print(f" > {query} / {sf_label} ...", end=" ", flush=True)
        entry = compute_checksum(con, sql)
        print(f"rowCount={entry['rowCount']} hashSum=0x{entry['hashSum']:016x}")
        results.setdefault(query, {})[sf_label] = entry

    return expand_home(cfg.get("checksumFile"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("configs", nargs="+", help="plume_bench-style config JSON file(s)")
    ap.add_argument("-o", "--output", help="checksum file to write (overrides each config's 'checksumFile')")
    args = ap.parse_args()

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
    except Exception as e:
        print(f"warning: could not load the httpfs extension ({e}); remote paths may fail.", file=sys.stderr)

    for config_path in args.configs:
        print(f"=== {config_path} ===")
        results = {}
        default_output = process_config(con, config_path, results)
        output_path = expand_home(args.output) if args.output else default_output
        if not output_path:
            sys.exit(f"error: no --output given and '{config_path}' has no 'checksumFile'.")

        existing = {}
        if os.path.exists(output_path):
            with open(output_path) as f:
                existing = json.load(f)
        for query, per_sf in results.items():
            existing.setdefault(query, {})
            for sf_label, entry in per_sf.items():
                existing[query][sf_label] = {
                    "rowCount": entry["rowCount"],
                    "hashSum": f"0x{entry['hashSum']:016x}",
                }

        out_dir = os.path.dirname(output_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(existing, f, indent=2, sort_keys=True)
            f.write("\n")
        print(f" > wrote '{output_path}'")


if __name__ == "__main__":
    main()
