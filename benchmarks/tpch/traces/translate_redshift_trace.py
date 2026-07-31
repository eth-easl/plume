#!/usr/bin/env python3
"""
Translate a raw Redshift-serverless-style query trace into plume's trace
format (arrival_timestamp, query, scale_factor).

The raw format carries a `sql` column with the literal query text against schema-qualified tables 
(e.g. tpch_sf10.lineitem) plus per-scale-factor substituted literals, rather than plume's `qN` query
names. This script matches each row's `sql` against the canonical templates under queries/*.sql 
(ignoring literals, table-qualification style, and aliases) to recover the query name, and derives 
the scale factor from the schema prefix on the referenced tables.

Usage:
    python3 translate_redshift_trace.py traces/redshift_slowed.csv > traces/redshift_slowed_translated.csv
"""

import csv
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
QUERIES_DIR = SCRIPT_DIR / "../queries"

_STRING_LITERAL_RE = re.compile(r"'[^']*'")
_SCHEMA_PREFIX_RE = re.compile(r"tpch_sf(\d+)")
_TOKEN_RE = re.compile(r"[a-z_][a-z0-9_]*")


# Reduce a query to the bag of identifier/keyword tokens that identify its shape.
# Discards literals, table-qualification style, brackets, and single-letter aliases. 
def normalize_tokens(sql: str) -> set:
    s = sql.lower()
    s = _STRING_LITERAL_RE.sub(" ", s)
    s = _SCHEMA_PREFIX_RE.sub("", s)
    s = s.replace("[", " ").replace("]", " ")
    tokens = _TOKEN_RE.findall(s)
    return {t for t in tokens if len(t) > 1}


def load_query_templates() -> dict:
    templates = {}
    for path in sorted(QUERIES_DIR.glob("q*.sql")):
        templates[path.stem] = normalize_tokens(path.read_text())
    if not templates:
        sys.exit(f"no query templates found under {QUERIES_DIR}")
    return templates


def match_query(sql: str, templates: dict) -> str:
    trace_tokens = normalize_tokens(sql)
    best_name, best_score = None, -1.0
    for name, template_tokens in templates.items():
        union = trace_tokens | template_tokens
        if not union:
            continue
        score = len(trace_tokens & template_tokens) / len(union)
        if score > best_score:
            best_name, best_score = name, score
    if best_name is None or best_score < 0.5:
        raise ValueError(f"no confident query match (best={best_name!r} score={best_score:.2f}) for sql: {sql[:200]}")
    return best_name


def find_scale_factor(row: dict) -> str:
    for field in ("read_tables", "sql", "read_table_ids", "write_table"):
        value = row.get(field) or ""
        m = _SCHEMA_PREFIX_RE.search(value.lower())
        if m:
            return f"sf{m.group(1)}"
    raise ValueError(f"could not determine scale factor for row: {row}")


def translate(input_path: Path, output_file) -> None:
    templates = load_query_templates()
    writer = csv.writer(output_file, lineterminator="\n")
    writer.writerow(["arrival_timestamp", "query", "scale_factor"])
    with open(input_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            query = match_query(row["sql"], templates)
            scale_factor = find_scale_factor(row)
            writer.writerow([row["arrival_timestamp"], query, scale_factor])


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <input_trace.csv>")
    translate(Path(sys.argv[1]), sys.stdout)


if __name__ == "__main__":
    main()
