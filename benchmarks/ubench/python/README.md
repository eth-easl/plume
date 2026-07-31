# ubench trace tools

Reads the `.trace` files `ubench_run --trace out.trace` writes (format:
[`../docs/trace-format.md`](../docs/trace-format.md)) and summarizes where time
went, per stage / operator / phase.

- `trace_reader.py` — binary parser + aggregation (stdlib only: `struct`,
  `dataclasses`). Import it directly if you want the raw `Trace` /
  `StageBreakdown` objects for your own analysis.
- `summarize_trace.py` — CLI: a text summary always; `--plot` additionally
  renders a per-stage breakdown chart (needs `matplotlib`).

```bash
python3 summarize_trace.py query.trace
python3 summarize_trace.py query.trace --plot breakdown.png --unit us --top 20
```

Text output has three parts: per-stage totals, a phase/operator breakdown for
each stage (in pipeline order), and a flat "hottest slots" list across the
whole trace.

The chart is a stacked horizontal bar per stage — not a literal Gantt — since a
trace only carries summed durations per invocation, not real timestamps. Each
segment is the operator's **exclusive** PUSH time (inclusive PUSH minus the
next operator's, per the trace-format doc) or its FINISH time (hatched, same
color as its operator). Color is fixed per operator type across the whole
chart, not per stage, so e.g. every `AGGREGATE` segment is the same hue
wherever it appears.
