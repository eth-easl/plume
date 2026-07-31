#!/usr/bin/env python3
"""Summarize (and optionally plot) a ubench `.trace` file.

    summarize_trace.py TRACE.trace [--plot out.png] [--top N] [--unit ms|us|ns]

Text output always works (stdlib only). `--plot` renders a per-stage
time-by-operator-and-phase breakdown and needs matplotlib (`pip install
matplotlib`); everything else works without it.

There is no literal timeline in a trace (only summed durations per
invocation), so this deliberately isn't a wall-clock Gantt chart — it's a
stacked bar per stage, in pipeline order, sized by accounted CPU time. See
docs/trace-format.md for exactly what's being summed.
"""

from __future__ import annotations

import argparse
import sys

from trace_reader import StageBreakdown, Trace, all_stage_breakdowns, invocation_count, load

UNIT_NS = {"ns": 1, "us": 1_000, "ms": 1_000_000}


def fmt_time(ns: int, unit: str) -> str:
    return f"{ns / UNIT_NS[unit]:,.1f} {unit}"


def pct(part: int, whole: int) -> str:
    return f"{100.0 * part / whole:5.1f}%" if whole else "    -%"


def print_summary(trace: Trace, breakdowns: list[StageBreakdown], top_n: int, unit: str) -> None:
    total_events = len(trace.events)
    ordered = sorted(breakdowns, key=lambda b: b.total_ns, reverse=True)
    grand_total = sum(b.total_ns for b in ordered)

    print(f"ubench trace: {trace.name!r}")
    print(f"  {len(trace.stages)} stages, {total_events} events")
    print("  (times are summed across all -- parallel -- invocations: aggregate CPU time, not wall-clock)")
    print()

    print("Per-stage totals (sorted by time desc):")
    header = f"  {'stage':>5}  {'source':<13} {'part':>4} {'inv':>4}  {'total':>12}  {'% of total':>10}"
    print(header)
    for b in ordered:
        inv = invocation_count(trace, b.stage.id)
        print(
            f"  {b.stage.id:>5}  {b.stage.source:<13} {b.stage.partitions:>4} {inv:>4}  "
            f"{fmt_time(b.total_ns, unit):>12}  {pct(b.total_ns, grand_total):>10}"
        )
    print(f"  {'':>5}  {'':<13} {'':>4} {'':>4}  {'-' * 12}")
    print(f"  {'':>5}  {'':<13} {'':>4} {'':>4}  {fmt_time(grand_total, unit):>12}  (grand total)")
    print()

    print("Per-stage breakdown (phase / operator, in pipeline order):")
    for b in ordered:
        inv = invocation_count(trace, b.stage.id)
        print(
            f"\nStage {b.stage.id} ({b.stage.source}, {b.stage.partitions} partitions, {inv} invocations) "
            f"-- {fmt_time(b.total_ns, unit)}:"
        )
        rows = []
        if b.setup_hits:
            rows.append(("(stage) setup", b.setup_hits, b.setup_ns))
        if b.deserialize_hits:
            rows.append(("(stage) deserialize", b.deserialize_hits, b.deserialize_ns))
        for op in b.ops:
            if op.push_excl_hits:
                rows.append((f"{op.label} push (excl)", op.push_excl_hits, op.push_excl_ns))
            if op.finish_hits:
                rows.append((f"{op.label} finish", op.finish_hits, op.finish_ns))
        for label, hits, ns in rows:
            print(f"    {label:<28} {hits:>6} hits  {fmt_time(ns, unit):>12}  {pct(ns, b.total_ns):>10}")

    print(f"\nHottest (stage, operator, phase) slots overall (top {top_n}):")
    flat = []
    for b in ordered:
        if b.setup_hits:
            flat.append((b.stage.id, "(stage) setup", b.setup_hits, b.setup_ns))
        if b.deserialize_hits:
            flat.append((b.stage.id, "(stage) deserialize", b.deserialize_hits, b.deserialize_ns))
        for op in b.ops:
            if op.push_excl_hits:
                flat.append((b.stage.id, f"{op.label} push (excl)", op.push_excl_hits, op.push_excl_ns))
            if op.finish_hits:
                flat.append((b.stage.id, f"{op.label} finish", op.finish_hits, op.finish_ns))
    flat.sort(key=lambda r: r[3], reverse=True)
    for i, (stage_id, label, hits, ns) in enumerate(flat[:top_n], start=1):
        print(
            f"  {i:>3}. stage {stage_id:<4} {label:<28} {hits:>6} hits  "
            f"{fmt_time(ns, unit):>12}  {pct(ns, grand_total):>10}"
        )


# --- plotting (optional; only imports matplotlib if --plot is requested) ---

CATEGORY_ORDER = ["OVERHEAD", "JOIN", "FILTER", "PROJECTION", "AGGREGATE", "ORDER_BY", "LIMIT", "OUTPUT"]
CATEGORY_COLOR = {
    "OVERHEAD": "#2a78d6",
    "JOIN": "#1baf7a",
    "FILTER": "#eda100",
    "PROJECTION": "#008300",
    "AGGREGATE": "#4a3aa7",
    "ORDER_BY": "#e34948",
    "LIMIT": "#e87ba4",
    "OUTPUT": "#eb6834",
}
FALLBACK_COLOR = "#898781"  # an operator type outside the fixed set (shouldn't happen)

SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"


def _op_category(label: str) -> str:
    return label.split("[", 1)[0]  # "FILTER[2]" -> "FILTER"


def render_plot(trace: Trace, breakdowns: list[StageBreakdown], out_path: str) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.patches as mpatches
        import matplotlib.pyplot as plt
    except ImportError as e:
        raise SystemExit("summarize_trace.py: --plot needs matplotlib (pip install matplotlib)") from e

    ordered = [b for b in sorted(breakdowns, key=lambda b: b.total_ns, reverse=True) if b.total_ns > 0]
    if not ordered:
        raise SystemExit("summarize_trace.py: nothing to plot (no timed events)")

    bar_height = 0.5
    fig_h = max(1.8, 0.62 * len(ordered) + 1.4)
    fig, ax = plt.subplots(figsize=(11, fig_h), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    used_categories: set[str] = set()
    has_finish = False
    y_ticks = []
    y_tick_labels = []

    for row, b in enumerate(ordered):
        y = len(ordered) - 1 - row  # hottest stage at the top
        y_ticks.append(y)
        y_tick_labels.append(f"stage {b.stage.id} ({b.stage.source})")
        x = 0.0

        overhead_ms = (b.setup_ns + b.deserialize_ns) / 1e6
        if overhead_ms > 0:
            ax.barh(y, overhead_ms, left=x, height=bar_height, color=CATEGORY_COLOR["OVERHEAD"],
                     edgecolor=SURFACE, linewidth=1.2)
            used_categories.add("OVERHEAD")
            x += overhead_ms

        # Streaming phase first (pipeline order), then the end-of-input flush phase --
        # PUSH happens while chunks stream through, FINISH only once input is exhausted.
        for op in b.ops:
            excl_ms = op.push_excl_ns / 1e6
            if excl_ms <= 0:
                continue
            cat = _op_category(op.label)
            color = CATEGORY_COLOR.get(cat, FALLBACK_COLOR)
            ax.barh(y, excl_ms, left=x, height=bar_height, color=color, edgecolor=SURFACE, linewidth=1.2)
            used_categories.add(cat)
            x += excl_ms

        for op in b.ops:
            finish_ms = op.finish_ns / 1e6
            if finish_ms <= 0:
                continue
            cat = _op_category(op.label)
            color = CATEGORY_COLOR.get(cat, FALLBACK_COLOR)
            ax.barh(y, finish_ms, left=x, height=bar_height, color=color, edgecolor=SURFACE,
                     linewidth=1.2, hatch="////")
            used_categories.add(cat)
            has_finish = True
            x += finish_ms

        ax.text(x + max(x, 1.0) * 0.015, y, f"{x:,.1f} ms", va="center", ha="left",
                 color=INK_PRIMARY, fontsize=9)

    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_tick_labels, color=INK_SECONDARY, fontsize=9)
    ax.set_xlabel("time (ms, summed across invocations)", color=INK_SECONDARY, fontsize=9)
    ax.set_title(f"ubench trace: {trace.name}", color=INK_PRIMARY, fontsize=12, loc="left", pad=12)

    ax.set_xlim(left=0)
    ax.margins(y=0.08)
    ax.grid(axis="x", color=GRIDLINE, linewidth=1, zorder=0)
    ax.set_axisbelow(True)
    for spine_name, spine in ax.spines.items():
        spine.set_visible(spine_name == "bottom")
        if spine_name == "bottom":
            spine.set_color(BASELINE)
    ax.tick_params(colors=INK_SECONDARY, length=0)

    legend_handles = [
        mpatches.Patch(facecolor=CATEGORY_COLOR[c], edgecolor=SURFACE, label=c.title().replace("_", " "))
        for c in CATEGORY_ORDER
        if c in used_categories
    ]
    if has_finish:
        legend_handles.append(
            mpatches.Patch(facecolor=INK_MUTED, edgecolor=SURFACE, hatch="////", label="finish phase (hatched)")
        )
    ax.legend(
        handles=legend_handles,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.14),
        ncol=min(len(legend_handles), 5),
        frameon=False,
        fontsize=9,
        labelcolor=INK_SECONDARY,
    )

    fig.savefig(out_path, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("trace", help="path to a ubench .trace file")
    parser.add_argument("--plot", metavar="PATH", help="write a per-stage breakdown chart here (needs matplotlib)")
    parser.add_argument("--top", type=int, default=12, help="how many hottest slots to list (default 12)")
    parser.add_argument("--unit", choices=["ns", "us", "ms"], default="ms", help="time unit for text output")
    args = parser.parse_args()

    try:
        trace = load(args.trace)
    except (OSError, ValueError) as e:
        print(f"summarize_trace.py: {e}", file=sys.stderr)
        return 1

    breakdowns = all_stage_breakdowns(trace)
    print_summary(trace, breakdowns, args.top, args.unit)

    if args.plot:
        render_plot(trace, breakdowns, args.plot)
        print(f"\nwrote chart to {args.plot}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
