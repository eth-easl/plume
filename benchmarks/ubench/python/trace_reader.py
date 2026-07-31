"""Parser and aggregation for ubench `.trace` files.

Binary layout is documented in `benchmarks/ubench/docs/trace-format.md`; this
mirrors it field-for-field. Stdlib only (`struct`), no third-party deps.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Dict, List, Tuple

MAGIC = 0x52544C50  # "PLTR"
SUPPORTED_VERSION = 1

SOURCE_KINDS = {0: "STAGE_OUTPUT", 1: "TABLE_BLOCKS", 2: "CSV", 3: "PARQUET"}

# exec::OpType values carried in the plan header's op_types bytes. 0xFF is the
# synthetic terminal output/serialize operator (always the last op index).
OP_TYPE_NAMES = {
    1: "PROJECTION",
    2: "FILTER",
    3: "LIMIT",
    4: "AGGREGATE",
    5: "ORDER_BY",
    6: "JOIN",
    0xFF: "OUTPUT",
}

PHASE_NAMES = {0: "SETUP", 1: "DESERIALIZE", 2: "PUSH", 3: "FINISH"}

STAGE_OP = -1  # trace::kStageOp: op_index sentinel for stage-level phases

_EVENT = struct.Struct("<HHIhBBIQ")  # stage_id, partition, invocation, op_index, phase, pad, hits, total_ns
assert _EVENT.size == 24


@dataclass
class Stage:
    id: int
    source: str
    partitions: int
    op_types: List[str]  # op_types[op_index]; last entry is always "OUTPUT"

    def op_label(self, op_index: int) -> str:
        if op_index == STAGE_OP:
            return "(stage)"
        if 0 <= op_index < len(self.op_types):
            return f"{self.op_types[op_index]}[{op_index}]"
        return f"op{op_index}"


@dataclass
class Event:
    stage_id: int
    partition: int
    invocation: int
    op_index: int
    phase: str
    hits: int
    total_ns: int


@dataclass
class Trace:
    name: str
    stages: Dict[int, Stage]
    events: List[Event]


def _read_exact(f, n: int) -> bytes:
    data = f.read(n)
    if len(data) != n:
        raise ValueError(f"truncated trace: expected {n} bytes, got {len(data)}")
    return data


def load(path: str) -> Trace:
    with open(path, "rb") as f:
        magic, version = struct.unpack("<II", _read_exact(f, 8))
        if magic != MAGIC:
            raise ValueError(f"'{path}' is not a ubench trace (bad magic 0x{magic:08x}, expected 0x{MAGIC:08x})")
        if version != SUPPORTED_VERSION:
            raise ValueError(f"unsupported trace version {version} (this reader supports {SUPPORTED_VERSION})")

        (name_len,) = struct.unpack("<I", _read_exact(f, 4))
        name = _read_exact(f, name_len).decode("utf-8", errors="replace")

        (num_stages,) = struct.unpack("<I", _read_exact(f, 4))
        stages: Dict[int, Stage] = {}
        for _ in range(num_stages):
            stage_id, source_byte, partitions, num_op_types = struct.unpack("<IBII", _read_exact(f, 13))
            op_type_bytes = _read_exact(f, num_op_types)
            op_types = [OP_TYPE_NAMES.get(b, f"UNKNOWN(0x{b:02x})") for b in op_type_bytes]
            stages[stage_id] = Stage(
                id=stage_id,
                source=SOURCE_KINDS.get(source_byte, f"UNKNOWN({source_byte})"),
                partitions=partitions,
                op_types=op_types,
            )

        (num_events,) = struct.unpack("<Q", _read_exact(f, 8))
        buf = _read_exact(f, num_events * _EVENT.size)
        events: List[Event] = []
        for i in range(num_events):
            stage_id, partition, invocation, op_index, phase_byte, _pad, hits, total_ns = _EVENT.unpack_from(
                buf, i * _EVENT.size
            )
            events.append(
                Event(
                    stage_id=stage_id,
                    partition=partition,
                    invocation=invocation,
                    op_index=op_index,
                    phase=PHASE_NAMES.get(phase_byte, f"?{phase_byte}"),
                    hits=hits,
                    total_ns=total_ns,
                )
            )

    return Trace(name=name, stages=stages, events=events)


@dataclass
class Segment:
    """One (stage, op_index, phase) slot, aggregated across every invocation/partition."""

    stage_id: int
    op_index: int
    phase: str
    hits: int
    total_ns: int


def aggregate(trace: Trace) -> List[Segment]:
    """Sum hits/total_ns for every event sharing a (stage, op_index, phase) key."""
    acc: Dict[Tuple[int, int, str], List[int]] = {}
    for e in trace.events:
        slot = acc.setdefault((e.stage_id, e.op_index, e.phase), [0, 0])
        slot[0] += e.hits
        slot[1] += e.total_ns
    return [
        Segment(stage_id=k[0], op_index=k[1], phase=k[2], hits=v[0], total_ns=v[1]) for k, v in acc.items()
    ]


def invocation_count(trace: Trace, stage_id: int) -> int:
    """Distinct (stage, invocation) ids seen for a stage — how many times it ran."""
    return len({e.invocation for e in trace.events if e.stage_id == stage_id})


@dataclass
class OpBreakdown:
    """Exclusive time for one op index within a stage.

    push_excl_ns is the operator's own share of PUSH time: inclusive PUSH minus
    the next op's inclusive PUSH (docs/trace-format.md), since PUSH is inclusive
    of everything downstream on a linear pipeline. finish_ns is not adjusted —
    FINISH already only covers that operator's own flush work.
    """

    op_index: int
    label: str
    push_excl_ns: int
    push_excl_hits: int
    finish_ns: int
    finish_hits: int


@dataclass
class StageBreakdown:
    stage: Stage
    setup_ns: int
    setup_hits: int
    deserialize_ns: int
    deserialize_hits: int
    ops: List[OpBreakdown]  # in pipeline order, op_index 0..n-1 (last is OUTPUT)

    @property
    def total_ns(self) -> int:
        return (
            self.setup_ns
            + self.deserialize_ns
            + sum(op.push_excl_ns + op.finish_ns for op in self.ops)
        )


def stage_breakdown(stage: Stage, segments: List[Segment]) -> StageBreakdown:
    by_key = {(s.op_index, s.phase): s for s in segments if s.stage_id == stage.id}

    def get(op_index: int, phase: str) -> Segment:
        return by_key.get((op_index, phase))

    def ns(op_index: int, phase: str) -> int:
        s = get(op_index, phase)
        return s.total_ns if s else 0

    def hits(op_index: int, phase: str) -> int:
        s = get(op_index, phase)
        return s.hits if s else 0

    n = len(stage.op_types)
    push_inclusive = [ns(i, "PUSH") for i in range(n)]

    ops: List[OpBreakdown] = []
    for i in range(n):
        downstream = push_inclusive[i + 1] if i + 1 < n else 0
        excl = max(push_inclusive[i] - downstream, 0)
        ops.append(
            OpBreakdown(
                op_index=i,
                label=stage.op_label(i),
                push_excl_ns=excl,
                push_excl_hits=hits(i, "PUSH"),
                finish_ns=ns(i, "FINISH"),
                finish_hits=hits(i, "FINISH"),
            )
        )

    return StageBreakdown(
        stage=stage,
        setup_ns=ns(STAGE_OP, "SETUP"),
        setup_hits=hits(STAGE_OP, "SETUP"),
        deserialize_ns=ns(STAGE_OP, "DESERIALIZE"),
        deserialize_hits=hits(STAGE_OP, "DESERIALIZE"),
        ops=ops,
    )


def all_stage_breakdowns(trace: Trace) -> List[StageBreakdown]:
    segments = aggregate(trace)
    return [stage_breakdown(stage, segments) for stage in trace.stages.values()]
