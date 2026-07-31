# ubench trace format

`ubench_run --trace out.trace` writes a compact binary of the timings collected
during a traced run. It has two parts: a **plan header** that maps every
`(stage, operator)` slot back to its place in the DAG, and a flat array of
fixed-width **event** records. All integers are **little-endian / host-endian**
(a trace is produced and consumed on the same machine) and there is no padding
beyond what is stated.

Two independent magics are involved: the `.ub` plan file starts with `PLUB`
(`0x42554C50`); this trace file starts with `PLTR` (`0x52544C50`).

## Layout

```
┌─ header ────────────────────────────────────────────────┐
│ u32   magic      = 0x52544C50 ("PLTR")                   │
│ u32   version    = 1                                     │
│ u32   name_len                                           │
│ u8[]  name       (name_len bytes, the plan/query label)  │
│ u32   num_stages                                         │
│ ── per stage (num_stages times) ──────────────────────── │
│   u32  stage_id                                          │
│   u8   source        (UbSourceKind: 0 STAGE_OUTPUT,      │
│                        1 TABLE_BLOCKS, 2 CSV, 3 PARQUET)  │
│   u32  partitions    (output fan-out)                    │
│   u32  num_op_types                                      │
│   u8[] op_types      (num_op_types bytes; see below)     │
├─ events ────────────────────────────────────────────────┤
│ u64   num_events                                         │
│ Event[num_events]    (24 bytes each, packed)             │
└─────────────────────────────────────────────────────────┘
```

### Operator-type bytes

`op_types[i]` is the type of the operator at op index `i` in that stage's
pipeline. The fused operators come first, in pipeline order (a leading join, when
present, is index 0), using the `exec::OpType` enum:

| value | operator   |
|------:|------------|
| 1     | PROJECTION |
| 2     | FILTER     |
| 3     | LIMIT      |
| 4     | AGGREGATE  |
| 5     | ORDER_BY   |
| 6     | JOIN       |
| 0xFF  | OUTPUT (the streaming output/serialize operator, always last) |

So `num_op_types == (#fused operators) + 1`, and the last entry is always `0xFF`
for the terminal output operator (op index `#fused operators`).

### Event record (24 bytes, packed)

```
offset size field       meaning
  0     u16  stage_id    the stage this timing belongs to
  2     u16  partition   the input partition this invocation processed
  4     u32  invocation  invocation index within the stage
  8     i16  op_index    operator index, or -1 for a stage-level phase
 10     u8   phase       0 SETUP, 1 DESERIALIZE, 2 PUSH, 3 FINISH
 11     u8   (padding)
 12     u32  hits        number of scope entries aggregated into this record
 16     u64  total_ns    summed wall-clock nanoseconds
```

`op_index == -1` marks the two **stage-level** phases, timed once per invocation:

- **SETUP** — building the executor / operator chain.
- **DESERIALIZE** — decoding the pipeline template.

`op_index >= 0` marks a **per-operator** phase, keyed by the op index the
header's `op_types` describes:

- **PUSH** — the operator consuming input chunks. `hits` is the number of chunks
  pushed through it; `total_ns` is the summed time. Because operators stream
  (each `Push` calls the next operator's `Push` synchronously), the time is
  **inclusive** of everything downstream of that operator. On a linear chain the
  *exclusive* time of operator `i` is `push_ns[i] − push_ns[i+1]`.
- **FINISH** — the operator flushing its buffered output at end of input
  (aggregate finalization, sort emit, the output operator's final serialize).

Records are only present for `(stage, invocation, op, phase)` combinations that
actually fired, so an operator never pushed produces no PUSH record.

## Reading it

No reader ships with the tool. A record's operator is
`op_types_of[stage_id][op_index]` (or a stage phase when `op_index == -1`); its
phase is the `phase` byte. A ~30-line struct-unpacking script (Python `struct`,
format `<HHihBBIQ` per event) is enough to dump or aggregate it.
