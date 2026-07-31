# Input/Ouput data

A **block** is one contiguous, self-describing, DuckDB-native columnar chunk: POD, little-endian, with 16-byte-aligned data regions.

  ```
  ┌───────────────────────────┐
  │ BlockHeader (64B)         │  magic, format_version, column_count, row_count,
  │                           │  total_size, and the schema/names/heap offsets
  ├───────────────────────────┤
  │ ColumnDescriptor[ncol]    │  ColumnDescriptor: type_id, decimal width/scale, flags,
  │  (32B each)               │  name (offset+len into the names blob), data_offset, 
  │                           │  validity_offset (0 -> all-valid, no mask stored)
  ├───────────────────────────┤
  │ names blob (UTF-8)        │  UTF-8 column names
  ├───────────────────────────┤
  │ per-column regions:       │  for each column: its data array (16-byte aligned),
  │   data, then validity     │  followed by its validity bitmask (8-bit aligned, if nullable)
  │   (each region aligned)   │
  ├───────────────────────────┤
  │ varchar heap              │  bytes of VARCHAR values too long to inline (>12 bytes),
  │                           │  referenced by an in-block offset
  └───────────────────────────┘
  ```
