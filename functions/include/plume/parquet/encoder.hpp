//===----------------------------------------------------------------------===//
// plume — Parquet writer (the mirror of decoder.cpp / metadata.cpp)
//
// Encodes DataChunks laid out per a plume Schema into a complete, standalone
// parquet file: PAR1 | column data | FileMetaData footer | [i32 len] | PAR1.
// One row group, one PLAIN data page per column (V1), uncompressed. Nullable
// columns carry RLE definition levels (bit width 1), matching the decoder's V1
// path. The footer is a full FileMetaData (schema + row-group/column metadata) so
// external readers (DuckDB, pandas, ...) can read the file.
//
// Custom + instance-free (no DuckDB parquet extension), like the rest of plume's
// parquet support.
//===----------------------------------------------------------------------===//

#pragma once

#include "plume/common/buffer.hpp"
#include "plume/common/result.hpp"
#include "plume/common/types.hpp"

#include "duckdb/common/types/data_chunk.hpp"

#include <vector>

namespace plume::parquet {

// Parquet CompressionCodec values the writer understands (match the parquet spec
// + the decoder's Codec enum).
enum WriteCodec : int32_t { WRITE_UNCOMPRESSED = 0, WRITE_SNAPPY = 1 };

// Encode `chunks` (each laid out per `schema`) into one parquet file. `codec`
// selects page compression (UNCOMPRESSED or SNAPPY). HUGEINT and DECIMAL with
// width > 18 are not supported (NotImplemented), consistent with the decoder.
Result<DataBuffer> WriteParquet(const Schema &schema, const std::vector<duckdb::DataChunk *> &chunks,
                                int32_t codec = WRITE_UNCOMPRESSED);

} // namespace plume::parquet
