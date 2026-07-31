#pragma once

#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace plume::exec {

// The SQL join kinds. 
//   INNER       left ++ right, matched rows only
//   LEFT        + each unmatched probe row, right NULL
//   RIGHT       + each unmatched build row, left NULL
//   OUTER       LEFT and RIGHT combined
//   SINGLE      like LEFT but at most one build match per probe row
//   SEMI        left only, probe rows with >=1 build match (once)
//   ANTI        left only, probe rows with no build match
//   MARK        left ++ a BOOLEAN marker (NULL-aware, for IN / NOT IN)
//   RIGHT_SEMI  right only, build rows with >=1 probe match
//   RIGHT_ANTI  right only, build rows with no probe match
enum class JoinKind : uint8_t {
    INNER = 0,
    LEFT = 1,
    RIGHT = 2,
    OUTER = 3,
    SEMI = 4,
    ANTI = 5,
    SINGLE = 6,
    MARK = 7,
    RIGHT_SEMI = 8,
    RIGHT_ANTI = 9,
};

struct JoinTemplate : OperatorTemplate {
    std::vector<uint32_t> left_keys;  // key column indices into the left input
    std::vector<uint32_t> right_keys; // key column indices into the right input
    Schema right_schema;              // left schema is the pipeline input_schema
    JoinKind kind = JoinKind::INNER;
    // An extra predicate checked per (probe, candidate build) row pair once the
    // hash keys already match — for join conditions with both an equi part (the
    // hash keys above) and a non-equi part (e.g. a self-join's `a.x = b.x AND
    // a.y <> b.y`), or a condition whose "equi" side isn't a bare column. Always
    // evaluated against the natural (left++right) layout, regardless of kind:
    // SEMI/ANTI/MARK/RIGHT_SEMI/RIGHT_ANTI need both sides' values to decide a
    // match even though their own output schema keeps only one side.
    bool has_residual = false;
    expr::ExprNode residual;

    JoinTemplate() : OperatorTemplate(OpType::JOIN) {}
    JoinTemplate(std::vector<uint32_t> left_keys, std::vector<uint32_t> right_keys,
            Schema right_schema, JoinKind kind = JoinKind::INNER)
        : OperatorTemplate(OpType::JOIN)
        , left_keys(std::move(left_keys)), right_keys(std::move(right_keys))
        , right_schema(std::move(right_schema)), kind(kind) {}

    void Serialize(duckdb::Serializer &s) const override;
};

class JoinOperator : public Operator {
public:
    JoinOperator(std::vector<uint32_t> left_keys, std::vector<uint32_t> right_keys,
                 duckdb::vector<duckdb::LogicalType> left_types, duckdb::vector<duckdb::LogicalType> right_types,
                 memory::Allocator &alloc, JoinKind kind = JoinKind::INNER,
                 duckdb::unique_ptr<duckdb::Expression> residual_predicate = nullptr);
    // Builds the build (right) side. The pushed chunks are retained (the table indexes into them) 
    // until the join is destroyed.
    void PushBuild(std::unique_ptr<duckdb::DataChunk> chunk);
    
    // Finishes the build side.
    void FinishBuild();

    // Probes the chunk against the hash table. Joined rows are emitted downstream. 
    // Requires FinishBuild() to have run.
    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;

    // Flushes any RIGHT/OUTER/RIGHT_SEMI/RIGHT_ANTI build-side rows.
    Result<void> Finish() override;

private:
    std::vector<uint32_t> left_keys_;
    std::vector<uint32_t> right_keys_;
    duckdb::vector<duckdb::LogicalType> left_types_;
    duckdb::vector<duckdb::LogicalType> right_types_;
    JoinKind kind_;
    memory::Allocator alloc_;

    // Build state (retained for the join's lifetime; probe rows reference it).
    ChunkList build_chunks_;
    std::unordered_map<std::string, std::vector<std::pair<duckdb::idx_t, duckdb::idx_t>>> table_; // key -> (chunk, row)
    bool build_finished_ = false;
    bool build_has_null_ = false;              // any build key NULL (NULL-aware MARK)
    bool track_build_ = false;                 // RIGHT/OUTER/RIGHT_SEMI/RIGHT_ANTI need matched flags
    std::vector<std::vector<char>> matched_;   // per build (chunk, row) match flag

    // The residual predicate (see JoinTemplate::residual), plus the scratch state
    // used to evaluate it one (probe, build) row pair at a time.
    duckdb::unique_ptr<duckdb::Expression> residual_predicate_;
    duckdb::ExpressionExecutor residual_executor_;
    std::unique_ptr<duckdb::DataChunk> residual_scratch_; // one row, natural (left++right) layout

    // True if there's no residual predicate (an unconditional match) or it holds
    // for this specific (probe, build) row pair.
    bool ResidualMatches(const duckdb::DataChunk &lchunk, duckdb::idx_t lr, const duckdb::DataChunk &rchunk,
                         duckdb::idx_t rr);
};

Result<std::unique_ptr<JoinOperator>> BuildJoinTemplate(std::shared_ptr<JoinTemplate> templ,
    duckdb::vector<duckdb::LogicalType> left_types, memory::Allocator &alloc);

Schema JoinSchema(const JoinTemplate &templ, const Schema &left_schema);

} // namespace plume::exec
