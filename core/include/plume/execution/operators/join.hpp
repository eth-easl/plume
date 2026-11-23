#pragma once

#include "plume/execution/group_table.hpp"
#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <cstdint>
#include <memory>
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
    // Per key: whether NULL matches NULL (IS NOT DISTINCT FROM). Empty = no key does.
    std::vector<bool> keys_match_null;
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

    bool Equals(const OperatorTemplate &other) const override;
    void Serialize(duckdb::Serializer &s) const override;
};

class JoinOperator : public Operator {
public:
    JoinOperator(std::vector<uint32_t> left_keys, std::vector<uint32_t> right_keys,
                 duckdb::vector<duckdb::LogicalType> left_types, duckdb::vector<duckdb::LogicalType> right_types,
                 memory::Allocator &alloc, JoinKind kind = JoinKind::INNER,
                 duckdb::unique_ptr<duckdb::Expression> residual_predicate = nullptr,
                 std::vector<bool> keys_match_null = {});

    // Sizes the build structures for `rows` build rows.
    void ReserveBuild(duckdb::idx_t rows);

    // Adds a chunk of the build (right) side and retains it (no copy).
    // NOTE: chunk data must stay alive and unchanged throughout join execution
    Result<void> PushBuild(std::unique_ptr<duckdb::DataChunk> chunk);

    // Finishes the build side.
    void FinishBuild();

    // Probes the chunk against the hash table. Joined rows are emitted downstream.
    // Requires FinishBuild() to have run.
    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;

    // Flushes any RIGHT/OUTER/RIGHT_SEMI/RIGHT_ANTI build-side rows.
    Result<void> Finish() override;

private:
    static constexpr uint32_t kNoRow = UINT32_MAX;

    struct BuildData;

    duckdb::idx_t FilterNullKeys(duckdb::DataChunk &keys, duckdb::idx_t count, duckdb::SelectionVector &valid,
                                 bool &all_valid);
    void BuildColumn(duckdb::idx_t col, const duckdb::SelectionVector &rsel, duckdb::idx_t count,
                     duckdb::Vector &out);
    void LocateBuildRows(const duckdb::SelectionVector &rsel, duckdb::idx_t count);
    Result<void> EmitRows(duckdb::DataChunk *left, const duckdb::SelectionVector *lsel,
                          const duckdb::SelectionVector *rsel, duckdb::idx_t count, bool with_left,
                          bool with_right);

    std::vector<uint32_t> left_keys_;
    std::vector<uint32_t> right_keys_;
    duckdb::vector<duckdb::LogicalType> left_types_;
    duckdb::vector<duckdb::LogicalType> right_types_;
    std::vector<bool> match_null_; // per key
    JoinKind kind_;
    memory::Allocator alloc_;

    // Build state.
    std::shared_ptr<BuildData> build_;
    duckdb::idx_t build_count_ = 0;
    std::unique_ptr<GroupTable> table_; // build key -> first build row id with it (uint32_t)
    std::vector<uint32_t> chain_next_;  // build row id -> next row id with the same key
    bool build_finished_ = false;
    bool build_has_null_ = false;       // a build row was dropped for a NULL key (NULL-aware MARK)
    bool track_build_ = false;          // RIGHT/OUTER/RIGHT_SEMI/RIGHT_ANTI need matched flags
    std::vector<uint8_t> matched_;      // per build row id

    // The residual predicate (see JoinTemplate::residual), evaluated on (probe, build) pair batches
    // in the natural (left++right) layout.
    duckdb::unique_ptr<duckdb::Expression> residual_predicate_;
    duckdb::ExpressionExecutor residual_executor_;

    // Per-chunk scratch.
    duckdb::DataChunk key_chunk_;
    std::vector<duckdb::data_ptr_t> payloads_;
    duckdb::SelectionVector new_groups_;
    std::vector<uint32_t> heads_, cur_;         // per probe row: chain head / current build row
    std::vector<uint8_t> found_;                // per probe row: matched at least once
    std::vector<uint32_t> loc_chunk_, loc_row_; // LocateBuildRows output
};

Result<std::unique_ptr<JoinOperator>> BuildJoinTemplate(std::shared_ptr<JoinTemplate> templ,
    duckdb::vector<duckdb::LogicalType> left_types, memory::Allocator &alloc);

Schema JoinSchema(const JoinTemplate &templ, const Schema &left_schema);

} // namespace plume::exec
