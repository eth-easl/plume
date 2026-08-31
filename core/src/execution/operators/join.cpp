#include "plume/execution/operators/join.hpp"

#include "plume/expression/expression_builder.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"

#include <string>
#include <unordered_map>

namespace plume::exec {

using duckdb::DataChunk;
using duckdb::FlatVector;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::PhysicalType;
using duckdb::string_t;

//===----------------------------------------------------------------------===//
// JoinTemplate
//===----------------------------------------------------------------------===//

bool JoinTemplate::Equals(const OperatorTemplate &other) const {
    if (other.type != type) return false;
    auto &o = static_cast<const JoinTemplate &>(other);
    if (left_keys != o.left_keys) return false;
    if (right_keys != o.right_keys) return false;
    if (!right_schema.Equals(o.right_schema)) return false;
    if (kind != o.kind) return false;
    if (has_residual != o.has_residual) return false;
    if (has_residual && !residual.Equals(o.residual)) return false;
    return true;
}

void JoinTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteList(101, "left_keys", left_keys.size(), [&](duckdb::Serializer::List &list, idx_t i) {
        list.WriteElement(left_keys[i]);
    });
    s.WriteList(102, "right_keys", right_keys.size(), [&](duckdb::Serializer::List &list, idx_t i) {
        list.WriteElement(right_keys[i]);
    });
    s.WriteProperty(103, "right_schema", right_schema);
    s.WriteProperty(104, "kind", static_cast<uint8_t>(kind));
    s.WriteProperty(105, "has_residual", has_residual);
    if (has_residual) {
        s.WriteProperty(106, "residual", residual);
    }
}

//===----------------------------------------------------------------------===//
// Execution
//===----------------------------------------------------------------------===//

namespace {

duckdb::vector<duckdb::LogicalType> ComputeOutputTypes(const duckdb::vector<LogicalType> &left_types, 
        const duckdb::vector<LogicalType> &right_types, JoinKind kind) {
    duckdb::vector<duckdb::LogicalType> out_types;
    switch (kind) {
    case JoinKind::SEMI:
    case JoinKind::ANTI:
        out_types = left_types;
        break;
    case JoinKind::RIGHT_SEMI:
    case JoinKind::RIGHT_ANTI:
        out_types = right_types;
        break;
    case JoinKind::MARK:
        out_types = left_types;
        out_types.push_back(LogicalType::BOOLEAN);
        break;
    default: // INNER / LEFT / RIGHT / OUTER / SINGLE
        out_types = left_types;
        for (auto &t : right_types) {
            out_types.push_back(t);
        }
        break;
    }
    return out_types;
}

bool EncodeKey(const DataChunk &chunk, const std::vector<uint32_t> &keys, idx_t row, std::string &out) {
    out.clear();
    for (uint32_t k : keys) {
        auto &vec = const_cast<DataChunk &>(chunk).data[k];
        if (!FlatVector::Validity(vec).RowIsValid(row)) {
            return false;
        }
        const PhysicalType pt = vec.GetType().InternalType();
        if (pt == PhysicalType::VARCHAR) {
            auto &s = FlatVector::GetData<string_t>(vec)[row];
            uint32_t len = s.GetSize();
            out.append(reinterpret_cast<const char *>(&len), sizeof(len));
            out.append(s.GetData(), len);
        } else {
            const idx_t w = duckdb::GetTypeIdSize(pt);
            auto raw = FlatVector::GetData(vec);
            out.append(reinterpret_cast<const char *>(raw + row * w), w);
        }
    }
    return true;
}

// Accumulates joined rows into <= STANDARD_VECTOR_SIZE output chunks, emitting each full chunk downstream.
struct OutputBuilder {
    const duckdb::vector<LogicalType> *types;
    memory::Allocator *alloc;
    Operator *next;
    std::unique_ptr<DataChunk> chunk;
    idx_t cur = 0;

    DataChunk &Row() {
        if (!chunk) {
            chunk = std::make_unique<DataChunk>();
            chunk->Initialize(alloc->Get(), *types);
            cur = 0;
        }
        return *chunk;
    }

    Result<void> Advance() {
        chunk->SetCardinality(++cur);
        if (cur == STANDARD_VECTOR_SIZE) {
            TRYV(next->Push(std::move(chunk)));
            chunk.reset();
            cur = 0;
        }
        return Ok();
    }

    Result<void> Flush() {
        if (chunk && cur > 0) {
            TRYV(next->Push(std::move(chunk)));
        }
        chunk.reset();
        cur = 0;
        return Ok();
    }

    void PutLeft(DataChunk &dst, const DataChunk &left, idx_t lr, idx_t n_left) {
        for (idx_t c = 0; c < n_left; c++) {
            dst.SetValue(c, cur, const_cast<DataChunk &>(left).GetValue(c, lr));
        }
    }
    void PutNullLeft(DataChunk &dst, idx_t n_left) {
        for (idx_t c = 0; c < n_left; c++) {
            dst.SetValue(c, cur, duckdb::Value((*types)[c]));
        }
    }
    void PutRight(DataChunk &dst, idx_t off, const DataChunk &right, idx_t rr, idx_t n_right) {
        for (idx_t c = 0; c < n_right; c++) {
            dst.SetValue(off + c, cur, const_cast<DataChunk &>(right).GetValue(c, rr));
        }
    }
    void PutNullRight(DataChunk &dst, idx_t off, idx_t n_right) {
        for (idx_t c = 0; c < n_right; c++) {
            dst.SetValue(off + c, cur, duckdb::Value((*types)[off + c]));
        }
    }

    // left ++ right (a matched row).
    Result<void> EmitMatch(const DataChunk &l, idx_t lr, idx_t nl, const DataChunk &r, idx_t rr, idx_t nr) {
        auto &dst = Row();
        PutLeft(dst, l, lr, nl);
        PutRight(dst, nl, r, rr, nr);
        return Advance();
    }
    // left ++ NULLs (an unmatched probe row under LEFT/OUTER/SINGLE).
    Result<void> EmitLeftNullRight(const DataChunk &l, idx_t lr, idx_t nl, idx_t nr) {
        auto &dst = Row();
        PutLeft(dst, l, lr, nl);
        PutNullRight(dst, nl, nr);
        return Advance();
    }
    // NULLs ++ right (an unmatched build row under RIGHT/OUTER).
    Result<void> EmitNullLeftRight(idx_t nl, const DataChunk &r, idx_t rr, idx_t nr) {
        auto &dst = Row();
        PutNullLeft(dst, nl);
        PutRight(dst, nl, r, rr, nr);
        return Advance();
    }
    // left columns only (SEMI / ANTI).
    Result<void> EmitLeftOnly(const DataChunk &l, idx_t lr, idx_t nl) {
        auto &dst = Row();
        PutLeft(dst, l, lr, nl);
        return Advance();
    }
    // right columns only (RIGHT_SEMI / RIGHT_ANTI).
    Result<void> EmitRightOnly(const DataChunk &r, idx_t rr, idx_t nr) {
        auto &dst = Row();
        PutRight(dst, 0, r, rr, nr);
        return Advance();
    }
    // left ++ a single BOOLEAN marker (MARK).
    Result<void> EmitMark(const DataChunk &l, idx_t lr, idx_t nl, duckdb::Value marker) {
        auto &dst = Row();
        PutLeft(dst, l, lr, nl);
        dst.SetValue(nl, cur, std::move(marker));
        return Advance();
    }
};

} // namespace

JoinOperator::JoinOperator(std::vector<uint32_t> left_keys, std::vector<uint32_t> right_keys,
        duckdb::vector<LogicalType> left_types, duckdb::vector<LogicalType> right_types,
        memory::Allocator &alloc, JoinKind kind, duckdb::unique_ptr<duckdb::Expression> residual_predicate)
    : Operator(ComputeOutputTypes(left_types, right_types, kind))
    , left_keys_(std::move(left_keys)), right_keys_(std::move(right_keys))
    , left_types_(std::move(left_types)), right_types_(std::move(right_types)), alloc_(alloc), kind_(kind)
    , residual_predicate_(std::move(residual_predicate)) {
    // RIGHT/OUTER and RIGHT_SEMI/RIGHT_ANTI need to know which build rows matched.
    track_build_ = kind_ == JoinKind::RIGHT || kind_ == JoinKind::OUTER
        || kind_ == JoinKind::RIGHT_SEMI || kind_ == JoinKind::RIGHT_ANTI;

    if (residual_predicate_) {
        residual_executor_.AddExpression(*residual_predicate_);
        duckdb::vector<LogicalType> natural_types = left_types_;
        for (auto &t : right_types_) {
            natural_types.push_back(t);
        }
        residual_scratch_ = std::make_unique<DataChunk>();
        residual_scratch_->Initialize(alloc_.Get(), natural_types);
        residual_scratch_->SetCardinality(1);
    }
}

bool JoinOperator::ResidualMatches(const DataChunk &lchunk, idx_t lr, const DataChunk &rchunk, idx_t rr) {
    if (!residual_predicate_) {
        return true;
    }
    const idx_t n_left = left_types_.size();
    const idx_t n_right = right_types_.size();
    for (idx_t c = 0; c < n_left; c++) {
        residual_scratch_->SetValue(c, 0, const_cast<DataChunk &>(lchunk).GetValue(c, lr));
    }
    for (idx_t c = 0; c < n_right; c++) {
        residual_scratch_->SetValue(n_left + c, 0, const_cast<DataChunk &>(rchunk).GetValue(c, rr));
    }
    duckdb::SelectionVector sel(1);
    return residual_executor_.SelectExpression(*residual_scratch_, sel) > 0;
}

void JoinOperator::PushBuild(std::unique_ptr<DataChunk> chunk) {
    // TODO: might want to have a different phase here to be able to differentiate
    PLUME_TRACE_OP(trace::Phase::PUSH);
    build_chunks_.push_back(std::move(chunk)); // retained: the hash table indexes into it
}

void JoinOperator::FinishBuild() {
    std::string key;
    const idx_t n_chunks = build_chunks_.size();
    for (idx_t ci = 0; ci < n_chunks; ci++) {
        const auto &chunk = *build_chunks_[ci];
        for (idx_t r = 0; r < chunk.size(); r++) {
            if (EncodeKey(chunk, right_keys_, r, key)) {
                table_[key].emplace_back(ci, r);
            } else {
                build_has_null_ = true;
            }
        }
    }
    if (track_build_) {
        matched_.resize(n_chunks);
        for (idx_t ci = 0; ci < n_chunks; ci++) {
            matched_[ci].assign(build_chunks_[ci]->size(), 0);
        }
    }
    build_finished_ = true;
}

Result<void> JoinOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    if (!build_finished_) {
        return Error("JoinOperator::Push was called before JoinOperator::FinishBuild was called.");
    }
    const idx_t n_left = left_types_.size();
    const idx_t n_right = right_types_.size();
    OutputBuilder builder {&output_types_, &alloc_, next_};
    std::string key;

    const auto &lchunk = *chunk;
    for (idx_t lr = 0; lr < lchunk.size(); lr++) {
        const bool has_key = EncodeKey(lchunk, left_keys_, lr, key); // false if any probe key is NULL
        auto it = has_key ? table_.find(key) : table_.end();
        const bool hash_match = has_key && it != table_.end();

        switch (kind_) {
        case JoinKind::INNER:
            if (hash_match) {
                for (auto &[rci, rr] : it->second) {
                    if (ResidualMatches(lchunk, lr, *build_chunks_[rci], rr)) {
                        TRYV(builder.EmitMatch(lchunk, lr, n_left, *build_chunks_[rci], rr, n_right));
                    }
                }
            }
            break;
        case JoinKind::LEFT: {
            bool any = false;
            if (hash_match) {
                for (auto &[rci, rr] : it->second) {
                    if (ResidualMatches(lchunk, lr, *build_chunks_[rci], rr)) {
                        TRYV(builder.EmitMatch(lchunk, lr, n_left, *build_chunks_[rci], rr, n_right));
                        any = true;
                    }
                }
            }
            if (!any) {
                TRYV(builder.EmitLeftNullRight(lchunk, lr, n_left, n_right));
            }
            break;
        }
        case JoinKind::SINGLE: { // at most one build partner (NULL right if none)
            bool any = false;
            if (hash_match) {
                for (auto &[rci, rr] : it->second) {
                    if (ResidualMatches(lchunk, lr, *build_chunks_[rci], rr)) {
                        TRYV(builder.EmitMatch(lchunk, lr, n_left, *build_chunks_[rci], rr, n_right));
                        any = true;
                        break;
                    }
                }
            }
            if (!any) {
                TRYV(builder.EmitLeftNullRight(lchunk, lr, n_left, n_right));
            }
            break;
        }
        case JoinKind::RIGHT:
            if (hash_match) {
                for (auto &[rci, rr] : it->second) {
                    if (ResidualMatches(lchunk, lr, *build_chunks_[rci], rr)) {
                        TRYV(builder.EmitMatch(lchunk, lr, n_left, *build_chunks_[rci], rr, n_right));
                        matched_[rci][rr] = 1;
                    }
                }
            }
            break;
        case JoinKind::OUTER: {
            bool any = false;
            if (hash_match) {
                for (auto &[rci, rr] : it->second) {
                    if (ResidualMatches(lchunk, lr, *build_chunks_[rci], rr)) {
                        TRYV(builder.EmitMatch(lchunk, lr, n_left, *build_chunks_[rci], rr, n_right));
                        matched_[rci][rr] = 1;
                        any = true;
                    }
                }
            }
            if (!any) {
                TRYV(builder.EmitLeftNullRight(lchunk, lr, n_left, n_right));
            }
            break;
        }
        case JoinKind::SEMI: {
            bool any = false;
            if (hash_match) {
                for (auto &[rci, rr] : it->second) {
                    if (ResidualMatches(lchunk, lr, *build_chunks_[rci], rr)) {
                        any = true;
                        break;
                    }
                }
            }
            if (any) {
                TRYV(builder.EmitLeftOnly(lchunk, lr, n_left));
            }
            break;
        }
        case JoinKind::ANTI: {
            bool any = false;
            if (hash_match) {
                for (auto &[rci, rr] : it->second) {
                    if (ResidualMatches(lchunk, lr, *build_chunks_[rci], rr)) {
                        any = true;
                        break;
                    }
                }
            }
            if (!any) {
                TRYV(builder.EmitLeftOnly(lchunk, lr, n_left));
            }
            break;
        }
        case JoinKind::MARK: {
            bool any = false;
            if (hash_match) {
                for (auto &[rci, rr] : it->second) {
                    if (ResidualMatches(lchunk, lr, *build_chunks_[rci], rr)) {
                        any = true;
                        break;
                    }
                }
            }
            duckdb::Value marker = any ? duckdb::Value::BOOLEAN(true)
                                   : (!has_key || build_has_null_) ? duckdb::Value(LogicalType::BOOLEAN)
                                                                    : duckdb::Value::BOOLEAN(false);
            TRYV(builder.EmitMark(lchunk, lr, n_left, std::move(marker)));
            break;
        }
        case JoinKind::RIGHT_SEMI:
        case JoinKind::RIGHT_ANTI:
            if (hash_match) {
                for (auto &[rci, rr] : it->second) {
                    if (ResidualMatches(lchunk, lr, *build_chunks_[rci], rr)) {
                        matched_[rci][rr] = 1;
                    }
                }
            }
            break;
        }
    }
    return builder.Flush();
}

Result<void> JoinOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    if (!track_build_) {
        return next_->Finish();
    }
    const idx_t n_left = left_types_.size();
    const idx_t n_right = right_types_.size();
    OutputBuilder builder {&output_types_, &alloc_, next_};

    const idx_t n_chunks = build_chunks_.size();
    for (idx_t ci = 0; ci < n_chunks; ci++) {
        const auto &rchunk = *build_chunks_[ci];
        for (idx_t rr = 0; rr < rchunk.size(); rr++) {
            const bool m = matched_[ci][rr] != 0;
            switch (kind_) {
            case JoinKind::RIGHT:
            case JoinKind::OUTER:
                if (!m) {
                    builder.EmitNullLeftRight(n_left, rchunk, rr, n_right);
                }
                break;
            case JoinKind::RIGHT_SEMI:
                if (m) {
                    builder.EmitRightOnly(rchunk, rr, n_right);
                }
                break;
            case JoinKind::RIGHT_ANTI:
                if (!m) {
                    builder.EmitRightOnly(rchunk, rr, n_right);
                }
                break;
            default:
                break;
            }
        }
    }
    builder.Flush();
    return next_->Finish();
}

//===----------------------------------------------------------------------===//
// Build / schema
//===----------------------------------------------------------------------===//

Result<std::unique_ptr<JoinOperator>> BuildJoinTemplate(std::shared_ptr<JoinTemplate> templ,
        duckdb::vector<LogicalType> left_types, memory::Allocator &alloc) {
    duckdb::vector<LogicalType> right_types;
    right_types.reserve(templ->right_schema.size());
    for (auto &c : templ->right_schema.columns) {
        right_types.push_back(ToLogicalType(c.type));
    }
    duckdb::unique_ptr<duckdb::Expression> residual;
    if (templ->has_residual) {
        TRY(residual, expr::BuildExpression(templ->residual));
    }
    return std::make_unique<JoinOperator>(templ->left_keys, templ->right_keys, std::move(left_types),
                                          std::move(right_types), alloc, templ->kind, std::move(residual));
}

Schema JoinSchema(const JoinTemplate &templ, const Schema &left_schema) {
    using K = JoinKind;
    const K k = templ.kind;
    const bool left_nullable = (k == K::RIGHT || k == K::OUTER);
    const bool right_nullable = (k == K::LEFT || k == K::OUTER || k == K::SINGLE);

    Schema out;
    if (k != K::RIGHT_SEMI && k != K::RIGHT_ANTI) {
        for (const auto &c : left_schema.columns) {
            Column col = c;
            col.nullable = col.nullable || left_nullable;
            out.columns.push_back(std::move(col));
        }
    }
    if (k != K::SEMI && k != K::ANTI && k != K::MARK) {
        for (const auto &c : templ.right_schema.columns) {
            Column col = c;
            col.nullable = col.nullable || right_nullable;
            out.columns.push_back(std::move(col));
        }
    }
    if (k == K::MARK) {
        Column marker;
        marker.name = "marker";
        marker.type = ColumnType {TypeId::BOOLEAN};
        marker.nullable = true;
        out.columns.push_back(std::move(marker));
    }
    return out;
}

} // namespace plume::exec
