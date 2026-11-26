#include "plume/execution/operators/join.hpp"

#include "plume/expression/expression_builder.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/vector/vector_writer.hpp"

#include <algorithm>
#include <cstring>

namespace plume::exec {

using duckdb::DataChunk;
using duckdb::FlatVector;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::SelectionVector;
using duckdb::UnifiedVectorFormat;
using duckdb::Value;
using duckdb::Vector;

//===----------------------------------------------------------------------===//
// JoinTemplate
//===----------------------------------------------------------------------===//

static std::vector<bool> MatchNull(const std::vector<bool> &keys_match_null, size_t n_keys) {
    std::vector<bool> out = keys_match_null;
    out.resize(n_keys, false);
    return out;
}

bool JoinTemplate::Equals(const OperatorTemplate &other) const {
    if (other.type != type) return false;
    auto &o = static_cast<const JoinTemplate &>(other);
    if (left_keys != o.left_keys) return false;
    if (right_keys != o.right_keys) return false;
    if (!right_schema.Equals(o.right_schema)) return false;
    if (kind != o.kind) return false;
    if (has_residual != o.has_residual) return false;
    if (has_residual && !residual.Equals(o.residual)) return false;
    if (MatchNull(keys_match_null, left_keys.size()) != MatchNull(o.keys_match_null, o.left_keys.size())) return false;
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
    // Bit i: key i matches NULL.
    uint64_t match_null_mask = 0;
    for (size_t i = 0; i < keys_match_null.size(); i++) {
        if (keys_match_null[i]) {
            if (i >= 64) {
                throw duckdb::NotImplementedException("IS NOT DISTINCT FROM on join key %d (max 64)", i);
            }
            match_null_mask |= uint64_t(1) << i;
        }
    }
    s.WritePropertyWithDefault<uint64_t>(107, "keys_match_null", match_null_mask, 0);
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

template <idx_t WIDTH>
void GatherFixed(const UnifiedVectorFormat *formats, const uint32_t *chunks, const uint32_t *rows, idx_t count,
                 duckdb::data_ptr_t out, duckdb::ValidityMask &validity) {
    for (idx_t i = 0; i < count; i++) {
        const UnifiedVectorFormat &f = formats[chunks[i]];
        const idx_t idx = f.sel->get_index(rows[i]);
        if (!f.validity.RowIsValid(idx)) {
            validity.SetInvalid(i);
            continue;
        }
        std::memcpy(out + i * WIDTH, f.data + idx * WIDTH, WIDTH);
    }
}

// Keeps the build chunks alive for VARCHAR vectors gathered from them.
struct BuildDataHolder : duckdb::AuxiliaryDataHolder {
    explicit BuildDataHolder(std::shared_ptr<void> data_p) : data(std::move(data_p)) {}
    std::shared_ptr<void> data;
};

} // namespace

struct JoinOperator::BuildData {
    std::vector<std::unique_ptr<DataChunk>> chunks;
    std::vector<idx_t> starts; // first build row id of each chunk
    // formats[col][chunk]: each build column of each chunk (set by FinishBuild).
    std::vector<std::vector<UnifiedVectorFormat>> formats;
};

JoinOperator::JoinOperator(std::vector<uint32_t> left_keys, std::vector<uint32_t> right_keys,
        duckdb::vector<LogicalType> left_types, duckdb::vector<LogicalType> right_types,
        memory::Allocator &alloc, JoinKind kind, duckdb::unique_ptr<duckdb::Expression> residual_predicate,
        std::vector<bool> keys_match_null)
    : Operator(ComputeOutputTypes(left_types, right_types, kind))
    , left_keys_(std::move(left_keys)), right_keys_(std::move(right_keys))
    , left_types_(std::move(left_types)), right_types_(std::move(right_types))
    , match_null_(std::move(keys_match_null)), kind_(kind), alloc_(alloc)
    , residual_predicate_(std::move(residual_predicate)) {
    match_null_.resize(right_keys_.size(), false);
    // RIGHT/OUTER and RIGHT_SEMI/RIGHT_ANTI need to know which build rows matched.
    track_build_ = kind_ == JoinKind::RIGHT || kind_ == JoinKind::OUTER
        || kind_ == JoinKind::RIGHT_SEMI || kind_ == JoinKind::RIGHT_ANTI;
    if (residual_predicate_) {
        residual_executor_.AddExpression(*residual_predicate_);
    }

    duckdb::vector<LogicalType> key_types;
    for (uint32_t k : right_keys_) {
        key_types.push_back(right_types_[k]);
    }
    // The build chunks outlive the table, so key strings are referenced rather than copied.
    table_ = std::make_unique<GroupTable>(key_types, sizeof(uint32_t), alloc_, /*copy_strings=*/false);
    key_chunk_.InitializeEmpty(key_types);
    build_ = std::make_shared<BuildData>();
    loc_chunk_.resize(STANDARD_VECTOR_SIZE);
    loc_row_.resize(STANDARD_VECTOR_SIZE);
    payloads_.resize(STANDARD_VECTOR_SIZE);
    new_groups_.Initialize(STANDARD_VECTOR_SIZE);
    heads_.resize(STANDARD_VECTOR_SIZE);
    cur_.resize(STANDARD_VECTOR_SIZE);
    found_.resize(STANDARD_VECTOR_SIZE);
}

idx_t JoinOperator::FilterNullKeys(DataChunk &keys, idx_t count, SelectionVector &valid, bool &all_valid) {
    all_valid = true;
    idx_t n = count;
    for (idx_t c = 0; c < keys.ColumnCount(); c++) {
        if (match_null_[c]) {
            continue;
        }
        UnifiedVectorFormat format;
        keys.data[c].ToUnifiedFormat(format);
        if (format.validity.CannotHaveNull()) {
            continue;
        }
        if (all_valid) {
            valid.Initialize(count);
            for (idx_t r = 0; r < count; r++) {
                valid.set_index(r, r);
            }
            all_valid = false;
        }
        idx_t keep = 0;
        for (idx_t i = 0; i < n; i++) {
            const idx_t r = valid.get_index(i);
            if (format.validity.RowIsValid(format.sel->get_index(r))) {
                valid.set_index(keep++, r);
            }
        }
        n = keep;
    }
    if (!all_valid) {
        keys.Slice(valid, n);
    }
    keys.SetChildCardinality(n);
    return n;
}

void JoinOperator::ReserveBuild(idx_t rows) {
    table_->Reserve(rows);
    chain_next_.reserve(build_count_ + rows);
}

Result<void> JoinOperator::PushBuild(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    const idx_t count = chunk->size();
    if (count == 0) {
        return Ok();
    }
    if (build_count_ + count >= kNoRow) {
        return Error("Join build side exceeds 2^32 rows", ErrorKind::NotImplemented);
    }
    const idx_t base = build_count_;
    build_->starts.push_back(base);
    build_->chunks.push_back(std::move(chunk));
    DataChunk &src = *build_->chunks.back();
    build_count_ += count;
    chain_next_.resize(build_count_, kNoRow);

    // Index the keys a vector at a time.
    SelectionVector valid;
    for (idx_t offset = 0; offset < count; offset += STANDARD_VECTOR_SIZE) {
        const idx_t window = std::min<idx_t>(STANDARD_VECTOR_SIZE, count - offset);
        for (idx_t c = 0; c < right_keys_.size(); c++) {
            if (offset == 0 && window == count) {
                key_chunk_.data[c].Reference(src.data[right_keys_[c]]);
            } else {
                key_chunk_.data[c].Slice(src.data[right_keys_[c]], offset, offset + window);
            }
        }
        bool all_valid;
        const idx_t n = FilterNullKeys(key_chunk_, window, valid, all_valid);
        build_has_null_ = build_has_null_ || n < window;
        if (n == 0) {
            continue;
        }
        Vector hashes(LogicalType::HASH, n);
        table_->Hash(key_chunk_, hashes, n);
        const idx_t n_new = table_->FindOrCreate(key_chunk_, hashes, n, payloads_.data(), new_groups_);
        for (idx_t i = 0; i < n_new; i++) {
            duckdb::Store<uint32_t>(kNoRow, payloads_[new_groups_.get_index(i)]);
        }
        // Prepend each row to its key's chain.
        for (idx_t i = 0; i < n; i++) {
            const auto row = static_cast<uint32_t>(base + offset + (all_valid ? i : valid.get_index(i)));
            chain_next_[row] = duckdb::Load<uint32_t>(payloads_[i]);
            duckdb::Store<uint32_t>(row, payloads_[i]);
        }
    }
    return Ok();
}

void JoinOperator::FinishBuild() {
    if (track_build_) {
        matched_.assign(build_count_, 0);
    }
    if (build_->chunks.size() > 1) {
        build_->formats.resize(right_types_.size());
        for (idx_t c = 0; c < right_types_.size(); c++) {
            build_->formats[c] = std::vector<UnifiedVectorFormat>(build_->chunks.size());
            for (idx_t k = 0; k < build_->chunks.size(); k++) {
                build_->chunks[k]->data[c].ToUnifiedFormat(build_->formats[c][k]);
            }
        }
    }
    build_finished_ = true;
}

void JoinOperator::LocateBuildRows(const SelectionVector &rsel, idx_t count) {
    const auto &starts = build_->starts;
    uint32_t chunk = 0;
    for (idx_t i = 0; i < count; i++) {
        const idx_t row = rsel.get_index(i);
        // Pairs often come from the same chunk as the previous one.
        if (row < starts[chunk] || (chunk + 1 < starts.size() && row >= starts[chunk + 1])) {
            chunk = static_cast<uint32_t>(std::upper_bound(starts.begin(), starts.end(), row) - starts.begin() - 1);
        }
        loc_chunk_[i] = chunk;
        loc_row_[i] = static_cast<uint32_t>(row - starts[chunk]);
    }
}

void JoinOperator::BuildColumn(idx_t col, const SelectionVector &rsel, idx_t count, Vector &out) {
    if (build_->chunks.size() == 1) {
        out.Slice(build_->chunks[0]->data[col], rsel, count);
        return;
    }
    // Gather into a flat vector (call LocateBuildRows first).
    Vector gathered(right_types_[col], count);
    auto data = FlatVector::GetDataMutable(gathered);
    auto &validity = FlatVector::ValidityMutable(gathered);
    const auto *formats = build_->formats[col].data();
    const auto type = right_types_[col].InternalType();
    switch (duckdb::GetTypeIdSize(type)) {
    case 1:  GatherFixed<1>(formats, loc_chunk_.data(), loc_row_.data(), count, data, validity); break;
    case 2:  GatherFixed<2>(formats, loc_chunk_.data(), loc_row_.data(), count, data, validity); break;
    case 4:  GatherFixed<4>(formats, loc_chunk_.data(), loc_row_.data(), count, data, validity); break;
    case 8:  GatherFixed<8>(formats, loc_chunk_.data(), loc_row_.data(), count, data, validity); break;
    case 16: GatherFixed<16>(formats, loc_chunk_.data(), loc_row_.data(), count, data, validity); break;
    default:
        throw duckdb::NotImplementedException("Join: cannot gather build column of type %s",
                                              right_types_[col].ToString());
    }
    if (type == duckdb::PhysicalType::VARCHAR) {
        // The strings stay in the build chunks.
        duckdb::StringVector::AddAuxiliaryData(gathered, duckdb::make_uniq<BuildDataHolder>(build_));
    }
    FlatVector::SetSize(gathered, count);
    out.Reference(gathered);
}

Result<void> JoinOperator::EmitRows(DataChunk *left, const SelectionVector *lsel, const SelectionVector *rsel,
                                    idx_t count, bool with_left, bool with_right) {
    if (count == 0) {
        return Ok();
    }
    auto out = std::make_unique<DataChunk>();
    out->InitializeEmpty(output_types_);
    idx_t c = 0;
    if (with_left) {
        for (idx_t i = 0; i < left_types_.size(); i++, c++) {
            if (left) {
                out->data[c].Slice(left->data[i], *lsel, count);
            } else {
                out->data[c].Reference(Value(left_types_[i]), duckdb::count_t(count));
            }
        }
    }
    if (with_right) {
        if (rsel && build_->chunks.size() > 1) {
            LocateBuildRows(*rsel, count);
        }
        for (idx_t i = 0; i < right_types_.size(); i++, c++) {
            if (rsel) {
                BuildColumn(i, *rsel, count, out->data[c]);
            } else {
                out->data[c].Reference(Value(right_types_[i]), duckdb::count_t(count));
            }
        }
    }
    out->SetChildCardinality(count);
    return next_->Push(std::move(out));
}

Result<void> JoinOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    if (!build_finished_) {
        return Error("JoinOperator::Push was called before JoinOperator::FinishBuild was called.");
    }
    DataChunk &probe = *chunk;
    const idx_t count = probe.size();
    if (count == 0) {
        return Ok();
    }

    // Each probe row's chain head (kNoRow: NULL key or no match).
    std::fill(heads_.begin(), heads_.begin() + count, kNoRow);
    std::fill(found_.begin(), found_.begin() + count, 0);
    SelectionVector valid;
    bool all_valid = true;
    idx_t n_keyed = 0; // rows whose key may match
    if (table_->Count() > 0) {
        for (idx_t c = 0; c < left_keys_.size(); c++) {
            key_chunk_.data[c].Reference(probe.data[left_keys_[c]]);
        }
        n_keyed = FilterNullKeys(key_chunk_, count, valid, all_valid);
        if (n_keyed > 0) {
            Vector hashes(LogicalType::HASH, n_keyed);
            table_->Hash(key_chunk_, hashes, n_keyed);
            table_->Find(key_chunk_, hashes, n_keyed, payloads_.data());
            for (idx_t i = 0; i < n_keyed; i++) {
                if (payloads_[i]) {
                    heads_[all_valid ? i : valid.get_index(i)] = duckdb::Load<uint32_t>(payloads_[i]);
                }
            }
        }
    }

    const bool emits_pairs = kind_ == JoinKind::INNER || kind_ == JoinKind::LEFT || kind_ == JoinKind::RIGHT
        || kind_ == JoinKind::OUTER || kind_ == JoinKind::SINGLE;
    // Kinds that only need a probe row's first match.
    const bool first_match_only = kind_ == JoinKind::SEMI || kind_ == JoinKind::ANTI || kind_ == JoinKind::MARK
        || kind_ == JoinKind::SINGLE;

    if (first_match_only && !residual_predicate_ && kind_ != JoinKind::SINGLE) {
        for (idx_t r = 0; r < count; r++) {
            found_[r] = heads_[r] != kNoRow;
        }
    } else {
        // Walk the chains a step at a time: each step pairs every active probe row with its current build row.
        SelectionVector active(STANDARD_VECTOR_SIZE);
        idx_t n_active = 0;
        for (idx_t r = 0; r < count; r++) {
            if (heads_[r] != kNoRow) {
                cur_[r] = heads_[r];
                active.set_index(n_active++, r);
            }
        }
        DataChunk pair_chunk;
        if (residual_predicate_) {
            duckdb::vector<LogicalType> natural = left_types_;
            natural.insert(natural.end(), right_types_.begin(), right_types_.end());
            pair_chunk.InitializeEmpty(natural);
        }
        SelectionVector match(STANDARD_VECTOR_SIZE);
        while (n_active > 0) {
            // Fresh selections per step: emitted dictionary vectors keep referencing them.
            SelectionVector lsel(n_active), rsel(n_active);
            for (idx_t i = 0; i < n_active; i++) {
                const idx_t r = active.get_index(i);
                lsel.set_index(i, r);
                rsel.set_index(i, cur_[r]);
            }
            idx_t n_match = n_active;
            if (residual_predicate_) {
                const idx_t n_left = left_types_.size();
                for (idx_t c = 0; c < n_left; c++) {
                    pair_chunk.data[c].Slice(probe.data[c], lsel, n_active);
                }
                if (build_->chunks.size() > 1) {
                    LocateBuildRows(rsel, n_active);
                }
                for (idx_t c = 0; c < right_types_.size(); c++) {
                    BuildColumn(c, rsel, n_active, pair_chunk.data[n_left + c]);
                }
                pair_chunk.SetChildCardinality(n_active);
                n_match = residual_executor_.SelectExpression(pair_chunk, match);
                if (n_match < n_active) { // compact the pairs to the matching ones
                    SelectionVector l2(std::max<idx_t>(n_match, 1)), r2(std::max<idx_t>(n_match, 1));
                    for (idx_t i = 0; i < n_match; i++) {
                        const idx_t p = match.get_index(i);
                        l2.set_index(i, lsel.get_index(p));
                        r2.set_index(i, rsel.get_index(p));
                    }
                    lsel = std::move(l2);
                    rsel = std::move(r2);
                }
            }
            for (idx_t i = 0; i < n_match; i++) {
                found_[lsel.get_index(i)] = 1;
            }
            if (track_build_) {
                for (idx_t i = 0; i < n_match; i++) {
                    matched_[rsel.get_index(i)] = 1;
                }
            }
            if (emits_pairs) {
                TRYV(EmitRows(&probe, &lsel, &rsel, n_match, true, true));
            }
            // Advance the rows that still need more of their chain.
            idx_t next_active = 0;
            for (idx_t i = 0; i < n_active; i++) {
                const idx_t r = active.get_index(i);
                if (first_match_only && found_[r]) {
                    continue;
                }
                const uint32_t nxt = chain_next_[cur_[r]];
                if (nxt != kNoRow) {
                    cur_[r] = nxt;
                    active.set_index(next_active++, r);
                }
            }
            n_active = next_active;
        }
    }

    // Probe rows by match status.
    auto select_rows = [&](bool want_found) {
        SelectionVector sel(count);
        idx_t n = 0;
        for (idx_t r = 0; r < count; r++) {
            if ((found_[r] != 0) == want_found) {
                sel.set_index(n++, r);
            }
        }
        return std::make_pair(std::move(sel), n);
    };
    switch (kind_) {
    case JoinKind::LEFT:
    case JoinKind::OUTER:
    case JoinKind::SINGLE: {
        auto [sel, n] = select_rows(false);
        return EmitRows(&probe, &sel, nullptr, n, true, true);
    }
    case JoinKind::SEMI:
    case JoinKind::ANTI: {
        auto [sel, n] = select_rows(kind_ == JoinKind::SEMI);
        if (n == count) {
            return next_->Push(std::move(chunk));
        }
        return EmitRows(&probe, &sel, nullptr, n, true, false);
    }
    case JoinKind::MARK: {
        // NULL-aware IN without a match: false for an empty build side, else NULL if the probe key
        // is NULL or the build side holds a NULL key.
        std::vector<uint8_t> keyed(count, all_valid ? 1 : 0);
        if (!all_valid) {
            for (idx_t i = 0; i < n_keyed; i++) {
                keyed[valid.get_index(i)] = 1;
            }
        }
        Vector marker(LogicalType::BOOLEAN, count);
        {
            auto writer = FlatVector::Writer<bool>(marker, count);
            for (idx_t r = 0; r < count; r++) {
                if (found_[r]) {
                    writer.WriteValue(true);
                } else if (build_count_ > 0 && (build_has_null_ || !keyed[r])) {
                    writer.WriteNull();
                } else {
                    writer.WriteValue(false);
                }
            }
        }
        const idx_t n_left = left_types_.size();
        auto out = std::make_unique<DataChunk>();
        out->InitializeEmpty(output_types_);
        for (idx_t c = 0; c < n_left; c++) {
            out->data[c].Reference(probe.data[c]);
        }
        out->data[n_left].Reference(marker);
        out->SetChildCardinality(count);
        return next_->Push(std::move(out));
    }
    default:
        return Ok();
    }
}

Result<void> JoinOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    if (track_build_) {
        const bool want_matched = kind_ == JoinKind::RIGHT_SEMI;
        const bool with_left = kind_ == JoinKind::RIGHT || kind_ == JoinKind::OUTER;
        for (idx_t b0 = 0; b0 < build_count_; b0 += STANDARD_VECTOR_SIZE) {
            const idx_t batch = std::min<idx_t>(STANDARD_VECTOR_SIZE, build_count_ - b0);
            SelectionVector sel(batch);
            idx_t n = 0;
            for (idx_t i = 0; i < batch; i++) {
                if ((matched_[b0 + i] != 0) == want_matched) {
                    sel.set_index(n++, b0 + i);
                }
            }
            TRYV(EmitRows(nullptr, nullptr, &sel, n, with_left, true));
        }
    }
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
    if (templ->left_keys.size() != templ->right_keys.size()) {
        return Error("Join key count mismatch", ErrorKind::InvalidInput);
    }
    for (size_t i = 0; i < templ->left_keys.size(); i++) {
        if (templ->left_keys[i] >= left_types.size() || templ->right_keys[i] >= right_types.size()) {
            return Error("Join key out of range", ErrorKind::InvalidInput);
        }
        const auto &lt = left_types[templ->left_keys[i]];
        const auto &rt = right_types[templ->right_keys[i]];
        if (lt != rt) {
            return Error("Join keys of different types: " + lt.ToString() + " vs " + rt.ToString(),
                         ErrorKind::NotImplemented);
        }
        if (!GroupTable::SupportsType(lt)) {
            return Error("Joining on " + lt.ToString() + " is not supported", ErrorKind::NotImplemented);
        }
    }
    return std::make_unique<JoinOperator>(templ->left_keys, templ->right_keys, std::move(left_types),
                                          std::move(right_types), alloc, templ->kind, std::move(residual),
                                          MatchNull(templ->keys_match_null, templ->left_keys.size()));
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
