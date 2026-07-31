#include "plume/execution/operators/aggregate.hpp"

#include "plume/expression/expression_builder.hpp"
#include "plume/expression/function_registry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/vector.hpp"

#include <cstring>
#include <unordered_set>

namespace plume::exec {

using plume::memory::Allocator;
using expr::BuildExpression;

using duckdb::AggregateInputData;
using duckdb::data_ptr_t;
using duckdb::DataChunk;
using duckdb::FlatVector;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::PhysicalType;
using duckdb::string_t;
using duckdb::unique_ptr;
using duckdb::Value;
using duckdb::Vector;

//===----------------------------------------------------------------------===//
// AggregateSpec
//===----------------------------------------------------------------------===//

void AggregateSpec::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "func_name", func_name);
    s.WriteProperty(101, "return_type", return_type);
    s.WriteList(102, "arguments", arguments.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteElement(arguments[i]);
    });
    s.WriteProperty(103, "distinct", distinct);
}

AggregateSpec AggregateSpec::Deserialize(duckdb::Deserializer &d) {
    AggregateSpec spec;
    spec.func_name = d.ReadProperty<std::string>(100, "func_name");
    spec.return_type = d.ReadProperty<ColumnType>(101, "return_type");
    d.ReadList(102, "arguments", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
        spec.arguments.push_back(list.ReadElement<expr::ExprNode>());
    });
    spec.distinct = d.ReadProperty<bool>(103, "distinct");
    return spec;
}

//===----------------------------------------------------------------------===//
// AggregateTemplate
//===----------------------------------------------------------------------===//

void AggregateTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteList(101, "group_keys", group_keys.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteElement(group_keys[i]);
    });
    s.WriteList(102, "aggregates", aggregates.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteElement(aggregates[i]);
    });
}


//===----------------------------------------------------------------------===//
// AggregateOperator
//===----------------------------------------------------------------------===//

enum class AggStrategy { KERNEL, MIN, MAX };

// Per-aggregate runtime state.
// TODO: may want to turn this into a variant...
struct AggregateOperator::AggImpl {
    AggStrategy strategy = AggStrategy::KERNEL;

    // KERNEL: resolved DuckDB function + per-group state pointers.
    std::unique_ptr<duckdb::AggregateFunction> function;
    idx_t state_size = 0;
    std::vector<data_ptr_t> states;

    // MIN/MAX: per-group running value.
    std::vector<Value> values;

    // Argument evaluation (shared by both strategies; <=1 arg in v1).
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> args;
    std::unique_ptr<duckdb::ExpressionExecutor> executor; // null if 0 args
    DataChunk arg_chunk;
    duckdb::vector<LogicalType> arg_types;

    // DISTINCT: dedup (group, argument-values) before feeding a row into the strategy above.
    bool distinct = false;
    std::unordered_set<std::string> distinct_seen;
    std::string distinct_scratch;
};

namespace {

uint64_t HashKey(const char *data, size_t len) {
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < len; i++) {
        h ^= static_cast<uint8_t>(data[i]);
        h *= 1099511628211ull;
    }
    return h;
}

} // namespace

AggregateOperator::AggregateOperator(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> group_exprs,
                                     std::vector<AggregateBuild> aggregates,
                                     duckdb::vector<LogicalType> output_types, Allocator &alloc)
    : Operator(std::move(output_types)), group_exprs_(std::move(group_exprs)), alloc_(alloc)
    , arena_(alloc_.Get()), ungrouped_(group_exprs_.empty()) {
    if (!ungrouped_) {
        group_executor_ = std::make_unique<duckdb::ExpressionExecutor>();
        for (auto &e : group_exprs_) {
            group_executor_->AddExpression(*e);
            key_types_.push_back(e->return_type);
        }
        key_chunk_.Initialize(alloc_.Get(), key_types_);

        group_slots_.assign(1024, -1);
        group_mask_ = group_slots_.size() - 1;
        group_key_off_.push_back(0);
    }

    for (auto &build : aggregates) {
        auto impl = std::make_unique<AggImpl>();
        impl->args = std::move(build.arguments);
        if (!impl->args.empty()) {
            impl->executor = std::make_unique<duckdb::ExpressionExecutor>();
            for (auto &a : impl->args) {
                impl->executor->AddExpression(*a);
                impl->arg_types.push_back(a->return_type);
            }
            impl->arg_chunk.Initialize(alloc_.Get(), impl->arg_types);
        }

        if (build.func_name == "min" || build.func_name == "max") {
            impl->strategy = (build.func_name == "min") ? AggStrategy::MIN : AggStrategy::MAX;
        } else {
            impl->strategy = AggStrategy::KERNEL;
            impl->function = std::move(build.function);
            impl->state_size = impl->function->state_size(*impl->function);
        }
        // MIN/MAX are already dedup-invariant so only KERNEL aggregates need the dedup pass.
        impl->distinct = build.distinct && impl->strategy == AggStrategy::KERNEL;
        aggs_.push_back(std::move(impl));
    }
}

AggregateOperator::~AggregateOperator() {
    for (auto &agg : aggs_) {
        if (agg->strategy != AggStrategy::KERNEL || !agg->function->destructor || agg->states.empty()) {
            continue;
        }
        Vector state_vec(LogicalType::POINTER, agg->states.size());
        auto sptr = FlatVector::GetData<data_ptr_t>(state_vec);
        for (size_t i = 0; i < agg->states.size(); i++) {
            sptr[i] = agg->states[i];
        }
        AggregateInputData aggr_input(nullptr, arena_);
        agg->function->destructor(state_vec, aggr_input, agg->states.size());
    }
}

Result<void> AggregateOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    const idx_t count = chunk->size();
    if (count == 0) {
        return Ok();
    }

    group_of_row_.resize(count);
    std::vector<idx_t> &group_of_row = group_of_row_;
    if (ungrouped_) {
        if (group_key_values_.empty()) {
            group_key_values_.emplace_back();
            AddGroupSlots();
        }
        std::fill(group_of_row.begin(), group_of_row.end(), idx_t(0));
    } else {
        key_chunk_.Reset();
        group_executor_->Execute(*chunk, key_chunk_);
        key_chunk_.Flatten();

        std::vector<KeyCol> cols(key_chunk_.ColumnCount());
        for (idx_t c = 0; c < key_chunk_.ColumnCount(); c++) {
            Vector &vec = key_chunk_.data[c];
            const PhysicalType pt = vec.GetType().InternalType();
            cols[c].vec = &vec;
            cols[c].raw = FlatVector::GetData(vec);
            cols[c].is_varchar = (pt == PhysicalType::VARCHAR);
            cols[c].width = cols[c].is_varchar ? 0 : static_cast<uint32_t>(duckdb::GetTypeIdSize(pt));
        }
        for (idx_t r = 0; r < count; r++) {
            group_of_row[r] = ProbeOrInsert(key_chunk_, cols, r);
        }
    }

    for (auto &agg : aggs_) {
        Vector *inputs = nullptr;
        idx_t n_args = 0;
        if (agg->executor) {
            agg->arg_chunk.Reset();
            agg->executor->Execute(*chunk, agg->arg_chunk);
            inputs = agg->arg_chunk.data.data();
            n_args = agg->arg_chunk.ColumnCount();
        }

        // DISTINCT -> drop rows whose tuple this aggregate has already seen.
        idx_t upd_count = count;
        duckdb::SelectionVector sel;
        const duckdb::SelectionVector *distinct_sel = nullptr;
        if (agg->distinct && inputs != nullptr) {
            agg->arg_chunk.Flatten();
            std::vector<KeyCol> arg_cols(n_args);
            for (idx_t c = 0; c < n_args; c++) {
                Vector &vec = agg->arg_chunk.data[c];
                const PhysicalType pt = vec.GetType().InternalType();
                arg_cols[c].vec = &vec;
                arg_cols[c].raw = FlatVector::GetData(vec);
                arg_cols[c].is_varchar = (pt == PhysicalType::VARCHAR);
                arg_cols[c].width = arg_cols[c].is_varchar ? 0 : static_cast<uint32_t>(duckdb::GetTypeIdSize(pt));
            }
            sel.Initialize(count);
            idx_t n_new = 0;
            for (idx_t r = 0; r < count; r++) {
                const idx_t gid = ungrouped_ ? 0 : group_of_row[r];
                agg->distinct_scratch.assign(reinterpret_cast<const char *>(&gid), sizeof(gid));
                for (const auto &col : arg_cols) {
                    EncodeCell(agg->distinct_scratch, col, r);
                }
                if (agg->distinct_seen.insert(agg->distinct_scratch).second) {
                    sel.set_index(n_new++, r);
                }
            }
            upd_count = n_new;
            if (upd_count == 0) {
                continue; // nothing new for this aggregate in this chunk
            }
            if (upd_count < count) {
                agg->arg_chunk.Slice(sel, upd_count);
                inputs = agg->arg_chunk.data.data();
            }
            distinct_sel = &sel;
        }

        if (agg->strategy == AggStrategy::KERNEL) {
            AggregateInputData aggr_input(nullptr, arena_);
            if (ungrouped_) {
                agg->function->simple_update(inputs, aggr_input, n_args, agg->states[0], upd_count);
            } else {
                Vector state_vec(LogicalType::POINTER);
                auto sptr = FlatVector::GetData<data_ptr_t>(state_vec);
                for (idx_t i = 0; i < upd_count; i++) {
                    const idx_t r = distinct_sel ? distinct_sel->get_index(i) : i;
                    sptr[i] = agg->states[group_of_row[r]];
                }
                agg->function->update(inputs, aggr_input, n_args, state_vec, upd_count);
            }
        } else {
            // Native MIN/MAX over the (single) argument column.
            const bool is_min = agg->strategy == AggStrategy::MIN;
            for (idx_t r = 0; r < count; r++) {
                Value v = agg->arg_chunk.GetValue(0, r);
                if (v.IsNull()) {
                    continue;
                }
                Value &cur = agg->values[group_of_row[r]];
                if (cur.IsNull() || (is_min ? (v < cur) : (v > cur))) {
                    cur = std::move(v);
                }
            }
        }
    }
    return Ok();
}

Result<void> AggregateOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    if (ungrouped_ && group_key_values_.empty()) {
        group_key_values_.emplace_back(); // ungrouped always emits one row
        AddGroupSlots();
    }

    const idx_t n_groups = group_key_values_.size();
    const idx_t n_key_cols = key_types_.size();

    AggregateInputData aggr_input(nullptr, arena_);
    for (idx_t g0 = 0; g0 < n_groups; g0 += STANDARD_VECTOR_SIZE) {
        const idx_t batch = std::min<idx_t>(STANDARD_VECTOR_SIZE, n_groups - g0);
        auto result = std::make_unique<DataChunk>();
        result->Initialize(alloc_.Get(), output_types_);

        for (idx_t c = 0; c < n_key_cols; c++) {
            for (idx_t i = 0; i < batch; i++) {
                result->SetValue(c, i, group_key_values_[g0 + i][c]);
            }
        }
        for (size_t j = 0; j < aggs_.size(); j++) {
            auto &agg = aggs_[j];
            Vector &col = result->data[n_key_cols + j];
            if (agg->strategy == AggStrategy::KERNEL) {
                Vector state_vec(LogicalType::POINTER, batch);
                auto sptr = FlatVector::GetData<data_ptr_t>(state_vec);
                for (idx_t i = 0; i < batch; i++) {
                    sptr[i] = agg->states[g0 + i];
                }
                agg->function->finalize(state_vec, aggr_input, col, batch, 0);
            } else {
                for (idx_t i = 0; i < batch; i++) {
                    result->SetValue(n_key_cols + j, i, agg->values[g0 + i]);
                }
            }
        }
        result->SetCardinality(batch);
        TRYV(next_->Push(std::move(result)));
    }
    return next_->Finish();
}

void AggregateOperator::EncodeCell(std::string &key, const AggregateOperator::KeyCol &col, idx_t row) {
    if (!FlatVector::Validity(*col.vec).RowIsValid(row)) {
        key.push_back('\0');
        return;
    }
    key.push_back('\1');
    if (col.is_varchar) {
        const auto &s = reinterpret_cast<const string_t *>(col.raw)[row];
        uint32_t len = s.GetSize();
        key.append(reinterpret_cast<const char *>(&len), 4);
        key.append(s.GetData(), len);
        return;
    }
    key.append(reinterpret_cast<const char *>(col.raw + row * col.width), col.width);
}

void AggregateOperator::RehashGroups() {
    group_slots_.assign(group_slots_.size() * 2, -1);
    group_mask_ = group_slots_.size() - 1;
    for (size_t g = 0; g < group_hashes_.size(); g++) {
        size_t pos = group_hashes_[g] & group_mask_;
        while (group_slots_[pos] >= 0) {
            pos = (pos + 1) & group_mask_;
        }
        group_slots_[pos] = static_cast<int32_t>(g);
    }
}

void AggregateOperator::AddGroupSlots() {
    for (auto &agg : aggs_) {
        if (agg->strategy == AggStrategy::KERNEL) {
            auto state = arena_.Allocate(agg->state_size);
            agg->function->initialize(*agg->function, state);
            agg->states.push_back(state);
        } else {
            agg->values.emplace_back(); // NULL until first non-null input
        }
    }
}

idx_t AggregateOperator::ProbeOrInsert(DataChunk &keys, const std::vector<KeyCol> &cols, idx_t row) {
    key_scratch_.clear();
    for (const auto &col : cols) {
        EncodeCell(key_scratch_, col, row);
    }
    const uint64_t hash = HashKey(key_scratch_.data(), key_scratch_.size());
    const size_t klen = key_scratch_.size();

    size_t pos = hash & group_mask_;
    while (group_slots_[pos] >= 0) {
        const int32_t g = group_slots_[pos];
        if (group_hashes_[g] == hash) {
            const uint32_t s = group_key_off_[g];
            const uint32_t e = group_key_off_[g + 1];
            if (e - s == klen && std::memcmp(group_key_store_.data() + s, key_scratch_.data(), klen) == 0) {
                return static_cast<idx_t>(g);
            }
        }
        pos = (pos + 1) & group_mask_;
    }

    const int32_t g = static_cast<int32_t>(group_key_values_.size());
    group_key_store_.insert(group_key_store_.end(), key_scratch_.begin(), key_scratch_.end());
    group_key_off_.push_back(static_cast<uint32_t>(group_key_store_.size()));
    group_hashes_.push_back(hash);
    group_slots_[pos] = g;

    std::vector<Value> values;
    values.reserve(keys.ColumnCount());
    for (idx_t c = 0; c < keys.ColumnCount(); c++) {
        values.push_back(keys.GetValue(c, row));
    }
    group_key_values_.push_back(std::move(values));
    AddGroupSlots();

    // Keep the load factor under 1/2 so probes stay short.
    if (group_key_values_.size() * 2 >= group_slots_.size()) {
        RehashGroups();
    }
    return static_cast<idx_t>(g);
}

//===----------------------------------------------------------------------===//
// Build / schema
//===----------------------------------------------------------------------===//

Result<std::unique_ptr<Operator>> BuildAggregateTemplate(std::shared_ptr<AggregateTemplate> templ,
        duckdb::vector<LogicalType> col_types, memory::Allocator &alloc) {
    (void)col_types; // output types derive from the group keys + aggregate specs
    duckdb::vector<unique_ptr<duckdb::Expression>> group_exprs;
    duckdb::vector<LogicalType> out_types;
    for (auto &g : templ->group_keys) {
        TRY(auto built, BuildExpression(g));
        out_types.push_back(built->return_type);
        group_exprs.push_back(std::move(built));
    }
    
    std::vector<AggregateBuild> builds;
    builds.reserve(templ->aggregates.size()); // avoid realloc (AggregateBuild is move-only)
    for (auto &spec : templ->aggregates) {
        duckdb::vector<unique_ptr<duckdb::Expression>> args;
        std::vector<LogicalType> arg_types;
        for (auto &a : spec.arguments) {
            TRY(auto built, BuildExpression(a));
            arg_types.push_back(built->return_type);
            args.push_back(std::move(built));
        }
        auto rettype = ToLogicalType(spec.return_type);
        out_types.push_back(rettype);
        
        // Resolve the kernel up front (min/max are Plume-native, no kernel needed).
        std::unique_ptr<duckdb::AggregateFunction> function;
        if (spec.func_name != "min" && spec.func_name != "max") {
            TRY(auto resolved, expr::ResolveAggregateFunction(spec.func_name, arg_types));
            function = std::make_unique<duckdb::AggregateFunction>(std::move(resolved));
        }

        builds.push_back(
            AggregateBuild {spec.func_name, std::move(args), rettype, std::move(function), spec.distinct});
    }

    return std::unique_ptr<Operator>(
        std::make_unique<AggregateOperator>(std::move(group_exprs), std::move(builds), out_types, alloc));
}

Schema AggregateSchema(const AggregateTemplate &templ, const Schema &input_schema) {
    Schema out;
    for (size_t i = 0; i < templ.group_keys.size(); i++) {
        const auto &g = templ.group_keys[i];
        Column col;
        if (g.kind == expr::ExprKind::REFERENCE && g.ref_index < input_schema.columns.size()) {
            col.name = input_schema.columns[g.ref_index].name;
        } else {
            col.name = "key" + std::to_string(i);
        }
        col.type = g.return_type;
        col.nullable = true;
        out.columns.push_back(std::move(col));
    }

    for (const auto &agg : templ.aggregates) {
        Column col;
        col.name = agg.func_name;
        col.type = agg.return_type;
        col.nullable = true;
        out.columns.push_back(std::move(col));
    }

    return out;
}

} // namespace plume::exec
