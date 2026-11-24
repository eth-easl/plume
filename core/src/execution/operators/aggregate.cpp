#include "plume/execution/operators/aggregate.hpp"

#include "plume/expression/expression_builder.hpp"
#include "plume/expression/function_registry.hpp"

#include "duckdb/common/clustered_aggregate.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/aggregate_state.hpp"

#include <cstring>

namespace plume::exec {

using plume::memory::Allocator;
using expr::BuildExpression;

using duckdb::AggregateFinalizeInputData;
using duckdb::AggregateInputData;
using duckdb::AggregateStateInput;
using duckdb::data_ptr_t;
using duckdb::DataChunk;
using duckdb::FlatVector;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::unique_ptr;
using duckdb::Vector;

//===----------------------------------------------------------------------===//
// AggregateSpec
//===----------------------------------------------------------------------===//

bool AggregateSpec::Equals(const AggregateSpec &other) const {
    if (func_name != other.func_name) return false;
    if (!(return_type == other.return_type)) return false;
    if (distinct != other.distinct) return false;
    if (arguments.size() != other.arguments.size()) return false;
    for (size_t i = 0; i < arguments.size(); i++) {
        if (!arguments[i].Equals(other.arguments[i])) return false;
    }
    return true;
}

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

bool AggregateTemplate::Equals(const OperatorTemplate &other) const {
    if (other.type != type) return false;
    auto &o = static_cast<const AggregateTemplate &>(other);
    if (aggregates.size() != o.aggregates.size()) return false;
    for (size_t i = 0; i < aggregates.size(); i++) {
        if (!aggregates[i].Equals(o.aggregates[i])) return false;
    }
    if (group_keys.size() != o.group_keys.size()) return false;
    for (size_t i = 0; i < group_keys.size(); i++) {
        if (!group_keys[i].Equals(o.group_keys[i])) return false;
    }
    return true;
}

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

// Per-aggregate runtime state.
struct AggregateOperator::AggImpl {
    // Bound DuckDB kernel + its bind data (passed to every callback).
    std::unique_ptr<duckdb::BoundAggregateFunction> function;
    duckdb::unique_ptr<duckdb::FunctionData> bind_data;
    idx_t state_offset = 0; // offset of this aggregate's state in a group's payload

    // Argument evaluation.
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> args;
    std::unique_ptr<duckdb::ExpressionExecutor> executor; // null if 0 args
    DataChunk arg_chunk;
    duckdb::vector<LogicalType> arg_types;

    // DISTINCT: the (group, argument values) tuples seen so far
    bool distinct = false;
    std::unique_ptr<GroupTable> distinct_table;
    DataChunk distinct_keys; // references the group-id vector and arg_chunk's columns
    duckdb::SelectionVector distinct_sel;
};

AggregateOperator::AggregateOperator(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> group_exprs,
                                     std::vector<AggregateBuild> aggregates,
                                     duckdb::vector<LogicalType> output_types, Allocator &alloc)
    : Operator(std::move(output_types)), group_exprs_(std::move(group_exprs)), alloc_(alloc)
    , arena_(alloc_.Get()), ungrouped_(group_exprs_.empty()) {
    // A group's payload holds every aggregate's state, each 8-byte aligned.
    for (auto &build : aggregates) {
        auto impl = std::make_unique<AggImpl>();
        impl->args = std::move(build.arguments);
        if (!impl->args.empty()) {
            impl->executor = std::make_unique<duckdb::ExpressionExecutor>();
            for (auto &a : impl->args) {
                impl->executor->AddExpression(*a);
                impl->arg_types.push_back(a->GetReturnType());
            }
            impl->arg_chunk.Initialize(alloc_.Get(), impl->arg_types);
        }

        impl->function = std::make_unique<duckdb::BoundAggregateFunction>(std::move(build.aggregate.function));
        impl->bind_data = std::move(build.aggregate.bind_data);
        impl->state_offset = payload_size_;
        payload_size_ += duckdb::AlignValue<idx_t>(impl->function->GetStateSize(impl->bind_data.get()), 8);
        // Aggregates the kernel declares dedup-invariant (e.g. min/max) skip the dedup pass.
        impl->distinct = build.distinct && impl->function->GetDistinctDependent() ==
                                               duckdb::AggregateDistinctDependent::DISTINCT_DEPENDENT;
        if (impl->distinct && !impl->args.empty()) {
            duckdb::vector<LogicalType> dedup_types;
            if (!ungrouped_) {
                dedup_types.push_back(LogicalType::UBIGINT);
            }
            dedup_types.insert(dedup_types.end(), impl->arg_types.begin(), impl->arg_types.end());
            impl->distinct_table = std::make_unique<GroupTable>(dedup_types, 0, alloc_);
            impl->distinct_keys.InitializeEmpty(dedup_types);
            impl->distinct_sel.Initialize(STANDARD_VECTOR_SIZE);
        }
        aggs_.push_back(std::move(impl));
    }

    if (!ungrouped_) {
        group_executor_ = std::make_unique<duckdb::ExpressionExecutor>();
        for (auto &e : group_exprs_) {
            group_executor_->AddExpression(*e);
            key_types_.push_back(e->GetReturnType());
        }
        key_chunk_.Initialize(alloc_.Get(), key_types_);
        table_ = std::make_unique<GroupTable>(key_types_, payload_size_, alloc_);
    }
    payloads_.resize(STANDARD_VECTOR_SIZE);
    new_payloads_.resize(STANDARD_VECTOR_SIZE);
    state_ptrs_.resize(STANDARD_VECTOR_SIZE);
    new_groups_.Initialize(STANDARD_VECTOR_SIZE);
}

AggregateOperator::~AggregateOperator() {
    const idx_t n_groups = ungrouped_ ? (ungrouped_payload_ ? 1 : 0) : table_->Count();
    for (auto &agg : aggs_) {
        if (!agg->function->HasStateDestructorCallback()) {
            continue;
        }
        for (idx_t g0 = 0; g0 < n_groups; g0 += STANDARD_VECTOR_SIZE) {
            const idx_t batch = std::min<idx_t>(STANDARD_VECTOR_SIZE, n_groups - g0);
            Vector state_vec(LogicalType::POINTER, batch);
            auto sptr = FlatVector::GetDataMutable<data_ptr_t>(state_vec);
            for (idx_t i = 0; i < batch; i++) {
                sptr[i] = GroupPayload(g0 + i) + agg->state_offset;
            }
            FlatVector::SetSize(state_vec, batch);
            AggregateInputData aggr_input(*agg->function, agg->bind_data.get(), arena_);
            agg->function->GetStateDestructorCallback()(state_vec, aggr_input, batch);
        }
    }
}

data_ptr_t AggregateOperator::GroupPayload(idx_t group) const {
    return ungrouped_ ? ungrouped_payload_ : table_->Payload(group);
}

void AggregateOperator::InitializeStates(const data_ptr_t *payloads, idx_t count) {
    for (auto &agg : aggs_) {
        for (idx_t i = 0; i < count; i++) {
            state_ptrs_[i] = payloads[i] + agg->state_offset;
        }
        AggregateStateInput state_input(*agg->function, agg->bind_data.get());
        agg->function->GetStateInitCallback()(state_input, state_ptrs_.data(), count);
    }
}

void AggregateOperator::EnsureUngroupedState() {
    if (!ungrouped_payload_) {
        ungrouped_payload_ = arena_.AllocateAligned(std::max<idx_t>(payload_size_, 8));
        InitializeStates(&ungrouped_payload_, 1);
    }
}

Result<void> AggregateOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    const idx_t count = chunk->size();
    if (count == 0) {
        return Ok();
    }

    // Each row's group payload.
    if (ungrouped_) {
        EnsureUngroupedState();
        std::fill(payloads_.begin(), payloads_.begin() + count, ungrouped_payload_);
    } else {
        key_chunk_.Reset();
        group_executor_->Execute(*chunk, key_chunk_);
        Vector hashes(LogicalType::HASH, count);
        table_->Hash(key_chunk_, hashes, count);
        const idx_t n_new = table_->FindOrCreate(key_chunk_, hashes, count, payloads_.data(), new_groups_);
        if (n_new > 0) {
            for (idx_t i = 0; i < n_new; i++) {
                new_payloads_[i] = payloads_[new_groups_.get_index(i)];
            }
            InitializeStates(new_payloads_.data(), n_new);
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
        const duckdb::SelectionVector *distinct_sel = nullptr;
        if (agg->distinct_table) {
            auto &keys = agg->distinct_keys;
            idx_t c0 = 0;
            Vector gids(LogicalType::UBIGINT, count);
            if (!ungrouped_) {
                // A group's payload address identifies it (payloads never move).
                auto gid = FlatVector::GetDataMutable<uint64_t>(gids);
                for (idx_t r = 0; r < count; r++) {
                    gid[r] = reinterpret_cast<uintptr_t>(payloads_[r]);
                }
                FlatVector::SetSize(gids, count);
                keys.data[c0++].Reference(gids);
            }
            for (idx_t c = 0; c < n_args; c++) {
                keys.data[c0 + c].Reference(agg->arg_chunk.data[c]);
            }
            Vector hashes(LogicalType::HASH, count);
            agg->distinct_table->Hash(keys, hashes, count);
            upd_count = agg->distinct_table->FindOrCreate(keys, hashes, count, state_ptrs_.data(), agg->distinct_sel);
            if (upd_count == 0) {
                continue; // nothing new for this aggregate in this chunk
            }
            if (upd_count < count) {
                agg->arg_chunk.Slice(agg->distinct_sel, upd_count);
                inputs = agg->arg_chunk.data.data();
            }
            distinct_sel = &agg->distinct_sel;
        }

        AggregateInputData aggr_input(*agg->function, agg->bind_data.get(), arena_);
        const auto cluster_update = agg->function->GetStateClusterUpdateCallback();
        if (ungrouped_ && cluster_update) {
            duckdb::ClusteredAggr clustered;
            clustered.SetSingleRun(ungrouped_payload_ + agg->state_offset, upd_count);
            aggr_input.clustered = &clustered;
            cluster_update(inputs, aggr_input, n_args, clustered, upd_count);
        } else {
            Vector state_vec(LogicalType::POINTER);
            auto sptr = FlatVector::GetDataMutable<data_ptr_t>(state_vec);
            for (idx_t i = 0; i < upd_count; i++) {
                const idx_t r = distinct_sel ? distinct_sel->get_index(i) : i;
                sptr[i] = payloads_[r] + agg->state_offset;
            }
            FlatVector::SetSize(state_vec, upd_count);
            agg->function->GetStateUpdateCallback()(inputs, aggr_input, n_args, state_vec, upd_count);
        }
    }
    return Ok();
}

Result<void> AggregateOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    if (ungrouped_) {
        EnsureUngroupedState(); // ungrouped always emits one row
    }

    const idx_t n_groups = ungrouped_ ? 1 : table_->Count();
    const idx_t n_key_cols = key_types_.size();

    for (idx_t g0 = 0; g0 < n_groups; g0 += STANDARD_VECTOR_SIZE) {
        const idx_t batch = std::min<idx_t>(STANDARD_VECTOR_SIZE, n_groups - g0);
        auto result = std::make_unique<DataChunk>();
        result->Initialize(alloc_.Get(), output_types_);

        if (ungrouped_) {
            payloads_[0] = ungrouped_payload_;
        } else {
            table_->ScanKeys(g0, batch, *result, payloads_.data());
        }
        for (size_t j = 0; j < aggs_.size(); j++) {
            auto &agg = aggs_[j];
            Vector &col = result->data[n_key_cols + j];
            Vector state_vec(LogicalType::POINTER, batch);
            auto sptr = FlatVector::GetDataMutable<data_ptr_t>(state_vec);
            for (idx_t i = 0; i < batch; i++) {
                sptr[i] = payloads_[i] + agg->state_offset;
            }
            FlatVector::SetSize(state_vec, batch);
            AggregateFinalizeInputData finalize_input(*agg->function, agg->bind_data.get(), arena_);
            agg->function->GetStateFinalizeCallback()(state_vec, finalize_input, col, batch, 0);
        }
        result->SetChildCardinality(batch);
        TRYV(next_->Push(std::move(result)));
    }
    return next_->Finish();
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
        if (!GroupTable::SupportsType(built->GetReturnType())) {
            return Error("Grouping by " + built->GetReturnType().ToString() + " is not supported",
                         ErrorKind::NotImplemented);
        }
        out_types.push_back(built->GetReturnType());
        group_exprs.push_back(std::move(built));
    }
    
    std::vector<AggregateBuild> builds;
    builds.reserve(templ->aggregates.size()); // avoid realloc (AggregateBuild is move-only)
    for (auto &spec : templ->aggregates) {
        duckdb::vector<unique_ptr<duckdb::Expression>> args;
        std::vector<LogicalType> arg_types;
        for (auto &a : spec.arguments) {
            TRY(auto built, BuildExpression(a));
            arg_types.push_back(built->GetReturnType());
            args.push_back(std::move(built));
        }
        auto rettype = ToLogicalType(spec.return_type);
        out_types.push_back(rettype);
        if (spec.distinct) {
            for (auto &t : arg_types) {
                if (!GroupTable::SupportsType(t)) {
                    return Error("DISTINCT over " + t.ToString() + " is not supported", ErrorKind::NotImplemented);
                }
            }
        }
        
        TRY(auto resolved, expr::ResolveAggregateFunction(spec.func_name, arg_types));
        builds.push_back(AggregateBuild {std::move(args), rettype, std::move(resolved), spec.distinct});
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
