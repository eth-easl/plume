#include "plume/parser/converter.hpp"

#include "plume/catalog/catalog.hpp"
#include "plume/catalog/local.hpp"
#include "plume/catalog/plume_remote.hpp"
#include "plume/common/result.hpp"
#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/execution/operators/limit.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/execution/operators/sort.hpp"
#include "plume/execution/operators/top_n.hpp"
#include "plume/expression/expression.hpp"
#include "plume/parser/expression_translator.hpp"
#include "plume/parser/physical_plan.hpp"
#include "plume/parser/plan_builder.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <optional>
#include <utility>

namespace plume::parser {

using duckdb::LogicalGet;
using duckdb::LogicalOperatorType;
using catalog::CreateTableSource;
using catalog::DataSource;
using catalog::kPlumeRemoteName;
using catalog::PlumeRemoteBindData;
using catalog::SourceCatalog;

Result<std::unique_ptr<PhysicalPlan>> Converter::Convert(duckdb::LogicalOperator &root) {
    TRY(auto root_node, Build(root));
    if (cfg_.dynamic_filter) {
        PlanDynamicFilters(builder_, root_node, dyn_filter_ctx_, cfg_);
    }
    return builder_.Export(root_node);
}

uint32_t Converter::SplitCount(uint64_t estimated_rows) const {
    if (cfg_.max_splits <= 1 || cfg_.target_rows_per_split == 0) {
        return cfg_.max_splits; // parallelism disabled, or "split as far as allowed"
    }
    uint64_t n = (estimated_rows + cfg_.target_rows_per_split - 1) / cfg_.target_rows_per_split;
    if (n < 1) {
        n = 1;
    }
    if (n > cfg_.max_splits) {
        n = cfg_.max_splits;
    }
    return static_cast<uint32_t>(n);
}

Result<PlanBuilder::SharedOpNode> Converter::BuildProjection(duckdb::LogicalOperator &op) {
    TRY(auto prev, Build(*op.children[0]));

    auto t = std::make_shared<exec::ProjectionTemplate>();
    t->projections.reserve(op.expressions.size());
    for (auto &e : op.expressions) {
        TRY(auto proj, TranslateExpression(*e));
        t->projections.push_back(std::move(proj));
    }

    Schema out = exec::ProjectionSchema(t->projections, prev->output_schema);
    return builder_.After(std::move(prev), std::move(t), std::move(out));
}

Result<PlanBuilder::SharedOpNode> Converter::BuildFilter(duckdb::LogicalFilter &filter) {
    TRY(auto prev, Build(*filter.children[0]));

    TRY(auto filter_expr, CombinePredicates(filter.expressions));
    auto t = std::make_shared<exec::FilterTemplate>(filter_expr);
    Schema out = prev->output_schema; // filter preserves schema
    PlanBuilder::SharedOpNode cur = builder_.After(std::move(prev), std::move(t), std::move(out));

    if (!filter.projection_map.empty()) {
        auto proj_templ = std::make_shared<exec::ProjectionTemplate>();
        Schema proj_schema;
        for (auto idx : filter.projection_map) {
            const Column &col = cur->output_schema.columns[idx];
            proj_templ->projections.push_back(expr::ExprNode::Reference(static_cast<uint32_t>(idx), col.type));
            proj_schema.columns.push_back(col);
        }
        cur = builder_.After(std::move(cur), std::move(proj_templ), std::move(proj_schema));
    }
    return cur;
}

Result<PlanBuilder::SharedOpNode> Converter::BuildAggregate(duckdb::LogicalAggregate &agg) {
    if (agg.grouping_sets.size() > 1) {
        return Error("GROUPING SETS / ROLLUP / CUBE not supported.", ErrorKind::NotImplemented);
    }
    
    TRY(auto prev, Build(*agg.children[0]));

    std::vector<ExprNode> groups;
    groups.reserve(agg.groups.size());
    for (auto &g : agg.groups) {
        TRY(auto key, TranslateExpression(*g));
        groups.push_back(std::move(key));
    }
    std::vector<exec::AggregateSpec> aggs;
    aggs.reserve(agg.expressions.size());
    for (auto &e : agg.expressions) {
        auto &agg = e->Cast<duckdb::BoundAggregateExpression>();
        TRY(auto spec, TranslateAggregate(agg));
        aggs.push_back(std::move(spec));
    }

    const uint32_t num_groups = static_cast<uint32_t>(groups.size());
    bool two_phase = cfg_.early_aggregation;
    bool has_distinct = false;
    for (const auto &spec : aggs) {
        if (spec.distinct) {
            has_distinct = true;
            break;
        }
    }
    if (two_phase) {
        for (const auto &agg : aggs) {
            const std::string &f = agg.func_name;
            if (agg.distinct || (f != "sum" && f != "count" && f != "count_star" && f != "min" && f != "max" && f != "avg")) {
                two_phase = false;
                break;
            }
        }
    }

    // single-phase aggregate (no early-aggregation)
    if (!two_phase) {
        // AVG on a DECIMAL requires a AverageDecimalBindData to descale its result. Since we always
        // use a null bind_data we sidestep this issue by casting to a DOUBLE.
        for (auto &spec : aggs) {
            if (spec.func_name == "avg" && spec.arguments.size() == 1 &&
                spec.arguments[0].return_type.id == TypeId::DECIMAL) {
                spec.arguments[0] =
                    expr::ExprNode::Cast(std::move(spec.arguments[0]), ColumnType{TypeId::DOUBLE}, /*try_cast=*/false);
            }
        }
        std::vector<uint32_t> split_keys;
        uint32_t partitions = 1;
        bool bare_columns = num_groups > 0;
        for (const auto &g : groups) {
            if (g.kind != expr::ExprKind::REFERENCE) {
                bare_columns = false;
                break;
            }
        }
        if (bare_columns) {
            split_keys.reserve(num_groups);
            for (const auto &g : groups) {
                split_keys.push_back(g.ref_index);
            }
            if (has_distinct && agg.children[0]->has_estimated_cardinality) {
                // FIXME: this *8 factor is a bit arbitrary -> come up with a better solution
                partitions = SplitCount(agg.children[0]->estimated_cardinality * 8);
            } else if (!has_distinct && agg.has_estimated_cardinality) {
                partitions = SplitCount(agg.estimated_cardinality);
            } else {
                partitions = cfg_.max_splits;
            }
        }
        auto t = std::make_shared<exec::AggregateTemplate>(std::move(aggs), std::move(groups));
        Schema out = exec::AggregateSchema(*t, prev->output_schema);
        return builder_.Shuffle(std::move(prev), std::move(t), std::move(out), std::move(split_keys), partitions);
    }

    // two-phase aggregation
    exec::AggregateTemplate desired_templ(aggs, groups);
    Schema desired = exec::AggregateSchema(desired_templ, prev->output_schema);

    std::vector<ColumnType> key_types;
    key_types.reserve(num_groups);
    for (const auto &g : groups) {
        key_types.push_back(g.return_type);
    }

    std::vector<exec::AggregateSpec> partials;
    std::vector<exec::AggregateSpec> combines;
    std::vector<ExprNode> finals;
    uint32_t pcol = num_groups; // next partial output column
    uint32_t ccol = num_groups; // next combine output column
    bool need_projection = false;

    const ColumnType kHugeint{TypeId::HUGEINT}; // sum over an integral partial
    const ColumnType kInt64{TypeId::INT64};     // count's result
    const ColumnType kDouble{TypeId::DOUBLE};

    auto partial_ref = [&](uint32_t idx, ColumnType type) { return expr::ExprNode::Reference(idx, type); };

    for (auto &agg : aggs) {
        const std::string &f = agg.func_name;
        if (f == "sum" || f == "min" || f == "max") {
            // sum/min/max are self-combining and the combine preserves the type:
            // sum(decimal(38,s))->decimal(38,s), sum(hugeint)->hugeint, etc.
            partials.push_back({f, agg.return_type, agg.arguments});
            uint32_t pi = pcol++;
            combines.push_back({f, agg.return_type, {partial_ref(pi, agg.return_type)}});
            uint32_t ci = ccol++;
            finals.push_back(partial_ref(ci, agg.return_type));
        } else if (f == "count" || f == "count_star") {
            // count partial -> sum of counts; the sum widens (BIGINT->HUGEINT), so
            // cast back to the original count type in the post-projection.
            partials.push_back({f, agg.return_type, agg.arguments});
            uint32_t pi = pcol++;
            combines.push_back({"sum", kHugeint, {partial_ref(pi, agg.return_type)}});
            uint32_t ci = ccol++;
            finals.push_back(expr::ExprNode::Cast(partial_ref(ci, kHugeint), agg.return_type, /*try_cast=*/false));
            need_projection = true;
        } else { 
            // avg = sum / count. avg is floating, so sum the argument as DOUBLE
            // (also sidesteps DuckDB's unbound decimal-sum return type).
            ExprNode arg_as_double = expr::ExprNode::Cast(agg.arguments.at(0), kDouble, /*try_cast=*/false);
            partials.push_back({"sum", kDouble, {std::move(arg_as_double)}});
            uint32_t pi_sum = pcol++;
            partials.push_back({"count", kInt64, agg.arguments});
            uint32_t pi_cnt = pcol++;

            combines.push_back({"sum", kDouble, {partial_ref(pi_sum, kDouble)}});
            uint32_t ci_sum = ccol++;
            combines.push_back({"sum", kHugeint, {partial_ref(pi_cnt, kInt64)}});
            uint32_t ci_cnt = ccol++;

            ExprNode den = expr::ExprNode::Cast(partial_ref(ci_cnt, kHugeint), kDouble, false);
            ExprNode quotient =
                expr::ExprNode::Function("/", kDouble, {partial_ref(ci_sum, kDouble), std::move(den)});
            finals.push_back(agg.return_type.id == TypeId::DOUBLE ? std::move(quotient)
                             : expr::ExprNode::Cast(std::move(quotient), agg.return_type, false));
            need_projection = true;
        }
    }

    // early aggregation node
    auto partial = std::make_shared<exec::AggregateTemplate>(std::move(partials), std::move(groups));
    Schema partial_out = exec::AggregateSchema(*partial, prev->output_schema);
    PlanBuilder::SharedOpNode produced = builder_.After(std::move(prev), std::move(partial), partial_out);

    // final aggregation node
    std::vector<uint32_t> split_keys;
    uint32_t partitions = 1;
    if (num_groups > 0) {
        split_keys.reserve(num_groups);
        for (uint32_t i = 0; i < num_groups; i++) {
            split_keys.push_back(i);
        }
        partitions = agg.has_estimated_cardinality ? SplitCount(agg.estimated_cardinality) : cfg_.max_splits;
    }
    std::vector<expr::ExprNode> final_group_keys;
    final_group_keys.reserve(num_groups);
    for (uint32_t i = 0; i < num_groups; i++) {
        final_group_keys.push_back(expr::ExprNode::Reference(i, key_types[i]));
    }
    auto final_agg = std::make_shared<exec::AggregateTemplate>(std::move(combines), std::move(final_group_keys));
    Schema final_out = exec::AggregateSchema(*final_agg, partial_out);
    PlanBuilder::SharedOpNode cur = builder_.Shuffle(
        std::move(produced), std::move(final_agg), final_out, std::move(split_keys), partitions);

    if (need_projection) {
        auto proj = std::make_shared<exec::ProjectionTemplate>();
        for (uint32_t i = 0; i < num_groups; i++) {
            proj->projections.push_back(expr::ExprNode::Reference(i, key_types[i]));
        }
        for (auto &fe : finals) {
            proj->projections.push_back(std::move(fe));
        }
        cur = builder_.After(std::move(cur), std::move(proj), desired);
    }
    return cur;
}

Result<PlanBuilder::SharedOpNode> Converter::BuildLimit(duckdb::LogicalLimit &limit) {
    TRY(auto prev, Build(*limit.children[0]));
    
    auto t = std::make_shared<exec::LimitTemplate>();
    if (limit.limit_val.Type() == duckdb::LimitNodeType::CONSTANT_VALUE) {
        t->has_limit = true;
        t->limit = limit.limit_val.GetConstantValue();
    } else if (limit.limit_val.Type() != duckdb::LimitNodeType::UNSET) {
        return Error("Only constant LIMIT is supported.", ErrorKind::NotImplemented);
    }
    if (limit.offset_val.Type() == duckdb::LimitNodeType::CONSTANT_VALUE) {
        t->offset = limit.offset_val.GetConstantValue();
    } else if (limit.offset_val.Type() != duckdb::LimitNodeType::UNSET) {
        return Error("Only constant OFFSET is supported.", ErrorKind::NotImplemented);
    }

    Schema out = prev->output_schema; // limit preserves schema
    // FIXME: We do not yet parallelize a global limit. As a result, this needs to gather all rows here.
    return builder_.Shuffle(std::move(prev), std::move(t), std::move(out), {}, 1);
}

Result<PlanBuilder::SharedOpNode> Converter::BuildOrder(duckdb::LogicalOrder &order) {
    if (!order.projection_map.empty()) {
        return Error("Order projection_map is not supported.", ErrorKind::NotImplemented);
    }

    TRY(auto prev, Build(*order.children[0]));

    std::vector<exec::SortKey> sort_keys;
    sort_keys.reserve(order.orders.size());
    for (auto &o : order.orders) {
        exec::SortKey key;
        TRY(key.expr, TranslateExpression(*o.expression));
        key.order = (o.type == duckdb::OrderType::DESCENDING) ? exec::SortOrder::DESCENDING
                                                                : exec::SortOrder::ASCENDING;
        key.null_order = (o.null_order == duckdb::OrderByNullType::NULLS_FIRST)
                                ? exec::NullOrder::NULLS_FIRST
                                : exec::NullOrder::NULLS_LAST;
        sort_keys.push_back(std::move(key));
    }
    auto t = std::make_shared<exec::SortTemplate>(std::move(sort_keys));

    Schema out = prev->output_schema; // sort preserves schema
    // FIXME: We have not yet implemented parallel sorting. As a result, we need to gather all rows here.
    return builder_.Shuffle(std::move(prev), std::move(t), std::move(out), {}, 1);
}

Result<PlanBuilder::SharedOpNode> Converter::BuildTopN(duckdb::LogicalTopN &top) {
    TRY(auto prev, Build(*top.children[0]));
    
    auto t = std::make_shared<exec::TopNTemplate>();
    t->sort_keys.reserve(top.orders.size());
    for (auto &o : top.orders) {
        exec::SortKey key;
        TRY(key.expr, TranslateExpression(*o.expression));
        key.order = (o.type == duckdb::OrderType::DESCENDING) ? exec::SortOrder::DESCENDING
                                                                : exec::SortOrder::ASCENDING;
        key.null_order = (o.null_order == duckdb::OrderByNullType::NULLS_FIRST)
                                ? exec::NullOrder::NULLS_FIRST
                                : exec::NullOrder::NULLS_LAST;
        t->sort_keys.push_back(std::move(key));
    }
    t->limit = top.limit;
    t->offset = top.offset;

    Schema schema = prev->output_schema; // top-n preserves schema
    // FIXME: We have not yet implemented a parallel top-n. As a result, this needs to gather all rows here.
    return builder_.Shuffle(std::move(prev), std::move(t), std::move(schema), {}, 1);
}

Result<PlanBuilder::SharedOpNode> Converter::BuildGet(duckdb::LogicalGet &get) {
    auto table = get.GetTable();
    std::shared_ptr<DataSource> data_source = nullptr;
    bool is_remote = false;
    if (!table && get.function.name == kPlumeRemoteName && get.bind_data) {
        data_source = get.bind_data->Cast<PlumeRemoteBindData>().info;
        if (!data_source->schema) {
            return Error("Remote data source should have been resolved by now.", ErrorKind::RuntimeError);
        }
        is_remote = true;
    }
    const duckdb::vector<duckdb::ColumnIndex> &column_ids = get.GetColumnIds();
    std::vector<bool> not_null_cols;
    if (table) {
        not_null_cols.assign(table->GetColumns().LogicalColumnCount(), false);
        for (auto &constraint : table->GetConstraints()) {
            if (constraint->type != duckdb::ConstraintType::NOT_NULL) {
                continue;
            }
            idx_t col_idx = constraint->Cast<duckdb::NotNullConstraint>().index.index;
            if (col_idx < not_null_cols.size()) {
                not_null_cols[col_idx] = true;
            }
        }
    }

    Schema schema_scanned;
    schema_scanned.columns.reserve(column_ids.size());
    for (const auto &cid : column_ids) {
        if (cid.IsVirtualColumn()) {
            return Error("Virtual/row-id columns are not supported.", ErrorKind::NotImplemented);
        }
        const idx_t idx = cid.GetPrimaryIndex();
        Column col;
        col.name = get.names[idx];
        col.type = FromLogicalType(get.returned_types[idx]);
        if (data_source && idx < data_source->schema->columns.size()) {
            col.nullable = data_source->schema->columns[idx].nullable;
        } else if (table && idx < not_null_cols.size()) {
            col.nullable = !not_null_cols[idx];
        } else {
            col.nullable = true;
        }
        schema_scanned.columns.push_back(std::move(col));
    }
    
    std::vector<expr::ExprNode> filter_preds;                     // pipeline FILTER (scanned-position refs)
    std::shared_ptr<expr::ExprNode> pushed_filter_pred = nullptr; // reader pruning (storage-index refs)
    if (!get.table_filters.filters.empty()) {
        std::vector<duckdb::idx_t> keys;
        for (auto &kv : get.table_filters.filters) {
            keys.push_back(kv.first);
        }

        std::vector<expr::ExprNode> pushed_filter_preds;
        for (auto storage_idx : keys) {
            const auto &filter = *get.table_filters.filters.at(storage_idx);
            
            // Skipping runtime/accelerator filters as Plume handles them differently.
            switch (filter.filter_type) {
            case duckdb::TableFilterType::OPTIONAL_FILTER:
            case duckdb::TableFilterType::DYNAMIC_FILTER:
            case duckdb::TableFilterType::BLOOM_FILTER:
                continue;
            default:
                break;
            }

            // reconstruct filter
            // TODO: is this necessary for materialized tables?
            uint32_t pos = 0;
            for (; pos < column_ids.size(); pos++) {
                if (column_ids[pos].GetPrimaryIndex() == storage_idx) {
                    break;
                }
            }
            if (pos == column_ids.size()) {
                return Error("Pushed-down filter on a non-scanned column.", ErrorKind::RuntimeError);
            }
            auto colref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(get.returned_types[storage_idx], pos);
            TRY(auto reconstructed, TryCatch([&] { return filter.ToExpression(*colref); }));
            TRY(auto filter_pred, TranslateExpression(*reconstructed));
            filter_preds.push_back(std::move(filter_pred));

            // filter pushed into stage preparation
            if (is_remote && cfg_.filter_pushdown) {
                auto sref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                    get.returned_types[storage_idx], static_cast<duckdb::idx_t>(storage_idx));
                TRY(auto sreconstructed, TryCatch([&] { return filter.ToExpression(*sref); }));
                TRY(auto spred, TranslateExpression(*sreconstructed));
                pushed_filter_preds.push_back(std::move(spred));
            }
        }

        if (!pushed_filter_preds.empty()) {
            if (pushed_filter_preds.size() == 1) {
                pushed_filter_pred = std::make_shared<ExprNode>(std::move(pushed_filter_preds[0]));
            } else {
                pushed_filter_pred = std::make_shared<ExprNode>(
                    expr::ExprNode::Conjunction(duckdb::ExpressionType::CONJUNCTION_AND, std::move(pushed_filter_preds)));
            }
        }
    }

    PlanBuilder::SharedOpNode cur;
    if (table) {
        data_source = CreateTableSource(table->name, schema_scanned);
        // TODO: do we need to push a projection if not all columns are to be read here?
        cur = builder_.Scan(data_source, schema_scanned, nullptr, nullptr);
    } else if (data_source) {
        LeafStage *leaf_stage;
        if (cfg_.projection_pushdown) {
            auto projection = std::make_shared<std::vector<uint32_t>>();
            projection->reserve(column_ids.size());
            for (auto &cid : column_ids) {
                projection->push_back(static_cast<uint32_t>(cid.GetPrimaryIndex()));
            }
            cur = builder_.Scan(data_source, schema_scanned, std::move(projection), pushed_filter_pred);
            leaf_stage = builder_.LeafStageAt(cur->stage_idx);
        } else {
            Schema schema_full = data_source->schema.value();
            cur = builder_.Scan(data_source, schema_full, nullptr, pushed_filter_pred);
            leaf_stage = builder_.LeafStageAt(cur->stage_idx);

            auto proj = std::make_shared<exec::ProjectionTemplate>();
            for (auto &cid : column_ids) {
                auto s = cid.GetPrimaryIndex();
                const Column &col = schema_full.columns[s];
                proj->projections.push_back(expr::ExprNode::Reference(static_cast<uint32_t>(s), col.type));
            }
            cur = builder_.After(std::move(cur), std::move(proj), schema_scanned);
        }
        leaf_stage->source_splits = SplitCount(data_source->cardinality_total);
    } else {
        return Error("Get has unknown source.", ErrorKind::NotImplemented);
    }

    if (!filter_preds.empty()) { // all filters may have been accelerator-only
        expr::ExprNode filter = filter_preds.size() == 1 ? std::move(filter_preds[0])
            : expr::ExprNode::Conjunction(duckdb::ExpressionType::CONJUNCTION_AND, std::move(filter_preds));
        auto t = std::make_shared<exec::FilterTemplate>(std::move(filter));
        cur = builder_.After(std::move(cur), std::move(t), schema_scanned); // filter preserves schema
    }

    // TODO: check if this is needed -> should prune unused filter columns away but no need for 
    //       double projections doing the same thing
    if (!get.projection_ids.empty()) {
        auto t = std::make_shared<exec::ProjectionTemplate>();
        Schema out;
        for (auto pid : get.projection_ids) {
            const Column &col = schema_scanned.columns[pid];
            t->projections.push_back(expr::ExprNode::Reference(static_cast<uint32_t>(pid), col.type));
            out.columns.push_back(col);
        }
        cur = builder_.After(std::move(cur), std::move(t), std::move(out));
    }
    return cur;
}

Result<PlanBuilder::SharedOpNode> Converter::BuildChunkGet(duckdb::LogicalColumnDataGet &get) {
    if (!get.collection) {
        return Error("CHUNK_GET has no backing collection.", ErrorKind::NotImplemented);
    }
    auto &collection = *get.collection;
    const size_t num_cols = get.chunk_types.size();

    Schema schema;
    schema.columns.reserve(num_cols);
    for (size_t c = 0; c < num_cols; c++) {
        Column col;
        col.name = "col" + std::to_string(c);
        col.type = FromLogicalType(get.chunk_types[c]);
        col.nullable = true;
        schema.columns.push_back(std::move(col));
    }

    const std::string table_name = "__chunk_get_" + std::to_string(get.table_index);

    std::string cols_ddl;
    for (size_t c = 0; c < num_cols; c++) {
        if (c) cols_ddl += ", ";
        cols_ddl += duckdb::KeywordHelper::WriteQuoted(schema.columns[c].name, '"') + " " +
                    get.chunk_types[c].ToString();
    }
    auto create = con_.Query("CREATE TABLE " + duckdb::KeywordHelper::WriteQuoted(table_name, '"') +
                             " (" + cols_ddl + ")");
    if (create->HasError()) {
        return Error("Failed to materialize CHUNK_GET table: " + create->GetError(), ErrorKind::RuntimeError);
    }

    const duckdb::idx_t num_rows = collection.Count();
    if (num_rows > 0) {
        auto rows = collection.GetRows();
        std::string insert =
            "INSERT INTO " + duckdb::KeywordHelper::WriteQuoted(table_name, '"') + " VALUES ";
        for (duckdb::idx_t r = 0; r < num_rows; r++) {
            if (r) insert += ", ";
            insert += "(";
            for (size_t c = 0; c < num_cols; c++) {
                if (c) insert += ", ";
                insert += rows.GetValue(static_cast<duckdb::idx_t>(c), r).ToSQLString();
            }
            insert += ")";
        }
        auto ins = con_.Query(insert);
        if (ins->HasError()) {
            return Error("Failed to materialize CHUNK_GET rows: " + ins->GetError(), ErrorKind::InvalidInput);
        }
    }

    auto blocks_source = CreateTableSource(table_name, schema);
    blocks_source->cardinality_total = static_cast<uint64_t>(num_rows);
    return builder_.Scan(std::move(blocks_source), schema, nullptr, nullptr);
}

Result<PlanBuilder::SharedOpNode> Converter::BuildEmptyResult(duckdb::LogicalEmptyResult &empty) {
    // DuckDB replaces subtrees it proves empty (e.g. by the sources' statistics) with an empty
    // result: scan an empty table of its types instead.
    Schema schema;
    std::string cols_ddl;
    for (size_t c = 0; c < empty.return_types.size(); c++) {
        Column col;
        col.name = "col" + std::to_string(c);
        col.type = FromLogicalType(empty.return_types[c]);
        col.nullable = true;
        if (c) cols_ddl += ", ";
        cols_ddl += duckdb::KeywordHelper::WriteQuoted(col.name, '"') + " " + empty.return_types[c].ToString();
        schema.columns.push_back(std::move(col));
    }
    if (schema.columns.empty()) {
        return Error("EMPTY_RESULT without columns.", ErrorKind::NotImplemented);
    }

    const std::string table_name = "__empty_result_" + std::to_string(num_empty_results_++);
    auto create = con_.Query("CREATE OR REPLACE TABLE " + duckdb::KeywordHelper::WriteQuoted(table_name, '"') +
                             " (" + cols_ddl + ")");
    if (create->HasError()) {
        return Error("Failed to materialize EMPTY_RESULT table: " + create->GetError(), ErrorKind::RuntimeError);
    }

    auto source = CreateTableSource(table_name, schema);
    source->cardinality_total = 0;
    return builder_.Scan(std::move(source), schema, nullptr, nullptr);
}

Result<PlanBuilder::SharedOpNode> Converter::BuildJoin(PlanBuilder::SharedOpNode left, 
        PlanBuilder::SharedOpNode right, duckdb::LogicalComparisonJoin &join, exec::JoinKind kind, 
        uint64_t left_card, uint64_t right_card) {
    uint32_t left_size_orig = static_cast<uint32_t>(left->output_schema.columns.size());
    uint32_t right_size_orig = static_cast<uint32_t>(right->output_schema.columns.size());

    // convert join conditions
    std::vector<uint32_t> left_keys;
    std::vector<uint32_t> right_keys;
    std::vector<bool> keys_match_null; // IS NOT DISTINCT FROM
    std::vector<const duckdb::JoinCondition *> residual_conditions;
    left_keys.reserve(join.conditions.size());
    right_keys.reserve(join.conditions.size());
    for (auto &cond : join.conditions) {
        if (!IsEquiCondition(cond)) {
            residual_conditions.push_back(&cond);
            continue;
        }
        uint32_t lk;
        if (cond.left->GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF) {
            lk = static_cast<uint32_t>(cond.left->Cast<duckdb::BoundReferenceExpression>().index);
        } else {
            TRY(auto value, TranslateExpression(*cond.left));
            auto type = value.return_type;
            std::tie(left, lk) = builder_.AppendColumn(std::move(left), std::move(value), type);
        }
        uint32_t rk;
        if (cond.right->GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF) {
            rk = static_cast<uint32_t>(cond.right->Cast<duckdb::BoundReferenceExpression>().index);
        } else {
            TRY(auto value, TranslateExpression(*cond.right));
            auto type = value.return_type;
            std::tie(right, rk) = builder_.AppendColumn(std::move(right), std::move(value), type);
        }
        left_keys.push_back(lk);
        right_keys.push_back(rk);
        keys_match_null.push_back(cond.comparison == duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM);
    }
    const bool has_equi_keys = !left_keys.empty();

    // FIXME: this works but is not the ideal way to do it in Dandelion
    bool is_broadcast_join = false;
    bool duck_swapped = false;
    if (!has_equi_keys) {
        is_broadcast_join = kind == exec::JoinKind::INNER;
        duck_swapped = is_broadcast_join && right_card > left_card;
        if (duck_swapped) {
            std::swap(left, right);
            std::swap(left_card, right_card);
            std::swap(left_size_orig, right_size_orig);
        }

        const auto broadcast_type = ColumnType{TypeId::INT32};
        uint32_t lk, rk;
        std::tie(left, lk) = builder_.AppendColumn(std::move(left),
            expr::ExprNode::Constant(duckdb::Value::INTEGER(0), broadcast_type), broadcast_type);
        std::tie(right, rk) = builder_.AppendColumn(std::move(right),
            expr::ExprNode::Constant(duckdb::Value::INTEGER(0), broadcast_type), broadcast_type);
        left_keys.push_back(lk);
        right_keys.push_back(rk);
    }

    // build template
    auto t = std::make_shared<exec::JoinTemplate>(left_keys, right_keys, right->output_schema, kind);
    const uint32_t left_size = static_cast<uint32_t>(left->output_schema.columns.size());
    const uint32_t right_size = static_cast<uint32_t>(right->output_schema.columns.size());
    if (!residual_conditions.empty()) {
        std::vector<expr::ExprNode> preds;
        preds.reserve(residual_conditions.size());
        for (auto *cond : residual_conditions) {
            TRY(auto lhs, TranslateExpression(*cond->left));
            TRY(auto rhs, TranslateExpression(*cond->right));
            expr::OffsetReferences(duck_swapped ? lhs : rhs, left_size);
            preds.push_back(expr::ExprNode::Comparison(cond->comparison, std::move(lhs), std::move(rhs)));
        }
        t->has_residual = true;
        t->residual = preds.size() == 1 ? std::move(preds[0])
            : expr::ExprNode::Conjunction(duckdb::ExpressionType::CONJUNCTION_AND, std::move(preds));
    }

    // compute partitions
    uint32_t probe_partitions;
    uint32_t build_partitions;
    std::vector<uint32_t> probe_split_columns;
    std::vector<uint32_t> build_split_columns;
    if (is_broadcast_join) {
        probe_partitions = SplitCount(left_card);
        build_partitions = 1;
        probe_split_columns.resize(left_size_orig);
        std::iota(probe_split_columns.begin(), probe_split_columns.end(), 0);
    } else {
        probe_partitions = SplitCount(std::max(left_card, right_card));
        build_partitions = probe_partitions;
        probe_split_columns = left_keys;
        build_split_columns = right_keys;
    }
    Schema natural = exec::JoinSchema(*t, left->output_schema); // the kind's natural layout
    PlanBuilder::SharedOpNode cur = builder_.Join(left, right, std::move(t), natural,
        std::move(probe_split_columns), std::move(build_split_columns), probe_partitions, build_partitions);
    
    // record the join for dynamic filter planning once the graph is built
    if (cfg_.dynamic_filter && has_equi_keys) {
        dyn_filter_ctx_.join_index[cur.get()] = dyn_filter_ctx_.joins.size();
        dyn_filter_ctx_.joins.push_back(EquiJoin{left, right, kind, left_keys, right_keys, keys_match_null});
    }

    const bool has_left = kind != exec::JoinKind::RIGHT_SEMI && kind != exec::JoinKind::RIGHT_ANTI;
    const bool has_right = kind != exec::JoinKind::SEMI && kind != exec::JoinKind::ANTI &&
                           kind != exec::JoinKind::MARK;
    const bool has_mark = kind == exec::JoinKind::MARK;

    // removed unused columns
    if (!join.left_projection_map.empty() || !join.right_projection_map.empty() ||
        left_size != left_size_orig || right_size != right_size_orig) {
        auto t2 = std::make_shared<exec::ProjectionTemplate>();
        Schema out;
        auto add = [&](uint32_t natural_idx) {
            const Column &col = natural.columns[natural_idx];
            t2->projections.push_back(expr::ExprNode::Reference(natural_idx, col.type));
            out.columns.push_back(col);
        };
        const auto &natural_left_map = duck_swapped ? join.right_projection_map : join.left_projection_map;
        const auto &natural_right_map = duck_swapped ? join.left_projection_map : join.right_projection_map;
        const uint32_t right_off = has_left ? left_size : 0;
        if (has_left) {
            if (natural_left_map.empty()) {
                for (uint32_t i = 0; i < left_size_orig; i++) {
                    add(i);
                }
            } else {
                for (auto li : natural_left_map) {
                    add(static_cast<uint32_t>(li));
                }
            }
        }
        if (has_right) {
            if (natural_right_map.empty()) {
                for (uint32_t i = 0; i < right_size_orig; i++) {
                    add(right_off + i);
                }
            } else {
                for (auto ri : natural_right_map) {
                    add(right_off + static_cast<uint32_t>(ri));
                }
            }
        }
        if (has_mark) {
            add(left_size); // the marker follows the left columns
        }
        cur = builder_.After(std::move(cur), std::move(t2), std::move(out));
    }
    return cur;
}

Result<PlanBuilder::SharedOpNode> Converter::BuildComparisonJoin(duckdb::LogicalComparisonJoin &join) {
    TRY(auto kind, JoinKindFromType(join.join_type));
    TRY(auto left, Build(*join.children[0]));
    TRY(auto right, Build(*join.children[1]));
    return BuildJoin(std::move(left), std::move(right), join, kind,
                     join.children[0]->estimated_cardinality, join.children[1]->estimated_cardinality);
}

Result<PlanBuilder::SharedOpNode> Converter::BuildDelimJoin(duckdb::LogicalComparisonJoin &join) {
    TRY(auto kind, JoinKindFromType(join.join_type));

    const size_t delim_idx = join.delim_flipped ? 1 : 0;
    const size_t other_idx = 1 - delim_idx;

    // build delim side
    TRY(auto delim_side, Build(*join.children[delim_idx]));

    // build distinct aggregate
    std::vector<uint32_t> delim_cols;
    auto delim_templ = std::make_shared<exec::AggregateTemplate>();
    delim_cols.reserve(join.duplicate_eliminated_columns.size());
    delim_templ->group_keys.reserve(delim_cols.size());
    for (auto &e : join.duplicate_eliminated_columns) {
        TRY(auto idx, JoinKeyIndex(*e)); // BoundReference into the delim side's output
        delim_cols.push_back(idx);
        const Column &col = delim_side->output_schema.columns[idx];
        delim_templ->group_keys.push_back(expr::ExprNode::Reference(idx, col.type));
    }
    if (delim_cols.empty()) {
        return Error("DELIM join without duplicate-eliminated columns.", ErrorKind::NotImplemented);
    }
    Schema delim_schema = exec::AggregateSchema(*delim_templ, delim_side->output_schema);
    auto delim_splits = SplitCount(join.children[delim_idx]->estimated_cardinality);
    PlanBuilder::SharedOpNode delim = builder_.Shuffle(
        delim_side, std::move(delim_templ), std::move(delim_schema), delim_cols, delim_splits);

    // build other side
    delim_stack_.push_back(std::move(delim));
    auto other_result = Build(*join.children[other_idx]); // a DELIM_GET inside resolves to the delim set
    delim_stack_.pop_back();
    TRY(auto other_side, std::move(other_result));

    PlanBuilder::SharedOpNode left = join.delim_flipped ? other_side : delim_side;
    PlanBuilder::SharedOpNode right = join.delim_flipped ? delim_side : other_side;

    return BuildJoin(std::move(left), std::move(right), join, kind,
                     join.children[0]->estimated_cardinality, join.children[1]->estimated_cardinality);
}

Result<PlanBuilder::SharedOpNode> Converter::BuildCrossProduct(duckdb::LogicalCrossProduct &cp) {
    TRY(auto left, Build(*cp.children[0]));
    TRY(auto right, Build(*cp.children[1]));
    uint64_t left_card = cp.children[0]->estimated_cardinality;
    uint64_t right_card = cp.children[1]->estimated_cardinality;

    bool swapped = right_card > left_card;
    if (swapped) {
        std::swap(left, right);
        std::swap(left_card, right_card);
    }

    const uint32_t left_size_orig = static_cast<uint32_t>(left->output_schema.columns.size());
    const uint32_t right_size_orig = static_cast<uint32_t>(right->output_schema.columns.size());

    uint32_t partitions = SplitCount(left_card);
    std::vector<uint32_t> probe_split_columns(left_size_orig);
    std::iota(probe_split_columns.begin(), probe_split_columns.end(), 0);

    const auto broadcast_type = ColumnType{TypeId::INT32};
    uint32_t lk, rk;
    std::tie(left, lk) = builder_.AppendColumn(std::move(left),
        expr::ExprNode::Constant(duckdb::Value::INTEGER(0), broadcast_type), broadcast_type);
    std::tie(right, rk) = builder_.AppendColumn(std::move(right),
        expr::ExprNode::Constant(duckdb::Value::INTEGER(0), broadcast_type), broadcast_type);

    auto t = std::make_shared<exec::JoinTemplate>(std::vector<uint32_t>{lk}, std::vector<uint32_t>{rk},
        right->output_schema, exec::JoinKind::INNER);
    const uint32_t right_off = static_cast<uint32_t>(left->output_schema.columns.size()); // post-append
    Schema natural = exec::JoinSchema(*t, left->output_schema);
    PlanBuilder::SharedOpNode cur = builder_.Join(left, right, std::move(t), natural,
        std::move(probe_split_columns), {}, partitions, /*build_num_splits=*/1);

    auto t2 = std::make_shared<exec::ProjectionTemplate>();
    Schema out;
    auto add = [&](uint32_t natural_idx) {
        const Column &col = natural.columns[natural_idx];
        t2->projections.push_back(expr::ExprNode::Reference(natural_idx, col.type));
        out.columns.push_back(col);
    };
    if (!swapped) {
        for (uint32_t i = 0; i < left_size_orig; i++) add(i);
        for (uint32_t i = 0; i < right_size_orig; i++) add(right_off + i);
    } else {
        for (uint32_t i = 0; i < right_size_orig; i++) add(right_off + i);
        for (uint32_t i = 0; i < left_size_orig; i++) add(i);
    }
    return builder_.After(std::move(cur), std::move(t2), std::move(out));
}

Result<PlanBuilder::SharedOpNode> Converter::BuildMaterializedCTE(duckdb::LogicalMaterializedCTE &cte) {
    TRY(auto def, Build(*cte.children[0]));
    cte_subs_[cte.table_index] = std::move(def);
    auto body = Build(*cte.children[1]);
    cte_subs_.erase(cte.table_index);
    return body;
}

Result<PlanBuilder::SharedOpNode> Converter::Build(duckdb::LogicalOperator &op) {
    Result<PlanBuilder::SharedOpNode> built = PlanBuilder::SharedOpNode{};
    switch (op.type) {
    case LogicalOperatorType::LOGICAL_PROJECTION:
        built = BuildProjection(op);
        break;

    case LogicalOperatorType::LOGICAL_FILTER:
        built = BuildFilter(op.Cast<duckdb::LogicalFilter>());
        break;

    case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
        built = BuildAggregate(op.Cast<duckdb::LogicalAggregate>());
        break;

    case LogicalOperatorType::LOGICAL_LIMIT:
        built = BuildLimit(op.Cast<duckdb::LogicalLimit>());
        break;

    case LogicalOperatorType::LOGICAL_ORDER_BY:
        built = BuildOrder(op.Cast<duckdb::LogicalOrder>());
        break;

    case LogicalOperatorType::LOGICAL_TOP_N:
        built = BuildTopN(op.Cast<duckdb::LogicalTopN>());
        break;

    case LogicalOperatorType::LOGICAL_GET:
        built = BuildGet(op.Cast<duckdb::LogicalGet>());
        break;

    case LogicalOperatorType::LOGICAL_CHUNK_GET:
        built = BuildChunkGet(op.Cast<duckdb::LogicalColumnDataGet>());
        break;

    case LogicalOperatorType::LOGICAL_DELIM_GET:
        if (delim_stack_.empty()) {
            return Error("DELIM_GET outside of a DELIM join.", ErrorKind::NotImplemented);
        }
        built = delim_stack_.back();
        break;

    case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
        built = BuildEmptyResult(op.Cast<duckdb::LogicalEmptyResult>());
        break;

    case LogicalOperatorType::LOGICAL_CTE_REF: {
        auto &ref = op.Cast<duckdb::LogicalCTERef>();
        auto it = cte_subs_.find(ref.cte_index);
        if (it == cte_subs_.end()) {
            return Error("Reference to a non-materialized CTE is not supported.",
                         ErrorKind::NotImplemented);
        }
        built = it->second;
        break;
    }

    case LogicalOperatorType::LOGICAL_DELIM_JOIN:
        built = BuildDelimJoin(op.Cast<duckdb::LogicalComparisonJoin>());
        break;

    case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        built = BuildComparisonJoin(op.Cast<duckdb::LogicalComparisonJoin>());
        break;

    case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
        built = BuildCrossProduct(op.Cast<duckdb::LogicalCrossProduct>());
        break;

    case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
        built = BuildMaterializedCTE(op.Cast<duckdb::LogicalMaterializedCTE>());
        break;

    default:
        return Error("Unsupported logical operator '" + op.GetName() + "'.",
                     ErrorKind::NotImplemented);
    }

    TRY(auto node, std::move(built));
    if (cfg_.dynamic_filter && op.has_estimated_cardinality) {
        dyn_filter_ctx_.node_card.try_emplace(node.get(), op.estimated_cardinality);
    }
    return node;
}

namespace {

void NameOutputColumns(duckdb::LogicalOperator &root, Schema &schema) {
    duckdb::LogicalOperator *node = &root;
    while (node->type != LogicalOperatorType::LOGICAL_PROJECTION) {
        switch (node->type) {
        case LogicalOperatorType::LOGICAL_ORDER_BY:
        case LogicalOperatorType::LOGICAL_LIMIT:
        case LogicalOperatorType::LOGICAL_TOP_N:
            node = node->children[0].get();
            continue;
        default:
            return; // no projection to source aliases from
        }
    }
    for (size_t i = 0; i < node->expressions.size() && i < schema.columns.size(); i++) {
        if (!node->expressions[i]->GetName().empty())
            schema.columns[i].name = node->expressions[i]->GetName();
    }
}

} // namespace

Result<std::unique_ptr<PhysicalPlan>> BuildPhysicalPlan(duckdb::Connection &con, 
        const std::string &sql, const SourceCatalog &sources, const ConverterConfig &config) {
    auto disable = con.Query("SET disabled_optimizers='compressed_materialization'");
    if (disable->HasError()) {
        return Error("Failed to configure optimizer: " + disable->GetError(), ErrorKind::InvalidInput);
    }

    TRY(auto plan, TryCatch([&] { return con.ExtractPlan(sql); }));

    Converter converter(con, sources, config);
    TRY(auto physical, converter.Convert(*plan));

    NameOutputColumns(*plan, physical->output_schema);
    return physical;
}

} // namespace plume::parser
