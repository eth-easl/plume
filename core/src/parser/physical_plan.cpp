#include "plume/parser/physical_plan.hpp"

#include "plume/common/result.hpp"
#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/execution/operators/limit.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/execution/operators/sort.hpp"

#include <sstream>

namespace plume::parser {

using catalog::DataSourceType;

namespace {

// --- name helpers ---

// TODO: move to definition files
static const char *DataSourceTypeName(DataSourceType k) {
    switch (k) {
    case DataSourceType::LOCAL_TABLE:    return "LOCAL_TABLE";
    case DataSourceType::REMOTE_CSV:     return "CSV";
    case DataSourceType::REMOTE_PARQUET: return "PARQUET";
    }
    return "?";
}

static const char *TypeIdName(TypeId id) {
    switch (id) {
    case TypeId::BOOLEAN: return "BOOLEAN";
    case TypeId::INT8:    return "INT8";
    case TypeId::INT16:   return "INT16";
    case TypeId::INT32:   return "INT32";
    case TypeId::INT64:   return "INT64";
    case TypeId::HUGEINT: return "HUGEINT";
    case TypeId::FLOAT:   return "FLOAT";
    case TypeId::DOUBLE:  return "DOUBLE";
    case TypeId::VARCHAR: return "VARCHAR";
    case TypeId::DECIMAL: return "DECIMAL";
    case TypeId::DATE:    return "DATE";
    case TypeId::TIME:    return "TIME";
    }
    return "?";
}

static const char *JoinKindName(exec::JoinKind k) {
    switch(k) {
    case exec::JoinKind::INNER:      return "INNER";
    case exec::JoinKind::LEFT:       return "LEFT";
    case exec::JoinKind::RIGHT:      return "RIGHT";
    case exec::JoinKind::OUTER:      return "OUTER";
    case exec::JoinKind::SEMI:       return "SEMI";
    case exec::JoinKind::ANTI:       return "ANTI";
    case exec::JoinKind::SINGLE:     return "SINGLE";
    case exec::JoinKind::MARK:       return "MARK";
    case exec::JoinKind::RIGHT_SEMI: return "RIGHT_SEMI";
    case exec::JoinKind::RIGHT_ANTI: return "RIGHT_ANTI";
    }
    return "?";
}

static std::string ColumnTypeName(const ColumnType &t) {
    if (t.id == TypeId::DECIMAL) {
        return "DECIMAL(" + std::to_string(t.decimal_width) + "," + std::to_string(t.decimal_scale) + ")";
    }
    return TypeIdName(t.id);
}

static std::string FormatSchema(const Schema &schema) {
    std::string result;
    for (size_t i = 0; i < schema.columns.size(); i++) {
        if (i) result += ", ";
        const auto &col = schema.columns[i];
        result += col.name + ": " + ColumnTypeName(col.type);
    }
    return result;
}

// --- expression formatting ---

static std::string FormatExpr(const ExprNode &e, const Schema *s);

static std::string FormatExprList(const std::vector<ExprNode> &exprs, const Schema *s) {
    std::string r;
    for (size_t i = 0; i < exprs.size(); i++) {
        if (i) r += ", ";
        r += FormatExpr(exprs[i], s);
    }
    return r;
}

static std::string FormatExpr(const ExprNode &e, const Schema *s) {
    using ET = duckdb::ExpressionType;
    switch (e.kind) {
    case ExprKind::REFERENCE:
        if (s && e.ref_index < s->columns.size())
            return s->columns[e.ref_index].name;
        return "#" + std::to_string(e.ref_index);
    case ExprKind::CONSTANT:
        return e.constant.ToString();
    case ExprKind::FUNCTION:
        return e.func_name + "(" + FormatExprList(e.children, s) + ")";
    case ExprKind::COMPARISON: {
        const char *op = "?";
        switch (static_cast<ET>(e.expr_type)) {
        case ET::COMPARE_EQUAL:                op = "=";  break;
        case ET::COMPARE_NOTEQUAL:             op = "<>"; break;
        case ET::COMPARE_LESSTHAN:             op = "<";  break;
        case ET::COMPARE_GREATERTHAN:          op = ">";  break;
        case ET::COMPARE_LESSTHANOREQUALTO:    op = "<="; break;
        case ET::COMPARE_GREATERTHANOREQUALTO: op = ">="; break;
        default: break;
        }
        return FormatExpr(e.children[0], s) + " " + op + " " + FormatExpr(e.children[1], s);
    }
    case ExprKind::CONJUNCTION: {
        const char *op = static_cast<ET>(e.expr_type) == ET::CONJUNCTION_AND ? " AND " : " OR ";
        std::string r;
        for (size_t i = 0; i < e.children.size(); i++) {
            if (i) r += op;
            r += FormatExpr(e.children[i], s);
        }
        return r;
    }
    case ExprKind::OPERATOR:
        switch (static_cast<ET>(e.expr_type)) {
        case ET::OPERATOR_NOT:         return "NOT " + FormatExpr(e.children[0], s);
        case ET::OPERATOR_IS_NULL:     return FormatExpr(e.children[0], s) + " IS NULL";
        case ET::OPERATOR_IS_NOT_NULL: return FormatExpr(e.children[0], s) + " IS NOT NULL";
        default:                       return "op(" + FormatExprList(e.children, s) + ")";
        }
    case ExprKind::CAST: {
        const char *fn = e.try_cast ? "TRY_CAST(" : "CAST(";
        return std::string(fn) + FormatExpr(e.children[0], s) + " AS " + ColumnTypeName(e.return_type) + ")";
    }
    case ExprKind::CASE_EXPR: {
        std::string r = "CASE";
        for (size_t i = 0; i + 1 < e.children.size(); i += 2)
            r += " WHEN " + FormatExpr(e.children[i], s) + " THEN " + FormatExpr(e.children[i + 1], s);
        if (!e.children.empty() && e.children.size() % 2 == 1)
            r += " ELSE " + FormatExpr(e.children.back(), s);
        return r + " END";
    }
    case ExprKind::BETWEEN:
        if (e.children.size() == 3)
            return FormatExpr(e.children[0], s) + " BETWEEN " +
                   FormatExpr(e.children[1], s) + " AND " + FormatExpr(e.children[2], s);
        return "BETWEEN(?)";
    case ExprKind::AGGREGATE:
        return e.func_name + "(" + FormatExprList(e.children, s) + ")";
    default:
        return "expr";
    }
}

// --- operator detail printing ---

// Prints each pipeline operator on its own line with `prefix`, using `s` (the
// pipeline input schema) to resolve column references by name.
static void PrintOperators(std::ostream &out, const exec::PipelineTemplate &pipeline,
                           const std::string &prefix) {
    out << prefix << "operators:" << std::endl;
    Schema current = pipeline.input_schema;
    const Schema *s = &current;
    for (size_t i = 0; i < pipeline.operators.size(); i++) {
        const auto &op = pipeline.operators[i];
        out << prefix << " " << i << ": ";
        switch (op->type) {
        case exec::OpType::JOIN: {
            const auto *j = static_cast<const exec::JoinTemplate*>(op.get());
            out << "JOIN";
            out << " { left=[";
            for (size_t i = 0; i < j->left_keys.size(); i++) {
                if (i) out << ", ";
                uint32_t k = j->left_keys[i];
                out << (k < s->columns.size() ? s->columns[k].name : "#" + std::to_string(k));
            }
            out << "], right=[";
            for (size_t i = 0; i < j->right_keys.size(); i++) {
                if (i) out << ", ";
                uint32_t k = j->right_keys[i];
                out << (k < j->right_schema.columns.size() ? j->right_schema.columns[k].name
                                                           : "#" + std::to_string(k));
            }
            out << "], kind=" << JoinKindName(j->kind) << " }";
            break;
        }
        case exec::OpType::PROJECTION: {
            const auto *p = static_cast<const exec::ProjectionTemplate*>(op.get());
            out << "PROJECTION { projections: [ " << FormatExprList(p->projections, s) << " ] }";
            break;
        }
        case exec::OpType::FILTER: {
            const auto *f = static_cast<const exec::FilterTemplate*>(op.get());
            out << "FILTER { filter: " << FormatExpr(f->filter, s) << " }";
            break;
        }
        case exec::OpType::LIMIT: {
            const auto *l = static_cast<const exec::LimitTemplate*>(op.get());
            out << "LIMIT { ";
            if (l->has_limit) {
                out << "limit: " << l->limit;
                if (l->offset > 0) out << ", ";
            }
            if (l->offset > 0) out << "offset: " << l->offset;
            out << " }";
            break;
        }
        case exec::OpType::AGGREGATE: {
            const auto *a = static_cast<const exec::AggregateTemplate*>(op.get());
            out << "AGGREGATE { ";
            if (!a->group_keys.empty())
                out << "group_keys: [ " << FormatExprList(a->group_keys, s) << " ]";
            if (!a->aggregates.empty()) {
                if (!a->group_keys.empty()) out << ", ";
                out << "aggregates: [ ";
                for (size_t i = 0; i < a->aggregates.size(); i++) {
                    if (i) out << ", ";
                    const auto &agg = a->aggregates[i];
                    out << agg.func_name << "(";
                    for (size_t j = 0; j < agg.arguments.size(); j++) {
                        if (j) out << ", ";
                        out << FormatExpr(agg.arguments[j], s);
                    }
                    out << ") → " << ColumnTypeName(agg.return_type);
                }
                out << " ]";
            }
            out << " }";
            break;
        }
        case exec::OpType::ORDER_BY: {
            const auto *srt = static_cast<const exec::SortTemplate*>(op.get());
            out << "ORDER_BY { sort_keys: [ ";
            for (size_t i = 0; i < srt->sort_keys.size(); i++) {
                if (i) out << ", ";
                const auto &k = srt->sort_keys[i];
                out << FormatExpr(k.expr, s);
                out << (k.order == exec::SortOrder::ASCENDING ? " ASC" : " DESC");
                if (k.null_order == exec::NullOrder::NULLS_FIRST) out << " NULLS FIRST";
            }
            out << " ] }";
            break;
        }
        default:
            out << "OP(" << static_cast<int>(op->type) << ")";
        }
        out << std::endl;

        // Advance the display schema past this operator so the next one's
        // column references resolve against its actual input, not the stage's.
        switch (op->type) {
        case exec::OpType::JOIN: {
            const auto *j = static_cast<const exec::JoinTemplate*>(op.get());
            current = exec::JoinSchema(*j, current);
            break;
        }
        case exec::OpType::PROJECTION: {
            const auto *p = static_cast<const exec::ProjectionTemplate*>(op.get());
            current = exec::ProjectionSchema(p->projections, current);
            break;
        }
        case exec::OpType::AGGREGATE: {
            const auto *a = static_cast<const exec::AggregateTemplate*>(op.get());
            current = exec::AggregateSchema(*a, current);
            break;
        }
        default:
            break; // FILTER / LIMIT / ORDER_BY preserve the schema
        }
    }
}

// --- tree stage printing ---

// Prints a stage and all its upstream stages recursively as a tree.
//
// `indent`  — the prefix inherited from the parent (determines the continuing
//             vertical bar lines). Empty for the root stage.
// `is_last` — whether this stage is the last child of its parent (selects
//             └─ vs ├─ and the continuation character for body lines).
// `label`   — optional short annotation appended to the header (e.g. "← probe").
static void PrintStageTree(std::ostream &out, const PhysicalPlan &plan, size_t idx,
                           const std::string &indent, bool is_last,
                           const std::string &label = "") {
    const Stage &stage = *plan.stages.at(idx);

    // header line
    const std::string connector = indent.empty() ? "" : (is_last ? "└─ " : "├─ ");
    out << indent << connector << "Stage #" << idx << "  ["
        << (stage.is_leaf() ? DataSourceTypeName(static_cast<const LeafStage &>(stage).data_source->type) : "STAGE")
        << "]";
    if (!label.empty()) out << " " << label;
    if (stage.leads_with_join()) out << "  (join)";
    out << std::endl;

    const std::string child_ident = indent + (is_last || indent.empty() ? "   " : "│  ");
    const std::string body_ident = child_ident + (stage.input_stages.empty() ? "  " : "│ ");

    // stage information (resolved from the leaf stage's data source)
    if (stage.is_leaf()) {
        const auto &leaf = static_cast<const LeafStage &>(stage);
        const auto &src = *leaf.data_source;
        if (leaf.data_source->type == DataSourceType::LOCAL_TABLE) {
            out << body_ident << "table:  " << src.name << std::endl;
        } else {
            out << body_ident << "path:   " << src.name << std::endl;
        }
        out << body_ident << "source_splits: " << leaf.source_splits << std::endl;
    }
    if (stage.leads_with_join()) {
        auto join = std::static_pointer_cast<exec::JoinTemplate>(stage.pipeline.operators[0]);
        out << body_ident << "input:  left ->  " << FormatSchema(stage.pipeline.input_schema) << std::endl;
        out << body_ident << "input:  right -> " << FormatSchema(join->right_schema) << std::endl;
    } else {
        out << body_ident << "input:  " << FormatSchema(stage.pipeline.input_schema) << std::endl;
    }
    out << body_ident << "output: " << FormatSchema(stage.output_schema) << std::endl;
    if (stage.pipeline.output_split.partitions > 1) {
        out << body_ident << "split:  " << stage.pipeline.output_split.partitions << " partitions (";
        for (const auto& col : stage.pipeline.output_split.key_columns) {
            out << col << ", ";
        }
        out.seekp(-2, out.cur);
        out << ")" << std::endl;
    }

    // stage operators
    PrintOperators(out, stage.pipeline, body_ident);

    // print children
    if (!stage.input_stages.empty()) {
        out << body_ident << std::endl;
        for (size_t i = 0; i < stage.input_stages.size(); i++) {
            bool child_last = (i + 1 == stage.input_stages.size());
            std::string child_label;
            if (stage.leads_with_join() && stage.input_stages.size() == 2)
                child_label = (i == 0) ? "← probe" : "← build";
            PrintStageTree(out, plan, stage.input_stages[i], child_ident, child_last, child_label);
            if (!child_last) out << child_ident << "│" << std::endl;
        }
    }
}

} // namespace

//===----------------------------------------------------------------------===//
// Stage
//===----------------------------------------------------------------------===//

std::string Stage::ToString() const {
    std::ostringstream out;
    out << "Stage " << idx << "  ["
        << (is_leaf() ? DataSourceTypeName(static_cast<const LeafStage &>(*this).data_source->type) : "STAGE")
        << "]";
    if (leads_with_join()) out << "  (join)";
    out << std::endl;

    const std::string body = "   ";

    if (!input_stages.empty()) {
        out << body << "inputs: ";
        for (size_t i = 0; i < input_stages.size(); i++) {
            if (i) out << ", ";
            out << "Stage " << input_stages[i];
        }
        out << std::endl;
    }
    if (is_leaf())
        out << body << "source: " << static_cast<const LeafStage &>(*this).data_source->name << std::endl;

    PrintOperators(out, pipeline, body);

    out << body << "input:  " << FormatSchema(pipeline.input_schema) << std::endl;
    out << body << "output: " << FormatSchema(output_schema);

    if (pipeline.output_split.partitions > 1)
        out << std::endl << body << "split:  " << pipeline.output_split.partitions << " partitions";

    return out.str();
}

//===----------------------------------------------------------------------===//
// PhysicalPlan
//===----------------------------------------------------------------------===//

std::string PhysicalPlan::ToString() const {
    std::ostringstream out;
    out << "PhysicalPlan root=" << root_idx << "  " << stages.size() << " stage"
        << (stages.size() == 1 ? "" : "s") << std::endl;
    out << "output: " << FormatSchema(output_schema) << std::endl << std::endl;
    PrintStageTree(out, *this, root_idx, "", true);
    return out.str();
}

} // namespace plume::parser
