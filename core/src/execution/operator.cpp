#include "plume/execution/operator.hpp"

#include "plume/execution/operators/aggregate.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/execution/operators/filter.hpp"
#include "plume/execution/operators/join.hpp"
#include "plume/execution/operators/limit.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/execution/operators/sort.hpp"
#include "plume/execution/operators/top_n.hpp"

#include "duckdb/common/serializer/deserializer.hpp"

namespace plume::exec {

std::shared_ptr<OperatorTemplate> OperatorTemplate::DeserializeOperator(OpType type, duckdb::Deserializer &d) {
    switch (type) {
    case OpType::FILTER: {
        auto t = std::make_shared<FilterTemplate>();
        t->type = type;
        t->filter = d.ReadProperty<expr::ExprNode>(101, "filter");
        return t;
    }
    case OpType::PROJECTION: {
        auto t = std::make_shared<ProjectionTemplate>();
        t->type = type;
        d.ReadList(101, "projections", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
            t->projections.push_back(list.ReadElement<expr::ExprNode>());
        });
        return t;
    }
    case OpType::LIMIT: {
        auto t = std::make_shared<LimitTemplate>();
        t->type = type;
        t->has_limit = d.ReadProperty<bool>(101, "has_limit");
        t->limit = d.ReadProperty<uint64_t>(102, "limit");
        t->offset = d.ReadProperty<uint64_t>(103, "offset");
        return t;
    }
    case OpType::AGGREGATE: {
        auto t = std::make_shared<AggregateTemplate>();
        t->type = type;
        d.ReadList(101, "group_keys", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
            t->group_keys.push_back(list.ReadElement<expr::ExprNode>());
        });
        d.ReadList(102, "aggregates", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
            t->aggregates.push_back(list.ReadElement<AggregateSpec>());
        });
        return t;
    }
    case OpType::ORDER_BY: {
        auto t = std::make_shared<SortTemplate>();
        t->type = type;
        d.ReadList(101, "sort_keys", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
            t->sort_keys.push_back(list.ReadElement<SortKey>());
        });
        return t;
    }
    case OpType::JOIN: {
        auto t = std::make_shared<JoinTemplate>();
        t->type = type;
        d.ReadList(101, "left_keys", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
            t->left_keys.push_back(list.ReadElement<uint32_t>());
        });
        d.ReadList(102, "right_keys", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
            t->right_keys.push_back(list.ReadElement<uint32_t>());
        });
        t->right_schema = d.ReadProperty<Schema>(103, "right_schema");
        t->kind = static_cast<JoinKind>(
            d.ReadPropertyWithExplicitDefault<uint8_t>(104, "kind", static_cast<uint8_t>(JoinKind::INNER)));
        t->has_residual = d.ReadPropertyWithExplicitDefault<bool>(105, "has_residual", false);
        if (t->has_residual) {
            t->residual = d.ReadProperty<expr::ExprNode>(106, "residual");
        }
        return t;
    }
    case OpType::TOP_N: {
        auto t = std::make_shared<TopNTemplate>();
        t->type = type;
        d.ReadList(101, "sort_keys", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
            t->sort_keys.push_back(list.ReadElement<SortKey>());
        });
        t->limit = d.ReadProperty<uint64_t>(102, "limit");
        t->offset = d.ReadProperty<uint64_t>(103, "offset");
        return t;
    }
    case OpType::DYNAMIC_FILTER_BUILD: {
        auto t = std::make_shared<DynamicFilterBuildTemplate>();
        t->type = type;
        t->column = d.ReadProperty<uint32_t>(101, "column");
        return t;
    }
    default:
        throw duckdb::InvalidInputException("Unknown operator type %d in pipeline blob",
                                            static_cast<int>(type));
    }
}

} // namespace plume::exec
