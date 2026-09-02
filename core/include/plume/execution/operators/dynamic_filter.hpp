#pragma once

#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"

#include "duckdb/common/types/value.hpp"

#include <cstdint>
#include <memory>
#include <optional>

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume::exec {

struct DynamicFilterBounds {
    bool valid = false;
    duckdb::Value min;
    duckdb::Value max;

    void Serialize(duckdb::Serializer &s) const;
    static DynamicFilterBounds Deserialize(duckdb::Deserializer &d);
};

// Return a nullopt if the bounds are invalid.
std::optional<expr::ExprNode> BuildRangeFilter(uint32_t column, const ColumnType &type,
                                               const DynamicFilterBounds &bounds);

struct DynamicFilterBuildTemplate : OperatorTemplate {
    uint32_t column = 0; // index into this operator's input schema

    DynamicFilterBuildTemplate() : OperatorTemplate(OpType::DYNAMIC_FILTER_BUILD) {}
    explicit DynamicFilterBuildTemplate(uint32_t column)
        : OperatorTemplate(OpType::DYNAMIC_FILTER_BUILD), column(column) {}

    bool Equals(const OperatorTemplate &other) const override;
    void Serialize(duckdb::Serializer &s) const override;
};

// Operator to build a dynamic filter. Computes a running min/max over the given column.
class DynamicFilterBuildOperator : public Operator {
public:
    DynamicFilterBuildOperator(uint32_t column, duckdb::vector<duckdb::LogicalType> output_types);

    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;
    Result<void> Finish() override;

    const DynamicFilterBounds &Bounds() const { return bounds_; }

private:
    uint32_t column_;
    DynamicFilterBounds bounds_;
};

Result<std::unique_ptr<Operator>> BuildDynamicFilterBuildTemplate(std::shared_ptr<DynamicFilterBuildTemplate> templ,
                                                                   duckdb::vector<duckdb::LogicalType> col_types);

} // namespace plume::exec
