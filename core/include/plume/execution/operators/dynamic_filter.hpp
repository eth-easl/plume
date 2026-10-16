#pragma once

#include "plume/common/result.hpp"
#include "plume/common/types.hpp"
#include "plume/execution/bloom_filter.hpp"
#include "plume/execution/operator.hpp"
#include "plume/expression/expression.hpp"

#include "duckdb/common/types/value.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

namespace duckdb {
class Serializer;
class Deserializer;
} // namespace duckdb

namespace plume::exec {

// Runtime filter over a join's build-side key column.
struct DynamicFilter {
    // Overall key range [min, max]. !has_bounds -> the build side has no non-NULL key at all.
    bool has_bounds = false;
    duckdb::Value min;
    duckdb::Value max;
    // The exact distinct keys, typed as the build template's key_type.
    bool has_values = false;
    std::vector<duckdb::Value> values;
    uint32_t max_values = 0;
    // Bloom filter over the key hashes (duckdb::Hash of the key cast to the build template's
    // key_type). Empty() -> not built.
    uint64_t bloom_bits = 0;
    BloomFilter bloom;

    // Widens the key range to also cover [min, max].
    void MergeBounds(const duckdb::Value &min, const duckdb::Value &max);
    // Moves the exact keys into a newly allocated Bloom filter, or drops them if there is no
    // Bloom filter configured.
    void ConvertToBloom();
    // Merges the filter of another build instance of the same template.
    void Merge(const DynamicFilter &other);

    void Serialize(duckdb::Serializer &s) const;
    static DynamicFilter Deserialize(duckdb::Deserializer &d);
};

// Range predicate over the filter's key range. Returns a nullopt if it has no bounds.
std::optional<expr::ExprNode> BuildRangeFilter(uint32_t column, const ColumnType &type,
                                               const DynamicFilter &filter);

struct DynamicFilterBuildTemplate : OperatorTemplate {
    // The column over which a dynamic filter is built.
    uint32_t column = 0;
    // Type the keys are cast to before they're collected/hashed.
    ColumnType key_type;
    // Upper limit on the exact distinct key set (0 -> no exact set).
    uint32_t max_values = 0;
    // Bloom filter size in bits (0 -> no Bloom filter).
    uint64_t bloom_bits = 0;

    DynamicFilterBuildTemplate() : OperatorTemplate(OpType::DYNAMIC_FILTER_BUILD) {}
    DynamicFilterBuildTemplate(uint32_t column, ColumnType key_type, uint32_t max_values = 0, uint64_t bloom_bits = 0)
        : OperatorTemplate(OpType::DYNAMIC_FILTER_BUILD), column(column), key_type(key_type),
          max_values(max_values), bloom_bits(bloom_bits) {}

    bool Equals(const OperatorTemplate &other) const override;
    void Serialize(duckdb::Serializer &s) const override;
};

class KeyCollector;

// Operator to build a dynamic filter.
class DynamicFilterBuildOperator : public Operator {
public:
    DynamicFilterBuildOperator(const DynamicFilterBuildTemplate &templ, std::unique_ptr<KeyCollector> collector,
                               duckdb::vector<duckdb::LogicalType> output_types);
    ~DynamicFilterBuildOperator() override;

    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;
    Result<void> Finish() override;

    // The collected filter. Ends the collection if Finish() wasn't called yet.
    const DynamicFilter &Filter();

private:
    void EndCollection();

    uint32_t column_;
    duckdb::LogicalType key_type_;
    std::variant<std::unique_ptr<KeyCollector>, DynamicFilter> state_;
};

Result<std::unique_ptr<Operator>> BuildDynamicFilterBuildTemplate(std::shared_ptr<DynamicFilterBuildTemplate> templ,
                                                                  duckdb::vector<duckdb::LogicalType> col_types);

} // namespace plume::exec
