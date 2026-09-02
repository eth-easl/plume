#include "plume/parquet/parquet.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace plume::parquet {

void ParquetConfig::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "num_splits", num_splits);
    s.WriteProperty(101, "coalesce_distance", coalesce_distance);
    s.WriteList(102, "projection", projection.size(),
                [&](duckdb::Serializer::List &list, duckdb::idx_t i) { list.WriteElement(projection[i]); });
    s.WriteProperty(103, "has_pushed_filter", has_pushed_filter);
    s.WriteProperty(104, "pushed_filter", pushed_filter);
    s.WriteProperty(105, "max_region_size", max_region_size);
    s.WriteProperty(106, "dynamic_filter_column", dynamic_filter_column);
}

ParquetConfig ParquetConfig::Deserialize(duckdb::Deserializer &d) {
    ParquetConfig c;
    c.num_splits = d.ReadProperty<uint32_t>(100, "num_splits");
    c.coalesce_distance = d.ReadProperty<uint64_t>(101, "coalesce_distance");
    d.ReadList(102, "projection", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
        c.projection.push_back(list.ReadElement<uint32_t>());
    });
    c.has_pushed_filter = d.ReadProperty<bool>(103, "has_pushed_filter");
    c.pushed_filter = d.ReadProperty<expr::ExprNode>(104, "pushed_filter");
    c.max_region_size = d.ReadProperty<uint64_t>(105, "max_region_size");
    c.dynamic_filter_column = d.ReadPropertyWithExplicitDefault<int32_t>(106, "dynamic_filter_column", -1);
    return c;
}

} // namespace plume::parquet
