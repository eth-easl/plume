#include "plume/execution/pipeline.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace plume::exec {

//===----------------------------------------------------------------------===//
// OutputSplit
//===----------------------------------------------------------------------===//

void OutputSplit::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "partitions", partitions);
    s.WriteList(101, "key_columns", key_columns.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteElement(key_columns[i]);
    });
}

OutputSplit OutputSplit::Deserialize(duckdb::Deserializer &d) {
    OutputSplit os;
    os.partitions = d.ReadProperty<uint32_t>(100, "partitions");
    d.ReadList(101, "key_columns", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
        os.key_columns.push_back(list.ReadElement<uint32_t>());
    });
    return os;
}

//===----------------------------------------------------------------------===//
// OutputSink
//===----------------------------------------------------------------------===//

void OutputSink::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "format", static_cast<uint8_t>(format));
    s.WriteProperty(101, "csv", csv);
    s.WriteProperty(102, "parquet_codec", parquet_codec);
}

OutputSink OutputSink::Deserialize(duckdb::Deserializer &d) {
    OutputSink sink;
    sink.format = static_cast<OutputFormat>(d.ReadProperty<uint8_t>(100, "format"));
    sink.csv = d.ReadProperty<csv::CSVOptions>(101, "csv");
    sink.parquet_codec = d.ReadProperty<int32_t>(102, "parquet_codec");
    return sink;
}

//===----------------------------------------------------------------------===//
// PipelineTemplate
//===----------------------------------------------------------------------===//

void PipelineTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "input_schema", input_schema);
    s.WriteList(101, "operators", operators.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteObject([&](duckdb::Serializer &os) {
            os.WriteProperty(100, "type", static_cast<uint8_t>(operators[i]->type));
            operators[i]->Serialize(os);
        });
    });
    s.WriteProperty(102, "output_split", output_split);
    s.WriteProperty(103, "output_sink", output_sink);
}

PipelineTemplate PipelineTemplate::Deserialize(duckdb::Deserializer &d) {
    PipelineTemplate pt;
    pt.input_schema = d.ReadProperty<Schema>(100, "input_schema");
    d.ReadList(101, "operators", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
        list.ReadObject([&](duckdb::Deserializer &od) {
            auto type = static_cast<OpType>(od.ReadProperty<uint8_t>(100, "type"));
            pt.operators.push_back(OperatorTemplate::DeserializeOperator(type, od));
        });
    });
    pt.output_split = d.ReadProperty<OutputSplit>(102, "output_split");
    pt.output_sink = d.ReadPropertyWithExplicitDefault<OutputSink>(103, "output_sink", OutputSink{});
    return pt;
}

} // namespace plume::exec

//===----------------------------------------------------------------------===//
// Top-level SerializePipeline / DeserializePipeline
//===----------------------------------------------------------------------===//

namespace plume {

std::vector<uint8_t> SerializePipeline(const exec::PipelineTemplate &pipeline) {
    duckdb::MemoryStream stream;
    duckdb::BinarySerializer serializer(stream);
    serializer.Begin();
    pipeline.Serialize(serializer);
    serializer.End();
    const auto *data = stream.GetData();
    const auto size = stream.GetPosition();
    return std::vector<uint8_t>(data, data + size);
}

Result<exec::PipelineTemplate> DeserializePipeline(const uint8_t *data, size_t size) {
    try {
        duckdb::MemoryStream stream(const_cast<duckdb::data_ptr_t>(data), size);
        duckdb::BinaryDeserializer deserializer(stream);
        deserializer.Begin();
        auto pipeline = exec::PipelineTemplate::Deserialize(deserializer);
        deserializer.End();
        return pipeline;
    } catch (const std::exception &e) {
        return Error(std::string("Malformed pipeline blob: ") + e.what(), ErrorKind::InvalidInput);
    }
}

} // namespace plume
