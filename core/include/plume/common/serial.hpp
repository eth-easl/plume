#pragma once

#include "plume/common/buffer.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include <cstring>

namespace plume {

template <class T>
DataBuffer SerializeToBuffer(const T &obj) {
    duckdb::MemoryStream stream;
    duckdb::BinarySerializer serializer(stream);
    serializer.Begin();
    obj.Serialize(serializer);
    serializer.End();
    const auto size = stream.GetPosition();
    DataBuffer buf(size);
    if (size > 0) {
        std::memcpy(buf.mutable_data(), stream.GetData(), size);
    }
    return buf;
}

template <class T>
T DeserializeFromBytes(const uint8_t *data, size_t size) {
    duckdb::MemoryStream stream(const_cast<duckdb::data_ptr_t>(data), size);
    duckdb::BinaryDeserializer deserializer(stream);
    deserializer.Begin();
    T obj = T::Deserialize(deserializer);
    deserializer.End();
    return obj;
}

} // namespace plume
