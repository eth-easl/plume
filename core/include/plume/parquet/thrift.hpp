#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace plume::parquet {

// Thrift compact field/element types.
enum CType : uint8_t {
    CT_STOP = 0,
    CT_BOOL_TRUE = 1,
    CT_BOOL_FALSE = 2,
    CT_BYTE = 3,
    CT_I16 = 4,
    CT_I32 = 5,
    CT_I64 = 6,
    CT_DOUBLE = 7,
    CT_BINARY = 8,
    CT_LIST = 9,
    CT_SET = 10,
    CT_MAP = 11,
    CT_STRUCT = 12,
};

class CompactReader {
public:
    CompactReader(const uint8_t *data, size_t size) : start_(data), p_(data), end_(data + size) {}

    size_t Consumed() const { return static_cast<size_t>(p_ - start_); }

    uint8_t Byte() {
        if (p_ >= end_) {
            throw std::runtime_error("parquet: truncated thrift data");
        }
        return *p_++;
    }

    uint64_t Varint() {
        uint64_t result = 0;
        int shift = 0;
        while (true) {
            uint8_t b = Byte();
            result |= uint64_t(b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 63) {
                throw std::runtime_error("parquet: varint too long");
            }
        }
        return result;
    }

    int64_t ZigZag() {
        uint64_t n = Varint();
        return static_cast<int64_t>(n >> 1) ^ -static_cast<int64_t>(n & 1);
    }

    void SkipBytes(size_t n) {
        if (static_cast<size_t>(end_ - p_) < n) {
            throw std::runtime_error("parquet: truncated thrift data");
        }
        p_ += n;
    }

    bool Field(int16_t &last_id, uint8_t &type, int16_t &id) {
        uint8_t b = Byte();
        if (b == CT_STOP) {
            return false;
        }
        type = b & 0x0f;
        uint8_t delta = (b & 0xf0) >> 4;
        id = delta ? static_cast<int16_t>(last_id + delta) : static_cast<int16_t>(ZigZag());
        last_id = id;
        return true;
    }

    void ListHeader(uint64_t &size, uint8_t &elem_type) {
        uint8_t b = Byte();
        elem_type = b & 0x0f;
        size = (b & 0xf0) >> 4;
        if (size == 15) {
            size = Varint();
        }
    }

    void Skip(uint8_t type) {
        switch (type) {
        case CT_BOOL_TRUE:
        case CT_BOOL_FALSE:
            return;
        case CT_BYTE:
            SkipBytes(1);
            return;
        case CT_I16:
        case CT_I32:
        case CT_I64:
            Varint();
            return;
        case CT_DOUBLE:
            SkipBytes(8);
            return;
        case CT_BINARY:
            SkipBytes(Varint());
            return;
        case CT_LIST:
        case CT_SET: {
            uint64_t n;
            uint8_t et;
            ListHeader(n, et);
            SkipElements(n, et);
            return;
        }
        case CT_MAP: {
            uint64_t n = Varint();
            if (n > 0) {
                uint8_t kv = Byte();
                uint8_t kt = (kv & 0xf0) >> 4;
                uint8_t vt = kv & 0x0f;
                for (uint64_t i = 0; i < n; i++) {
                    Skip(kt);
                    Skip(vt);
                }
            }
            return;
        }
        case CT_STRUCT: {
            int16_t last = 0, id;
            uint8_t t;
            while (Field(last, t, id)) {
                Skip(t);
            }
            return;
        }
        default:
            throw std::runtime_error("parquet: unknown thrift type");
        }
    }

    void SkipElements(uint64_t n, uint8_t elem_type) {
        if (elem_type == CT_BOOL_TRUE || elem_type == CT_BOOL_FALSE) {
            SkipBytes(n); // bool list elements are one byte each
            return;
        }
        for (uint64_t i = 0; i < n; i++) {
            Skip(elem_type);
        }
    }

private:
    const uint8_t *start_;
    const uint8_t *p_;
    const uint8_t *end_;
};

class CompactWriter {
public:
    const std::vector<uint8_t> &bytes() const { return buf_; }

    void Varint(uint64_t v) {
        while (v > 0x7f) {
            buf_.push_back(uint8_t((v & 0x7f) | 0x80));
            v >>= 7;
        }
        buf_.push_back(uint8_t(v));
    }
    void ZigZag(int64_t v) { Varint((uint64_t(v) << 1) ^ uint64_t(v >> 63)); }
    void Raw(const uint8_t *p, size_t n) { buf_.insert(buf_.end(), p, p + n); }

    void I32(int16_t id, int32_t v) { Field(id, CT_I32); ZigZag(v); }
    void I64(int16_t id, int64_t v) { Field(id, CT_I64); ZigZag(v); }
    void Bool(int16_t id, bool v) { Field(id, v ? CT_BOOL_TRUE : CT_BOOL_FALSE); } // value rides the type
    void String(int16_t id, const std::string &s) {
        Field(id, CT_BINARY);
        StringElem(s);
    }

    void StructField(int16_t id) { Field(id, CT_STRUCT); PushScope(); }
    void ElementBegin() { PushScope(); }
    void StructEnd() {
        buf_.push_back(CT_STOP);
        PopScope();
    }

    void List(int16_t id, uint8_t elem_type, size_t n) {
        Field(id, CT_LIST);
        if (n < 15) {
            buf_.push_back(uint8_t((n << 4) | elem_type));
        } else {
            buf_.push_back(uint8_t(0xf0 | elem_type));
            Varint(n);
        }
    }
    void I32Elem(int32_t v) { ZigZag(v); }
    void StringElem(const std::string &s) {
        Varint(s.size());
        Raw(reinterpret_cast<const uint8_t *>(s.data()), s.size());
    }

    void Stop() { buf_.push_back(CT_STOP); }

private:
    void Field(int16_t id, uint8_t type) {
        int16_t delta = id - last_id_;
        if (delta > 0 && delta <= 15) {
            buf_.push_back(uint8_t((delta << 4) | type));
        } else {
            buf_.push_back(type);
            ZigZag(id);
        }
        last_id_ = id;
    }
    void PushScope() {
        id_stack_.push_back(last_id_);
        last_id_ = 0;
    }
    void PopScope() {
        last_id_ = id_stack_.back();
        id_stack_.pop_back();
    }

    std::vector<uint8_t> buf_;
    int16_t last_id_ = 0;
    std::vector<int16_t> id_stack_;
};

} // namespace plume::parquet
