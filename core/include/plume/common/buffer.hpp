#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <utility>

namespace plume {

class DataBuffer {
  public:
    DataBuffer() : data_(nullptr) {}
    DataBuffer(uint8_t* data, size_t size, bool owns_data) 
        : data_(data), size_(size), owns_data_(owns_data) {}
    DataBuffer(size_t size)
        : size_(size), owns_data_(true) {
        if (size_ > 0) {
            data_ = (uint8_t*)std::malloc(size);
        } else {
            data_ = nullptr;
        }
    }

    // destructor
    ~DataBuffer() {
        if (owns_data_ && data_) {
            std::free(data_);
        }
    }

    // move constructor (transfers ownership)
    DataBuffer(DataBuffer&& other) noexcept 
        : data_(std::exchange(other.data_, nullptr)),
          size_(std::exchange(other.size_, 0)),
          owns_data_(std::exchange(other.owns_data_, false)) {}
    
    // disable copying
    DataBuffer(const DataBuffer&) = delete;
    DataBuffer& operator=(const DataBuffer&) = delete;

    // move assignment operator
    DataBuffer& operator=(DataBuffer&& other) noexcept {
        if (this != &other) {
            // free existing resource if we own it
            if (owns_data_ && data_) {
                std::free(data_);
            }

            // take ownership of other buffer
            data_ = std::exchange(other.data_, nullptr);
            size_ = std::exchange(other.size_, 0);
            owns_data_ = std::exchange(other.owns_data_, false);
        }
        return *this;
    }

    inline bool TakeBufferOwnership() {
        if (!owns_data_) { return false; }
        owns_data_ = false;
        return true;
    }

    inline const uint8_t* data() const { return data_; }
    inline uint8_t* mutable_data() { return data_; }
    inline size_t size() const { return size_; }

  protected:
    uint8_t* data_ = nullptr;
    size_t size_ = 0;
    bool owns_data_ = false;
};

} // namespace plume
