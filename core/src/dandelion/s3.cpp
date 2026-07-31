#include "plume/dandelion/s3.hpp"

#include <cstring>
#include <sstream>

namespace plume::s3 {

DataBuffer CreateGetRequest(std::string_view url, int64_t bytes_size, int64_t bytes_offset) {
    std::stringstream req;

    req << "GET " << url << " HTTP/1.1\n";
    if (bytes_size != 0) {
        if (bytes_offset >= 0) {
            req << "Range: bytes=" << bytes_offset << "-" << bytes_offset + bytes_size - 1 << "\n";
        } else {
            req << "Range: bytes=" << bytes_size << "\n";
        }
    }

    std::string req_str = req.str();
    DataBuffer req_buf(req_str.size());
    std::memcpy((void*)req_buf.data(), req_str.data(), req_str.size());
    return req_buf;
}
    
} // namespace plume::s3
