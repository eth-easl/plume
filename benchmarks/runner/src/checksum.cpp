#include "checksum.hpp"

#include "plume/memory/adapter.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <nlohmann/json.hpp>

#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace plume::bench {

namespace {

using json = nlohmann::json;

// "0x1a2b..." -> uint64_t. Accepts a missing "0x" prefix too.
uint64_t ParseHex64(const std::string &s) {
    return static_cast<uint64_t>(std::strtoull(s.c_str(), nullptr, 16));
}

std::string ToHex64(uint64_t v) {
    std::ostringstream oss;
    oss << "0x" << std::hex << std::setw(16) << std::setfill('0') << v;
    return oss.str();
}

double CanonicalizeDouble(double x) {
    if (!std::isfinite(x)) {
        return x;
    }
    return std::round(x * 1e6) / 1e6;
}

duckdb::Vector &HashableColumn(duckdb::DataChunk &chunk, duckdb::idx_t c, duckdb::idx_t count,
                               std::vector<duckdb::unique_ptr<duckdb::Vector>> &storage) {
    duckdb::Vector &src = chunk.data[c];
    auto id = src.GetType().id();
    if (id != duckdb::LogicalTypeId::DOUBLE && id != duckdb::LogicalTypeId::FLOAT) {
        return src;
    }
    src.Flatten(count);
    auto canon = duckdb::make_uniq<duckdb::Vector>(src.GetType(), count);
    duckdb::FlatVector::SetValidity(*canon, duckdb::FlatVector::Validity(src));
    if (id == duckdb::LogicalTypeId::DOUBLE) {
        auto *sdata = duckdb::FlatVector::GetData<double>(src);
        auto *ddata = duckdb::FlatVector::GetData<double>(*canon);
        for (duckdb::idx_t r = 0; r < count; r++) {
            ddata[r] = CanonicalizeDouble(sdata[r]);
        }
    } else {
        auto *sdata = duckdb::FlatVector::GetData<float>(src);
        auto *ddata = duckdb::FlatVector::GetData<float>(*canon);
        for (duckdb::idx_t r = 0; r < count; r++) {
            ddata[r] = static_cast<float>(CanonicalizeDouble(sdata[r]));
        }
    }
    duckdb::Vector &ref = *canon;
    storage.push_back(std::move(canon));
    return ref;
}

} // namespace

Result<ChecksumFile> ChecksumFile::FromJsonFile(const std::string &path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        return Error("Could not open checksum file '" + path + "'.", ErrorKind::InvalidInput);
    }

    json data;
    try {
        file >> data;
    } catch (const std::exception &e) {
        return Error("Failed to parse checksum JSON: " + std::string(e.what()), ErrorKind::InvalidInput);
    }

    ChecksumFile result;
    try {
        for (auto &[query, per_sf] : data.items()) {
            for (auto &[scale_factor, obj] : per_sf.items()) {
                ChecksumEntry entry;
                entry.row_count = obj.at("rowCount").get<uint64_t>();
                entry.hash_sum = ParseHex64(obj.at("hashSum").get<std::string>());
                result.entries_[query][scale_factor] = entry;
            }
        }
    } catch (const std::exception &e) {
        return Error("Malformed checksum file '" + path + "': " + e.what(), ErrorKind::InvalidInput);
    }
    return result;
}

std::optional<ChecksumEntry> ChecksumFile::Find(const std::string &query, const std::string &scale_factor) const {
    auto qit = entries_.find(query);
    if (qit == entries_.end()) {
        return std::nullopt;
    }
    auto sit = qit->second.find(scale_factor);
    if (sit == qit->second.end()) {
        return std::nullopt;
    }
    return sit->second;
}

Result<ChecksumEntry> ComputeChecksum(const dandelion::DataSetVec &sets) {
    ChecksumEntry result;
    for (const auto &set : sets) {
        for (const auto &block : set) {
            // ImportBlockChunks mutates the buffer (varchar swizzle); copy first.
            std::vector<uint8_t> buf = block.data;
            plume::Schema schema;
            TRY(auto chunks, plume::memory::ImportBlockChunks(buf.data(), buf.size(), schema));
            for (auto &chunk : chunks) {
                if (chunk->size() == 0 || chunk->ColumnCount() == 0) {
                    continue;
                }
                TRYV(TryCatch([&]() {
                    const duckdb::idx_t count = chunk->size();
                    std::vector<duckdb::unique_ptr<duckdb::Vector>> storage;
                    duckdb::Vector hashes(duckdb::LogicalType::HASH, count);
                    duckdb::VectorOperations::Hash(HashableColumn(*chunk, 0, count, storage), hashes, count);
                    for (duckdb::idx_t c = 1; c < chunk->ColumnCount(); c++) {
                        duckdb::VectorOperations::CombineHash(hashes, HashableColumn(*chunk, c, count, storage),
                                                              count);
                    }
                    hashes.Flatten(count);
                    auto *data = duckdb::FlatVector::GetData<duckdb::hash_t>(hashes);
                    for (duckdb::idx_t r = 0; r < count; r++) {
                        result.hash_sum += data[r];
                    }
                }));
                result.row_count += chunk->size();
            }
        }
    }
    return result;
}

std::string ToString(const ChecksumEntry &entry) {
    return "row_count=" + std::to_string(entry.row_count) + " hash_sum=" + ToHex64(entry.hash_sum);
}

} // namespace plume::bench
