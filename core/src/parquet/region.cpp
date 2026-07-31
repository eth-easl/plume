#include "plume/parquet/parquet.hpp"
#include "plume/parquet/regions.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include <algorithm>
#include <cstring>
#include <numeric>

namespace plume::parquet {

using duckdb::idx_t;

//===----------------------------------------------------------------------===//
// Region serialization
//===----------------------------------------------------------------------===//

DataBuffer Region::Serialize() const {
    duckdb::MemoryStream stream;
    duckdb::BinarySerializer s(stream);
    s.Begin();
    s.WriteList(100, "row_groups", row_groups.size(), [&](duckdb::Serializer::List &rg_list, idx_t i) {
        rg_list.WriteObject([&](duckdb::Serializer &rs) {
            rs.WriteList(100, "columns", row_groups[i].size(), [&](duckdb::Serializer::List &c_list, idx_t j) {
                const DataChunk &dc = row_groups[i][j];
                c_list.WriteObject([&](duckdb::Serializer &cs) {
                    cs.WriteProperty(100, "row_group_idx", dc.row_group_idx);
                    cs.WriteProperty(101, "column_idx", dc.column_idx);
                    cs.WriteProperty(102, "offset", dc.offset);
                    cs.WriteProperty(103, "size", dc.size);
                    cs.WriteProperty(104, "req_idx", dc.req_idx);
                    cs.WriteProperty(105, "req_offset", dc.req_offset);
                    cs.WriteProperty(106, "codec", dc.codec);
                });
            });
        });
    });
    s.WriteList(101, "requests", requests.size(), [&](duckdb::Serializer::List &r_list, idx_t i) {
        r_list.WriteObject([&](duckdb::Serializer &rs) {
            rs.WriteProperty(100, "offset", requests[i].offset);
            rs.WriteProperty(101, "size", requests[i].size);
        });
    });
    s.End();

    const auto size = stream.GetPosition();
    DataBuffer buf(size);
    if (size > 0) {
        std::memcpy(buf.mutable_data(), stream.GetData(), size);
    }
    return buf;
}

Region Region::Deserialize(DataBuffer &buf) {
    duckdb::MemoryStream stream(const_cast<duckdb::data_ptr_t>(buf.data()), buf.size());
    duckdb::BinaryDeserializer d(stream);
    d.Begin();
    Region region;
    d.ReadList(100, "row_groups", [&](duckdb::Deserializer::List &rg_list, idx_t /*i*/) {
        rg_list.ReadObject([&](duckdb::Deserializer &rd) {
            std::vector<DataChunk> cols;
            rd.ReadList(100, "columns", [&](duckdb::Deserializer::List &c_list, idx_t /*j*/) {
                c_list.ReadObject([&](duckdb::Deserializer &cd) {
                    DataChunk dc;
                    dc.row_group_idx = cd.ReadProperty<uint64_t>(100, "row_group_idx");
                    dc.column_idx = cd.ReadProperty<uint64_t>(101, "column_idx");
                    dc.offset = cd.ReadProperty<int64_t>(102, "offset");
                    dc.size = cd.ReadProperty<int64_t>(103, "size");
                    dc.req_idx = cd.ReadProperty<uint64_t>(104, "req_idx");
                    dc.req_offset = cd.ReadProperty<uint64_t>(105, "req_offset");
                    dc.codec = cd.ReadProperty<int32_t>(106, "codec");
                    cols.push_back(dc);
                });
            });
            region.row_groups.push_back(std::move(cols));
        });
    });
    d.ReadList(101, "requests", [&](duckdb::Deserializer::List &r_list, idx_t /*i*/) {
        r_list.ReadObject([&](duckdb::Deserializer &rd) {
            ChunkRequest req;
            req.offset = rd.ReadProperty<int64_t>(100, "offset");
            req.size = rd.ReadProperty<int64_t>(101, "size");
            region.requests.push_back(req);
        });
    });
    d.End();
    return region;
}

//===----------------------------------------------------------------------===//
// Region definition
//===----------------------------------------------------------------------===//

std::vector<Region> DefineRegions(const FileMeta &meta, uint32_t num_splits, uint64_t coalesce_distance,
                                  const std::vector<uint32_t> &projection,
                                  const std::vector<uint32_t> &keep_row_groups, uint64_t max_region_size) {
    std::vector<Region> regions;

    std::vector<uint32_t> rgs;
    if (keep_row_groups.empty()) {
        rgs.resize(meta.row_groups.size());
        std::iota(rgs.begin(), rgs.end(), 0u);
    } else {
        rgs = keep_row_groups;
    }
    if (rgs.empty()) {
        return regions;
    }

    auto rg_size = [&](uint32_t rg) -> uint64_t {
        const auto &rgm = meta.row_groups[rg];
        uint64_t bytes = 0;
        if (projection.empty()) {
            for (const auto &c : rgm.columns) {
                bytes += static_cast<uint64_t>(c.size);
            }
        } else {
            for (uint32_t file_ci : projection) {
                if (file_ci < rgm.columns.size()) {
                    bytes += static_cast<uint64_t>(rgm.columns[file_ci].size);
                }
            }
        }
        return bytes;
    };

    auto build_region = [&](size_t start, size_t end) {
        Region region;
        for (size_t s = start; s < end; s++) {
            const uint32_t rg = rgs[s];
            const auto &rgm = meta.row_groups[rg];
            auto add_col = [&](std::vector<DataChunk> &cols, uint32_t file_ci, uint64_t out_idx) {
                DataChunk dc;
                dc.row_group_idx = rg;
                dc.column_idx = out_idx;
                dc.offset = rgm.columns[file_ci].offset;
                dc.size = rgm.columns[file_ci].size;
                dc.codec = rgm.columns[file_ci].codec;
                cols.push_back(dc);
            };
            std::vector<DataChunk> cols;
            if (projection.empty()) {
                cols.reserve(rgm.columns.size());
                for (uint32_t ci = 0; ci < rgm.columns.size(); ci++) {
                    add_col(cols, ci, ci);
                }
            } else {
                cols.reserve(projection.size());
                for (uint32_t out = 0; out < projection.size(); out++) {
                    if (projection[out] < rgm.columns.size()) {
                        add_col(cols, projection[out], out);
                    }
                }
            }
            region.row_groups.push_back(std::move(cols));
        }

        std::vector<DataChunk *> ptrs;
        for (auto &rgcols : region.row_groups) {
            for (auto &dc : rgcols) {
                ptrs.push_back(&dc);
            }
        }
        std::sort(ptrs.begin(), ptrs.end(), [](DataChunk *a, DataChunk *b) { return a->offset < b->offset; });
        for (auto *dc : ptrs) {
            if (region.requests.empty() ||
                static_cast<uint64_t>(dc->offset) > region.requests.back().end() + coalesce_distance) {
                ChunkRequest req;
                req.offset = dc->offset;
                req.size = dc->size;
                region.requests.push_back(req);
            } else {
                auto &req = region.requests.back();
                const uint64_t new_end = std::max<uint64_t>(req.end(), dc->end());
                req.size = static_cast<int64_t>(new_end - static_cast<uint64_t>(req.offset));
            }
            dc->req_idx = region.requests.size() - 1;
            dc->req_offset = static_cast<uint64_t>(dc->offset - region.requests.back().offset);
        }
        regions.push_back(std::move(region));
    };

    const uint32_t n = std::max<uint32_t>(1, num_splits);
    const size_t rg_per = rgs.size() / n;
    const size_t rounding_regions = rgs.size() - rg_per * n;
    size_t start = 0;
    uint64_t cur_bytes = 0;
    size_t region_count = 0;
    for (size_t i = 0; i < rgs.size(); i++) {
        const uint64_t sz = rg_size(rgs[i]);
        const size_t rounding = region_count < rounding_regions ? 1 : 0;
        const bool over_count = (i - start) >= rg_per + rounding;
        const bool over_size = max_region_size > 0 && cur_bytes + sz > max_region_size;
        if ((over_count || over_size)) {
            build_region(start, i);
            region_count += 1;
            start = i;
            cur_bytes = 0;
        }
        cur_bytes += sz;
    }
    build_region(start, rgs.size());

    return regions;
}

} // namespace plume::parquet
