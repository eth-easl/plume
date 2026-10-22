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
                    cs.WriteList(107, "pages", dc.pages.size(), [&](duckdb::Serializer::List &p_list, idx_t k) {
                        const PageRun &run = dc.pages[k];
                        p_list.WriteObject([&](duckdb::Serializer &ps) {
                            ps.WriteProperty(100, "offset", run.offset);
                            ps.WriteProperty(101, "size", run.size);
                            ps.WriteProperty(102, "first_row", run.first_row);
                            ps.WriteProperty(103, "num_rows", run.num_rows);
                            ps.WriteProperty(104, "req_idx", run.req_idx);
                            ps.WriteProperty(105, "req_offset", run.req_offset);
                        });
                    });
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
    s.WriteProperty(102, "dynamic_filter_column", dynamic_filter_column);
    s.WriteProperty(103, "dynamic_filter_size", static_cast<uint64_t>(dynamic_filter.size()));
    s.WriteProperty(104, "dynamic_filter", dynamic_filter.data(), dynamic_filter.size());
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
                    cd.ReadList(107, "pages", [&](duckdb::Deserializer::List &p_list, idx_t /*k*/) {
                        p_list.ReadObject([&](duckdb::Deserializer &pd) {
                            PageRun run;
                            run.offset = pd.ReadProperty<int64_t>(100, "offset");
                            run.size = pd.ReadProperty<int64_t>(101, "size");
                            run.first_row = pd.ReadProperty<uint64_t>(102, "first_row");
                            run.num_rows = pd.ReadProperty<uint64_t>(103, "num_rows");
                            run.req_idx = pd.ReadProperty<uint64_t>(104, "req_idx");
                            run.req_offset = pd.ReadProperty<uint64_t>(105, "req_offset");
                            dc.pages.push_back(run);
                        });
                    });
                    cols.push_back(std::move(dc));
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
    region.dynamic_filter_column = d.ReadProperty<int32_t>(102, "dynamic_filter_column");
    region.dynamic_filter.resize(d.ReadProperty<uint64_t>(103, "dynamic_filter_size"));
    d.ReadProperty(104, "dynamic_filter", region.dynamic_filter.data(), region.dynamic_filter.size());
    d.End();
    return region;
}

//===----------------------------------------------------------------------===//
// Region definition
//===----------------------------------------------------------------------===//

std::vector<Region> DefineRegions(const FileMeta &meta, uint32_t num_splits, uint64_t coalesce_distance,
                                  const std::vector<uint32_t> &projection,
                                  const std::vector<uint32_t> &keep_row_groups, uint64_t max_region_size,
                                  const std::map<uint32_t, RowGroupPages> &page_selection) {
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
        if (auto it = page_selection.find(rg); it != page_selection.end()) {
            for (auto dict : it->second.dict_sizes) {
                bytes += static_cast<uint64_t>(dict);
            }
            for (const auto &runs : it->second.runs) {
                for (const auto &run : runs) {
                    bytes += static_cast<uint64_t>(run.size);
                }
            }
            return bytes;
        }
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
            if (auto it = page_selection.find(rg); it != page_selection.end()) {
                for (size_t c = 0; c < cols.size() && c < it->second.runs.size(); c++) {
                    cols[c].size = it->second.dict_sizes[c];
                    cols[c].pages = it->second.runs[c];
                }
            }
            region.row_groups.push_back(std::move(cols));
        }

        // A byte range to fetch: a whole column chunk, its dictionary page or a page run.
        struct Piece {
            int64_t offset;
            int64_t size;
            uint64_t *req_idx;
            uint64_t *req_offset;
        };
        std::vector<Piece> pieces;
        for (auto &rgcols : region.row_groups) {
            for (auto &dc : rgcols) {
                if (dc.pages.empty() || dc.size > 0) {
                    pieces.push_back({dc.offset, dc.size, &dc.req_idx, &dc.req_offset});
                } else {
                    dc.req_idx = 0;
                    dc.req_offset = 0;
                }
                for (auto &run : dc.pages) {
                    pieces.push_back({run.offset, run.size, &run.req_idx, &run.req_offset});
                }
            }
        }
        std::sort(pieces.begin(), pieces.end(), [](const Piece &a, const Piece &b) { return a.offset < b.offset; });
        for (auto &piece : pieces) {
            const uint64_t end = static_cast<uint64_t>(piece.offset + piece.size);
            if (region.requests.empty() ||
                static_cast<uint64_t>(piece.offset) > region.requests.back().end() + coalesce_distance) {
                ChunkRequest req;
                req.offset = piece.offset;
                req.size = piece.size;
                region.requests.push_back(req);
            } else {
                auto &req = region.requests.back();
                const uint64_t new_end = std::max<uint64_t>(req.end(), end);
                req.size = static_cast<int64_t>(new_end - static_cast<uint64_t>(req.offset));
            }
            *piece.req_idx = region.requests.size() - 1;
            *piece.req_offset = static_cast<uint64_t>(piece.offset - region.requests.back().offset);
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

uint32_t FileSplitCount(uint32_t num_splits, uint32_t num_files, uint32_t file_idx,
                        size_t kept_row_groups, size_t total_row_groups) {
    num_files = std::max<uint32_t>(1, num_files);
    const uint32_t total = std::max<uint32_t>(num_files, std::max<uint32_t>(1, num_splits));
    const uint32_t base_splits = total / num_files;
    const uint32_t rounding_files = total - base_splits * num_files;
    uint64_t splits = file_idx < rounding_files ? base_splits + 1 : base_splits;
    if (total_row_groups > 0 && kept_row_groups < total_row_groups) {
        splits = (splits * kept_row_groups + total_row_groups - 1) / total_row_groups;
    }
    return static_cast<uint32_t>(std::max<uint64_t>(1, splits));
}

} // namespace plume::parquet
