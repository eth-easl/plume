#include "plume/catalog/catalog.hpp"

#include <cstdlib>
#include <string>

namespace plume::catalog {

Result<size_t> SourceCatalog::Add(std::shared_ptr<DataSource> source) {
    std::string name = source->name;
    auto it = name_map_.find(source->name);
    if (it != name_map_.end()) {
        return Error("Name already taken in catalog.", ErrorKind::InvalidInput);
    }
    size_t idx = sources_.size();
    sources_.push_back(std::move(source));
    name_map_.insert({std::move(name), idx});
    return idx;
}

Result<const std::shared_ptr<DataSource>> SourceCatalog::Get(size_t source_idx) const {
    if (source_idx >= sources_.size()) {
        return Error("Invalid source index.", ErrorKind::OutOfRange);
    }
    return sources_[source_idx];
}

Result<const std::shared_ptr<DataSource>> SourceCatalog::Get(const std::string &source_name) const {
    auto it = name_map_.find(source_name);
    if (it == name_map_.end()) {
        return Error("Invalid source name.", ErrorKind::InvalidInput);
    }
    return sources_[it->second];
}

} // namespace plume::catalog
