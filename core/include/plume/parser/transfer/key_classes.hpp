#pragma once

#include "plume/parser/transfer/relation.hpp"

#include <map>
#include <optional>
#include <vector>

namespace plume::parser::transfer {

class KeyClasses {
public:
    void Union(RelationColumn a, RelationColumn b);
    RelationColumn Find(RelationColumn col) const;
    double Domain(const RelationSet &relations, RelationColumn col,
                  std::optional<RelationColumn> exclude = std::nullopt) const;

private:
    mutable std::map<RelationColumn, RelationColumn> parent_;
    std::map<RelationColumn, std::vector<RelationColumn>> members_; // by representative
};

} // namespace plume::parser::transfer
