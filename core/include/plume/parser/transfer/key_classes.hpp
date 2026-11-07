#pragma once

// Key equivalence classes (predicate-transfer.md §4.2-4.3): relation columns some equi join
// equates, of any join kind. Used to estimate a key's value domain.

#include "plume/parser/transfer/relation.hpp"

#include <map>
#include <optional>
#include <vector>

namespace plume::parser::transfer {

class KeyClasses {
public:
    // Puts `a` and `b` into one class.
    void Union(RelationColumn a, RelationColumn b);
    // The class representative of `col` (itself if it is in no class).
    RelationColumn Find(RelationColumn col) const;

    // The value domain of `col` estimated over its class: the smallest known domain of any member
    // but `exclude`. Without distinct statistics a column's domain is its scan's row count, far
    // too large for a foreign key column; the primary key it is joined with bounds it much better.
    // 0: unknown.
    double Domain(const RelationSet &relations, RelationColumn col,
                  std::optional<RelationColumn> exclude = std::nullopt) const;

private:
    mutable std::map<RelationColumn, RelationColumn> parent_;
    std::map<RelationColumn, std::vector<RelationColumn>> members_; // by representative
};

} // namespace plume::parser::transfer
