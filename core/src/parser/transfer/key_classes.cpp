#include "plume/parser/transfer/key_classes.hpp"

#include "plume/parser/dynamic_filter_planning.hpp"

#include <algorithm>

namespace plume::parser::transfer {

namespace {

double ColumnDomain(const RelationSet &relations, RelationColumn col) {
    const auto leaf = relations.LeafColumnOf(col);
    return leaf ? static_cast<double>(LeafColumnDomain(*leaf)) : 0.0;
}

} // namespace

RelationColumn KeyClasses::Find(RelationColumn col) const {
    auto it = parent_.find(col);
    if (it == parent_.end()) {
        return col;
    }
    if (it->second == col) {
        return col;
    }
    const RelationColumn root = Find(it->second);
    it->second = root; // path compression
    return root;
}

void KeyClasses::Union(RelationColumn a, RelationColumn b) {
    for (const auto &c : {a, b}) {
        if (parent_.try_emplace(c, c).second) {
            members_[c].push_back(c);
        }
    }
    RelationColumn ra = Find(a);
    RelationColumn rb = Find(b);
    if (ra == rb) {
        return;
    }
    if (rb < ra) {
        std::swap(ra, rb);
    }
    parent_[rb] = ra;
    auto &into = members_[ra];
    auto &from = members_[rb];
    into.insert(into.end(), from.begin(), from.end());
    members_.erase(rb);
}

double KeyClasses::Domain(const RelationSet &relations, RelationColumn col,
                          std::optional<RelationColumn> exclude) const {
    auto it = members_.find(Find(col));
    if (it == members_.end()) {
        return exclude && *exclude == col ? 0.0 : ColumnDomain(relations, col);
    }
    double domain = 0;
    for (const auto &member : it->second) {
        if (exclude && *exclude == member) {
            continue;
        }
        const double d = ColumnDomain(relations, member);
        if (d > 0 && (domain == 0 || d < domain)) {
            domain = d;
        }
    }
    return domain;
}

} // namespace plume::parser::transfer
