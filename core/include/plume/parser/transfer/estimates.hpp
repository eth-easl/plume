#pragma once

// Selectivity estimates for predicate transfer (predicate-transfer.md §4.3).

#include "plume/parser/transfer/relation.hpp"

namespace plume::parser::transfer {

// The thresholds an edge's estimated selectivity must pass.
struct SelectivityThresholds {
    // With both key domains known: the share of the key domain the source keeps.
    double max_match_fraction;
    // Otherwise: the source's distinct keys relative to the target's rows.
    double max_card_fraction;
};

struct Selectivity {
    double fraction; // estimated share of the target rows the filter keeps
    bool passes;     // below the applicable threshold
};

// Distinct keys of a column with `domain` values (0: unknown) among `card` rows.
double DistinctKeys(double card, double domain);

// The selectivity of a filter with `source_distinct` keys from a key domain of `key_domain` values
// (KeyClasses::Domain) into a relation with `target_card` rows (0 / infinity: unknown).
Selectivity EstimateSelectivity(double source_distinct, double key_domain, double target_card,
                                const SelectivityThresholds &thresholds);

} // namespace plume::parser::transfer
