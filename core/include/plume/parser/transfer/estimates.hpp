#pragma once

namespace plume::parser::transfer {

// The thresholds an edge's estimated selectivity must pass.
struct SelectivityThresholds {
    // The share of the keys the source keeps.
    double max_match_fraction;
    // The source's distinct keys relative to the target's rows.
    double max_card_fraction;
};

struct Selectivity {
    double fraction; // estimated share of the target rows the filter keeps
    bool passes;     // below the applicable threshold
};

Selectivity EstimateSelectivity(double source_distinct, double key_domain, double target_card,
                                const SelectivityThresholds &thresholds);

} // namespace plume::parser::transfer
