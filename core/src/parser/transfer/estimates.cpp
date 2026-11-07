#include "plume/parser/transfer/estimates.hpp"

#include <algorithm>
#include <cmath>

namespace plume::parser::transfer {

double DistinctKeys(double card, double domain) {
    return domain > 0 ? std::min(card, domain) : card;
}

Selectivity EstimateSelectivity(double source_distinct, double key_domain, double target_card,
                                const SelectivityThresholds &thresholds) {
    if (!std::isfinite(source_distinct)) {
        return {1.0, false};
    }
    if (key_domain > 0) {
        const double fraction = std::min(1.0, source_distinct / key_domain);
        return {fraction, fraction <= thresholds.max_match_fraction};
    }
    if (target_card <= 0 || !std::isfinite(target_card)) {
        return {1.0, false};
    }
    const double fraction = std::min(1.0, source_distinct / target_card);
    return {fraction, fraction <= thresholds.max_card_fraction};
}

} // namespace plume::parser::transfer
