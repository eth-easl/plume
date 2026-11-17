// plume_df_combine: merges the dynamic filter items of every build instance into one item per
// filter id, equal to what a consumer would get by merging the raw items itself.

#include "abi_mock.hpp"
#include "test_util.hpp"

#include "plume/common/serial.hpp"
#include "plume/execution/operators/dynamic_filter.hpp"
#include "plume/functions/dynamic_filter.hpp"
#include "plume/util/dynamic_filter.hpp"

#include "duckdb/common/types/value.hpp"

#include <cstdint>
#include <map>
#include <vector>

using namespace plume;
using duckdb::Value;
using exec::DynamicFilter;

namespace {

constexpr uint64_t kBloomBits = 1024;

// A filter holding the exact keys `keys` (converted to Bloom once more than `max_values`).
DynamicFilter Keys(const std::vector<int32_t> &keys, uint32_t max_values = 4) {
    DynamicFilter f;
    f.max_values = max_values;
    f.bloom_bits = kBloomBits;
    f.has_values = true;
    for (int32_t k : keys) {
        f.MergeBounds(Value::INTEGER(k), Value::INTEGER(k));
        f.values.push_back(Value::INTEGER(k));
    }
    if (f.values.size() > max_values) {
        f.ConvertToBloom();
    }
    return f;
}

// A filter whose build side had no non-NULL key.
DynamicFilter Empty() {
    DynamicFilter f;
    f.max_values = 4;
    f.bloom_bits = kBloomBits;
    f.has_values = true;
    return f;
}

void AddFilter(uint32_t id, const DynamicFilter &f) {
    mock::AddInput(0, SerializeToBuffer(f), exec::DynamicFilterIdent(id));
}

// Runs the combiner and returns its outputs by filter id, checking there is one item per id.
std::map<uint32_t, DynamicFilter> Combine() {
    CHECK(fn::RunDynamicFilterCombine().is_ok());
    std::map<uint32_t, DynamicFilter> out;
    for (const auto *o : mock::OutputsForSet(0)) {
        auto id = exec::ParseDynamicFilterIdent(o->ident);
        CHECK(id.is_ok());
        CHECK(out.count(id.unwrap()) == 0);
        out.emplace(id.unwrap(), DeserializeFromBytes<DynamicFilter>(o->buffer.data(), o->buffer.size()));
    }
    CHECK(mock::Outputs().size() == out.size());
    return out;
}

bool MayContain(const DynamicFilter &f, int32_t key) {
    Value v = Value::INTEGER(key);
    if (!f.has_bounds || v < f.min || v > f.max) {
        return false;
    }
    if (f.has_values) {
        for (const auto &x : f.values) {
            if (x == v) {
                return true;
            }
        }
        return false;
    }
    return f.bloom.Empty() || f.bloom.MayContain(v.Hash());
}

} // namespace

TEST_CASE("df_combine: exact key sets of several instances are unioned") {
    mock::Reset();
    AddFilter(3, Keys({1, 5}));
    AddFilter(3, Keys({5, 9}));
    AddFilter(3, Keys({2}));
    auto out = Combine();
    CHECK(out.size() == 1);
    const auto &f = out.at(3);
    CHECK(f.has_bounds);
    CHECK(f.min == Value::INTEGER(1));
    CHECK(f.max == Value::INTEGER(9));
    CHECK(f.has_values);
    CHECK(f.values.size() == 4);
    for (int32_t k : {1, 2, 5, 9}) {
        CHECK(MayContain(f, k));
    }
    CHECK(!MayContain(f, 3));
}

TEST_CASE("df_combine: key sets overflowing max_values become a Bloom filter") {
    mock::Reset();
    AddFilter(0, Keys({1, 2, 3}));
    AddFilter(0, Keys({4, 5, 6}));
    auto out = Combine();
    const auto &f = out.at(0);
    CHECK(!f.has_values);
    CHECK(!f.bloom.Empty());
    for (int32_t k = 1; k <= 6; k++) {
        CHECK(MayContain(f, k));
    }
    CHECK(!MayContain(f, 7)); // outside the bounds
}

TEST_CASE("df_combine: Bloom and exact-key instances merge into a Bloom filter") {
    mock::Reset();
    AddFilter(1, Keys({10, 11}));
    AddFilter(1, Keys({20, 21, 22, 23, 24})); // already Bloom
    AddFilter(1, Keys({30}));
    auto out = Combine();
    const auto &f = out.at(1);
    CHECK(!f.has_values);
    CHECK(!f.bloom.Empty());
    for (int32_t k : {10, 11, 20, 21, 22, 23, 24, 30}) {
        CHECK(MayContain(f, k));
    }
    CHECK(f.min == Value::INTEGER(10));
    CHECK(f.max == Value::INTEGER(30));
}

TEST_CASE("df_combine: several filter ids are combined independently") {
    mock::Reset();
    AddFilter(7, Keys({1}));
    AddFilter(2, Keys({100}));
    AddFilter(7, Keys({3}));
    AddFilter(2, Keys({200}));
    auto out = Combine();
    CHECK(out.size() == 2);
    CHECK(out.at(7).values.size() == 2);
    CHECK(out.at(7).max == Value::INTEGER(3));
    CHECK(out.at(2).values.size() == 2);
    CHECK(out.at(2).min == Value::INTEGER(100));
}

TEST_CASE("df_combine: empty instances keep the filter, without bounds only if all are empty") {
    mock::Reset();
    AddFilter(0, Empty());
    AddFilter(0, Empty());
    AddFilter(1, Empty());
    AddFilter(1, Keys({4}));
    auto out = Combine();
    CHECK(out.size() == 2);
    CHECK(!out.at(0).has_bounds);
    CHECK(out.at(1).has_bounds);
    CHECK(MayContain(out.at(1), 4));
}

TEST_CASE("df_combine: no input items yield no output") {
    mock::Reset();
    mock::SetInput(0, std::vector<DataBuffer>{});
    CHECK(Combine().empty());
}

TEST_CASE("df_combine: a non-filter ident is rejected") {
    mock::Reset();
    mock::AddInput(0, SerializeToBuffer(Keys({1})), "not_a_filter");
    CHECK(fn::RunDynamicFilterCombine().is_error());
}

TEST_CASE("df_combine: consumers bind the combined items like the raw ones") {
    const std::vector<std::pair<uint32_t, DynamicFilter>> raw = {
        {0, Keys({1, 2})}, {0, Keys({8})}, {0, Keys({3, 4, 5, 6, 7})}, {5, Keys({42})}, {5, Empty()}};

    mock::Reset();
    for (const auto &[id, f] : raw) {
        AddFilter(id, f);
    }
    auto direct = util::ReadDynamicFilters(0);
    CHECK(direct.is_ok());

    auto combined = Combine();
    mock::Reset();
    for (const auto &[id, f] : combined) {
        AddFilter(id, f);
    }
    auto via_combine = util::ReadDynamicFilters(0);
    CHECK(via_combine.is_ok());

    const auto &a = direct.unwrap();
    const auto &b = via_combine.unwrap();
    CHECK(a.size() == b.size());
    for (const auto &[id, f] : a) {
        const auto &g = b.at(id);
        CHECK(f.has_bounds == g.has_bounds);
        CHECK(f.min == g.min);
        CHECK(f.max == g.max);
        CHECK(f.has_values == g.has_values);
        CHECK(f.values == g.values);
        for (int32_t k = -1; k <= 50; k++) {
            CHECK(MayContain(f, k) == MayContain(g, k));
        }
    }
}

int main() { return plume_test::RunAll(); }
