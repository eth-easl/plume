#include "plume/execution/operators/dynamic_filter.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <type_traits>
#include <unordered_set>

namespace plume::exec {

using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::PhysicalType;
using duckdb::UnifiedVectorFormat;
using duckdb::Value;
using duckdb::Vector;

//===----------------------------------------------------------------------===//
// DynamicFilter
//===----------------------------------------------------------------------===//

void DynamicFilter::MergeBounds(const Value &other_min, const Value &other_max) {
    if (!has_bounds) {
        has_bounds = true;
        min = other_min;
        max = other_max;
        return;
    }
    if (other_min < min) min = other_min;
    if (other_max > max) max = other_max;
}

void DynamicFilter::ConvertToBloom() {
    if (bloom_bits > 0) {
        bloom = BloomFilter(bloom_bits);
        // NOTE: Value::Hash() hashes the physical value like duckdb::Hash<T>, so these hashes match
        // the ones the build operator inserts.
        for (const auto &v : values) {
            bloom.Insert(v.Hash());
        }
    }
    has_values = false;
    values.clear();
}

void DynamicFilter::Merge(const DynamicFilter &other) {
    if (other.has_bounds) {
        MergeBounds(other.min, other.max);
    }

    if (has_values && other.has_values) {
        duckdb::value_set_t distinct(values.begin(), values.end());
        for (const auto &v : other.values) {
            if (distinct.insert(v).second) {
                values.push_back(v);
            }
        }
        if (values.size() > max_values) {
            ConvertToBloom();
        }
    } else if (has_values && !other.bloom.Empty()) {
        bloom = other.bloom;
        for (const auto &v : values) {
            bloom.Insert(v.Hash());
        }
        has_values = false;
        values.clear();
    } else if (!bloom.Empty() && other.has_values) {
        for (const auto &v : other.values) {
            bloom.Insert(v.Hash());
        }
    } else if (bloom.Empty() || other.bloom.Empty() || !bloom.Merge(other.bloom)) {
        // One side has no membership structure (or the Bloom filters differ in size).
        has_values = false;
        values.clear();
        bloom.Clear();
    }
}

void DynamicFilter::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "has_bounds", has_bounds);
    s.WriteProperty(101, "min", min);
    s.WriteProperty(102, "max", max);
    s.WriteProperty(103, "has_values", has_values);
    s.WriteList(104, "values", values.size(),
                [&](duckdb::Serializer::List &list, idx_t i) { list.WriteElement(values[i]); });
    s.WriteProperty(105, "max_values", max_values);
    s.WriteProperty(106, "bloom_bits", bloom_bits);
    s.WriteProperty(107, "bloom", bloom);
}

DynamicFilter DynamicFilter::Deserialize(duckdb::Deserializer &d) {
    DynamicFilter f;
    f.has_bounds = d.ReadProperty<bool>(100, "has_bounds");
    f.min = d.ReadProperty<Value>(101, "min");
    f.max = d.ReadProperty<Value>(102, "max");
    f.has_values = d.ReadProperty<bool>(103, "has_values");
    d.ReadList(104, "values", [&](duckdb::Deserializer::List &list, idx_t /*i*/) {
        f.values.push_back(list.ReadElement<Value>());
    });
    f.max_values = d.ReadProperty<uint32_t>(105, "max_values");
    f.bloom_bits = d.ReadProperty<uint64_t>(106, "bloom_bits");
    f.bloom = d.ReadProperty<BloomFilter>(107, "bloom");
    return f;
}

//===----------------------------------------------------------------------===//
// BuildRangeFilter
//===----------------------------------------------------------------------===//

std::optional<expr::ExprNode> BuildRangeFilter(uint32_t column, const ColumnType &type,
                                                const DynamicFilter &filter) {
    if (!filter.has_bounds) {
        return std::nullopt;
    }
    if (filter.min == filter.max) {
        return expr::ExprNode::Comparison(duckdb::ExpressionType::COMPARE_EQUAL,
                                          expr::ExprNode::Reference(column, type),
                                          expr::ExprNode::Constant(filter.min, type));
    }
    auto ge = expr::ExprNode::Comparison(duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                                         expr::ExprNode::Reference(column, type),
                                         expr::ExprNode::Constant(filter.min, type));
    auto le = expr::ExprNode::Comparison(duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO,
                                         expr::ExprNode::Reference(column, type),
                                         expr::ExprNode::Constant(filter.max, type));
    return expr::ExprNode::Conjunction(duckdb::ExpressionType::CONJUNCTION_AND, {std::move(ge), std::move(le)});
}

//===----------------------------------------------------------------------===//
// DynamicFilterBuildTemplate
//===----------------------------------------------------------------------===//

bool DynamicFilterBuildTemplate::Equals(const OperatorTemplate &other) const {
    if (other.type != type) return false;
    auto &o = static_cast<const DynamicFilterBuildTemplate &>(other);
    return column == o.column && key_type == o.key_type && max_values == o.max_values && bloom_bits == o.bloom_bits;
}

void DynamicFilterBuildTemplate::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(101, "column", column);
    s.WriteProperty(102, "key_type", key_type);
    s.WriteProperty(103, "max_values", max_values);
    s.WriteProperty(104, "bloom_bits", bloom_bits);
}

//===----------------------------------------------------------------------===//
// DynamicFilterProbe
//===----------------------------------------------------------------------===//

namespace {

struct KeyHash {
    template <class T> size_t operator()(const T &k) const { return duckdb::Hash<T>(k); }
};

template <class T>
class TypedProbe final : public DynamicFilterProbe {
public:
    // Keys of the exact set and the bounds point into filter_'s Values for strings.
    explicit TypedProbe(DynamicFilter filter)
        : filter_(std::move(filter)), lo_(filter_.min.GetValueUnsafe<T>()), hi_(filter_.max.GetValueUnsafe<T>()) {
        if (filter_.has_values) {
            mode_ = Mode::VALUES;
            for (const auto &v : filter_.values) {
                set_.insert(v.GetValueUnsafe<T>());
            }
        } else if (!filter_.bloom.Empty()) {
            mode_ = Mode::BLOOM;
        }
    }

protected:
    idx_t SelectRows(Vector &keys, idx_t size, duckdb::SelectionVector &sel, idx_t count) override {
        UnifiedVectorFormat udata;
        keys.ToUnifiedFormat(size, udata);
        auto data = UnifiedVectorFormat::GetData<T>(udata);
        idx_t passed = 0;
        for (idx_t i = 0; i < count; i++) {
            const auto row = sel.get_index(i);
            const auto idx = udata.sel->get_index(row);
            if (!udata.validity.RowIsValid(idx)) {
                continue; // NULL keys never join
            }
            const T &v = data[idx];
            if (v < lo_ || hi_ < v) {
                continue; // exceeds bounds
            }
            if (mode_ == Mode::VALUES && set_.find(v) == set_.end()) {
                continue; // no match in exact values
            } else if (mode_ == Mode::BLOOM && !filter_.bloom.MayContain(duckdb::Hash<T>(v))) {
                continue; // no match in Bloom filter
            }
            sel.set_index(passed++, row);
        }
        return passed;
    }

private:
    // What is checked next to the key range.
    enum class Mode { BOUNDS, VALUES, BLOOM };

    DynamicFilter filter_;
    T lo_;
    T hi_;
    Mode mode_ = Mode::BOUNDS;
    std::unordered_set<T, KeyHash> set_;
};

template <class T>
std::unique_ptr<DynamicFilterProbe> MakeProbe(DynamicFilter filter) {
    return std::make_unique<TypedProbe<T>>(std::move(filter));
}

} // namespace

Result<std::unique_ptr<DynamicFilterProbe>> DynamicFilterProbe::Create(DynamicFilter filter,
                                                                        const LogicalType &key_type) {
    // The bounds and exact keys are Values of the build template's key type, and the Bloom
    // filter hashes keys of that type: a probe column of another type can't be checked.
    if (!filter.has_bounds || filter.min.type() != key_type) {
        return std::unique_ptr<DynamicFilterProbe>();
    }
    switch (key_type.InternalType()) {
    case PhysicalType::BOOL: return MakeProbe<bool>(std::move(filter));
    case PhysicalType::INT8: return MakeProbe<int8_t>(std::move(filter));
    case PhysicalType::INT16: return MakeProbe<int16_t>(std::move(filter));
    case PhysicalType::INT32: return MakeProbe<int32_t>(std::move(filter));
    case PhysicalType::INT64: return MakeProbe<int64_t>(std::move(filter));
    case PhysicalType::UINT8: return MakeProbe<uint8_t>(std::move(filter));
    case PhysicalType::UINT16: return MakeProbe<uint16_t>(std::move(filter));
    case PhysicalType::UINT32: return MakeProbe<uint32_t>(std::move(filter));
    case PhysicalType::UINT64: return MakeProbe<uint64_t>(std::move(filter));
    case PhysicalType::INT128: return MakeProbe<duckdb::hugeint_t>(std::move(filter));
    case PhysicalType::UINT128: return MakeProbe<duckdb::uhugeint_t>(std::move(filter));
    case PhysicalType::FLOAT: return MakeProbe<float>(std::move(filter));
    case PhysicalType::DOUBLE: return MakeProbe<double>(std::move(filter));
    case PhysicalType::VARCHAR: return MakeProbe<duckdb::string_t>(std::move(filter));
    default:
        return Error("Dynamic filter probe over an unsupported column type", ErrorKind::NotImplemented);
    }
}

idx_t DynamicFilterProbe::SetDictionary(const Vector *dictionary, idx_t size) {
    dict_data_ = nullptr;
    dict_pass_.clear();
    if (!dictionary) {
        return 0;
    }
    // SelectRows only reads the keys.
    auto &dict = const_cast<Vector &>(*dictionary);
    duckdb::SelectionVector sel(size);
    for (idx_t i = 0; i < size; i++) {
        sel.set_index(i, i);
    }
    const idx_t passed = SelectRows(dict, size, sel, size);
    dict_pass_.assign(size, 0);
    for (idx_t i = 0; i < passed; i++) {
        dict_pass_[sel.get_index(i)] = 1;
    }
    dict_data_ = duckdb::FlatVector::GetData(dict);
    return passed;
}

idx_t DynamicFilterProbe::SelectDictionaryRows(Vector &keys, duckdb::SelectionVector &sel, idx_t count) {
    const auto &dict_sel = duckdb::DictionaryVector::SelVector(keys);
    idx_t passed = 0;
    for (idx_t i = 0; i < count; i++) {
        const auto row = sel.get_index(i);
        const auto entry = dict_sel.get_index(row);
        if (entry < dict_pass_.size() && dict_pass_[entry]) {
            sel.set_index(passed++, row);
        }
    }
    return passed;
}

idx_t DynamicFilterProbe::Select(Vector &keys, idx_t size, duckdb::SelectionVector &sel, idx_t count) {
    idx_t passed;
    if (dict_data_ && keys.GetVectorType() == duckdb::VectorType::DICTIONARY_VECTOR &&
        duckdb::DictionaryVector::Child(keys).GetVectorType() == duckdb::VectorType::FLAT_VECTOR &&
        duckdb::FlatVector::GetData(duckdb::DictionaryVector::Child(keys)) == dict_data_) {
        passed = SelectDictionaryRows(keys, sel, count);
    } else {
        passed = SelectRows(keys, size, sel, count);
    }
    // The pass rate is judged once, over the first kWarmupRows checked rows.
    if (rows_checked_ < kWarmupRows) {
        rows_checked_ += count;
        rows_passed_ += passed;
        if (rows_checked_ >= kWarmupRows &&
            static_cast<double>(rows_passed_) > kMaxPassRate * static_cast<double>(rows_checked_)) {
            active_ = false;
        }
    }
    return passed;
}

//===----------------------------------------------------------------------===//
// DynamicFilterBuildOperator
//===----------------------------------------------------------------------===//

// Folds a key column into a DynamicFilter.
class KeyCollector {
public:
    virtual ~KeyCollector() = default;
    // `vec` holds `count` keys already cast to the template's key type.
    virtual void Add(Vector &vec, idx_t count) = 0;
    // Ends the collection and returns the collected filter.
    virtual DynamicFilter Finish() = 0;
};

namespace {

template <class T>
inline void Accumulate(const T &v, bool &has_value, T &chunk_min, T &chunk_max) {
    if (!has_value) {
        chunk_min = v;
        chunk_max = v;
        has_value = true;
    } else {
        if (v < chunk_min) chunk_min = v;
        if (chunk_max < v) chunk_max = v;
    }
}

// No selection-vector or per-row validity check -> allow the compiler to autovectorize.
template <class T>
void ReduceFlatAllValid(const T *__restrict data, idx_t count, bool &has_value, T &chunk_min, T &chunk_max) {
    idx_t i = 0;
    if (!has_value) {
        if (count == 0) {
            return;
        }
        chunk_min = chunk_max = data[0];
        has_value = true;
        i = 1;
    }
    for (; i < count; i++) {
        const T &v = data[i];
        if (v < chunk_min) chunk_min = v;
        if (chunk_max < v) chunk_max = v;
    }
}

// The key as a Value of the key type: physical values of e.g. DATE or DECIMAL keys are
// reinterpreted instead of surfacing as their plain integer representation.
template <class T> Value MakeValue(const T &v, const LogicalType &key_type) {
    Value val = Value::CreateValue<T>(v);
    if (val.type() != key_type) {
        val.Reinterpret(key_type);
    }
    return val;
}

template <class T>
class TypedKeyCollector final : public KeyCollector {
public:
    TypedKeyCollector(LogicalType key_type, uint32_t max_values, uint64_t bloom_bits, bool exact_set)
        : key_type_(std::move(key_type)), max_values_(max_values), bloom_bits_(bloom_bits) {
        if (exact_set && max_values > 0) {
            mode_ = Mode::VALUES;
        } else if (bloom_bits > 0) {
            mode_ = Mode::BLOOM;
            bloom_ = BloomFilter(bloom_bits);
        }
    }

    void Add(Vector &vec, idx_t count) override {
        bool has_value = false;
        T chunk_min {};
        T chunk_max {};

        if (mode_ == Mode::BOUNDS && vec.GetVectorType() == duckdb::VectorType::FLAT_VECTOR &&
            duckdb::FlatVector::Validity(vec).AllValid()) {
            ReduceFlatAllValid<T>(duckdb::FlatVector::GetData<T>(vec), count, has_value, chunk_min, chunk_max);
        } else {
            UnifiedVectorFormat udata;
            vec.ToUnifiedFormat(count, udata);
            auto data = UnifiedVectorFormat::GetData<T>(udata);
            for (idx_t i = 0; i < count; i++) {
                const auto idx = udata.sel->get_index(i);
                if (!udata.validity.RowIsValid(idx)) {
                    continue; // NULL keys never join
                }
                const T &v = data[idx];
                Accumulate(v, has_value, chunk_min, chunk_max);
                if (mode_ == Mode::VALUES) {
                    InsertValue(v);
                    if (set_.size() > max_values_) {
                        ConvertToBloom();
                    }
                } else if (mode_ == Mode::BLOOM) {
                    bloom_.Insert(duckdb::Hash<T>(v));
                }
            }
        }
        if (!has_value) {
            return; // every row in this chunk was NULL
        }

        if (!has_bounds_ || chunk_min < min_) min_ = Own(chunk_min);
        if (!has_bounds_ || max_ < chunk_max) max_ = Own(chunk_max);
        has_bounds_ = true;
    }

    DynamicFilter Finish() override {
        DynamicFilter filter;
        filter.max_values = max_values_;
        filter.bloom_bits = bloom_bits_;
        if (has_bounds_) {
            filter.MergeBounds(MakeValue(min_, key_type_), MakeValue(max_, key_type_));
        }
        if (mode_ == Mode::VALUES) {
            filter.has_values = true;
            filter.values.reserve(set_.size());
            for (const auto &k : set_) {
                filter.values.push_back(MakeValue(k, key_type_));
            }
            set_.clear();
        } else if (mode_ == Mode::BLOOM) {
            filter.bloom = std::move(bloom_);
        }
        return filter;
    }

private:
    enum class Mode { BOUNDS, VALUES, BLOOM };

    // Keys that outlive their chunk: non-inlined strings are copied into the collector's heap.
    T Own(const T &v) {
        if constexpr (std::is_same_v<T, duckdb::string_t>) {
            return v.IsInlined() ? v : heap_.AddBlob(v);
        } else {
            return v;
        }
    }

    void InsertValue(const T &v) {
        if constexpr (std::is_same_v<T, duckdb::string_t>) {
            // Look up first so only new keys are copied.
            if (set_.find(v) == set_.end()) {
                set_.insert(Own(v));
            }
        } else {
            set_.insert(v);
        }
    }

    void ConvertToBloom() {
        if (bloom_bits_ > 0) {
            mode_ = Mode::BLOOM;
            bloom_ = BloomFilter(bloom_bits_);
            for (const auto &k : set_) {
                bloom_.Insert(duckdb::Hash<T>(k));
            }
        } else {
            mode_ = Mode::BOUNDS;
        }
        set_.clear();
    }

    LogicalType key_type_;
    uint32_t max_values_;
    uint64_t bloom_bits_;
    Mode mode_ = Mode::BOUNDS;

    // Backs the non-inlined strings in min_, max_ and set_.
    duckdb::StringHeap heap_;
    bool has_bounds_ = false;
    T min_ {};
    T max_ {};
    std::unordered_set<T, KeyHash> set_;
    BloomFilter bloom_;
};

template <class T>
std::unique_ptr<KeyCollector> MakeCollector(const DynamicFilterBuildTemplate &templ, bool exact_set) {
    return std::make_unique<TypedKeyCollector<T>>(ToLogicalType(templ.key_type), templ.max_values, templ.bloom_bits,
                                                  exact_set);
}

Result<std::unique_ptr<KeyCollector>> CreateKeyCollector(const DynamicFilterBuildTemplate &templ) {
    switch (ToLogicalType(templ.key_type).InternalType()) {
    case PhysicalType::BOOL: return MakeCollector<bool>(templ, true);
    case PhysicalType::INT8: return MakeCollector<int8_t>(templ, true);
    case PhysicalType::INT16: return MakeCollector<int16_t>(templ, true);
    case PhysicalType::INT32: return MakeCollector<int32_t>(templ, true);
    case PhysicalType::INT64: return MakeCollector<int64_t>(templ, true);
    case PhysicalType::UINT8: return MakeCollector<uint8_t>(templ, true);
    case PhysicalType::UINT16: return MakeCollector<uint16_t>(templ, true);
    case PhysicalType::UINT32: return MakeCollector<uint32_t>(templ, true);
    case PhysicalType::UINT64: return MakeCollector<uint64_t>(templ, true);
    case PhysicalType::INT128: return MakeCollector<duckdb::hugeint_t>(templ, true);
    case PhysicalType::UINT128: return MakeCollector<duckdb::uhugeint_t>(templ, true);
    // No exact set for floating point keys: NaN/-0.0 don't behave under set equality.
    case PhysicalType::FLOAT: return MakeCollector<float>(templ, false);
    case PhysicalType::DOUBLE: return MakeCollector<double>(templ, false);
    case PhysicalType::VARCHAR: return MakeCollector<duckdb::string_t>(templ, true);
    default:
        return Error("Dynamic filter build over an unsupported column type", ErrorKind::NotImplemented);
    }
}

} // namespace

DynamicFilterBuildOperator::DynamicFilterBuildOperator(const DynamicFilterBuildTemplate &templ,
        std::unique_ptr<KeyCollector> collector, duckdb::vector<LogicalType> output_types)
    : Operator(std::move(output_types)), column_(templ.column), key_type_(ToLogicalType(templ.key_type)),
      state_(std::move(collector)) {}

DynamicFilterBuildOperator::~DynamicFilterBuildOperator() = default;

Result<void> DynamicFilterBuildOperator::Push(std::unique_ptr<DataChunk> chunk) {
    PLUME_TRACE_OP(trace::Phase::PUSH);
    auto *collector = std::get_if<std::unique_ptr<KeyCollector>>(&state_);
    if (!collector) {
        return Error("Dynamic filter build: chunk pushed after the collection ended");
    }
    const idx_t count = chunk->size();
    if (count > 0) {
        auto &vec = chunk->data[column_];
        if (vec.GetType() == key_type_) {
            (*collector)->Add(vec, count);
        } else {
            Vector cast(key_type_, count);
            TRYV(TryCatch([&] { duckdb::VectorOperations::DefaultCast(vec, cast, count, /*strict=*/true); }));
            (*collector)->Add(cast, count);
        }
    }
    return next_->Push(std::move(chunk));
}

Result<void> DynamicFilterBuildOperator::Finish() {
    PLUME_TRACE_OP(trace::Phase::FINISH);
    EndCollection();
    return next_->Finish();
}

void DynamicFilterBuildOperator::EndCollection() {
    if (auto *collector = std::get_if<std::unique_ptr<KeyCollector>>(&state_)) {
        auto filter = (*collector)->Finish();
        state_ = std::move(filter);
    }
}

const DynamicFilter &DynamicFilterBuildOperator::Filter() {
    EndCollection();
    return std::get<DynamicFilter>(state_);
}

//===----------------------------------------------------------------------===//
// Build
//===----------------------------------------------------------------------===//

Result<std::unique_ptr<Operator>> BuildDynamicFilterBuildTemplate(std::shared_ptr<DynamicFilterBuildTemplate> templ,
        duckdb::vector<LogicalType> col_types) {
    if (templ->column >= col_types.size()) {
        return Error("Filter build column index out of range", ErrorKind::OutOfRange);
    }
    TRY(auto collector, CreateKeyCollector(*templ));
    return std::unique_ptr<Operator>(
        std::make_unique<DynamicFilterBuildOperator>(*templ, std::move(collector), std::move(col_types)));
}

} // namespace plume::exec
