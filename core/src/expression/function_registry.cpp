#include "plume/expression/function_registry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/aggregate/contextless_aggregate.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/function/scalar/operator_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/distributive_functions.hpp"
#include "core_functions/scalar/date_functions.hpp"
#include "core_functions/scalar/math_functions.hpp"

#include <functional>
#include <unordered_map>

namespace plume::expr {

using duckdb::LogicalType;
using duckdb::ScalarFunction;
using duckdb::ScalarFunctionSet;

//===----------------------------------------------------------------------===//
// Scalar registry
//===----------------------------------------------------------------------===//

namespace {

using SetFactory = std::function<ScalarFunctionSet()>;

SetFactory Single(ScalarFunction (*get)()) {
    return [get] {
        auto f = get();
        ScalarFunctionSet set(f.name);
        set.AddFunction(std::move(f));
        return set;
    };
}

// Maps function name to DuckDB ScalarFunctionSet.
// Extend here as the supported set grows.
const std::unordered_map<std::string, SetFactory> &ScalarRegistry() {
    static const std::unordered_map<std::string, SetFactory> registry = {
        {"+", duckdb::OperatorAddFun::GetFunctions},
        {"-", duckdb::OperatorSubtractFun::GetFunctions},
        {"*", duckdb::OperatorMultiplyFun::GetFunctions},
        {"/", duckdb::OperatorFloatDivideFun::GetFunctions},
        {"//", duckdb::OperatorIntegerDivideFun::GetFunctions},
        {"%", duckdb::OperatorModuloFun::GetFunctions},
        // common math (core_functions); concrete kernels, no bind callback.
        {"abs", duckdb::AbsOperatorFun::GetFunctions},
        {"floor", duckdb::FloorFun::GetFunctions},
        {"ceil", duckdb::CeilFun::GetFunctions},
        {"ceiling", duckdb::CeilFun::GetFunctions},
        {"round", duckdb::RoundFun::GetFunctions},
        {"trunc", duckdb::TruncFun::GetFunctions},
        {"sign", duckdb::SignFun::GetFunctions},
        {"log", duckdb::LogFun::GetFunctions},
        {"sqrt", Single(duckdb::SqrtFun::GetFunction)},
        {"cbrt", Single(duckdb::CbrtFun::GetFunction)},
        {"exp", Single(duckdb::ExpFun::GetFunction)},
        {"ln", Single(duckdb::LnFun::GetFunction)},
        {"log2", Single(duckdb::Log2Fun::GetFunction)},
        {"log10", Single(duckdb::Log10Fun::GetFunction)},
        {"pow", Single(duckdb::PowOperatorFun::GetFunction)},
        {"power", Single(duckdb::PowOperatorFun::GetFunction)},
        {"**", Single(duckdb::PowOperatorFun::GetFunction)},
        {"^", Single(duckdb::PowOperatorFun::GetFunction)},
        // string functions (used by LIKE rewrite rules)
        {"prefix", Single(duckdb::PrefixFun::GetFunction)},
        {"suffix", Single(duckdb::SuffixFun::GetFunction)},
        {"contains", duckdb::ContainsFun::GetFunctions},
        {"~~", Single(duckdb::LikeFun::GetFunction)},
        {"!~~", Single(duckdb::NotLikeFun::GetFunction)},
        {"substring", duckdb::SubstringFun::GetFunctions},
        {"substr", duckdb::SubstringFun::GetFunctions},
        // date part extraction (`extract(year/month/day FROM ...)` binds to date_part(), which 
        // the optimizer simplifies to these direct calls).
        {"year", duckdb::YearFun::GetFunctions},
        {"month", duckdb::MonthFun::GetFunctions},
        {"day", duckdb::DayFun::GetFunctions},
        // The runtime "more than one row" guard DuckDB wraps scalar-subquery results in (a CASE 
        // whose THEN branch calls this). Is never taken for well-formed data, but still resolved 
        // eagerly when the pipeline builds.
        {"error", Single(duckdb::ErrorFun::GetFunction)},
    };
    return registry;
}

using duckdb::PhysicalType;
using duckdb::scalar_function_t;

// NOTE: DuckDB picks the unchecked kernel when its width analysis proves the result cannot overflow, 
//       but Plume always uses the overflow-check operator.
template <class OP>
scalar_function_t SelectDecimalKernel(PhysicalType ptype) {
    switch (ptype) {
    case PhysicalType::INT16:
        return &duckdb::ScalarFunction::BinaryFunction<int16_t, int16_t, int16_t, OP>;
    case PhysicalType::INT32:
        return &duckdb::ScalarFunction::BinaryFunction<int32_t, int32_t, int32_t, OP>;
    case PhysicalType::INT64:
        return &duckdb::ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, OP>;
    case PhysicalType::INT128:
        return &duckdb::ScalarFunction::BinaryFunction<duckdb::hugeint_t, duckdb::hugeint_t, duckdb::hugeint_t, OP>;
    default:
        return nullptr;
    }
}

// Binds a binary kernel instance-free.
// NOTE: Some DuckDB scalar overloads ship with a NULL kernel plus a bind callback that selects the
//       physical kernel at bind time. However, that bind needs a ClientContext which Plume's
//       lightweight execution framework does not have. Instead we reconstruct the kernel from 
//       public templates using the resolved physical type.
scalar_function_t SelectBinaryKernel(const std::string &name, PhysicalType ptype) {
    if (name == "/") {
        if (ptype == PhysicalType::FLOAT) {
            return &duckdb::ScalarFunction::BinaryFunction<float, float, float, duckdb::DivideOperator>;
        }
        if (ptype == PhysicalType::DOUBLE) {
            return &duckdb::ScalarFunction::BinaryFunction<double, double, double, duckdb::DivideOperator>;
        }
    } else if (name == "%") {
        if (ptype == PhysicalType::FLOAT) {
            return &duckdb::ScalarFunction::BinaryFunction<float, float, float, duckdb::ModuloOperator>;
        }
        if (ptype == PhysicalType::DOUBLE) {
            return &duckdb::ScalarFunction::BinaryFunction<double, double, double, duckdb::ModuloOperator>;
        }
    } else if (name == "+") {
        return SelectDecimalKernel<duckdb::DecimalAddOverflowCheck>(ptype);
    } else if (name == "-") {
        return SelectDecimalKernel<duckdb::DecimalSubtractOverflowCheck>(ptype);
    } else if (name == "*") {
        return SelectDecimalKernel<duckdb::DecimalMultiplyOverflowCheck>(ptype);
    }
    return nullptr;
}

Result<void> EnsureKernel(const std::string &name, ScalarFunction &func, const LogicalType &return_type) {
    if (func.HasFunctionCallback()) {
        return Ok();
    }
    auto kernel = SelectBinaryKernel(name, return_type.InternalType());
    if (!kernel) {
        return Error("Scalar function '" + name + 
                     "' needs a bind callback that requires an instance; not supported without an instance",
                     ErrorKind::NotImplemented);
    }
    func.SetFunctionCallback(kernel);
    return Ok();
}

std::vector<LogicalType> PositionalArguments(const duckdb::SimpleFunction &func) {
    std::vector<LogicalType> out;
    for (auto &param : func.GetSignature().GetParameters()) {
        if (param.GetKind() == duckdb::FunctionParameterKind::POSITIONAL_ONLY ||
            param.GetKind() == duckdb::FunctionParameterKind::STANDARD) {
            out.push_back(param.GetType());
        }
    }
    return out;
}

bool ArgumentsMatch(const std::vector<LogicalType> &have, const std::vector<LogicalType> &want) {
    if (have.size() != want.size()) {
        return false;
    }
    for (size_t i = 0; i < have.size(); i++) {
        // ANY in the candidate signature is a wildcard (generic functions).
        if (have[i].id() == duckdb::LogicalTypeId::ANY) {
            continue;
        }
        if (have[i].id() != want[i].id()) {
            return false;
        }
    }
    return true;
}

} // namespace

bool HasScalarFunction(const std::string &name) {
    return ScalarRegistry().count(name) != 0;
}

Result<ScalarFunction> ResolveScalarFunction(const std::string &name, const std::vector<LogicalType> &arguments,
                                             const LogicalType &return_type) {
    auto it = ScalarRegistry().find(name);
    if (it == ScalarRegistry().end()) {
        return Error("Scalar function '" + name + "' is not registered", ErrorKind::NotImplemented);
    }
    ScalarFunctionSet set = it->second();
    for (auto &candidate : set.functions) {
        if (ArgumentsMatch(PositionalArguments(*candidate), arguments)) {
            ScalarFunction func = *candidate;
            TRYV(EnsureKernel(name, func, return_type));
            return func;
        }
    }
    return Error("No overload of '" + name + "' matches the given argument types", ErrorKind::NotImplemented);
}

//===----------------------------------------------------------------------===//
// Aggregate registry
//===----------------------------------------------------------------------===//

namespace {

using duckdb::AggregateFunction;
using duckdb::AggregateFunctionSet;
using duckdb::BoundAggregateFunction;

ResolvedAggregate Unbound(const AggregateFunction &function) {
    return ResolvedAggregate{BoundAggregateFunction(function), nullptr};
}

using ContextlessBind = Result<ResolvedAggregate> (*)(const std::string &name, const LogicalType &arg);

Result<ResolvedAggregate> BindFirst(const std::string &, const LogicalType &arg) {
    return Unbound(duckdb::FirstFunctionGetter::GetFunction(arg));
}

Result<ResolvedAggregate> BindMinMax(const std::string &name, const LogicalType &arg) {
    if (arg.id() == duckdb::LogicalTypeId::VARCHAR && !duckdb::StringType::GetCollation(arg).empty()) {
        return Error("Aggregate '" + name + "' over a collated VARCHAR is not supported", ErrorKind::NotImplemented);
    }
    return ResolvedAggregate{duckdb::GetContextlessMinMax(name, arg), nullptr};
}

Result<ResolvedAggregate> BindDecimalSum(const std::string &, const LogicalType &arg) {
    return ResolvedAggregate{duckdb::GetContextlessDecimalSum(arg), nullptr};
}

Result<ResolvedAggregate> BindSumNoOverflow(const std::string &, const LogicalType &arg) {
    return ResolvedAggregate{duckdb::GetContextlessSumNoOverflow(arg), nullptr};
}

Result<ResolvedAggregate> BindDecimalAvg(const std::string &, const LogicalType &arg) {
    duckdb::unique_ptr<duckdb::FunctionData> bind_data;
    auto function = duckdb::GetContextlessDecimalAvg(arg, bind_data);
    return ResolvedAggregate{std::move(function), std::move(bind_data)};
}

AggregateFunctionSet CountStarSet() {
    return AggregateFunctionSet(duckdb::CountStarFun::GetFunction());
}

struct AggregateEntry {
    AggregateFunctionSet (*overloads)(); // the catalog's overloads of the function
    ContextlessBind bind;                // for the overloads with a bind callback (null: none)
};

const std::unordered_map<std::string, AggregateEntry> &AggregateRegistry() {
    static const std::unordered_map<std::string, AggregateEntry> registry = {
        {"count", {duckdb::CountFun::GetFunctions, nullptr}},
        {"count_star", {CountStarSet, nullptr}},
        // NOTE: `first` shows up in the scalar-subquery pattern DuckDB emits for
        //       `HAVING x > (SELECT scalar)` / `WHERE x < (SELECT scalar)`.
        {"first", {duckdb::FirstFun::GetFunctions, BindFirst}},
        {"min", {duckdb::MinFun::GetFunctions, BindMinMax}},
        {"max", {duckdb::MaxFun::GetFunctions, BindMinMax}},
        {"sum", {duckdb::SumFun::GetFunctions, BindDecimalSum}},
        {"sum_no_overflow", {duckdb::SumNoOverflowFun::GetFunctions, BindSumNoOverflow}},
        {"avg", {duckdb::AvgFun::GetFunctions, BindDecimalAvg}},
    };
    return registry;
}

} // namespace

Result<ResolvedAggregate> ResolveAggregateFunction(const std::string &name,
                                                   const std::vector<LogicalType> &arguments) {
    auto it = AggregateRegistry().find(name);
    if (it == AggregateRegistry().end()) {
        return Error("Aggregate function '" + name + "' is not registered", ErrorKind::NotImplemented);
    }
    const AggregateEntry &entry = it->second;
    AggregateFunctionSet set = entry.overloads();
    for (auto &candidate : set.functions) {
        if (!ArgumentsMatch(PositionalArguments(*candidate), arguments)) {
            continue;
        }
        if (!candidate->GetCallbacks().HasBindCallback()) {
            return Unbound(*candidate); // a concrete kernel
        }
        if (!entry.bind || arguments.size() != 1) {
            return Error("Aggregate '" + name + "' needs a bind step that requires an instance",
                         ErrorKind::NotImplemented);
        }
        TRY(auto bound, TryCatch([&] { return entry.bind(name, arguments[0]); }));
        return bound;
    }
    return Error("No overload of aggregate '" + name + "' matches the given argument types",
                 ErrorKind::NotImplemented);
}

} // namespace plume::expr
