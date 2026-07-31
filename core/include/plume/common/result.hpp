#pragma once

#include <exception>
#include <functional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace plume {

// A C++17 polyfill for std::source_location (C++20).
// The `current()` default arguments are evaluated at the *call site* via compiler builtins, 
// so `SourceLocation::current()` records where it was named.
struct SourceLocation {
    const char *file = "";
    const char *function = "";
    int line = 0;

#if defined(__GNUC__) || defined(__clang__)
    static SourceLocation current(const char *file = __builtin_FILE(), const char *function = __builtin_FUNCTION(),
                                  int line = __builtin_LINE()) {
        return SourceLocation{file, function, line};
    }
#else
    static SourceLocation current() {
        return SourceLocation{};
    }
#endif
};

enum class ErrorKind { Generic, InvalidInput, NotImplemented, OutOfRange, RuntimeError };

class Error {
  public:
    Error(std::string msg, SourceLocation loc = SourceLocation::current())
        : message_(std::move(msg)), kind_(ErrorKind::Generic) {
        trace_.push_back(loc);
    }
    Error(std::string msg, ErrorKind kind, SourceLocation loc = SourceLocation::current())
        : message_(std::move(msg)), kind_(kind) {
        trace_.push_back(loc);
    }

    ErrorKind kind() const {
        return kind_;
    }

    const std::string &message() const {
        return message_;
    }

    const std::vector<SourceLocation> &trace() const {
        return trace_;
    }

    Error &add_frame(SourceLocation loc) {
        trace_.push_back(loc);
        return *this;
    }

    std::string to_string() const {
        std::string out = message_;
        for (auto it = trace_.rbegin(); it != trace_.rend(); ++it) {
            out += "\n  at ";
            out += (it->function && *it->function) ? it->function : "?";
            out += " (";
            out += (it->file && *it->file) ? it->file : "?";
            out += ":";
            out += std::to_string(it->line);
            out += ")";
        }
        return out;
    }

  private:
    std::string message_;
    ErrorKind kind_;
    std::vector<SourceLocation> trace_;
};

class ResultError : public std::runtime_error {
  public:
    explicit ResultError(Error err) : std::runtime_error(err.to_string()), error_(std::move(err)) {}
    const Error &error() const {
        return error_;
    }

  private:
    Error error_;
};

template <typename T>
class Result {
  public:
    Result(const T &val) : data_(val) {}
    Result(T &&val) : data_(std::move(val)) {}

    Result(const Error &err) : data_(err) {}
    Result(Error &&err) : data_(std::move(err)) {}

    template <typename U, typename std::enable_if_t<std::is_convertible_v<U, T> &&
        !std::is_same_v<std::decay_t<U>, Error> && !std::is_same_v<std::decay_t<U>, Result<T>>,
        int> = 0>
    Result(U &&val) : data_(std::in_place_type<T>, std::forward<U>(val)) {}

    // Converts from an l-value Result<U> where U is convertible to T
    template <typename U, typename std::enable_if_t<std::is_convertible_v<U, T> && !std::is_same_v<U, T>, int> = 0>
    Result(const Result<U> &other)
        : data_(other.is_ok() ? std::variant<T, Error>(other.unwrap()) : std::variant<T, Error>(other.error())) {}

    // Converts from an r-value Result<U> where U is convertible to T
    template <typename U, typename std::enable_if_t<std::is_convertible_v<U, T> && !std::is_same_v<U, T>, int> = 0>
    Result(Result<U> &&other)
        : data_(other.is_ok() ? std::variant<T, Error>(std::move(other).unwrap())
                              : std::variant<T, Error>(std::move(other).take_error())) {}

    bool is_ok() const {
        return std::holds_alternative<T>(data_);
    }
    bool is_error() const {
        return std::holds_alternative<Error>(data_);
    }

    // Returns (copy) the value if present, otherwise throws.
    // Used when: auto x = res.unwrap(); (use std::move(res).unwrap() to move).
    const T &unwrap() const & {
        ThrowIfError();
        return std::get<T>(data_);
    }

    // Returns (move) the value if present, otherwise throws.
    // Used when: auto x = std::move(res).unwrap();
    T &&unwrap() && {
        ThrowIfError();
        return std::move(std::get<T>(data_));
    }

    const Error &error() const & {
        return std::get<Error>(data_);
    }
    Error &&take_error() && {
        return std::move(std::get<Error>(data_));
    }

    std::string error_msg() const {
        return is_ok() ? std::string() : std::get<Error>(data_).message();
    }

    T value_or(const T &default_val) const {
        return is_ok() ? std::get<T>(data_) : default_val;
    }

  private:
    void ThrowIfError() const {
        if (is_error()) {
            throw ResultError(std::get<Error>(data_));
        }
    }

    std::variant<T, Error> data_;
};

// Reference specialization (Result<T&>).
template <typename T>
class Result<T &> {
  public:
    Result(T &val) : data_(std::ref(val)) {}
    Result(const Error &err) : data_(err) {}
    Result(Error &&err) : data_(std::move(err)) {}

    bool is_ok() const {
        return std::holds_alternative<std::reference_wrapper<T>>(data_);
    }
    bool is_error() const {
        return std::holds_alternative<Error>(data_);
    }

    T &unwrap() const {
        if (is_error()) {
            throw ResultError(std::get<Error>(data_));
        }
        return std::get<std::reference_wrapper<T>>(data_).get();
    }

    const Error &error() const & {
        return std::get<Error>(data_);
    }
    Error take_error() const {
        return std::get<Error>(data_);
    }

    std::string error_msg() const {
        return is_ok() ? std::string() : std::get<Error>(data_).message();
    }

    T &value_or(T &default_val) const {
        return is_ok() ? unwrap() : default_val;
    }

  private:
    std::variant<std::reference_wrapper<T>, Error> data_;
};

// Empty result specialization (Result<void>).
template <>
class Result<void> {
  public:
    Result() : data_(std::monostate{}) {}
    Result(const Error &err) : data_(err) {}
    Result(Error &&err) : data_(std::move(err)) {}

    bool is_ok() const {
        return std::holds_alternative<std::monostate>(data_);
    }
    bool is_error() const {
        return std::holds_alternative<Error>(data_);
    }

    void unwrap() const {
        if (is_error()) {
            throw ResultError(std::get<Error>(data_));
        }
    }

    const Error &error() const & {
        return std::get<Error>(data_);
    }
    Error &&take_error() && {
        return std::move(std::get<Error>(data_));
    }

    std::string error_msg() const {
        return is_ok() ? std::string() : std::get<Error>(data_).message();
    }

  private:
    std::variant<std::monostate, Error> data_;
};

inline Result<void> Ok() {
    return Result<void>();
}

// Runs a lambda that may throw (e.g. a DuckDB kernel, which still throws internally) and convert
// any exception into an Error.
template <typename Fn>
auto TryCatch(Fn &&fn, SourceLocation loc = SourceLocation::current())
    -> Result<decltype(std::forward<Fn>(fn)())> {
    using R = decltype(std::forward<Fn>(fn)());
    try {
        if constexpr (std::is_void_v<R>) {
            std::forward<Fn>(fn)();
            return Ok();
        } else {
            return std::forward<Fn>(fn)();
        }
    } catch (const std::exception &e) {
        return Error(e.what(), loc);
    } catch (...) {
        return Error("unknown exception", loc);
    }
}

} // namespace plume

//===----------------------------------------------------------------------===//
// Propagation macros.
//
//   TRY(decl_or_lhs, expr) — evaluate `expr` (a Result<U>); on error, append
//       this site to the trace and return it; on success, bind the value.
//       The first argument may declare a new variable or name an existing one:
//           TRY(auto x, foo());   // declares x
//           TRY(y, foo());        // assigns existing y
//   TRYV(expr) — same, for Result<void> (nothing to bind).
//
// Together these give the one thing exceptions provided for free — a
// backtrace — without unwinding:
//
//     Result<int> parse(std::string_view s);
//
//     Result<Config> load(std::string_view path) {
//         TRY(auto raw, read_file(path));   // declares `raw`, returns on error
//         int n;
//         TRY(n, parse(raw));               // assigns existing `n`
//         TRYV(validate(n));                // void result: check-or-return
//         return Config{n};
//     }
//===----------------------------------------------------------------------===//
#define PLUME_RESULT_CONCAT_IMPL(x, y) x##y
#define PLUME_RESULT_CONCAT(x, y) PLUME_RESULT_CONCAT_IMPL(x, y)

#define PLUME_TRY_IMPL(tmp, decl, expr)                                                                   \
    auto &&tmp = (expr);                                                                                  \
    if (tmp.is_error()) {                                                                                 \
        return ::plume::Error(std::move(tmp).take_error().add_frame(::plume::SourceLocation::current())); \
    }                                                                                                     \
    decl = std::move(tmp).unwrap();

#define TRY(decl, expr) PLUME_TRY_IMPL(PLUME_RESULT_CONCAT(_plume_try_, __COUNTER__), decl, expr)

#define PLUME_TRYV_IMPL(tmp, expr)                                                                            \
    do {                                                                                                      \
        auto &&tmp = (expr);                                                                                  \
        if (tmp.is_error()) {                                                                                 \
            return ::plume::Error(std::move(tmp).take_error().add_frame(::plume::SourceLocation::current())); \
        }                                                                                                     \
    } while (0)

#define TRYV(expr) PLUME_TRYV_IMPL(PLUME_RESULT_CONCAT(_plume_tryv_, __COUNTER__), expr)
