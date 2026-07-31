// Minimal stdlib-only test scaffolding (TASK.md: as few external deps as
// possible). No GTest/Catch2 — just CHECK macros and a tiny runner.
#pragma once

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

namespace plume_test {

struct Case {
    std::string name;
    std::function<void()> fn;
};

inline std::vector<Case> &Registry() {
    static std::vector<Case> r;
    return r;
}

struct Registrar {
    Registrar(const std::string &name, std::function<void()> fn) { Registry().push_back({name, std::move(fn)}); }
};

struct Failure {
    std::string msg;
};

inline int RunAll() {
    int failed = 0;
    for (auto &c : Registry()) {
        try {
            c.fn();
            printf("[ PASS ] %s\n", c.name.c_str());
        } catch (const Failure &f) {
            printf("[ FAIL ] %s\n         %s\n", c.name.c_str(), f.msg.c_str());
            failed++;
        } catch (const std::exception &e) {
            printf("[ ERR  ] %s\n         unexpected exception: %s\n", c.name.c_str(), e.what());
            failed++;
        }
    }
    printf("\n%zu test(s), %d failed\n", Registry().size(), failed);
    return failed == 0 ? 0 : 1;
}

} // namespace plume_test

#define PLUME_CONCAT_(a, b) a##b
#define PLUME_CONCAT(a, b) PLUME_CONCAT_(a, b)
#define TEST_CASE(name)                                                                                                \
    static void PLUME_CONCAT(plume_test_fn_, __LINE__)();                                                              \
    static ::plume_test::Registrar PLUME_CONCAT(plume_test_reg_, __LINE__)(name,                                       \
                                                                           PLUME_CONCAT(plume_test_fn_, __LINE__));    \
    static void PLUME_CONCAT(plume_test_fn_, __LINE__)()

#define CHECK(cond)                                                                                                    \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            throw ::plume_test::Failure {std::string(__FILE__) + ":" + std::to_string(__LINE__) + " CHECK(" #cond     \
                                         ")"};                                                                         \
        }                                                                                                              \
    } while (0)

#define CHECK_MSG(cond, msg)                                                                                           \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            throw ::plume_test::Failure {std::string(__FILE__) + ":" + std::to_string(__LINE__) + " " + (msg)};       \
        }                                                                                                              \
    } while (0)
