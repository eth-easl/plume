#include "test_util.hpp"

#include "plume/execution/operators/join.hpp"
#include "plume/expression/expression_builder.hpp"
#include "plume/memory/allocator.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <algorithm>
#include <set>
#include <string>
#include <vector>

using namespace plume;
using namespace plume::exec;
using duckdb::DataChunk;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;

namespace {

using Row = std::vector<Value>;

// Collects every chunk pushed into it as rendered rows.
class Capture : public Operator {
public:
    explicit Capture(duckdb::vector<LogicalType> types) : Operator(std::move(types)) {}
    Result<void> Push(std::unique_ptr<DataChunk> chunk) override {
        for (idx_t r = 0; r < chunk->size(); r++) {
            std::string s;
            for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
                s += chunk->GetValue(c, r).ToString() + "|";
            }
            rows.insert(s);
        }
        chunks.push_back(std::move(chunk)); // kept: outputs must stay valid after the join is gone
        return Ok();
    }
    Result<void> Finish() override { return Ok(); }
    std::multiset<std::string> rows;
    std::vector<std::unique_ptr<DataChunk>> chunks;
};

std::unique_ptr<DataChunk> MakeChunk(memory::Allocator &alloc, const duckdb::vector<LogicalType> &types,
                                     const std::vector<Row> &rows, idx_t begin, idx_t end) {
    auto chunk = std::make_unique<DataChunk>();
    chunk->Initialize(alloc.Get(), types, std::max<idx_t>(end - begin, 1));
    for (idx_t r = begin; r < end; r++) {
        for (idx_t c = 0; c < types.size(); c++) {
            chunk->data[c].SetValue(r - begin, rows[r][c]);
        }
    }
    chunk->SetChildCardinality(end - begin);
    return chunk;
}

std::string Render(const Row &row) {
    std::string s;
    for (auto &v : row) {
        s += v.ToString() + "|";
    }
    return s;
}

Row Concat(const Row &a, const Row &b) {
    Row out = a;
    out.insert(out.end(), b.begin(), b.end());
    return out;
}

Row Nulls(const duckdb::vector<LogicalType> &types) {
    Row out;
    for (auto &t : types) {
        out.emplace_back(t);
    }
    return out;
}

// Nested-loop reference for every kind except SINGLE (whose partner is arbitrary).
std::multiset<std::string> Reference(JoinKind kind, const std::vector<Row> &probe, const std::vector<Row> &build,
                                     const duckdb::vector<LogicalType> &ltypes,
                                     const duckdb::vector<LogicalType> &rtypes, bool residual) {
    auto matches = [&](const Row &l, const Row &r) {
        if (l[0].IsNull() || r[0].IsNull() || l[0] != r[0]) {
            return false;
        }
        return !residual || r[2].GetValue<int64_t>() > l[1].GetValue<int64_t>();
    };
    bool build_null = false;
    for (auto &r : build) {
        build_null = build_null || r[0].IsNull();
    }
    std::multiset<std::string> out;
    std::vector<bool> build_matched(build.size(), false);
    for (auto &l : probe) {
        bool any = false;
        for (idx_t b = 0; b < build.size(); b++) {
            if (!matches(l, build[b])) {
                continue;
            }
            any = true;
            build_matched[b] = true;
            if (kind == JoinKind::INNER || kind == JoinKind::LEFT || kind == JoinKind::RIGHT ||
                kind == JoinKind::OUTER) {
                out.insert(Render(Concat(l, build[b])));
            }
        }
        switch (kind) {
        case JoinKind::LEFT:
        case JoinKind::OUTER:
            if (!any) {
                out.insert(Render(Concat(l, Nulls(rtypes))));
            }
            break;
        case JoinKind::SEMI:
            if (any) {
                out.insert(Render(l));
            }
            break;
        case JoinKind::ANTI:
            if (!any) {
                out.insert(Render(l));
            }
            break;
        case JoinKind::MARK: {
            Value marker = any ? Value::BOOLEAN(true)
                         : (!build.empty() && (build_null || l[0].IsNull())) ? Value(LogicalType::BOOLEAN)
                                                                             : Value::BOOLEAN(false);
            out.insert(Render(Concat(l, {marker})));
            break;
        }
        default:
            break;
        }
    }
    for (idx_t b = 0; b < build.size(); b++) {
        const bool m = build_matched[b];
        if ((kind == JoinKind::RIGHT || kind == JoinKind::OUTER) && !m) {
            out.insert(Render(Concat(Nulls(ltypes), build[b])));
        } else if ((kind == JoinKind::RIGHT_SEMI && m) || (kind == JoinKind::RIGHT_ANTI && !m)) {
            out.insert(Render(build[b]));
        }
    }
    return out;
}

std::multiset<std::string> RunJoin(JoinKind kind, const std::vector<Row> &probe, const std::vector<Row> &build,
                                   const duckdb::vector<LogicalType> &ltypes,
                                   const duckdb::vector<LogicalType> &rtypes, bool residual,
                                   const std::vector<idx_t> &build_chunk_sizes, memory::Allocator &alloc,
                                   std::vector<std::unique_ptr<DataChunk>> &keep, idx_t &capture_rows) {
    duckdb::unique_ptr<duckdb::Expression> pred;
    if (residual) {
        // build.v (natural index 3 + 2) > probe.w (index 1)
        auto node = expr::ExprNode::Comparison(duckdb::ExpressionType::COMPARE_GREATERTHAN,
                                               expr::ExprNode::Reference(5, ColumnType {TypeId::INT64}),
                                               expr::ExprNode::Reference(1, ColumnType {TypeId::INT64}));
        pred = expr::BuildExpression(node).unwrap();
    }
    auto join = std::make_unique<JoinOperator>(std::vector<uint32_t> {0}, std::vector<uint32_t> {0}, ltypes, rtypes,
                                               alloc, kind, std::move(pred));
    Capture capture(join->OutputTypes());
    join->SetNext(&capture);
    idx_t begin = 0;
    for (idx_t size : build_chunk_sizes) {
        join->PushBuild(MakeChunk(alloc, rtypes, build, begin, begin + size)).unwrap();
        begin += size;
    }
    join->FinishBuild();
    for (idx_t b = 0; b < probe.size(); b += STANDARD_VECTOR_SIZE) {
        join->Push(MakeChunk(alloc, ltypes, probe, b, std::min<idx_t>(probe.size(), b + STANDARD_VECTOR_SIZE)))
            .unwrap();
    }
    join->Finish().unwrap();
    join.reset(); // emitted chunks must not depend on the operator
    // Re-render after the join is gone (catches dangling string references).
    std::multiset<std::string> rows;
    for (auto &chunk : capture.chunks) {
        for (idx_t r = 0; r < chunk->size(); r++) {
            std::string s;
            for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
                s += chunk->GetValue(c, r).ToString() + "|";
            }
            rows.insert(s);
        }
    }
    capture_rows = capture.rows.size();
    for (auto &chunk : capture.chunks) {
        keep.push_back(std::move(chunk));
    }
    return rows;
}

} // namespace

TEST_CASE("join operator: every kind matches a nested-loop reference (single and many build chunks)") {
    memory::Allocator alloc;
    const duckdb::vector<LogicalType> ltypes = {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::VARCHAR};
    const duckdb::vector<LogicalType> rtypes = {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::BIGINT};
    // Build: key 0 has a chain longer than a vector, NULL keys, long strings.
    std::vector<Row> build;
    for (int64_t i = 0; i < 5000; i++) {
        Value k = i < 2500 ? Value::INTEGER(0) : (i % 17 == 0 ? Value(LogicalType::INTEGER)
                                                              : Value::INTEGER(static_cast<int32_t>(i % 300)));
        build.push_back({k, Value("build string number " + std::to_string(i) + std::string(i % 20, 'x')),
                         Value::BIGINT(i)});
    }
    std::vector<Row> probe;
    for (int64_t i = 0; i < 3000; i++) {
        Value k = i % 13 == 0 ? Value(LogicalType::INTEGER)
                              : Value::INTEGER(static_cast<int32_t>(i < 4 ? 0 : i % 400));
        probe.push_back({k, Value::BIGINT(i * 2), Value("probe " + std::to_string(i))});
    }
    const std::vector<std::vector<idx_t>> layouts = {{5000}, {700, 2048, 1, 2251}};
    std::vector<std::unique_ptr<DataChunk>> keep;
    for (auto kind : {JoinKind::INNER, JoinKind::LEFT, JoinKind::RIGHT, JoinKind::OUTER, JoinKind::SEMI,
                      JoinKind::ANTI, JoinKind::MARK, JoinKind::RIGHT_SEMI, JoinKind::RIGHT_ANTI}) {
        for (bool residual : {false, true}) {
            if (kind == JoinKind::MARK && residual) {
                continue; // MARK with a residual has no NULL-aware reference here
            }
            const auto expected = Reference(kind, probe, build, ltypes, rtypes, residual);
            for (const auto &layout : layouts) {
                idx_t capture_rows = 0;
                const auto got = RunJoin(kind, probe, build, ltypes, rtypes, residual, layout, alloc, keep,
                                         capture_rows);
                if (got != expected) {
                    throw std::runtime_error("join kind " + std::to_string(static_cast<int>(kind)) +
                                             (residual ? " (residual)" : "") + ", " +
                                             std::to_string(layout.size()) + " build chunks: got " +
                                             std::to_string(got.size()) + " rows, expected " +
                                             std::to_string(expected.size()));
                }
                CHECK(capture_rows == expected.size());
            }
        }
    }
}

TEST_CASE("join operator: SINGLE emits each probe row once") {
    memory::Allocator alloc;
    const duckdb::vector<LogicalType> ltypes = {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::VARCHAR};
    const duckdb::vector<LogicalType> rtypes = {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::BIGINT};
    std::vector<Row> build, probe;
    for (int64_t i = 0; i < 3000; i++) {
        build.push_back({Value::INTEGER(static_cast<int32_t>(i % 10)), Value("b" + std::to_string(i)), Value::BIGINT(i)});
    }
    for (int64_t i = 0; i < 100; i++) {
        probe.push_back({Value::INTEGER(static_cast<int32_t>(i % 20)), Value::BIGINT(i), Value("p")});
    }
    std::vector<std::unique_ptr<DataChunk>> keep;
    idx_t capture_rows = 0;
    auto got = RunJoin(JoinKind::SINGLE, probe, build, ltypes, rtypes, false, {1000, 2000}, alloc, keep,
                       capture_rows);
    CHECK(got.size() == probe.size());
}

int main() { return plume_test::RunAll(); }
