#pragma once

#include "plume/common/result.hpp"
#include "plume/execution/operator.hpp"

namespace plume::exec {

struct LimitTemplate : OperatorTemplate {
    bool has_limit = false;
    uint64_t limit = 0;
    uint64_t offset = 0;

    LimitTemplate() : OperatorTemplate(OpType::LIMIT) {}
    LimitTemplate(uint64_t limit, uint64_t offset = 0) 
        : OperatorTemplate(OpType::LIMIT), limit(limit), offset(offset) {}

    bool Equals(const OperatorTemplate &other) const override;
    void Serialize(duckdb::Serializer &s) const override;
};

class LimitOperator : public Operator {
public:
    LimitOperator(bool has_limit, uint64_t limit, uint64_t offset, duckdb::vector<duckdb::LogicalType> output_types);
    
    Result<void> Push(std::unique_ptr<duckdb::DataChunk> chunk) override;
    Result<void> Finish() override;

private:
    bool has_limit_;
    uint64_t limit_;
    uint64_t offset_;

    uint64_t skipped_ = 0;
    uint64_t emitted_ = 0;
};

Result<std::unique_ptr<Operator>> BuildLimitTemplate(std::shared_ptr<LimitTemplate> templ,
                                                     duckdb::vector<duckdb::LogicalType> col_types);

} // namespace plume::exec
