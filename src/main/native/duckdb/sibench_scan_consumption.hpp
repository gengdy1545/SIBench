#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

struct SibenchIndexBatch {
	idx_t access = 0;
	idx_t batch = 0;
	idx_t rows = 0;
};

// One source fetch or downstream call; nested executors restore the enclosing capture scope.
struct SibenchBatchScope {
	inline static thread_local SibenchBatchScope *current = nullptr;
	SibenchIndexBatch &batch;
	bool direct;
	SibenchBatchScope *previous;

	SibenchBatchScope(SibenchIndexBatch &batch_p, bool direct_p)
	    : batch(batch_p), direct(direct_p), previous(current) {
		current = this;
	}
	~SibenchBatchScope() {
		current = previous;
	}
};

class SibenchConsumptionLogType : public LogType {
public:
	static constexpr const char *NAME = "SibenchConsumption";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;
	SibenchConsumptionLogType() : LogType(NAME, LEVEL, GetLogType()) {
	}
	static LogicalType GetLogType() {
		return LogicalType::STRUCT({{"access_id", LogicalType::UBIGINT}, {"batch_id", LogicalType::UBIGINT},
		                            {"stage", LogicalType::VARCHAR}, {"operator_name", LogicalType::VARCHAR},
		                            {"direct_from_scan", LogicalType::BOOLEAN}, {"input_rows", LogicalType::UBIGINT},
		                            {"consumed_prefix", LogicalType::UBIGINT}});
	}
	static string ConstructLogMessage(const SibenchIndexBatch &batch, const char *stage, const string &op,
	                                  bool direct, idx_t input_rows, idx_t consumed) {
		return Value::STRUCT({{"access_id", Value::UBIGINT(batch.access)}, {"batch_id", Value::UBIGINT(batch.batch)},
		                      {"stage", Value(stage)}, {"operator_name", Value(op)},
		                      {"direct_from_scan", Value::BOOLEAN(direct)}, {"input_rows", Value::UBIGINT(input_rows)},
		                      {"consumed_prefix", Value::UBIGINT(consumed)}}).ToString();
	}
};

inline void SibenchLimitConsumption(ClientContext &context, const char *op, idx_t input_rows, idx_t consumed) {
	auto scope = SibenchBatchScope::current;
	if (scope && scope->batch.access) {
		DUCKDB_LOG(context, SibenchConsumptionLogType, scope->batch, "limit", op, scope->direct, input_rows, consumed);
	}
}

} // namespace duckdb
