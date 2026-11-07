#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_type.hpp"

namespace duckdb {

// Fetched batches are source facts, not proof that downstream operators consumed every row.
class SibenchIndexRowsLogType : public LogType {
public:
	static constexpr const char *NAME = "SibenchIndexRows";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	SibenchIndexRowsLogType() : LogType(NAME, LEVEL, GetLogType()) {
	}

	static LogicalType GetLogType() {
		return LogicalType::STRUCT({{"access_id", LogicalType::UBIGINT},
		                            {"batch_id", LogicalType::UBIGINT},
		                            {"phase", LogicalType::VARCHAR},
		                            {"candidate_offset", LogicalType::UBIGINT},
		                            {"candidates_read", LogicalType::UBIGINT},
		                            {"candidate_rows", LogicalType::LIST(LogicalType::BIGINT)},
		                            {"physical_rows", LogicalType::LIST(LogicalType::BIGINT)},
		                            {"primary_key_type", LogicalType::VARCHAR},
		                            {"primary_keys_sql", LogicalType::LIST(LogicalType::VARCHAR)},
		                            {"source_drained", LogicalType::BOOLEAN}});
	}

	static string ConstructLogMessage(idx_t access_id, idx_t batch_id, const char *phase, idx_t offset, idx_t candidates,
	                                  const vector<Value> &candidate_rows, const vector<Value> &row_ids, const string &primary_type,
	                                  const vector<Value> &primary_keys, bool drained) {
		return Value::STRUCT({{"access_id", Value::UBIGINT(access_id)},
		                      {"batch_id", Value::UBIGINT(batch_id)},
		                      {"phase", Value(phase)},
		                      {"candidate_offset", Value::UBIGINT(offset)},
		                      {"candidates_read", Value::UBIGINT(candidates)},
		                      {"candidate_rows", Value::LIST(LogicalType::BIGINT, candidate_rows)},
		                      {"physical_rows", Value::LIST(LogicalType::BIGINT, row_ids)},
		                      {"primary_key_type", Value(primary_type)},
		                      {"primary_keys_sql", Value::LIST(LogicalType::VARCHAR, primary_keys)},
		                      {"source_drained", Value::BOOLEAN(drained)}})
		    .ToString();
	}
};

} // namespace duckdb
