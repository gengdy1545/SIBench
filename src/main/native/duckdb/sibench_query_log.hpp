#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_type.hpp"

namespace duckdb {

class SibenchQueryLogType : public LogType {
public:
	static constexpr const char *NAME = "SibenchQuery";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	SibenchQueryLogType() : LogType(NAME, LEVEL, GetLogType()) {
	}

	static LogicalType GetLogType() {
		return LogicalType::STRUCT({{"root_query", LogicalType::UBIGINT},
		                            {"event", LogicalType::VARCHAR},
		                            {"outcome", LogicalType::VARCHAR},
		                            {"submission_end", LogicalType::BOOLEAN},
		                            {"invalidates_transaction", LogicalType::BOOLEAN},
		                            {"sql_text", LogicalType::VARCHAR},
		                            {"error", LogicalType::VARCHAR}});
	}

	static string ConstructLogMessage(idx_t root_query, const char *event, const char *outcome,
	                                  bool submission_end, bool invalidates_transaction, const string &sql,
	                                  const string &error) {
		return Value::STRUCT({{"root_query", Value::UBIGINT(root_query)},
		                      {"event", Value(event)},
		                      {"outcome", Value(outcome)},
		                      {"submission_end", Value::BOOLEAN(submission_end)},
		                      {"invalidates_transaction", Value::BOOLEAN(invalidates_transaction)},
		                      {"sql_text", Value(sql)},
		                      {"error", Value(error)}})
		    .ToString();
	}
};

} // namespace duckdb
