#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_type.hpp"

namespace duckdb {

class SibenchTransactionLogType : public LogType {
public:
	static constexpr const char *NAME = "SibenchTransaction";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	SibenchTransactionLogType() : LogType(NAME, LEVEL, GetLogType()) {
	}

	static LogicalType GetLogType() {
		return LogicalType::STRUCT({{"database_name", LogicalType::VARCHAR},
		                            {"event", LogicalType::VARCHAR},
		                            {"engine_transaction", LogicalType::UBIGINT},
		                            {"snapshot_start", LogicalType::UBIGINT},
		                            {"commit_stamp", LogicalType::UBIGINT},
		                            {"resolution_order", LogicalType::UBIGINT}});
	}

	static string ConstructLogMessage(const string &database, const char *event, transaction_t transaction,
	                                  transaction_t snapshot, transaction_t commit, uint64_t resolution_order) {
		return Value::STRUCT({{"database_name", Value(database)},
		                      {"event", Value(event)},
		                      {"engine_transaction", Value::UBIGINT(transaction)},
		                      {"snapshot_start", Value::UBIGINT(snapshot)},
		                      {"commit_stamp", Value::UBIGINT(commit)},
		                      {"resolution_order", Value::UBIGINT(resolution_order)}})
		    .ToString();
	}
};

} // namespace duckdb
