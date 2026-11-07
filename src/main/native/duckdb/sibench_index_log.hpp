#pragma once

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_type.hpp"

namespace duckdb {

struct IndexScanState;
Value SibenchArtPredicates(IndexScanState &state);

inline LogicalType SibenchPredicateType() {
	return LogicalType::STRUCT({{"comparison", LogicalType::VARCHAR}, {"value_type", LogicalType::VARCHAR},
	                            {"value_sql", LogicalType::VARCHAR}});
}

inline LogicalType SibenchProbeType() {
	return LogicalType::STRUCT({{"origin", LogicalType::VARCHAR},
	                            {"predicates", LogicalType::LIST(SibenchPredicateType())},
	                            {"completed", LogicalType::BOOLEAN},
	                            {"candidates_after", LogicalType::UBIGINT},
	                            {"probe_time_ns", LogicalType::UBIGINT}});
}


// Diagnostics for an executed ART attempt, not a Search result or a complete capture stream.
struct SibenchIndexAttempt {
	explicit SibenchIndexAttempt(bool enabled_p) : enabled(enabled_p) {
	}
	bool enabled;
	idx_t access_id = 0;
	idx_t scan_calls = 0;
	idx_t observed_candidates = 0;
	uint64_t probe_time_ns = 0;
	vector<Value> probes;

	template <class INDEX, class STATE, class ROWS>
	bool Scan(const INDEX &index, STATE &state, idx_t limit, ROWS &rows, const char *origin) {
		if (!enabled) {
			return index->Scan(state, limit, rows);
		}
		if (access_id == 0) {
			static atomic<idx_t> next_access {0};
			access_id = ++next_access;
		}
		auto predicates = SibenchArtPredicates(state);
		auto start = steady_clock::now();
		auto completed = index->Scan(state, limit, rows);
		auto elapsed = duration_cast<nanoseconds>(steady_clock::now() - start).count();
		probe_time_ns += elapsed;
		probes.push_back(Value::STRUCT({{"origin", Value(origin)}, {"predicates", std::move(predicates)},
		                               {"completed", Value::BOOLEAN(completed)},
		                               {"candidates_after", Value::UBIGINT(rows.size())},
		                               {"probe_time_ns", Value::UBIGINT(elapsed)}}));
		scan_calls++;
		observed_candidates = MaxValue<idx_t>(observed_candidates, rows.size());
		return completed;
	}
};

class SibenchIndexLogType : public LogType {
public:
	static constexpr const char *NAME = "SibenchIndex";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	SibenchIndexLogType() : LogType(NAME, LEVEL, GetLogType()) {
	}
	static LogicalType ColumnType() {
		return LogicalType::STRUCT({{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}});
	}

	static LogicalType GetLogType() {
		return LogicalType::STRUCT({{"access_id", LogicalType::UBIGINT},
		                            {"database_name", LogicalType::VARCHAR},
		                            {"schema_name", LogicalType::VARCHAR},
		                            {"table_name", LogicalType::VARCHAR},
		                            {"index_name", LogicalType::VARCHAR},
		                            {"index_primary", LogicalType::BOOLEAN},
		                            {"index_is_column", LogicalType::BOOLEAN},
		                            {"index_columns", LogicalType::LIST(ColumnType())},
		                            {"adopted", LogicalType::BOOLEAN},
		                            {"snapshot_start", LogicalType::UBIGINT},
		                            {"candidate_limit", LogicalType::UBIGINT},
		                            {"scan_calls", LogicalType::UBIGINT},
		                            {"observed_candidates", LogicalType::UBIGINT},
		                            {"probe_time_ns", LogicalType::UBIGINT},
		                            {"probes", LogicalType::LIST(SibenchProbeType())}});
	}

	static string ConstructLogMessage(const string &database, const string &schema, const string &table,
	                                  const string &index, bool primary, bool is_column, const vector<Value> &columns,
	                                  bool adopted, transaction_t snapshot_start,
	                                  idx_t candidate_limit, const SibenchIndexAttempt &attempt) {
		return Value::STRUCT({{"access_id", Value::UBIGINT(attempt.access_id)},
		                      {"database_name", Value(database)},
		                      {"schema_name", Value(schema)},
		                      {"table_name", Value(table)},
		                      {"index_name", Value(index)},
		                      {"index_primary", Value::BOOLEAN(primary)},
		                      {"index_is_column", Value::BOOLEAN(is_column)},
		                      {"index_columns", Value::LIST(ColumnType(), columns)},
		                      {"adopted", Value::BOOLEAN(adopted)},
		                      {"snapshot_start", Value::UBIGINT(snapshot_start)},
		                      {"candidate_limit", Value::UBIGINT(candidate_limit)},
		                      {"scan_calls", Value::UBIGINT(attempt.scan_calls)},
		                      {"observed_candidates", Value::UBIGINT(attempt.observed_candidates)},
		                      {"probe_time_ns", Value::UBIGINT(attempt.probe_time_ns)},
		                      {"probes", Value::LIST(SibenchProbeType(), attempt.probes)}})
		    .ToString();
	}
};

} // namespace duckdb
