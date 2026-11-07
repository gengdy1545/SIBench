#pragma once

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

class SibenchMutationLogType : public LogType {
public:
	static constexpr const char *NAME = "SibenchMutation";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	SibenchMutationLogType() : LogType(NAME, LEVEL, GetLogType()) {
	}
	static LogicalType ColumnType() {
		return LogicalType::STRUCT({{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}});
	}
	static LogicalType RowType() {
		return LogicalType::STRUCT({{"physical_row", LogicalType::BIGINT},
		                            {"values_sql", LogicalType::LIST(LogicalType::VARCHAR)}});
	}
	static LogicalType GetLogType() {
		return LogicalType::STRUCT({{"mutation_id", LogicalType::UBIGINT},
		                            {"database_name", LogicalType::VARCHAR},
		                            {"schema_name", LogicalType::VARCHAR},
		                            {"table_name", LogicalType::VARCHAR},
		                            {"kind", LogicalType::VARCHAR},
		                            {"event", LogicalType::VARCHAR},
		                            {"requested_rows", LogicalType::UBIGINT},
		                            {"affected_rows", LogicalType::BIGINT},
		                            {"columns", LogicalType::LIST(ColumnType())},
		                            {"rows", LogicalType::LIST(RowType())}});
	}
	static string ConstructLogMessage(idx_t id, const string &database, const string &schema, const string &table,
	                                  const char *kind, const char *event, idx_t requested, int64_t affected,
	                                  const vector<Value> &columns, const vector<Value> &rows) {
		return Value::STRUCT({{"mutation_id", Value::UBIGINT(id)},
		                      {"database_name", Value(database)},
		                      {"schema_name", Value(schema)},
		                      {"table_name", Value(table)},
		                      {"kind", Value(kind)},
		                      {"event", Value(event)},
		                      {"requested_rows", Value::UBIGINT(requested)},
		                      {"affected_rows", Value::BIGINT(affected)},
		                      {"columns", Value::LIST(ColumnType(), columns)},
		                      {"rows", Value::LIST(RowType(), rows)}})
		    .ToString();
	}
};

// Row changes and their indexed columns; these are not claims about physical ART maintenance.
class SibenchMutation {
public:
	SibenchMutation(ClientContext &context, DuckTableEntry &table, const char *kind, idx_t count)
	    : context(context), table(table), kind(kind), requested(count) {
		if (!count || !Logger::Get(context).ShouldLog(SibenchMutationLogType::NAME, SibenchMutationLogType::LEVEL)) return;
		static atomic<idx_t> next_id {0};
		id = ++next_id;
		auto indexed = table.GetStorage().GetDataTableInfo()->GetIndexes().GetIndexedColumns();
		column_ids.assign(indexed.begin(), indexed.end());
		std::sort(column_ids.begin(), column_ids.end());
		for (auto column : column_ids) {
			auto &definition = table.GetColumns().GetColumn(PhysicalIndex(column));
			columns.push_back(Value::STRUCT({{"name", Value(definition.Name().GetIdentifierName())},
			                                 {"type", Value(definition.Type().ToString())}}));
		}
	}
	~SibenchMutation() {
		if (began && !finished) {
			try { Log("failed", -1, {}); } catch (...) { }
		}
	}
	void BeginExisting(Vector &row_ids) {
		if (!id) return;
		auto rows = Fetch(row_ids);
		Log("begin", -1, rows);
		began = true;
	}
	void BeginAppend(DataChunk &chunk, row_t first_row) {
		AppendRows(chunk, first_row, "begin");
		began = id != 0;
	}
	void BeginMerge() {
		if (!id) return;
		Log("begin", -1, {});
		began = true;
	}
	void AppendRows(DataChunk &chunk, row_t first_row, const char *event = "rows") {
		if (!id) return;
		vector<Value> rows;
		for (idx_t row = 0; row < chunk.size(); row++) {
			vector<Value> values;
			for (auto column : column_ids) values.push_back(Value(chunk.GetValue(column, row).ToSQLString()));
			rows.push_back(Row(first_row + row, values));
		}
		Log(event, -1, rows);
	}
	void Complete(idx_t affected) {
		if (!id) return;
		Log("complete", NumericCast<int64_t>(affected), {});
		finished = true;
	}
	void CompleteUpdate(Vector &row_ids) {
		if (!id) return;
		Log("complete", NumericCast<int64_t>(requested), Fetch(row_ids));
		finished = true;
	}

private:
	static Value Row(row_t physical_row, const vector<Value> &values) {
		return Value::STRUCT({{"physical_row", Value::BIGINT(physical_row)},
		                      {"values_sql", Value::LIST(LogicalType::VARCHAR, values)}});
	}
	vector<Value> Fetch(Vector &row_ids) {
		auto &storage = table.GetStorage();
		vector<StorageIndex> fetch_columns;
		vector<LogicalType> types;
		for (auto column : column_ids) {
			fetch_columns.emplace_back(column);
			types.push_back(storage.GetTypes()[column]);
		}
		fetch_columns.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
		types.push_back(LogicalType::ROW_TYPE);
		DataChunk chunk;
		chunk.Initialize(Allocator::Get(context), types, requested);
		Vector flattened(Vector::Ref(row_ids));
		flattened.Flatten();
		ColumnFetchState fetch_state;
		storage.Fetch(DuckTransaction::Get(context, storage.db), chunk, fetch_columns, flattened, requested, fetch_state);
		vector<Value> rows;
		for (idx_t row = 0; row < chunk.size(); row++) {
			vector<Value> values;
			for (idx_t column = 0; column < column_ids.size(); column++)
				values.push_back(Value(chunk.GetValue(column, row).ToSQLString()));
			rows.push_back(Row(chunk.GetValue(column_ids.size(), row).GetValue<row_t>(), values));
		}
		return rows;
	}
	void Log(const char *event, int64_t affected, const vector<Value> &rows) {
		auto &storage = table.GetStorage();
		auto &info = storage.GetDataTableInfo();
		DUCKDB_LOG(context, SibenchMutationLogType, id, storage.GetAttached().name.GetIdentifierName(),
		           info->GetSchemaName().GetIdentifierName(), info->GetTableName().GetIdentifierName(),
		           kind, event, requested, affected, columns, rows);
	}
	ClientContext &context;
	DuckTableEntry &table;
	const char *kind;
	idx_t requested, id = 0;
	bool began = false, finished = false;
	vector<column_t> column_ids;
	vector<Value> columns;
};

} // namespace duckdb
