#include "duckdb.hpp"

#include <iostream>
#include <stdexcept>

using namespace duckdb;

static unique_ptr<QueryResult> Query(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	result->RowCount();
	if (result->HasError()) {
		throw std::runtime_error(result->GetError());
	}
	return result;
}

static void Require(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

static int64_t Count(Connection &connection) {
	return Query(connection, "SELECT count(*) FROM items WHERE id=42")->Fetch()->GetValue(0, 0).GetValue<int64_t>();
}

static void CheckSnapshots(idx_t threshold, bool adopted) {
	DuckDB database(nullptr);
	Connection reader(database), writer(database);
	Query(writer, "SET index_scan_percentage=0");
	Query(writer, "SET index_scan_max_count=" + to_string(threshold));
	Query(writer, "CREATE TABLE items AS SELECT i % 100 AS id FROM range(10000) t(i)");
	Query(writer, "CREATE INDEX items_id ON items(id)");
	Query(writer, "CALL enable_logging(['SibenchIndex','SibenchIndexRows'])");
	Query(reader, "BEGIN");
	Require(Count(reader) == 100, "initial snapshot must contain 100 matches");
	Query(writer, "DELETE FROM items WHERE id=42");
	Query(writer, "CALL truncate_duckdb_logs()");
	Require(Count(reader) == 100, "old snapshot must retain the deleted matches");
	Require(Count(writer) == 0, "new snapshot must exclude the committed deletes");
	auto result = Query(writer,
	                  "SELECT adopted, scan_calls, observed_candidates, snapshot_start, connection_id, "
	                  "probes[1].origin, probes[2].origin, probes[1].predicates=probes[2].predicates "
	                  "FROM duckdb_logs_parsed('SibenchIndex') ORDER BY timestamp");
	Require(result->RowCount() == 2, "each actual access must yield one attempt, including its delta probes");
	auto logs = result->Fetch();
	for (idx_t row = 0; row < 2; row++) {
		Require(logs->GetValue(0, row).GetValue<bool>() == adopted, "incorrect final adoption");
		Require(logs->GetValue(1, row).GetValue<uint64_t>() == 2, "base and deleted-row delta must both be probed");
		Require(logs->GetValue(5, row).ToString() == "base", "first probe must identify the main ART");
		Require(logs->GetValue(6, row).ToString() == "deleted_rows", "second probe must identify the delta ART");
		Require(logs->GetValue(7, row).GetValue<bool>(), "base and delta predicates must agree");
		if (adopted) {
			Require(logs->GetValue(2, row).GetValue<uint64_t>() == 100, "delta candidates must be retained");
		}
	}
	Require(logs->GetValue(3, 0).GetValue<uint64_t>() < logs->GetValue(3, 1).GetValue<uint64_t>(),
	        "old and new accesses must retain their actual snapshot starts");
	Require(logs->GetValue(4, 0) != logs->GetValue(4, 1), "accesses must retain their connection context");
	auto fetched = Query(writer,
	                     "SELECT connection_id, sum(len(physical_rows)) AS visible "
	                     "FROM duckdb_logs_parsed('SibenchIndexRows') GROUP BY connection_id ORDER BY connection_id");
	if (adopted) {
		Require(fetched->RowCount() == 2, "both adopted accesses must retain fetched row facts");
		auto batches = fetched->Fetch();
		for (idx_t row = 0; row < 2; row++) {
			auto old_snapshot = batches->GetValue(0, row) == logs->GetValue(4, 0);
			Require(batches->GetValue(1, row).GetValue<int64_t>() == (old_snapshot ? 100 : 0),
			        "fetched identities must reflect the actual snapshot, not ART candidate count");
		}
	} else {
		Require(fetched->RowCount() == 0, "abandoned probes must not produce adopted row batches");
	}
	Query(reader, "ROLLBACK");
}

static void CheckTransactionEvents() {
	DuckDB database(nullptr);
	Connection connection(database);
	Query(connection, "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER)");
	Query(connection, "INSERT INTO items VALUES(1, 0)");
	Query(connection, "CALL enable_logging('SibenchTransaction')");
	uint64_t previous_order = 0;
	for (const auto committed : {true, false}) {
		Query(connection, "BEGIN");
		auto tx = Query(connection, "SELECT current_transaction_id()")->Fetch()->GetValue(0, 0).GetValue<uint64_t>();
		Query(connection, "UPDATE items SET value=value+1 WHERE id=1");
		Query(connection, committed ? "COMMIT" : "ROLLBACK");
		auto result = Query(connection,
		                    "SELECT event, engine_transaction, snapshot_start, commit_stamp, resolution_order "
		                    "FROM duckdb_logs_parsed('SibenchTransaction') WHERE transaction_id=" +
		                        to_string(tx) + " ORDER BY timestamp");
		Require(result->RowCount() == 2, "source transaction must have one begin and one terminal event");
		auto rows = result->Fetch();
		Require(rows->GetValue(0, 0).ToString() == "begin", "missing source begin");
		Require(rows->GetValue(0, 1).ToString() == (committed ? "commit" : "rollback"), "wrong source outcome");
		Require(rows->GetValue(1, 0) == rows->GetValue(1, 1), "engine transaction identity changed");
		Require(rows->GetValue(2, 0) == rows->GetValue(2, 1), "snapshot start changed within transaction");
		Require(rows->GetValue(3, 0).GetValue<uint64_t>() == 0, "begin must not invent a commit stamp");
		auto stamp = rows->GetValue(3, 1).GetValue<uint64_t>();
		Require(committed ? stamp > rows->GetValue(2, 1).GetValue<uint64_t>() : stamp == 0,
		        "terminal event must carry the actual commit stamp or zero for rollback");
		Require(rows->GetValue(4, 0).GetValue<uint64_t>() == 0, "begin is not a resolved transaction");
		auto order = rows->GetValue(4, 1).GetValue<uint64_t>();
		Require(order > previous_order, "commit and abort must share an increasing resolution order");
		previous_order = order;
	}
	Require(Query(connection, "SELECT value FROM items")->Fetch()->GetValue(0, 0).GetValue<int32_t>() == 1,
	        "rollback must not retain the second update");
}

static void CheckDisconnectRollback() {
	DuckDB database(nullptr);
	Connection observer(database);
	Query(observer, "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER)");
	Query(observer, "INSERT INTO items VALUES(1, 0)");
	Query(observer, "CALL enable_logging('SibenchTransaction')");
	uint64_t tx;
	{
		Connection writer(database);
		Query(writer, "BEGIN");
		Query(writer, "UPDATE items SET value=1 WHERE id=1");
		tx = Query(writer, "SELECT current_transaction_id()")->Fetch()->GetValue(0, 0).GetValue<uint64_t>();
	}
	auto result = Query(observer,
	                    "SELECT event, resolution_order, connection_id, engine_transaction "
	                    "FROM duckdb_logs_parsed('SibenchTransaction') WHERE transaction_id=" +
	                        to_string(tx) + " ORDER BY resolution_order");
	Require(result->RowCount() == 2, "disconnect must retain begin and rollback with the source transaction identity");
	auto rows = result->Fetch();
	Require(rows->GetValue(0, 0).ToString() == "begin", "disconnect begin missing");
	Require(rows->GetValue(0, 1).ToString() == "rollback", "disconnect rollback missing");
	Require(rows->GetValue(1, 1).GetValue<uint64_t>() > 0, "disconnect has no resolution order");
	Require(rows->GetValue(2, 0) == rows->GetValue(2, 1), "disconnect lost source connection identity");
	Require(rows->GetValue(3, 0) == rows->GetValue(3, 1), "disconnect changed engine transaction identity");
	Require(Query(observer, "SELECT value FROM items")->Fetch()->GetValue(0, 0).GetValue<int32_t>() == 0,
	        "disconnect must roll back the source write");
}

int main() {
	try {
		CheckSnapshots(20000, true);
		CheckSnapshots(5, false);
		CheckTransactionEvents();
		CheckDisconnectRollback();
		std::cout << "Snapshot visibility, delta probes, and transaction outcomes passed\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
