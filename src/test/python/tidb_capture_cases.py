#!/usr/bin/env python3
"""Controlled TiDB capture cases: hand-checkable statements on two TiDB instances.

Each step is appended to cases.jsonl with its session, connection ID, SQL, client-observed
rows or error, and the transaction start_ts TiDB reports. Hand-derived expectations are
asserted here against client results; capture-side checks are done by the Java converter.
Hints and explicit Region splits make these controlled plans, not natural benchmark plans.
"""
import argparse
import json
import threading
import time

import pymysql

ROWS = 2000


class Session:
    def __init__(self, name, host, port, db, out):
        self.name, self.out = name, out
        self.conn = pymysql.connect(host=host, port=port, user="root", database=db, autocommit=True)
        self.conn_id = self.query("SELECT CONNECTION_ID()")[0][0]

    def query(self, sql):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return [list(r) for r in cursor.fetchall()] if cursor.description else []

    def step(self, case, sql, expect=None, expect_error=None):
        record = {"case": case, "session": self.name, "conn": self.conn_id, "sql": sql, "t": time.time()}
        try:
            rows = self.query(sql)
            record["rows"] = [[str(v) if v is not None else None for v in r] for r in rows]
        except pymysql.MySQLError as error:
            record["error"] = [error.args[0], str(error.args[-1])]
            rows = None
        # 0 outside an explicit transaction.
        record["txn_start_ts"] = int(self.query("SELECT @@tidb_current_ts")[0][0])
        self.out.write(json.dumps(record) + "\n")
        self.out.flush()
        if expect_error is not None:
            assert "error" in record and record["error"][0] == expect_error, (case, sql, record)
        elif "error" in record:
            raise AssertionError((case, sql, record["error"]))
        if expect is not None:
            got = record["rows"]
            assert got == expect, (case, sql, got, expect)
        return record


def load(s):
    s.query("DROP TABLE IF EXISTS t1")
    s.query("CREATE TABLE t1 (id INT PRIMARY KEY CLUSTERED, k INT NOT NULL, c VARCHAR(32), KEY k_idx (k))")
    for start in range(1, ROWS + 1, 500):
        values = ",".join("(%d,%d,'c%d')" % (i, i % 100, i) for i in range(start, min(start + 500, ROWS + 1)))
        s.query("INSERT INTO t1 VALUES " + values)
    # Controlled layout: several Regions for both the record and index key spaces.
    s.query("SPLIT TABLE t1 BY (500), (1000), (1500)")
    s.query("SPLIT TABLE t1 INDEX k_idx BY (25), (50), (75)")
    s.query("ANALYZE TABLE t1")


def baseline(s):
    """Export the initial state through observed scans in one read-only transaction; its start_ts is T0."""
    s.query("BEGIN")
    t0 = int(s.query("SELECT @@tidb_current_ts")[0][0])
    s.step("baseline", "SELECT /*+ USE_INDEX(t1, PRIMARY) */ id, k, c FROM t1 ORDER BY id")
    s.step("baseline", "SELECT /*+ USE_INDEX(t1, k_idx) */ k, id FROM t1 ORDER BY k, id")
    s.query("COMMIT")
    assert t0 > 0
    return t0


def cases(a, b):
    # 1. Primary-key point read and miss, autocommit and inside a transaction.
    a.step("pk_point", "SELECT id, k, c FROM t1 WHERE id = 5", [["5", "5", "c5"]])
    a.step("pk_miss", "SELECT id, k, c FROM t1 WHERE id = 999999", [])
    a.step("pk_point_txn", "BEGIN")
    a.step("pk_point_txn", "SELECT id, k FROM t1 WHERE id = 6", [["6", "6"]])
    a.step("pk_point_txn", "SELECT id, k FROM t1 WHERE id = 999998", [])
    a.step("pk_point_txn", "COMMIT")
    # 2. Non-unique secondary index with table lookup, and covering.
    a.step("secondary_lookup", "SELECT /*+ USE_INDEX(t1, k_idx) */ id, c FROM t1 WHERE k = 7 ORDER BY id",
           [[str(i), "c%d" % i] for i in range(7, ROWS + 1, 100)])
    a.step("secondary_covering", "SELECT /*+ USE_INDEX(t1, k_idx) */ id FROM t1 WHERE k = 8 ORDER BY id",
           [[str(i)] for i in range(8, ROWS + 1, 100)])
    # 3. Ranges, bounds, direction and early stop.
    a.step("pk_range_desc_limit", "SELECT id FROM t1 WHERE id >= 100 AND id < 200 ORDER BY id DESC LIMIT 5",
           [[str(i)] for i in range(199, 194, -1)])
    a.step("secondary_range_limit",
           "SELECT /*+ USE_INDEX(t1, k_idx) */ k, id FROM t1 WHERE k > 10 AND k <= 12 ORDER BY k, id LIMIT 7",
           [["11", str(i)] for i in range(11, ROWS + 1, 100)][:7])
    a.step("cross_region_range", "SELECT id FROM t1 WHERE id BETWEEN 495 AND 1005 ORDER BY id",
           [[str(i)] for i in range(495, 1006)])
    # 4. One transaction updates a row twice with reads in between.
    a.step("double_update", "BEGIN")
    a.step("double_update", "UPDATE t1 SET k = k + 1000 WHERE id = 10")
    a.step("double_update", "SELECT id, k FROM t1 WHERE id = 10", [["10", "1010"]])
    a.step("double_update", "SELECT /*+ USE_INDEX(t1, k_idx) */ id FROM t1 WHERE k = 1010", [["10"]])
    a.step("double_update", "UPDATE t1 SET k = k + 1000 WHERE id = 10")
    a.step("double_update", "COMMIT")
    # 5. Delete then reinsert, and a rolled back update.
    a.step("delete_reinsert", "BEGIN")
    a.step("delete_reinsert", "DELETE FROM t1 WHERE id = 20")
    a.step("delete_reinsert", "INSERT INTO t1 VALUES (20, 777, 'r20')")
    a.step("delete_reinsert", "COMMIT")
    a.step("rollback", "BEGIN")
    a.step("rollback", "UPDATE t1 SET k = 888 WHERE id = 21")
    a.step("rollback", "ROLLBACK")
    a.step("rollback", "SELECT k FROM t1 WHERE id = 21", [["21"]])
    # 6. Interleaved sessions on two TiDB instances: snapshot read vs current read.
    a.step("interleave", "BEGIN")
    a.step("interleave", "SELECT k FROM t1 WHERE id = 30", [["30"]])
    b.step("interleave", "UPDATE t1 SET k = k + 1 WHERE id = 30")
    a.step("interleave", "SELECT k FROM t1 WHERE id = 30", [["30"]])
    a.step("interleave", "UPDATE t1 SET k = k + 5 WHERE id = 30")
    a.step("interleave", "SELECT k FROM t1 WHERE id = 30", [["36"]])
    a.step("interleave", "COMMIT")
    # 7. Pessimistic current read, a blocked statement that must retry, and a lock wait timeout.
    a.step("pessimistic_retry", "BEGIN")
    a.step("pessimistic_retry", "SELECT id, k FROM t1 WHERE id = 40 FOR UPDATE", [["40", "40"]])
    b.step("pessimistic_retry", "BEGIN")
    blocked = {}
    thread = threading.Thread(target=lambda: blocked.update(
        r=b.step("pessimistic_retry", "UPDATE t1 SET c = 'b40' WHERE id = 40 AND k = 41")))
    thread.start()
    time.sleep(1.0)
    a.step("pessimistic_retry", "UPDATE t1 SET k = 41 WHERE id = 40")
    a.step("pessimistic_retry", "COMMIT")
    thread.join(30)
    b.step("pessimistic_retry", "COMMIT")
    a.step("pessimistic_retry", "SELECT k, c FROM t1 WHERE id = 40", [["41", "b40"]])
    a.step("lock_timeout", "BEGIN")
    a.step("lock_timeout", "SELECT id FROM t1 WHERE id = 50 FOR UPDATE")
    b.step("lock_timeout", "BEGIN")
    b.step("lock_timeout", "SET @@innodb_lock_wait_timeout = 1")
    b.step("lock_timeout", "UPDATE t1 SET k = 0 WHERE id = 50", expect_error=1205)
    b.step("lock_timeout", "UPDATE t1 SET k = k + 2 WHERE id = 51")
    b.step("lock_timeout", "COMMIT")
    a.step("lock_timeout", "COMMIT")
    # 8. Pushed-down aggregation and selection: TiKV does not return row identities.
    a.step("pushdown_agg", "SELECT /*+ USE_INDEX(t1, k_idx) */ COUNT(*) FROM t1 WHERE k BETWEEN 5 AND 9",
           [["100"]])
    a.step("pushdown_selection", "SELECT /*+ USE_INDEX(t1, PRIMARY) */ COUNT(*) FROM t1 WHERE c LIKE 'c199%'")
    # 7b. A DML statement buffers writes, then conflicts at lock time and retries: the first
    # attempt's index writes must be discarded (k of row 61 becomes 162, not 161).
    a.step("dml_retry", "BEGIN")
    a.step("dml_retry", "UPDATE t1 SET k = k + 1 WHERE id = 61")
    b.step("dml_retry", "BEGIN")
    retried = {}
    thread = threading.Thread(target=lambda: retried.update(
        r=b.step("dml_retry", "UPDATE t1 SET k = k + 100 WHERE id BETWEEN 60 AND 61")))
    thread.start()
    time.sleep(1.0)
    a.step("dml_retry", "COMMIT")
    thread.join(30)
    b.step("dml_retry", "COMMIT")
    a.step("dml_retry", "SELECT id, k FROM t1 WHERE id BETWEEN 60 AND 61 ORDER BY id", [["60", "160"], ["61", "162"]])
    # 9. Explicit read-only transaction whose read point is its start_ts.
    b.step("readonly_txn", "BEGIN")
    b.step("readonly_txn", "SELECT id, k FROM t1 WHERE id BETWEEN 9 AND 11 ORDER BY id",
           [["9", "9"], ["10", "2010"], ["11", "11"]])
    b.step("readonly_txn", "COMMIT")
    # Final complete scans: every index entry and row at the end of the workload.
    a.step("final_state", "SELECT /*+ USE_INDEX(t1, k_idx) */ k, id FROM t1 ORDER BY k, id")
    a.step("final_state", "SELECT /*+ USE_INDEX(t1, PRIMARY) */ id, k FROM t1 ORDER BY id")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--a", required=True, help="host:port of TiDB instance A")
    parser.add_argument("--b", required=True, help="host:port of TiDB instance B")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    a_host, a_port = args.a.split(":")
    admin = pymysql.connect(host=a_host, port=int(a_port), user="root", autocommit=True)
    with admin.cursor() as cursor:
        cursor.execute("DROP DATABASE IF EXISTS sib_cases")
        cursor.execute("CREATE DATABASE sib_cases")
    with open(args.out, "x") as out:
        a = Session("a", a_host, int(a_port), "sib_cases", out)
        b_host, b_port = args.b.split(":")
        b = Session("b", b_host, int(b_port), "sib_cases", out)
        load(a)
        t0 = baseline(a)
        out.write(json.dumps({"baseline_ts": t0, "regions": a.query("SHOW TABLE t1 REGIONS"),
                              "index_regions": a.query("SHOW TABLE t1 INDEX k_idx REGIONS")}, default=str) + "\n")
        cases(a, b)
        out.write(json.dumps({"end_ts": int(a.query("SELECT TIDB_CURRENT_TSO()")[0][0])}) + "\n")


if __name__ == "__main__":
    main()
