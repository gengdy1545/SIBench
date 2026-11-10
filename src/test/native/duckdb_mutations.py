#!/usr/bin/env python3
"""Check native mutation facts against source rows and transaction outcomes."""

import argparse
import csv
import io
import subprocess
import tempfile
from pathlib import Path
import unittest


class MutationsTest(unittest.TestCase):
    cli: str

    def query(self, setup, query):
        run = subprocess.run(
            [self.cli, "-batch", "-bail", "-csv", "-header", ":memory:"],
            input=".output /dev/null\nSET memory_limit='256MiB'; SET threads=2; "
            "SET logging_storage='memory';\n" + setup + "\n.output stdout\n" + query,
            text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(run.returncode, 0, run.stderr)
        return list(csv.DictReader(io.StringIO(run.stdout)))

    def test_aborted_mutations_remain_observable_without_changing_rows(self):
        rows = self.query("""
            CREATE TABLE items(id BIGINT PRIMARY KEY, k BIGINT, payload VARCHAR);
            CREATE INDEX items_k ON items(k);
            INSERT INTO items VALUES(1,10,'A');
            CALL enable_logging(['SibenchMutation','SibenchTransaction']);
            BEGIN;
            INSERT INTO items VALUES(2,20,'B');
            UPDATE items SET k=11 WHERE id=1;
            UPDATE items SET payload='C' WHERE id=1;
            DELETE FROM items WHERE id=2;
            ROLLBACK;
        """, """
            SELECT b.kind, b.rows[1].values_sql[1] AS id,
                   b.rows[1].values_sql[2] AS old_k,
                   coalesce(e.rows[1].values_sql[2], '') AS new_k,
                   e.affected_rows, len(b.columns) AS captured_columns,
                   (SELECT count(*) FROM items WHERE id=1 AND k=10 AND payload='A') AS original,
                   (SELECT count(*) FROM items) AS final_rows,
                   (SELECT count(*) FROM duckdb_logs_parsed('SibenchTransaction') t
                    WHERE t.transaction_id=b.transaction_id AND t.event='rollback') AS aborted
            FROM duckdb_logs_parsed('SibenchMutation') b
            JOIN duckdb_logs_parsed('SibenchMutation') e USING(mutation_id)
            WHERE b.event='begin' AND e.event='complete' AND b.table_name='items'
            ORDER BY b.mutation_id;
        """)
        self.assertEqual([(r["kind"], r["id"], r["old_k"], r["new_k"]) for r in rows], [
            ("append", "2", "20", ""), ("delete", "1", "10", ""),
            ("append", "1", "11", ""), ("update", "1", "11", "11"),
            ("delete", "2", "20", ""),
        ])
        for row in rows:
            self.assertEqual(row["affected_rows"], "1")
            self.assertEqual(row["captured_columns"], "2")
            self.assertEqual(row["original"], "1")
            self.assertEqual(row["final_rows"], "1")
            self.assertEqual(row["aborted"], "1")

    def test_typed_index_values_preserve_precision_null_and_quotes(self):
        rows = self.query("""
            CREATE TABLE items(id VARCHAR PRIMARY KEY, k DECIMAL(38,9), stamp TIMESTAMP);
            CREATE INDEX items_k ON items(k);
            CREATE INDEX items_stamp ON items(stamp);
            CALL enable_logging('SibenchMutation');
            INSERT INTO items VALUES('quoted''雪',12345678901234567890123456789.123456789,
                                     '2026-11-04 01:02:03.123456'),('null',NULL,NULL);
        """, """
            SELECT columns[1].type AS key_type, columns[2].type AS decimal_type,
                   columns[3].type AS timestamp_type,
                   rows[1].values_sql[1] AS quoted_key, rows[1].values_sql[2] AS amount,
                   rows[1].values_sql[3] AS stamp, rows[2].values_sql[2] AS null_value
            FROM duckdb_logs_parsed('SibenchMutation') WHERE event='begin' AND table_name='items';
        """)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["key_type"], "VARCHAR")
        self.assertEqual(row["decimal_type"], "DECIMAL(38,9)")
        self.assertEqual(row["timestamp_type"], "TIMESTAMP")
        self.assertEqual(row["quoted_key"], "'quoted''雪'")
        self.assertIn("12345678901234567890123456789.123456789", row["amount"])
        self.assertIn("2026-11-04 01:02:03.123456", row["stamp"])
        self.assertEqual(row["null_value"], "NULL::DECIMAL(38,9)")

    def test_native_row_ids_match_source_observations_for_batch_and_parallel_merge(self):
        for path, ordered, from_table in (("append", "true", False), ("batch_merge", "true", True),
                                          ("parallel_merge", "false", True)):
            with self.subTest(path=path), tempfile.TemporaryDirectory(prefix="sib-merge-") as directory:
                source = "SELECT id,k FROM sink.source" if from_table else "SELECT range,range*2 FROM range(16000)"
                rows = self.query(f"""
                    ATTACH '{Path(directory) / 'sink.duckdb'}' AS sink (ROW_GROUP_SIZE 2048);
                    CREATE TABLE sink.source AS SELECT range AS id,range*2 AS k FROM range(16000);
                    CREATE TABLE sink.items(id BIGINT PRIMARY KEY, k BIGINT);
                    SET preserve_insertion_order={ordered};
                    CALL enable_logging('SibenchMutation');
                    BEGIN;
                    INSERT INTO sink.items {source};
                    CREATE TEMP TABLE observed AS SELECT id,rowid AS physical_row FROM sink.items;
                    COMMIT;
                """, """
                    WITH events AS (SELECT * FROM duckdb_logs_parsed('SibenchMutation')
                                    WHERE table_name='items'),
                         captured AS (SELECT unnest(rows) AS r FROM events WHERE event IN('begin','rows'))
                    SELECT (SELECT sum(affected_rows) FROM events WHERE event='complete') AS completed,
                           (SELECT count(*) FROM events WHERE kind='merge' AND event='complete') AS merges,
                           (SELECT count(*) FROM events WHERE event='failed') AS failures,
                           count(*) AS captured_rows, count(DISTINCT r.values_sql[1]) AS keys,
                           count(*) FILTER (WHERE o.id IS NULL OR o.physical_row<>r.physical_row) AS wrong
                    FROM captured LEFT JOIN observed o ON CAST(r.values_sql[1] AS BIGINT)=o.id;
                """)
                self.assertEqual(len(rows), 1)
                row = rows[0]
                self.assertEqual(row["completed"], "16000")
                if from_table:
                    self.assertGreater(int(row["merges"]), 0)
                else:
                    self.assertEqual(row["merges"], "0")
                self.assertEqual(row["captured_rows"], "16000")
                self.assertEqual(row["keys"], "16000")
                self.assertEqual(row["failures"], "0")
                self.assertEqual(row["wrong"], "0")

    def test_delete_reports_actual_count_for_duplicate_candidates(self):
        rows = self.query("""
            CREATE TABLE items(id BIGINT PRIMARY KEY);
            INSERT INTO items VALUES(1),(2);
            CALL enable_logging('SibenchMutation');
            DELETE FROM items USING (VALUES(1),(1)) d(id) WHERE items.id=d.id;
        """, """
            SELECT sum(affected_rows) AS deleted,
                   (SELECT count(*) FROM items WHERE id=2) AS remaining
            FROM duckdb_logs_parsed('SibenchMutation')
            WHERE event='complete' AND kind='delete' AND table_name='items';
        """)
        self.assertEqual(rows, [{"deleted": "1", "remaining": "1"}])

    def test_logging_is_opt_in(self):
        rows = self.query("""
            CREATE TABLE items(id BIGINT PRIMARY KEY);
            INSERT INTO items VALUES(1); DELETE FROM items;
        """, "SELECT count(*) AS records FROM duckdb_logs_parsed('SibenchMutation');")
        self.assertEqual(rows, [{"records": "0"}])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("cli")
    args, remaining = parser.parse_known_args()
    MutationsTest.cli = args.cli
    unittest.main(argv=[__file__, *remaining])
