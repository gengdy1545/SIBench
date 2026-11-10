#!/usr/bin/env python3
"""Exercise the instrumented DuckDB CLI; no benchmark or external service is required."""

import argparse
import csv
import io
from pathlib import Path
import subprocess
import unittest


class IndexAttemptsTest(unittest.TestCase):
    cli: str

    def query(self, setup, query):
        result = subprocess.run(
            [self.cli, "-batch", "-bail", "-csv", "-header", ":memory:"],
            input=f"SET logging_storage='memory';\n.output /dev/null\n{setup}\n.output stdout\n{query}",
            text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return list(csv.DictReader(io.StringIO(result.stdout)))

    def capture(self, query, *, limit=20000, before=""):
        return self.query(f"""
            SET threads=4;
            SET index_scan_max_count={limit};
            SET index_scan_percentage=0;
            CREATE TABLE items AS SELECT i * 2 AS id, i % 7 AS payload FROM range(10000) t(i);
            CREATE INDEX items_id ON items(id);
            CALL enable_logging('SibenchIndex');
            {before}
            CREATE TEMP TABLE answer AS {query};
        """, """
            SELECT table_name, index_name, adopted, scan_calls, observed_candidates,
                   candidate_limit, probe_time_ns, snapshot_start,
                   connection_id IS NOT NULL AND transaction_id IS NOT NULL
                       AND query_id IS NOT NULL AS has_context,
                   (SELECT count(*) FROM answer) AS answer_rows
            FROM duckdb_logs_parsed('SibenchIndex') ORDER BY timestamp;
        """)

    def only_attempt(self, rows, adopted, candidates):
        self.assertEqual(len(rows), 1, rows)
        row = rows[0]
        self.assertEqual(row["table_name"], "items")
        self.assertEqual(row["index_name"], "items_id")
        self.assertEqual(row["adopted"], str(adopted).lower())
        self.assertEqual(row["has_context"], "true")
        self.assertGreaterEqual(int(row["scan_calls"]), 1)
        self.assertGreater(int(row["probe_time_ns"]), 0)
        self.assertGreater(int(row["snapshot_start"]), 0)
        if candidates is not None:
            self.assertEqual(int(row["observed_candidates"]), candidates)
        return row

    def test_adopted_point_and_empty_lookup(self):
        for key, count in ((42, 1), (43, 0)):
            with self.subTest(key=key):
                row = self.only_attempt(self.capture(f"SELECT * FROM items WHERE id={key}"), True, count)
                self.assertEqual(int(row["answer_rows"]), count)

    def test_index_identity_comes_from_native_constraint_and_columns(self):
        rows = self.query("""
            CREATE TABLE items(id BIGINT PRIMARY KEY, k VARCHAR);
            INSERT INTO items VALUES(10,'x'),(20,'y'),(30,'x');
            CREATE INDEX items_k ON items(k);
            CALL enable_logging('SibenchIndex');
            SELECT id FROM items WHERE id=20;
            SELECT id FROM items WHERE k='x';
            CALL disable_logging();
        """, """
            SELECT index_primary, index_is_column, index_columns[1].name AS column_name,
                   index_columns[1].type AS column_type, adopted
            FROM duckdb_logs_parsed('SibenchIndex') ORDER BY access_id;
        """)
        self.assertEqual(rows, [
            {"index_primary": "true", "index_is_column": "true", "column_name": "id", "column_type": "BIGINT", "adopted": "true"},
            {"index_primary": "false", "index_is_column": "true", "column_name": "k", "column_type": "VARCHAR", "adopted": "true"},
        ])

    def test_abandoned_probe_keeps_partial_candidate_diagnostics(self):
        row = self.only_attempt(self.capture("SELECT * FROM items WHERE id<200", limit=5), False, None)
        self.assertEqual(row["candidate_limit"], "5")
        self.assertLess(int(row["observed_candidates"]), 100)
        self.assertEqual(row["answer_rows"], "100")

    def test_eligibility_checks_are_not_attempts(self):
        self.assertEqual(self.capture("SELECT * FROM items WHERE payload=3"), [])
        self.assertEqual(self.capture("SELECT * FROM items WHERE abs(id-42)=0"), [])

    def test_selection_stops_after_adoption_but_keeps_abandoned_attempts(self):
        for limit, count, adopted in ((5, 2, "false"), (20000, 1, "true")):
            with self.subTest(limit=limit):
                rows = self.capture(
                    "SELECT * FROM items WHERE id<200", limit=limit,
                    before="CREATE INDEX alternate_id ON items(id);",
                )
                self.assertEqual(len(rows), count, rows)
                self.assertEqual(len({row["index_name"] for row in rows}), count)
                for row in rows:
                    self.assertEqual(row["adopted"], adopted)
                    self.assertEqual(row["answer_rows"], "100")

    def test_limit_and_aggregate_do_not_change_candidate_count(self):
        for query, candidates in (
            ("SELECT * FROM items WHERE id<10000 LIMIT 1", 5000),
            ("SELECT count(*) FROM items WHERE id<200", 100),
        ):
            with self.subTest(query=query):
                row = self.only_attempt(self.capture(query), True, candidates)
                self.assertEqual(row["answer_rows"], "1")

    def test_own_insert_is_visible_without_becoming_a_persistent_candidate(self):
        row = self.only_attempt(self.capture(
            "SELECT * FROM items WHERE id=42", before="BEGIN; INSERT INTO items VALUES (42, 99);"
        ), True, 1)
        self.assertEqual(row["answer_rows"], "2")

    def test_own_delete_is_filtered_after_the_probe(self):
        row = self.only_attempt(self.capture(
            "SELECT * FROM items WHERE id=42",
            before="BEGIN; DELETE FROM items WHERE id=42; CALL truncate_duckdb_logs();",
        ), True, 1)
        self.assertEqual(row["answer_rows"], "0")

    def test_logging_is_opt_in(self):
        rows = self.query("""
            CREATE TABLE items AS SELECT i AS id FROM range(10000) t(i);
            CREATE INDEX items_id ON items(id);
            CREATE TABLE answer AS SELECT * FROM items WHERE id=42;
        """, """
            SELECT count(*) AS attempts FROM duckdb_logs WHERE type='SibenchIndex';
        """)
        self.assertEqual(rows, [{"attempts": "0"}])


    def test_bounds_come_from_initialized_art_state(self):
        cases = (
            ("id = 42", [("EQUAL", "42")]),
            ("id >= 42", [("GREATERTHANOREQUALTO", "42")]),
        )
        for predicate, expected in cases:
            with self.subTest(predicate=predicate):
                rows = self.query(f"""
                    SET index_scan_max_count=20000;
                    CREATE TABLE items AS SELECT i AS id FROM range(10000) t(i);
                    CREATE INDEX items_id ON items(id);
                    CALL enable_logging('SibenchIndex');
                    SELECT * FROM items WHERE {predicate};
                """, """
                    SELECT p.origin, p.completed, bound.comparison, bound.value_type, bound.value_sql
                    FROM duckdb_logs_parsed('SibenchIndex'), UNNEST(probes) AS probe(p),
                         UNNEST(p.predicates) WITH ORDINALITY AS bounds(bound, position)
                    ORDER BY position;
                """)
                self.assertEqual([(r["comparison"], r["value_sql"]) for r in rows], expected)
                for row in rows:
                    self.assertEqual(row["value_type"], "BIGINT")
                    self.assertEqual(row["origin"], "base")
                    self.assertEqual(row["completed"], "true")

    def test_predicate_values_keep_type_precision_and_quoting(self):
        for type_name, literal, expected, neighbors in (
            ("BIGINT", "9223372036854775806", "9223372036854775806", "(0),(9223372036854775807)"),
            ("DECIMAL(18,4)", "12345678901234.5678", "12345678901234.5678", "(0),(12345678901234.5679)"),
            ("VARCHAR", "'O''Brien 🦆'", "'O''Brien 🦆'", "('A'),('Z')"),
            ("TIMESTAMP", "TIMESTAMP '2026-11-04 12:34:56.123456'", "'2026-11-04 12:34:56.123456'::TIMESTAMP", "('2026-11-03'),('2026-11-05')"),
        ):
            with self.subTest(type_name=type_name):
                rows = self.query(f"""
                    CREATE TABLE items(id {type_name});
                    INSERT INTO items VALUES ({literal}), {neighbors};
                    CREATE INDEX items_id ON items(id);
                    CALL enable_logging('SibenchIndex');
                    SELECT * FROM items WHERE id={literal};
                """, """
                    SELECT probes[1].predicates[1].value_type AS value_type,
                           probes[1].predicates[1].value_sql AS value_sql
                    FROM duckdb_logs_parsed('SibenchIndex');
                """)
                self.assertEqual(rows, [{"value_type": type_name, "value_sql": expected}])

    def test_unsupported_conjunctive_range_does_not_invent_an_index_access(self):
        rows = self.query("""
            SET index_scan_max_count=20000;
            CREATE TABLE items AS SELECT i AS id FROM range(10000) t(i);
            CREATE INDEX items_id ON items(id);
            CALL enable_logging('SibenchIndex');
            SELECT * FROM items WHERE 42 < id AND id <= 50;
        """, """
            SELECT count(*) AS attempts FROM duckdb_logs_parsed('SibenchIndex');
        """)
        self.assertEqual(rows, [{"attempts": "0"}])

    def row_batches(self, before="", query="SELECT payload FROM items WHERE k>=2"):
        return self.query(f"""
            SET threads=1;
            SET index_scan_max_count=20000;
            CREATE TABLE items(id INTEGER PRIMARY KEY, k INTEGER, payload VARCHAR);
            INSERT INTO items VALUES (30,3,'thirty'),(10,1,'ten'),(20,2,'twenty');
            CREATE INDEX items_k ON items(k);
            CALL enable_logging(['SibenchIndex', 'SibenchIndexRows']);
            {before}
            CREATE TEMP TABLE answer AS {query};
        """, """
            SELECT r.phase, r.candidate_rows, r.physical_rows, r.primary_keys_sql, r.primary_key_type,
                   r.candidates_read, r.source_drained, a.adopted,
                   (SELECT string_agg(payload, ',' ORDER BY payload) FROM answer) AS answer
            FROM duckdb_logs_parsed('SibenchIndexRows') r
            JOIN duckdb_logs_parsed('SibenchIndex') a USING(access_id)
            ORDER BY r.timestamp;
        """)

    def test_visible_batches_preserve_primary_keys_without_changing_projection(self):
        rows = self.row_batches()
        storage = [row for row in rows if row["phase"] == "storage"]
        self.assertEqual(len(storage), 1, rows)
        self.assertEqual(storage[0]["primary_keys_sql"], "[30, 20]")
        self.assertEqual(storage[0]["physical_rows"], "[0, 2]")
        self.assertEqual(storage[0]["primary_key_type"], "INTEGER")
        self.assertEqual(storage[0]["candidates_read"], "2")
        self.assertEqual(storage[0]["answer"], "thirty,twenty")
        self.assertEqual(rows[-1]["phase"], "closed")
        self.assertEqual(rows[-1]["source_drained"], "true")

    def test_batches_apply_own_delete_and_include_own_reinsert(self):
        rows = self.row_batches("""
            BEGIN;
            DELETE FROM items WHERE id=30;
            INSERT INTO items VALUES(30,4,'replacement');
            CALL truncate_duckdb_logs();
        """)
        storage = [r for r in rows if r["phase"] == "storage"]
        local = [r for r in rows if r["phase"] == "local" and r["primary_keys_sql"] != "[]"]
        self.assertEqual(len(storage), 1, rows)
        self.assertEqual(storage[0]["primary_keys_sql"], "[20]")
        self.assertEqual(storage[0]["physical_rows"], "[2]")
        self.assertEqual(storage[0]["candidate_rows"], "[0, 2]")
        self.assertEqual(storage[0]["candidates_read"], "2")
        self.assertEqual(len(local), 1, rows)
        self.assertEqual(local[0]["primary_keys_sql"], "[30]")
        self.assertGreater(int(local[0]["physical_rows"].strip("[]")), 2)
        self.assertEqual(local[0]["answer"], "replacement,twenty")

    def test_limit_does_not_turn_a_fetched_batch_into_consumed_rows(self):
        rows = self.row_batches(query="SELECT payload FROM items WHERE k>=2 LIMIT 1")
        storage = [r for r in rows if r["phase"] == "storage"]
        self.assertEqual(storage[0]["primary_keys_sql"], "[30, 20]")
        self.assertEqual(storage[0]["answer"], "thirty")
        self.assertEqual(rows[-1]["source_drained"], "true")

    def test_limit_before_local_rows_does_not_claim_complete_index_output(self):
        rows = self.row_batches("BEGIN; INSERT INTO items VALUES(40,4,'forty');",
                                query="SELECT payload FROM items WHERE k>=2 LIMIT 1")
        self.assertEqual(rows[-1]["source_drained"], "false")

    def test_limit_consumption_is_bound_to_the_actual_batch(self):
        for suffix, prefix in (("LIMIT 1", 1), ("LIMIT 1 OFFSET 1", 2)):
            with self.subTest(suffix=suffix):
                rows = self.query(f"""
                    SET threads=1;
                    CREATE TABLE items(id INTEGER PRIMARY KEY, k INTEGER);
                    INSERT INTO items VALUES(30,3),(10,1),(20,2);
                    CREATE INDEX items_k ON items(k);
                    CALL enable_logging(['SibenchIndex','SibenchIndexRows','SibenchConsumption']);
                    SELECT id FROM items WHERE k>=2 {suffix};
                """, """
                    SELECT c.direct_from_scan, c.input_rows, c.consumed_prefix,
                           r.primary_keys_sql
                    FROM duckdb_logs_parsed('SibenchConsumption') c
                    JOIN duckdb_logs_parsed('SibenchIndexRows') r USING(access_id,batch_id)
                    WHERE c.stage='limit' AND c.consumed_prefix>0;
                """)
                self.assertEqual(rows, [{"direct_from_scan": "true", "input_rows": "2",
                                        "consumed_prefix": str(prefix), "primary_keys_sql": "[30, 20]"}])

    def test_multi_batch_count_keeps_each_visible_primary_key_once(self):
        rows = self.query("""
            SET threads=4;
            SET index_scan_max_count=20000;
            CREATE TABLE items(id INTEGER PRIMARY KEY, k INTEGER);
            INSERT INTO items SELECT i, i%10 FROM range(10000) t(i);
            CREATE INDEX items_k ON items(k);
            CALL enable_logging(['SibenchIndex','SibenchIndexRows']);
            CREATE TEMP TABLE answer AS SELECT count(*) AS n FROM items WHERE k>=2;
        """, """
            SELECT count(*) AS captured, count(DISTINCT pk) AS distinct_keys,
                   (SELECT n FROM answer) AS answer
            FROM duckdb_logs_parsed('SibenchIndexRows'), UNNEST(primary_keys_sql) AS keys(pk);
        """)
        self.assertEqual(rows, [{"captured": "8000", "distinct_keys": "8000", "answer": "8000"}])

    def test_limit_after_residual_filter_does_not_claim_a_direct_scan_prefix(self):
        rows = self.query("""
            SET threads=1;
            CREATE TABLE items(id INTEGER PRIMARY KEY, k INTEGER);
            INSERT INTO items VALUES(30,3),(10,1),(20,2);
            CREATE INDEX items_k ON items(k);
            CALL enable_logging(['SibenchIndex','SibenchIndexRows','SibenchConsumption']);
            SELECT id FROM items WHERE k>=2 AND (id+k)%2=1 LIMIT 1;
        """, """
            SELECT c.direct_from_scan, c.input_rows, c.consumed_prefix, r.primary_keys_sql
            FROM duckdb_logs_parsed('SibenchConsumption') c
            JOIN duckdb_logs_parsed('SibenchIndexRows') r USING(access_id,batch_id)
            WHERE c.stage='limit' AND c.consumed_prefix>0;
        """)
        self.assertEqual(rows, [{"direct_from_scan": "false", "input_rows": "1",
                                "consumed_prefix": "1", "primary_keys_sql": "[30, 20]"}])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("duckdb", type=Path, help="CLI produced by scripts/build-duckdb.sh")
    args, remaining = parser.parse_known_args()
    IndexAttemptsTest.cli = str(args.duckdb.resolve(strict=True))
    unittest.main(argv=[__file__, *remaining])
