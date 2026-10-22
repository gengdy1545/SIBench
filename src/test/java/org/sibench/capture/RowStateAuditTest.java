package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.ConfigValues;
import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class RowStateAuditTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private SourceSchema schema(String benchmark, int count) throws Exception {
        Path profile =
                Path.of(
                        "config/"
                                + (benchmark.equals("hybench")
                                        ? "hybench_sf1"
                                        : benchmark + "_large")
                                + ".yaml");
        var document = ConfigValues.parse(Files.readString(profile));
        var nativeConfig = ConfigValues.object(document, "native");
        if (benchmark.equals("ycsb")) nativeConfig.put("fieldcount", count);
        if (benchmark.equals("sysbench")) nativeConfig.put("tables", count);
        document.put("native", nativeConfig);
        return SourceSchema.read(Path.of(""), document);
    }

    private JsonObject row(SourceSchema.Table table, String key) {
        var row = new JsonObject();
        for (var column : table.columns()) {
            if (column.nullable()) row.add(column.name(), JsonNull.INSTANCE);
            else if (column.type().equals("integer")) row.addProperty(column.name(), 0);
            else row.addProperty(column.name(), "");
        }
        row.addProperty(SourceRow.primary(table).name(), key);
        return row;
    }

    private Path baseline(Path fixture, SourceSchema schema, Map<String, List<JsonObject>> rows)
            throws Exception {
        Path directory = RowAuditTestSupport.baseline(fixture, schema, rows);
        PgBaselineVerify.verify(
                RowAuditTestSupport.connection(schema, rows, new int[1]), schema, directory);
        return directory;
    }

    @Test
    public void threeSchemasAuditAllTablesAndReuseWithoutAnotherPgQuery() throws Exception {
        for (String benchmark : List.of("hybench", "ycsb", "sysbench")) {
            var schema = schema(benchmark, 3);
            Path fixture = temporary.newFolder().toPath();
            var initial = new LinkedHashMap<String, List<JsonObject>>();
            var actual = new LinkedHashMap<String, List<JsonObject>>();
            var changes = new ArrayList<JsonObject>();
            for (var table : schema.tables()) {
                JsonObject before = row(table, benchmark.equals("ycsb") ? "user020 " : "20");
                JsonObject after = before.deepCopy();
                var column =
                        table.columns().stream()
                                .filter(c -> !c.name().equals(SourceRow.primary(table).name()))
                                .findFirst()
                                .orElseThrow();
                after.addProperty(
                        column.name(), column.type().equals("integer") ? "21" : "changed ");
                initial.put(table.name(), List.of(before));
                actual.put(table.name(), List.of(after));
                changes.add(RowAuditTestSupport.change(table.name(), "update", before, after));
            }
            Path baseline = baseline(fixture, schema, initial);
            Path raw = RowAuditTestSupport.capture(fixture, List.of(changes), Set.of());
            Path report = RowStateAudit.defaultReport(raw);
            int[] queries = {0};
            var connection = RowAuditTestSupport.connection(schema, actual, queries);
            var result = RowStateAudit.audit(connection, schema, baseline, raw, report);
            assertEquals("passed_source_row_state", result.get("status"));
            assertEquals(schema.tables().size(), queries[0]);
            assertEquals((long) changes.size(), result.get("committed_row_changes"));
            long size = Files.size(report);
            RowStateAudit.audit(connection, schema, baseline, raw, report);
            assertEquals(schema.tables().size(), queries[0]);
            assertEquals(size, Files.size(report));
            assertEquals(
                    changes.size(), Files.readAllLines(Path.of(report + ".changes.jsonl")).size());
            Files.writeString(raw, "\n", StandardOpenOption.APPEND);
            assertThrows(
                    IOException.class, () -> RowStateAudit.reuse(schema, baseline, raw, report));
        }
    }

    @Test
    public void deleteReinsertAndLowerInsertedKeysMergeWithUnchangedRowsAndDiscardAborts()
            throws Exception {
        var schema = schema("sysbench", 1);
        var table = schema.tables().get(0);
        JsonObject first = row(table, "-20"), last = row(table, "10000000000");
        JsonObject reinserted = first.deepCopy(),
                lower = row(table, "-30"),
                middle = row(table, "0");
        reinserted.addProperty("c", "reinserted");
        Path fixture = temporary.newFolder().toPath();
        Path baseline = baseline(fixture, schema, Map.of(table.name(), List.of(first, last)));
        var aborted = last.deepCopy();
        aborted.addProperty("k", 50);
        Path raw =
                RowAuditTestSupport.capture(
                        fixture,
                        List.of(
                                List.of(
                                        RowAuditTestSupport.change(
                                                table.name(), "delete", first, null),
                                        RowAuditTestSupport.change(
                                                table.name(), "insert", null, reinserted),
                                        RowAuditTestSupport.change(
                                                table.name(), "insert", null, lower),
                                        RowAuditTestSupport.change(
                                                table.name(), "insert", null, middle)),
                                List.of(
                                        RowAuditTestSupport.change(
                                                table.name(), "update", last, aborted))),
                        Set.of(1));
        var report =
                RowStateAudit.audit(
                        RowAuditTestSupport.connection(
                                schema,
                                Map.of(table.name(), List.of(lower, reinserted, middle, last)),
                                new int[1]),
                        schema,
                        baseline,
                        raw,
                        RowStateAudit.defaultReport(raw));
        assertEquals(4L, report.get("committed_row_changes"));
        assertEquals(1L, report.get("discarded_aborted_row_changes"));
    }

    @Test
    public void wrongOrdinaryBeforeValueFailsEvenWhenFinalPgValueMatches() throws Exception {
        var schema = schema("ycsb", 1);
        var table = schema.tables().get(0);
        JsonObject initial = row(table, "user7"),
                wrong = initial.deepCopy(),
                after = initial.deepCopy();
        initial.addProperty("field0", "original ");
        wrong.addProperty("field0", "original");
        after.addProperty("field0", "final");
        Path fixture = temporary.newFolder().toPath();
        Path baseline = baseline(fixture, schema, Map.of(table.name(), List.of(initial)));
        Path raw =
                RowAuditTestSupport.capture(
                        fixture,
                        List.of(
                                List.of(
                                        RowAuditTestSupport.change(
                                                table.name(), "update", wrong, after))),
                        Set.of());
        Path report = RowStateAudit.defaultReport(raw);
        assertTrue(
                assertThrows(
                                IOException.class,
                                () ->
                                        RowStateAudit.audit(
                                                RowAuditTestSupport.connection(
                                                        schema,
                                                        Map.of(table.name(), List.of(after)),
                                                        new int[1]),
                                                schema,
                                                baseline,
                                                raw,
                                                report))
                        .getMessage()
                        .contains("before-image"));
        assertFalse(Files.exists(report));
    }

    @Test
    public void rejectsPrimaryKeyValueUpdatesForEverySourceIncludingRolledBackAttempts()
            throws Exception {
        for (String benchmark : List.of("hybench", "ycsb", "sysbench"))
            for (boolean abort : List.of(false, true)) {
                var schema = schema(benchmark, 1);
                var table = schema.tables().get(0);
                JsonObject before = row(table, benchmark.equals("ycsb") ? "user7" : "7");
                JsonObject after = row(table, benchmark.equals("ycsb") ? "user8" : "8");
                Path fixture = temporary.newFolder().toPath();
                Path baseline = baseline(fixture, schema, Map.of(table.name(), List.of(before)));
                Path raw =
                        RowAuditTestSupport.capture(
                                fixture,
                                List.of(
                                        List.of(
                                                RowAuditTestSupport.change(
                                                        table.name(), "update", before, after))),
                                abort ? Set.of(0) : Set.of());
                Path report = RowStateAudit.defaultReport(raw);
                assertTrue(
                        assertThrows(
                                        IOException.class,
                                        () ->
                                                RowStateAudit.audit(
                                                        RowAuditTestSupport.connection(
                                                                schema,
                                                                Map.of(
                                                                        table.name(),
                                                                        List.of(
                                                                                abort
                                                                                        ? before
                                                                                        : after)),
                                                                new int[1]),
                                                        schema,
                                                        baseline,
                                                        raw,
                                                        report))
                                .getMessage()
                                .contains("Primary key value UPDATE"));
                assertFalse(Files.exists(report));
            }
    }

    @Test
    public void typeComparisonKeepsTextWhitespaceButNormalizesCharRealAndTimestamp()
            throws Exception {
        var hy = schema("hybench", 1);
        var table =
                hy.tables().stream()
                        .filter(t -> t.name().equals("company"))
                        .findFirst()
                        .orElseThrow();
        var a = row(table, "7");
        a.addProperty("loan_balance", "0.1");
        a.addProperty("province", "X ");
        a.addProperty("last_update_timestamp", "2024-01-01 00:00:00");
        var b = a.deepCopy();
        b.addProperty("loan_balance", Double.toString((double) 0.1f));
        b.addProperty("province", "X");
        b.addProperty("last_update_timestamp", "2024-01-01T00:00");
        assertEquals(SourceRow.values(table, a), SourceRow.values(table, b));
        b.addProperty("name", "different ");
        assertNotEquals(SourceRow.values(table, a), SourceRow.values(table, b));
        b.remove("name");
        assertThrows(IOException.class, () -> SourceRow.values(table, b));
    }

    @Test
    public void causalChainPreservesSessionOrderWithoutClaimingGlobalCommits() throws Exception {
        var table = schema("sysbench", 1).tables().get(0);
        var a = row(table, "7");
        var b = a.deepCopy();
        b.addProperty("k", 1);
        var c = b.deepCopy();
        c.addProperty("k", 2);
        var first =
                new RowStateAudit.Change(
                        RowAuditTestSupport.change(table.name(), "update", a, b), "a", 1);
        var second =
                new RowStateAudit.Change(
                        RowAuditTestSupport.change(table.name(), "update", b, c), "b", 1);
        assertEquals(c, RowStateAudit.chain(table, a, List.of(second, first), false).last());
        assertThrows(
                IOException.class,
                () -> RowStateAudit.chain(table, a, List.of(second, first), true));
        var reversed = new RowStateAudit.Change(second.source(), "a", 1);
        assertThrows(
                IOException.class,
                () -> RowStateAudit.chain(table, a, List.of(reversed, first), false));
    }

    @Test
    public void htapSessionsShareOneAuditAndPersistPerKeyCausalOrder() throws Exception {
        var schema = schema("sysbench", 1);
        var table = schema.tables().get(0);
        JsonObject initial = row(table, "7"),
                middle = initial.deepCopy(),
                last = initial.deepCopy();
        middle.addProperty("k", 1);
        last.addProperty("k", 2);
        Path fixture = temporary.newFolder().toPath();
        Path baseline = baseline(fixture, schema, Map.of(table.name(), List.of(initial)));
        Path raw = Files.createDirectory(fixture.resolve("raw"));
        var sessions = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 2; i++) {
            Path directory = Files.createDirectory(fixture.resolve("session-" + i));
            Path server =
                    RowAuditTestSupport.capture(
                            directory,
                            List.of(
                                    List.of(
                                            RowAuditTestSupport.change(
                                                    table.name(),
                                                    "update",
                                                    i == 0 ? middle : initial,
                                                    i == 0 ? last : middle))),
                            Set.of());
            Path client = directory.resolve("jdbc-fixture.jsonl");
            Path pg = raw.resolve("pg-" + i + ".jsonl"), jdbc = raw.resolve("jdbc-" + i + ".jsonl");
            Files.copy(server, pg);
            Files.copy(client, jdbc);
            Files.writeString(
                    pg,
                    new Gson()
                                    .toJson(
                                            Map.of(
                                                    "event",
                                                    "executor_run_end",
                                                    "application",
                                                    "fixture-0",
                                                    "pid",
                                                    1))
                            + "\n",
                    StandardOpenOption.APPEND);
            sessions.add(
                    Map.of(
                            "pg_file",
                            pg.getFileName().toString(),
                            "pg_sha256",
                            BaselineGenerator.sha256(pg),
                            "jdbc_file",
                            jdbc.getFileName().toString(),
                            "jdbc_sha256",
                            BaselineGenerator.sha256(jdbc),
                            "bindings",
                            false));
        }
        Files.writeString(
                raw.resolve("capture-run.json"),
                new Gson()
                        .toJson(
                                Map.of(
                                        "status",
                                        "complete",
                                        "session_order",
                                        "concurrent",
                                        "sessions",
                                        sessions)));
        Path report = RowStateAudit.defaultReport(raw);
        var result =
                RowStateAudit.audit(
                        RowAuditTestSupport.connection(
                                schema, Map.of(table.name(), List.of(last)), new int[1]),
                        schema,
                        baseline,
                        raw,
                        report);
        assertEquals("per_key_causal", result.get("ordering"));
        assertEquals(2L, result.get("committed_row_changes"));
        var transitions =
                Files.readAllLines(Path.of(report + ".changes.jsonl")).stream()
                        .map(line -> JsonParser.parseString(line).getAsJsonObject())
                        .toList();
        assertEquals("pg-1.jsonl", transitions.get(0).get("source_session").getAsString());
        assertEquals("pg-0.jsonl", transitions.get(1).get("source_session").getAsString());
        assertEquals(
                new JsonPrimitive("passed_source_row_state"),
                RowStateAudit.reuse(schema, baseline, raw, report).get("status"));
    }

    @Test
    public void rejectsFinalMismatchAndChangedAuditTransitions() throws Exception {
        var schema = schema("sysbench", 1);
        var table = schema.tables().get(0);
        var a = row(table, "7");
        var b = a.deepCopy();
        b.addProperty("k", 1);
        Path fixture = temporary.newFolder().toPath();
        Path baseline = baseline(fixture, schema, Map.of(table.name(), List.of(a)));
        Path raw =
                RowAuditTestSupport.capture(
                        fixture,
                        List.of(List.of(RowAuditTestSupport.change(table.name(), "update", a, b))),
                        Set.of());
        Path report = RowStateAudit.defaultReport(raw);
        assertThrows(
                IOException.class,
                () ->
                        RowStateAudit.audit(
                                RowAuditTestSupport.connection(
                                        schema, Map.of(table.name(), List.of(a)), new int[1]),
                                schema,
                                baseline,
                                raw,
                                report));
        assertFalse(Files.exists(report));
        RowStateAudit.audit(
                RowAuditTestSupport.connection(
                        schema, Map.of(table.name(), List.of(b)), new int[1]),
                schema,
                baseline,
                raw,
                report);
        Files.writeString(Path.of(report + ".changes.jsonl"), "\n", StandardOpenOption.APPEND);
        assertThrows(IOException.class, () -> RowStateAudit.reuse(schema, baseline, raw, report));
    }
}
