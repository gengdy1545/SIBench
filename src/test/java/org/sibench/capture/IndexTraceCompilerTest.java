package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.*;

import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.generation.*;
import org.sibench.replay.*;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

import java.nio.file.*;
import java.util.*;

public class IndexTraceCompilerTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private CaptureConfig baseline(Path fixture) throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var original = CaptureConfig.read(root, Path.of("config/hybench_tp_subset_sf1.yaml"));
        Path baseline = Files.createDirectory(fixture.resolve("baseline"));
        var c = original.layout();
        var layout =
                new GenerationConfig(
                        c.root(),
                        c.document(),
                        c.benchmark(),
                        c.seed(),
                        c.targetBytes(),
                        c.maxRows(),
                        c.groupBytes(),
                        c.pageBytes(),
                        c.dictionaryBytes(),
                        c.dictionary(),
                        c.codec(),
                        c.codecLevel(),
                        c.buckets(),
                        c.baselineTimestamp(),
                        baseline,
                        1);
        var files = new ArrayList<Map<String, Object>>();
        var hashes = new ArrayList<Map<String, Object>>();
        for (var sourceTable : SourceSchema.hybench(original.source()).tables()) {
            var table = Map.entry(sourceTable.name(), sourceTable.indexes().get(0).column());
            var columns = new LinkedHashMap<String, String>();
            for (var column : sourceTable.columns())
                columns.put(
                        column.name(),
                        column.type().equals("integer")
                                ? "int64"
                                : column.type().equals("real") ? "double" : "binary");
            StringBuilder text = new StringBuilder("message " + table.getKey() + " { ");
            columns.forEach(
                    (name, type) ->
                            text.append("optional ")
                                    .append(type)
                                    .append(" ")
                                    .append(name)
                                    .append(type.equals("binary") ? " (UTF8); " : "; "));
            text.append("required int64 _sib_row_id; required int64 _sib_timestamp; }");
            var schema = MessageTypeParser.parseMessageType(text.toString());
            var row = new SimpleGroupFactory(schema).newGroup();
            for (var column : sourceTable.columns()) {
                boolean indexed =
                        sourceTable.indexes().stream()
                                .anyMatch(i -> i.column().equals(column.name()));
                if (!indexed) continue;
                if (column.type().equals("integer")) row.add(column.name(), 1L);
                else
                    row.add(
                            column.name(),
                            column.type().equals("timestamp without time zone")
                                    ? "2024-01-01 00:00:00"
                                    : "initial");
            }
            int id = IndexCatalog.fromSchema(org.sibench.schema.SourceSchema.hybench(original.source())).tableId(table.getKey());
            row.add("_sib_row_id", (long) id);
            row.add("_sib_timestamp", 0L);
            Path file = baseline.resolve(table.getKey() + ".parquet");
            try (var writer =
                    new ParquetDataFile(
                            file,
                            new TableData(table.getKey(), 1, id, table.getValue(), schema, null),
                            layout,
                            Map.of())) {
                writer.write(row);
                writer.finish();
            }
            files.add(
                    Map.of(
                            "table",
                            table.getKey(),
                            "path",
                            file.getFileName().toString(),
                            "file_id",
                            id,
                            "rows",
                            1));
            hashes.add(
                    Map.of(
                            "path",
                            file.getFileName().toString(),
                            "sha256",
                            BaselineGenerator.sha256(file)));
        }
        Files.write(
                baseline.resolve("files.jsonl"), files.stream().map(new Gson()::toJson).toList());
        Files.writeString(
                baseline.resolve("schema-preflight.json"),
                "{\"status\":\"schema_verified\",\"environment\":{\"lc_collate\":\"C\"}}");
        Files.writeString(baseline.resolve("locators.tsv"), "");
        Files.writeString(
                baseline.resolve("verified-manifest.json"),
                new Gson()
                        .toJson(
                                Map.of(
                                        "status",
                                        "verified_complete",
                                        "rows",
                                        8,
                                        "files_manifest_sha256",
                                        BaselineGenerator.sha256(baseline.resolve("files.jsonl")),
                                        "locators_sha256",
                                        BaselineGenerator.sha256(baseline.resolve("locators.tsv")),
                                        "files",
                                        hashes)));
        return new CaptureConfig(
                root,
                original.source(),
                fixture,
                fixture,
                original.jdbc(),
                original.user(),
                original.seconds(),
                1,
                original.percentages(),
                layout,
                original.document());
    }

    private Path capture(Path directory, List<String> transactions) throws Exception {
        var client = new ArrayList<String>(List.of("{\"event\":\"connection\",\"pid\":1}"));
        var server = new ArrayList<String>();
        for (int i = 0; i < transactions.size(); i++) {
            String application = "fixture-" + i;
            client.add(
                    new Gson()
                            .toJson(
                                    Map.of(
                                            "event",
                                            "request_start",
                                            "application",
                                            application,
                                            "sql",
                                            "SELECT 1",
                                            "parameters",
                                            Map.of("1", 1, "2", "2024-01-02 00:00:00"))));
            client.add(
                    new Gson()
                            .toJson(Map.of("event", "request_return", "application", application)));
            server.add(
                    new Gson()
                            .toJson(
                                    Map.of(
                                            "event",
                                            "executor_start",
                                            "application",
                                            application,
                                            "pid",
                                            1,
                                            "req",
                                            i + 1,
                                            "depth",
                                            1,
                                            "query_id",
                                            42,
                                            "sql",
                                            "SELECT 1")));
            String boundary = null;
            for (String line : transactions.get(i).lines().toList()) {
                JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                event.addProperty("pid", 1);
                event.addProperty("application", application);
                event.addProperty("req", i + 1);
                event.addProperty("depth", event.get("event").getAsString().startsWith("transaction_") ? 0 : 1);
                String kind = event.get("event").getAsString();
                if (kind.startsWith("transaction_")) {
                    boundary = event.toString();
                    client.add(
                            new Gson()
                                    .toJson(
                                            Map.of(
                                                    "event",
                                                    kind.equals("transaction_commit")
                                                            ? "commit"
                                                            : "rollback",
                                                    "application",
                                                    application)));
                } else server.add(event.toString());
            }
            server.add(
                    new Gson()
                            .toJson(
                                    Map.of(
                                            "event",
                                            "executor_run_end",
                                            "application",
                                            application,
                                            "pid",
                                            1,
                                            "req",
                                            i + 1,
                                            "depth",
                                            1,
                                            "processed",
                                            0)));
            server.add(
                    new Gson()
                            .toJson(
                                    Map.of(
                                            "event",
                                            "executor_end",
                                            "application",
                                            application,
                                            "pid",
                                            1,
                                            "req",
                                            i + 1,
                                            "depth",
                                            1)));
            if (boundary != null) server.add(boundary);
        }
        client.add(
                new Gson()
                        .toJson(
                                Map.of(
                                        "event",
                                        "close",
                                        "application",
                                        "fixture-" + (transactions.size() - 1))));
        Path jdbc = directory.resolve("jdbc-fixture.jsonl");
        Files.write(jdbc, client);
        Path raw = directory.resolve("pg-1.jsonl");
        Files.write(raw, server);
        RowAuditTestSupport.complete(directory, jdbc, raw);
        return raw;
    }

    private static String change(String operation, String before, String after) {
        return """
{"event":"row_change","xid":9,"relation":"loantrans","operation":"%s","old":%s,"new":%s}
"""
                .formatted(operation, before, after);
    }

    private static String row(String timestamp) {
        return "{\"id\":1,\"applicantid\":1,\"appid\":null,\"amount\":null,\"status\":null,"
                + "\"timestamp\":\""
                + timestamp
                + "\",\"duration\":null,\"contract_timestamp\":null,"
                + "\"delinquency\":null}";
    }

    private void audit(CaptureConfig config, Path raw, String finalRow) throws Exception {
        var schema = SourceSchema.hybench(config.source());
        var contents = RowAuditTestSupport.baseline(config.layout().output());
        contents.put(
                "loantrans",
                finalRow == null
                        ? List.of()
                        : List.of(JsonParser.parseString(finalRow).getAsJsonObject()));
        RowStateAudit.audit(
                RowAuditTestSupport.connection(schema, contents, new int[1]),
                schema,
                config.layout().output(),
                raw,
                RowStateAudit.defaultReport(raw));
    }

    private static String search(String scan, int found) {
        return search(scan, found, 3, 0);
    }

    /** One bound, fetched-to-the-end index scan of idx_loantrans_2 with one scan key. */
    private static String search(String scan, int found, int strategy, int flags) {
        String next =
                "{\"event\":\"scan_next\",\"scan\":\""
                        + scan
                        + "\",\"direction\":1,\"recheck\":false,\"found\":";
        return "{\"event\":\"plan_scan\",\"scan\":\""
                + scan
                + "\",\"relation\":\"idx_loantrans_2\",\"plan_node\":1,\"kind\":\"index\"}\n"
                + "{\"event\":\"scan_rescan\",\"scan\":\""
                + scan
                + "\",\"relation\":\"idx_loantrans_2\",\"keys\":[{\"attribute\":1,\"strategy\":"
                + strategy
                + ",\"flags\":"
                + flags
                + ",\"type_oid\":1114,\"value\":\"2024-01-02 00:00:00\"}]}\n"
                + (found == 1
                        ? next
                                + "true}\n{\"event\":\"heap_visibility\",\"scan\":\""
                                + scan
                                + "\",\"found\":true}\n"
                        : "")
                + next
                + "false}\n"
                + "{\"event\":\"scan_end\",\"scan\":\""
                + scan
                + "\"}\n";
    }

    private static Map<String, Object> compile(CaptureConfig config, Path raw, Path output)
            throws Exception {
        return IndexTraceCompiler.compile(
                SourceSchema.hybench(config.source()),
                config.layout(),
                raw,
                output,
                RowStateAudit.defaultReport(raw));
    }

    private static IndexKey timestamp(String value) throws java.io.IOException {
        return IndexKey.parse("timestamp", value);
    }

    @Test
    public void allIndexesAndValueChangesPreserveIdentityHistoryQueriesAndVariants()
            throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig config = baseline(fixture);
        String first = row("2024-01-01 00:00:00"), second = row("2024-01-02 00:00:00");
        Path raw =
                capture(
                        fixture,
                        List.of(
                                change("update", first, second)
                                        + search("local", 1)
                                        + change("update", second, first)
                                        + search("restored", 0)
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n",
                                change("update", first, second)
                                        + search("aborted", 1)
                                        + "{\"event\":\"transaction_abort\",\"xid\":9}\n",
                                change("delete", first, "null")
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n",
                                change("insert", "null", second)
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n"));
        Path output = fixture.resolve("compiled"), repeat = fixture.resolve("repeat");
        audit(config, raw, second);
        Path sourceAudit = RowStateAudit.defaultReport(raw);
        String auditHash = BaselineGenerator.sha256(sourceAudit);
        var maintenance =
                IndexMaintenanceValidation.validate(
                        IndexCatalog.fromSchema(org.sibench.schema.SourceSchema.hybench(config.source())),
                        SourceSchema.hybench(config.source()),
                        config.layout().output(),
                        raw,
                        fixture.resolve("maintenance.json"),
                        sourceAudit);
        assertEquals("passed", maintenance.get("status"));
        assertEquals(auditHash, maintenance.get("row_state_audit_sha256"));
        assertEquals(auditHash, BaselineGenerator.sha256(sourceAudit));
        compile(config, raw, output);
        compile(config, raw, repeat);
        var manifest =
                JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                        .getAsJsonObject();
        assertEquals(8, manifest.get("operation_trace_version").getAsInt());
        assertEquals("hybench", manifest.get("benchmark").getAsString());
        assertEquals(21, manifest.get("baseline_operations").getAsLong());
        assertEquals(21, manifest.getAsJsonArray("index_definitions").size());
        assertEquals(1, manifest.get("discarded_aborted_row_changes").getAsLong());
        assertEquals(1, manifest.get("discarded_aborted_transaction_local_searches").getAsLong());
        try (var files = Files.list(output)) {
            for (Path file : files.toList())
                assertEquals(
                        file.getFileName().toString(),
                        -1,
                        Files.mismatch(file, repeat.resolve(file.getFileName())));
        }
        for (boolean secondary : new boolean[] {true, false})
            for (String mode : List.of("operation", "transaction")) {
                var replay = new ReplayConfig(secondary, "none", 0, 0, 0, 0, mode);
                assertEquals(
                        "passed",
                        ReferenceReplay.run(
                                        output,
                                        replay,
                                        fixture.resolve(
                                                "replay-" + secondary + "-" + mode + ".json"))
                                .get("status"));
                assertEquals(
                        "passed",
                        SnapshotLookupValidation.run(
                                        output,
                                        replay,
                                        fixture.resolve(
                                                "snapshots-" + secondary + "-" + mode + ".json"))
                                .get("status"));
                for (boolean rowIds : new boolean[] {true, false}) {
                    var model = new IndexReferenceReplay(rowIds, secondary);
                    try (var load = new ReplayProjection(replay, false, model::accept);
                            var runtime = new ReplayProjection(replay, true, model::accept)) {
                        IndexOperationTrace.read(output.resolve("baseline.ops"), load);
                        load.finish();
                        IndexOperationTrace.read(output.resolve("workload.ops"), runtime);
                        runtime.finish();
                    }
                    IndexKey one = IndexKey.ofInt(1);
                    IndexKey before = timestamp("2024-01-01 00:00:00");
                    IndexKey after = timestamp("2024-01-02 00:00:00");
                    assertNotNull(model.lookupPrimary(8, one, 1));
                    assertNull(model.lookupPrimary(8, one, 2));
                    assertEquals(3, model.lookupPrimary(8, one, 3).commitVersion());
                    if (rowIds) {
                        assertEquals(8, model.lookupPrimary(8, one, 1).value());
                        assertEquals(9, model.lookupPrimary(8, one, 3).value());
                    }
                    if (secondary) {
                        assertEquals(1, model.lookupSecondary(8, 2, before, 0).size());
                        assertEquals(1, model.lookupSecondary(8, 2, before, 1).size());
                        assertTrue(model.lookupSecondary(8, 2, after, 1).isEmpty());
                        assertTrue(model.lookupSecondary(8, 2, before, 3).isEmpty());
                        assertEquals(1, model.lookupSecondary(8, 2, after, 3).size());
                    }
                }
            }
        RelocationCheck.validate(config.layout(), output, fixture.resolve("relocated"));
        var relocated =
                JsonParser.parseString(Files.readString(fixture.resolve("relocated/report.json")))
                        .getAsJsonObject();
        assertEquals(3, relocated.get("retained_index_entries").getAsLong());
        var sourceManifest = manifest.deepCopy();
        sourceManifest.getAsJsonArray("index_definitions").remove(20);
        Files.writeString(output.resolve("manifest.json"), new Gson().toJson(sourceManifest));
        assertThrows(
                java.io.IOException.class,
                () -> ReferenceReplay.check(output, ReplayConfig.defaults()));
        Files.writeString(output.resolve("manifest.json"), new Gson().toJson(manifest));
        var records = new ArrayList<IndexOperationTrace.Operation>();
        IndexOperationTrace.read(output.resolve("baseline.ops"), records::add);
        Files.delete(output.resolve("baseline.ops"));
        try (var writer = new IndexOperationTrace.Writer(output.resolve("baseline.ops"))) {
            for (int i = 0; i < records.size() - 1; i++) writer.append(records.get(i));
        }
        manifest.addProperty(
                "baseline_trace_sha256", BaselineGenerator.sha256(output.resolve("baseline.ops")));
        manifest.addProperty("baseline_operations", records.size() - 1);
        Files.writeString(output.resolve("manifest.json"), new Gson().toJson(manifest));
        assertThrows(
                java.io.IOException.class,
                () -> ReferenceReplay.check(output, ReplayConfig.defaults()));
    }

    @Test
    public void rejectsMissingBeforeImagesAndUnsupportedPredicates() throws Exception {
        for (String request :
                List.of(
                        "{\"event\":\"row_change\",\"xid\":9,\"relation\":\"loantrans\",\"operation\":\"update\","
                            + "\"new\":"
                                + row("2024-01-02 00:00:00")
                                + "}\n",
                        // An array key (= ANY) is not one interval.
                        search("s", 1, 3, AccessUnits.SK_SEARCHARRAY))) {
            Path fixture = temporary.newFolder().toPath();
            var config = baseline(fixture);
            Path raw =
                    capture(
                            fixture,
                            List.of(request + "{\"event\":\"transaction_commit\",\"xid\":9}\n"));
            if (request.contains("scan_rescan")) audit(config, raw, row("2024-01-01 00:00:00"));
            else
                assertThrows(
                        java.io.IOException.class,
                        () -> audit(config, raw, row("2024-01-02 00:00:00")));
            assertThrows(
                    java.io.IOException.class,
                    () -> compile(config, raw, fixture.resolve("compiled")));
            assertFalse(Files.exists(fixture.resolve("compiled/manifest.json")));
        }
    }

    @Test
    public void rangeScansCompileToOrderedSearchesThatReplay() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        var config = baseline(fixture);
        String first = row("2024-01-01 00:00:00");
        // timestamp <= '2024-01-02' finds the baseline row; > '2024-01-02' finds nothing.
        Path raw =
                capture(
                        fixture,
                        List.of(
                                search("le", 1, 2, 0)
                                        + search("gt", 0, 5, 0)
                                        + "{\"event\":\"transaction_commit\",\"xid\":0}\n"));
        audit(config, raw, first);
        Path output = fixture.resolve("compiled");
        var manifest = compile(config, raw, output);
        assertEquals(Map.of("range", 2L), manifest.get("searches_by_shape"));
        var searches = new ArrayList<IndexOperationTrace.Operation>();
        IndexOperationTrace.read(
                output.resolve("workload.ops"),
                o -> {
                    if (o.kind() == IndexOperationTrace.SEARCH) searches.add(o);
                });
        var atMost = searches.get(0).range();
        assertNull(atMost.lower());
        assertEquals(timestamp("2024-01-02 00:00:00"), atMost.upper());
        assertTrue(atMost.upperInclusive());
        var above = searches.get(1).range();
        assertEquals(timestamp("2024-01-02 00:00:00"), above.lower());
        assertFalse(above.lowerInclusive());
        // A comparison never matches NULL, which sorts after every value.
        assertEquals(IndexKey.NULL, above.upper());
        assertFalse(above.upperInclusive());
        assertEquals(List.of(1L, 0L), searches.stream().map(o -> o.expectedCount()).toList());
        assertEquals(
                "passed",
                ReferenceReplay.run(output, ReplayConfig.defaults(), fixture.resolve("replay.json"))
                        .get("status"));
    }

    @Test
    public void abortDoesNotAllocateVersionsOrRowIdsAndPrewriteReadsSurvive() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        var config = baseline(fixture);
        String first = row("2024-01-01 00:00:00"), second = row("2024-01-02 00:00:00");
        Path raw =
                capture(
                        fixture,
                        List.of(
                                search("before", 0)
                                        + change("delete", first, "null")
                                        + change("insert", "null", second)
                                        + search("after", 1)
                                        + "{\"event\":\"transaction_abort\",\"xid\":9}\n",
                                change("delete", first, "null")
                                        + change("insert", "null", second)
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n",
                                "{\"event\":\"transaction_abort\",\"xid\":9}\n",
                                search("published", 1)
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n"));
        Path output = fixture.resolve("compiled");
        audit(config, raw, second);
        compile(config, raw, output);
        JsonObject manifest =
                JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                        .getAsJsonObject();
        assertEquals(2, manifest.get("discarded_aborted_row_changes").getAsLong());
        assertEquals(1, manifest.get("discarded_aborted_transaction_local_searches").getAsLong());
        assertEquals(2, manifest.get("searches").getAsLong());
        var writes = new ArrayList<IndexOperationTrace.Operation>();
        IndexOperationTrace.read(output.resolve("workload.ops"), writes::add);
        var insertion =
                writes.stream()
                        .filter(o -> o.kind() == IndexOperationTrace.UPSERT && o.index() == 0)
                        .findFirst()
                        .orElseThrow();
        assertEquals(1, insertion.version());
        assertEquals(9, insertion.rowId());
        assertEquals(
                "passed",
                SnapshotLookupValidation.run(
                                output, ReplayConfig.defaults(), fixture.resolve("oracle.json"))
                        .get("status"));
    }

    @Test
    public void rejectsWrongAbortIdentityAndUnfinishedScansOrTransactions() throws Exception {
        for (String request :
                List.of(
                        change("update", row("2024-01-01 00:00:00"), row("2024-01-02 00:00:00"))
                                + "{\"event\":\"transaction_abort\",\"xid\":10}\n",
                        "{\"event\":\"scan_rescan\",\"scan\":\"s\",\"relation\":\"idx_loantrans_2\",\"keys\":[{\"strategy\":3,\"flags\":0,\"value\":\"2024-01-02"
                            + " 00:00:00\"}]}\n"
                            + "{\"event\":\"transaction_commit\",\"xid\":9}\n",
                        change("update", row("2024-01-01 00:00:00"), row("2024-01-02 00:00:00")))) {
            Path fixture = temporary.newFolder().toPath();
            var config = baseline(fixture);
            Path raw = capture(fixture, List.of(request));
            assertThrows(
                    Exception.class,
                    () -> compile(config, raw, fixture.resolve("compiled")));
            assertFalse(Files.exists(fixture.resolve("compiled/manifest.json")));
        }
    }

    @Test
    public void sourceAuditIsRequiredBeforeOutputAndRejectsPrimaryKeyRewriteEvenAfterAbort()
            throws Exception {
        Path fixture = temporary.newFolder().toPath();
        var config = baseline(fixture);
        String initial = row("2024-01-01 00:00:00");
        Path raw =
                capture(
                        fixture,
                        List.of(
                                change("update", initial, initial)
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n"));
        Path output = fixture.resolve("compiled");
        assertTrue(
                assertThrows(
                                java.io.IOException.class,
                                () -> compile(config, raw, output))
                        .getMessage()
                        .contains("row audit required"));
        assertFalse(Files.exists(output));

        for (String boundary : List.of("transaction_commit", "transaction_abort")) {
            Path another = temporary.newFolder().toPath();
            var next = baseline(another);
            Path rewrite =
                    capture(
                            another,
                            List.of(
                                    change(
                                                    "update",
                                                    initial,
                                                    initial.replace("\"id\":1", "\"id\":2"))
                                            + "{\"event\":\""
                                            + boundary
                                            + "\",\"xid\":9}\n"));
            assertTrue(
                    assertThrows(java.io.IOException.class, () -> audit(next, rewrite, initial))
                            .getMessage()
                            .contains("Primary key value UPDATE"));
            assertFalse(Files.exists(RowStateAudit.defaultReport(rewrite)));
        }
    }
}
