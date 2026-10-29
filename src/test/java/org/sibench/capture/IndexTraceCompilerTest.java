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
        return baseline(fixture, -1, -1);
    }

    /** A target size and row-group limit below zero keep the profile's own. */
    private CaptureConfig baseline(Path fixture, long targetBytes, long maxRows) throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var original = CaptureConfig.read(root, Path.of("config/hybench_sf1.yaml"));
        Path baseline = Files.createDirectory(fixture.resolve("baseline"));
        var c = original.layout();
        var layout =
                new GenerationConfig(
                        c.root(),
                        c.document(),
                        c.benchmark(),
                        c.seed(),
                        targetBytes < 0 ? c.targetBytes() : targetBytes,
                        maxRows < 0 ? c.maxRows() : maxRows,
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
                layout,
                original.document());
    }

    private Path capture(Path directory, List<String> transactions) throws Exception {
        var client = new ArrayList<String>(List.of("{\"event\":\"connection\",\"pid\":1}"));
        var server = new ArrayList<String>();
        server.add(header(1, "rows"));
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
                                            0,
                                            "returned",
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
        Path jdbc = RowAuditTestSupport.writeTrace(directory.resolve("jdbc-fixture.jsonl.zst"), client);
        Path raw = RowAuditTestSupport.writeTrace(directory.resolve("pg-1.jsonl.zst"), server);
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
        audit(
                config,
                raw,
                finalRow == null
                        ? Map.of("loantrans", List.of())
                        : Map.of(
                                "loantrans",
                                List.of(JsonParser.parseString(finalRow).getAsJsonObject())));
    }

    private void audit(
            CaptureConfig config, Path raw, Map<String, List<JsonObject>> finalRows)
            throws Exception {
        var schema = SourceSchema.hybench(config.source());
        var contents = RowAuditTestSupport.baseline(config.layout().output());
        contents.putAll(finalRows);
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
        return search(
                scan,
                "idx_loantrans_2",
                1114,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                "1",
                found,
                strategy,
                flags);
    }

    private static String search(
            String scan,
            String relation,
            int typeOid,
            String value,
            String indexed,
            String pk,
            int found,
            int strategy,
            int flags) {
        String next =
                "{\"event\":\"scan_next\",\"scan\":\""
                        + scan
                        + "\",\"direction\":1,\"recheck\":false,\"found\":";
        return "{\"event\":\"plan_scan\",\"scan\":\""
                + scan
                + "\",\"relation\":\""
                + relation
                + "\",\"plan_node\":1,\"kind\":\"index\"}\n"
                + "{\"event\":\"scan_rescan\",\"scan\":\""
                + scan
                + "\",\"relation\":\""
                + relation
                + "\",\"keys\":[{\"attribute\":1,\"strategy\":"
                + strategy
                + ",\"flags\":"
                + flags
                + ",\"type_oid\":"
                + typeOid
                + ",\"value\":\""
                + value
                + "\"}]}\n"
                + (found == 1
                        ? next
                                + "true}\n{\"event\":\"heap_visibility\",\"scan\":\""
                                + scan
                                + "\",\"found\":true,\"row\":{\"indexed\":[\""
                                + indexed
                                + "\"],\"pk\":[\""
                                + pk
                                + "\"]}}\n"
                        : "")
                + next
                + "false}\n"
                + "{\"event\":\"scan_end\",\"scan\":\""
                + scan
                + "\"}\n";
    }

    /** One request of a backend trace: its executor start, the given events and its return. */
    private static List<String> request(int pid, long ordinal, String sql, List<String> events) {
        var result = new ArrayList<String>();
        result.add(
                new Gson()
                        .toJson(
                                Map.<String, Object>of(
                                        "event", "executor_start",
                                        "pid", pid,
                                        "req", ordinal,
                                        "depth", 1,
                                        "application", "sibench:fixture",
                                        "sql", sql,
                                        "query_id", 42)));
        for (String line : events) {
            var event = JsonParser.parseString(line).getAsJsonObject();
            event.addProperty("pid", pid);
            event.addProperty("req", ordinal);
            event.addProperty("depth", 1);
            result.add(event.toString());
        }
        result.add(
                new Gson()
                        .toJson(
                                Map.<String, Object>of(
                                        "event", "executor_run_end",
                                        "pid", pid,
                                        "req", ordinal,
                                        "depth", 1,
                                        "processed", 0,
                                        "returned", 0)));
        result.add(
                new Gson()
                        .toJson(
                                Map.<String, Object>of(
                                        "event", "executor_end",
                                        "pid", pid,
                                        "req", ordinal,
                                        "depth", 1)));
        return result;
    }

    /** The commit record of one transaction, carrying PostgreSQL's completion counter. */
    private static String commit(int pid, long ordinal, long xid, long csn) {
        // The merge detects transaction records by their leading event name, as the capture writes
        // it, so the order of members matters here.
        var record = new LinkedHashMap<String, Object>();
        record.put("event", "transaction_commit");
        record.put("pid", pid);
        record.put("xid", xid);
        record.put("cid", 0);
        record.put("req", ordinal);
        record.put("depth", 0);
        record.put("csn", csn);
        return new Gson().toJson(record);
    }

    /** A fetched-to-the-end scan whose rescan carries the snapshot PostgreSQL took for it. */
    private static List<String> snapshotSearch(
            String scan, String value, long csn, String xip, long xmax, boolean suboverflowed) {
        return snapshotSearch(
                scan,
                "idx_loantrans_2",
                1114,
                value,
                value,
                "1",
                csn,
                xip,
                xmax,
                suboverflowed);
    }

    private static List<String> snapshotSearch(
            String scan,
            String relation,
            int typeOid,
            String value,
            String indexed,
            String pk,
            long csn,
            String xip,
            long xmax,
            boolean suboverflowed) {
        var lines = new ArrayList<String>();
        for (String line :
                search(scan, relation, typeOid, value, indexed, pk, 1, 3, 0).lines().toList()) {
            var event = JsonParser.parseString(line).getAsJsonObject();
            if (event.get("event").getAsString().equals("scan_rescan")) {
                event.getAsJsonArray("keys").get(0).getAsJsonObject().addProperty("value", value);
                event.addProperty("csn", csn);
                event.addProperty("xmin", xmax - 1);
                event.addProperty("xmax", xmax);
                event.addProperty("suboverflowed", suboverflowed);
                event.add("xip", JsonParser.parseString(xip).getAsJsonArray());
            }
            lines.add(event.toString());
        }
        return lines;
    }

    private static String rowChange(long xid, String before, String after) {
        return "{\"event\":\"row_change\",\"xid\":"
                + xid
                + ",\"relation\":\"loantrans\",\"operation\":\"update\",\"old\":"
                + before
                + ",\"new\":"
                + after
                + "}";
    }

    /** One row change on any table; a missing side is the JSON null an insert/delete carries. */
    private static String rowChange(
            String relation, long xid, String operation, JsonObject before, JsonObject after) {
        var event = new JsonObject();
        event.addProperty("event", "row_change");
        event.addProperty("xid", xid);
        event.addProperty("relation", relation);
        event.addProperty("operation", operation);
        if (before == null) event.add("old", com.google.gson.JsonNull.INSTANCE);
        else event.add("old", before);
        if (after == null) event.add("new", com.google.gson.JsonNull.INSTANCE);
        else event.add("new", after);
        return event.toString();
    }

    private Path concurrentCapture(Path directory, List<List<String>> sessions) throws Exception {
        var servers = new ArrayList<Path>();
        for (int i = 0; i < sessions.size(); i++) {
            var lines = new ArrayList<>(sessions.get(i));
            if (lines.isEmpty() || !lines.get(0).contains("\"capture_start\""))
                lines.add(0, header(101 + i, "rows"));
            Path server =
                    RowAuditTestSupport.writeTrace(
                            directory.resolve("pg-" + (101 + i) + ".jsonl.zst"), lines);
            servers.add(server);
        }
        RowAuditTestSupport.completeConcurrent(directory, servers);
        return directory;
    }

    /**
     * Two sessions that overlap: the second reads after the first updated, but its snapshot was
     * taken while that update was still running, so it must read the version before the update.
     */
    private List<List<String>> overlappingSessions(String before, String after, long updateCounter)
            throws Exception {
        var writer = new ArrayList<String>();
        writer.addAll(
                request(101, 1, "SELECT 1", snapshotSearch("a1", "2024-01-01 00:00:00", 100, "[]", 51, false)));
        writer.add(commit(101, 1, 0, 100));
        writer.addAll(request(101, 2, "UPDATE loantrans SET timestamp = ?", List.of(rowChange(50, before, after))));
        writer.add(commit(101, 2, 50, updateCounter));
        var reader = new ArrayList<String>();
        reader.addAll(
                request(102, 1, "SELECT 1", snapshotSearch("b1", "2024-01-01 00:00:00", 150, "[50]", 51, false)));
        reader.add(commit(102, 1, 0, 250));
        return List.of(writer, reader);
    }

    @Test
    public void concurrentSessionsReadTheCommitTheirSnapshotSaw() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig config = baseline(fixture);
        String before = row("2024-01-01 00:00:00"), after = row("2024-01-02 00:00:00");
        Path raw =
                concurrentCapture(fixture, overlappingSessions(before, after, 200));
        audit(config, raw, after);
        Path output = fixture.resolve("compiled");
        compile(config, raw, output);
        var manifest =
                JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                        .getAsJsonObject();
        assertEquals("concurrent", manifest.get("session_order").getAsString());
        assertEquals(3, manifest.get("committed_transactions").getAsLong());
        assertEquals(2, manifest.get("search_operations").getAsLong());
        // The reader's snapshot never saw the update, so it reads behind the commit before it.
        assertEquals(1, manifest.get("snapshot_searches").getAsLong());
        var crosscheck = manifest.getAsJsonObject("snapshot_crosscheck");
        assertEquals(2, crosscheck.get("searches_checked").getAsLong());
        assertEquals(0, crosscheck.get("searches_without_usable_snapshot").getAsLong());
        assertEquals(
                "passed",
                ReferenceReplay.run(
                                output,
                                new ReplayConfig(true, "none", 0, 0, 0, 0, "operation"),
                                fixture.resolve("replay.json"))
                        .get("status"));
        assertEquals(
                "passed",
                SnapshotLookupValidation.run(
                                output,
                                new ReplayConfig(true, "none", 0, 0, 0, 0, "operation"),
                                fixture.resolve("snapshots.json"))
                        .get("status"));
    }


    private static String header(int pid, String detail) {
        return "{\"event\":\"capture_start\",\"pid\":"
                + pid
                + ",\"xid\":0,\"cid\":0,\"req\":0,\"depth\":0,\"format\":2,\"detail\":\""
                + detail
                + "\"}";
    }
    /**
     * A backend trace as the summary capture writes it: a header, no per-row events, one summary
     * per access right before the event that ends it, result rows counted at the return.
     */
    private static List<String> summarized(int pid, List<String> rows) {
        var result = new ArrayList<String>();
        result.add("{\"event\":\"capture_start\",\"pid\":" + pid + ",\"xid\":0,\"cid\":0,\"req\":0,\"depth\":0,"
                + "\"format\":2,\"detail\":\"summary\"}");
        var counts = new LinkedHashMap<String, long[]>();
        for (String line : rows) {
            var event = JsonParser.parseString(line).getAsJsonObject();
            String type = event.get("event").getAsString();
            switch (type) {
                case "scan_next" -> {
                    long[] c = counts.get(event.get("scan").getAsString());
                    c[0]++;
                    if (event.get("found").getAsBoolean()) c[1]++;
                    c[2] = event.get("found").getAsBoolean() ? 1 : 0;
                    continue;
                }
                case "heap_visibility" -> {
                    long[] c = counts.get(event.get("scan").getAsString());
                    c[3]++;
                    if (event.get("found").getAsBoolean()) c[4]++;
                    continue;
                }
                case "scan_end" -> {
                    long[] c = counts.remove(event.get("scan").getAsString());
                    var summary = JsonParser.parseString(event.toString()).getAsJsonObject();
                    summary.addProperty("event", "scan_summary");
                    summary.addProperty("calls", c[0]);
                    summary.addProperty("found", c[1]);
                    summary.addProperty("last_found", c[2] == 1);
                    summary.addProperty("forward", c[0] > 0);
                    summary.addProperty("backward", false);
                    summary.addProperty("recheck", 0);
                    summary.addProperty("heap_fetches", c[3]);
                    summary.addProperty("heap_visible", c[4]);
                    summary.addProperty("vm_visible", 0);
                    summary.addProperty("bitmap_tids", 0);
                    summary.addProperty("bitmap_distinct", 0);
                    summary.addProperty("bitmap_unresolved", 0);
                    summary.addProperty("bitmap_ended", false);
                    summary.addProperty("identity_missing", false);
                    result.add(summary.toString());
                }
                case "scan_rescan" -> counts.put(event.get("scan").getAsString(), new long[5]);
                case "executor_run_end" -> {
                    event.addProperty("returned", 0);
                    event.add("row_hashes", new com.google.gson.JsonArray());
                    result.add(event.toString());
                    continue;
                }
                default -> {}
            }
            result.add(line);
        }
        return result;
    }

    /** Concurrent compilation of a summary capture: every merged part keeps its file's format. */
    @Test
    public void concurrentSummaryCapturesCompileLikeTheirRows() throws Exception {
        Path rowsFixture = temporary.newFolder().toPath(), summaryFixture = temporary.newFolder().toPath();
        String before = row("2024-01-01 00:00:00"), after = row("2024-01-02 00:00:00");
        var sessions = new ArrayList<>(overlappingSessions(before, after, 200));
        // A later transaction of the writer searches too, so a summary sits in a later merged part.
        var writer = new ArrayList<>(sessions.get(0));
        writer.addAll(request(101, 3, "SELECT 1", snapshotSearch("a3", "2024-01-02 00:00:00", 260, "[]", 52, false)));
        writer.add(commit(101, 3, 0, 300));
        sessions.set(0, writer);
        var manifests = new ArrayList<JsonObject>();
        for (Path fixture : List.of(rowsFixture, summaryFixture)) {
            CaptureConfig config = baseline(fixture);
            var input = fixture == rowsFixture
                    ? sessions
                    : List.of(summarized(101, sessions.get(0)), summarized(102, sessions.get(1)));
            Path raw = concurrentCapture(fixture, input);
            audit(config, raw, after);
            Path output = fixture.resolve("compiled");
            compile(config, raw, output);
            manifests.add(JsonParser.parseString(Files.readString(output.resolve("manifest.json"))).getAsJsonObject());
            assertEquals("passed", ReferenceReplay.run(output, new ReplayConfig(true, "none", 0, 0, 0, 0, "operation"),
                    fixture.resolve("replay.json")).get("status"));
        }
        for (String key : List.of("committed_transactions", "search_operations", "snapshot_searches", "workload_operations"))
            assertEquals(key, manifests.get(0).get(key), manifests.get(1).get(key));
        assertEquals(3, manifests.get(1).get("search_operations").getAsLong());
    }

    /**
     * T1 updates A and then reads B. T2 moves B out of B's index value and inserts C into it, with
     * the same result count, before T1 commits. T1's search keeps its own snapshot: B0, not C.
     */
    @Test
    public void transactionLocalSearchKeepsItsSnapshotAndOnlyItsOwnWrites() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig config = baseline(fixture);
        var contents = RowAuditTestSupport.baseline(config.layout().output());
        JsonObject loanBefore = contents.get("loantrans").get(0).deepCopy();
        JsonObject loanAfter = loanBefore.deepCopy();
        loanAfter.addProperty("timestamp", "2024-01-02 00:00:00");
        JsonObject transferBefore = contents.get("transfer").get(0).deepCopy();
        JsonObject transferAfter = transferBefore.deepCopy();
        transferAfter.addProperty("sourceid", 2);
        JsonObject transferNew = transferBefore.deepCopy();
        transferNew.addProperty("id", 2);

        var readerEvents = new ArrayList<String>();
        readerEvents.add(rowChange("loantrans", 50, "update", loanBefore, loanAfter));
        readerEvents.addAll(
                snapshotSearch(
                        "b1", "idx_transfer_1", 23, "1", "1", "1", 150, "[60]", 61, false));
        var writerEvents = new ArrayList<String>();
        writerEvents.add(rowChange("transfer", 60, "update", transferBefore, transferAfter));
        writerEvents.add(rowChange("transfer", 60, "insert", null, transferNew));
        var reader = new ArrayList<String>();
        reader.addAll(
                request(
                        101,
                        1,
                        "UPDATE loantrans; SELECT transfer",
                        readerEvents));
        reader.add(commit(101, 1, 50, 300));
        var writer = new ArrayList<String>();
        writer.addAll(
                request(
                        102,
                        1,
                        "UPDATE transfer; INSERT transfer",
                        writerEvents));
        writer.add(commit(102, 1, 60, 200));

        Path raw = concurrentCapture(fixture, List.of(reader, writer));
        audit(
                config,
                raw,
                Map.of(
                        "loantrans", List.of(loanAfter),
                        "transfer", List.of(transferAfter, transferNew)));
        Path output = fixture.resolve("compiled");
        var manifest = compile(config, raw, output);
        assertEquals(1L, ((Number) manifest.get("transaction_local_searches")).longValue());
        assertEquals(2L, ((Number) manifest.get("committed_transactions")).longValue());

        var local = new ArrayList<IndexOperationTrace.Operation>();
        var workload = new ArrayList<IndexOperationTrace.Operation>();
        IndexOperationTrace.read(
                output.resolve("workload.ops"),
                operation -> {
                    workload.add(operation);
                    if (operation.kind() == IndexOperationTrace.SEARCH
                            && (operation.flags()
                                            & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
                                    != 0) local.add(operation);
                });
        assertEquals(1, local.size());
        var search = local.get(0);
        assertEquals(2, search.version());
        assertEquals(0, search.readTimestamp());
        assertEquals(1, search.mutationOrdinal());

        try (var model = new IndexReferenceReplay(true, true)) {
            try (var load =
                            new ReplayProjection(
                                    ReplayConfig.defaults(), false, model::accept);
                    var runtime =
                            new ReplayProjection(
                                    ReplayConfig.defaults(), true, model::accept)) {
                org.sibench.capture.BaselineOperations.of(output).read(load);
                load.finish();
                IndexOperationTrace.read(output.resolve("workload.ops"), runtime);
                runtime.finish();
            }
            var results =
                    model.search(
                            search.table(),
                            search.index(),
                            search.range(),
                            search.readTimestamp(),
                            search.version(),
                            search.transactionId(),
                            search.mutationOrdinal());
            assertEquals(1, results.size());
            assertEquals(IndexKey.ofInt(1), results.get(0).key());
            assertEquals(0, results.get(0).commitVersion());
            // The former single-timestamp projection reads T1's commit version and returns C,
            // which has the same count but is a different row: the counterexample this fixes.
            var old =
                    model.search(
                            search.table(),
                            search.index(),
                            search.range(),
                            search.version(),
                            0,
                            0,
                            0);
            assertEquals(1, old.size());
            assertEquals(IndexKey.ofInt(2), old.get(0).key());
        }

        assertEquals(
                "passed",
                ReferenceReplay.run(
                                output,
                                ReplayConfig.defaults(),
                                fixture.resolve("replay.json"))
                        .get("status"));
        assertEquals(
                "passed",
                SnapshotLookupValidation.run(
                                output,
                                ReplayConfig.defaults(),
                                fixture.resolve("snapshots.json"))
                        .get("status"));
        var service =
                new org.sibench.adapters.ServiceConfig(
                        "rocksdb",
                        null,
                        fixture.resolve("service-index"),
                        0,
                        org.sibench.adapters.proto.ValueKind.ROW_ID,
                        null,
                        false);
        var served =
                org.sibench.adapters.ServiceReplay.run(
                        output,
                        ReplayConfig.defaults(),
                        service,
                        null,
                        fixture.resolve("service.json"));
        assertEquals("passed", served.get("status"));
        var servedResult = (Map<?, ?>) served.get("result");
        assertEquals(1L, ((Number) servedResult.get("per_query_digest_checks")).longValue());
        var referenceRowId = com.google.gson.JsonParser.parseString(
                        Files.readString(fixture.resolve("replay.json")))
                .getAsJsonObject().getAsJsonArray("results").get(1).getAsJsonObject();
        assertEquals("rowid", referenceRowId.get("mode").getAsString());
        assertEquals(referenceRowId.get("query_digest_sha256").getAsString(),
                servedResult.get("expected_results_sha256"));
        // Reopened, the index has nothing to check here: the trace's one search is
        // transaction-local and its answer is gone once the transaction's later writes landed.
        var reopened =
                new org.sibench.adapters.ServiceConfig(
                        "rocksdb",
                        null,
                        fixture.resolve("service-index"),
                        0,
                        org.sibench.adapters.proto.ValueKind.ROW_ID,
                        null,
                        false);
        var verified =
                (Map<?, ?>)
                        org.sibench.adapters.ServiceReplay.verify(
                                        output,
                                        ReplayConfig.defaults(),
                                        reopened,
                                        fixture.resolve("replay.json"),
                                        fixture.resolve("verify.json"))
                                .get("result");
        assertEquals(0L, ((Number) verified.get("per_query_digest_checks")).longValue());
        assertEquals(1L, ((Number) verified.get("unchecked_transaction_local_queries")).longValue());
        // A locator-only reference replay reports that mode alone.
        var locatorOnly =
                ReferenceReplay.run(
                        output, ReplayConfig.defaults(), List.of(false), fixture.resolve("replay-locator.json"));
        assertEquals(
                List.of("locator"),
                ((List<?>) locatorOnly.get("results"))
                        .stream()
                        .map(item -> ((Map<?, ?>) item).get("mode"))
                        .toList());
    }

    @Test
    public void concurrentCompilationRejectsACounterThatDisagreesWithItsSnapshot()
            throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig config = baseline(fixture);
        String before = row("2024-01-01 00:00:00"), after = row("2024-01-02 00:00:00");
        // The update commits at counter 120, before the reader's snapshot at 150, yet the reader's
        // snapshot still lists XID 50 as running: no single read timestamp can express it.
        Path raw = concurrentCapture(fixture, overlappingSessions(before, after, 120));
        audit(config, raw, after);
        Path output = fixture.resolve("compiled");
        var failure = assertThrows(java.io.IOException.class, () -> compile(config, raw, output));
        assertTrue(failure.getMessage(), failure.getMessage().contains("one read timestamp"));
        assertTrue(Files.exists(output.resolve("INCOMPLETE")));
    }

    @Test
    public void concurrentCompilationSkipsSnapshotsItCannotCrossCheck() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig config = baseline(fixture);
        String before = row("2024-01-01 00:00:00"), after = row("2024-01-02 00:00:00");
        var sessions = overlappingSessions(before, after, 200);
        var reader = new ArrayList<String>();
        reader.addAll(
                request(
                        102,
                        1,
                        "SELECT 1",
                        snapshotSearch("b1", "2024-01-01 00:00:00", 150, "[50]", 51, true)));
        reader.add(commit(102, 1, 0, 250));
        Path raw = concurrentCapture(fixture, List.of(sessions.get(0), reader));
        audit(config, raw, after);
        Path output = fixture.resolve("compiled");
        compile(config, raw, output);
        var crosscheck =
                JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                        .getAsJsonObject()
                        .getAsJsonObject("snapshot_crosscheck");
        assertEquals(1, crosscheck.get("searches_checked").getAsLong());
        assertEquals(1, crosscheck.get("searches_without_usable_snapshot").getAsLong());
        verifyReopenedPublishedIndex(fixture, output);
    }

    /**
     * Loads the trace into a published index, reopens it and checks every snapshot search again;
     * an index that claims the same watermark without the trace's entries must fail.
     */
    private static void verifyReopenedPublishedIndex(Path fixture, Path output) throws Exception {
        var reference = fixture.resolve("verify-reference.json");
        assertEquals("passed", ReferenceReplay.run(output, ReplayConfig.defaults(), reference).get("status"));
        var service = publishedInProcess(fixture.resolve("verify-index"));
        assertEquals(
                "passed",
                org.sibench.adapters.ServiceReplay.run(
                                output, ReplayConfig.defaults(), service, reference, fixture.resolve("verify-load.json"))
                        .get("status"));
        var verified =
                (Map<?, ?>)
                        org.sibench.adapters.ServiceReplay.verify(
                                        output,
                                        ReplayConfig.defaults(),
                                        service,
                                        reference,
                                        fixture.resolve("verify.json"))
                                .get("result");
        var manifest =
                JsonParser.parseString(Files.readString(fixture.resolve("verify-load.json.index-manifest.json")))
                        .getAsJsonObject();
        var traceManifest = JsonParser.parseString(Files.readString(output.resolve("manifest.json"))).getAsJsonObject();
        assertEquals(traceManifest.get("workload_trace_sha256"), manifest.get("workload_trace_sha256"));
        assertEquals(traceManifest.get("index_definitions"), manifest.get("index_definitions"));
        assertEquals(verified.get("published_version"), manifest.get("commit_version").getAsLong());
        assertEquals("encoded_key", manifest.get("versioning_mode").getAsString());
        assertEquals("N/A", manifest.get("source_snapshot_id").getAsString());
        assertTrue(manifest.get("index_binding").getAsString().endsWith("mode=rowid"));
        // An index bound to another trace is refused before any query, even at the same watermark.
        long published = manifest.get("commit_version").getAsLong();
        try (var foreign =
                org.sibench.adapters.RocksDbBackend.create(
                        fixture.resolve("verify-foreign-index"), org.sibench.adapters.proto.ValueKind.ROW_ID)) {
            foreign.bind("another-trace mode=rowid");
            foreign.recordPublishedVersion(published);
            foreign.flush();
        }
        var mismatch =
                assertThrows(
                        java.io.IOException.class,
                        () ->
                                org.sibench.adapters.ServiceReplay.verify(
                                        output,
                                        ReplayConfig.defaults(),
                                        publishedInProcess(fixture.resolve("verify-foreign-index")),
                                        reference,
                                        fixture.resolve("verify-foreign.json")));
        assertTrue(mismatch.getMessage(), mismatch.getMessage().contains("Index holds another-trace"));
        long checked = ((Number) verified.get("per_query_digest_checks")).longValue();
        assertTrue(checked > 0);
        assertEquals(0L, ((Number) verified.get("per_query_digest_mismatches")).longValue());
        assertTrue(((Number) verified.get("checked_result_items")).longValue() > 0);
        long watermark = ((Number) verified.get("published_version")).longValue();
        assertTrue(watermark > 0);
        try (var empty =
                org.sibench.adapters.RocksDbBackend.create(
                        fixture.resolve("verify-empty-index"), org.sibench.adapters.proto.ValueKind.ROW_ID)) {
            empty.recordPublishedVersion(watermark);
            empty.flush();
        }
        var wrong =
                assertThrows(
                        java.io.IOException.class,
                        () ->
                                org.sibench.adapters.ServiceReplay.verify(
                                        output,
                                        ReplayConfig.defaults(),
                                        publishedInProcess(fixture.resolve("verify-empty-index")),
                                        reference,
                                        fixture.resolve("verify-empty.json")));
        var message = new StringBuilder();
        for (Throwable cause = wrong; cause != null; cause = cause.getCause())
            message.append(cause.getMessage()).append(" | ");
        assertTrue(message.toString(), message.toString().contains("Search count mismatch"));
    }

    private static org.sibench.adapters.ServiceConfig publishedInProcess(Path index) {
        return new org.sibench.adapters.ServiceConfig(
                "rocksdb",
                null,
                index,
                0,
                org.sibench.adapters.proto.ValueKind.ROW_ID,
                null,
                false);
    }

    @Test
    public void incrementalFilesRollOverWithUniqueInt64FileIds() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        // Every second row closes a row group, and any closed group reaches the target size.
        CaptureConfig config = baseline(fixture, 1, 2);
        String[] days = {"01", "02", "03", "04", "05", "06"};
        var transactions = new ArrayList<String>();
        for (int i = 0; i + 1 < days.length; i++)
            transactions.add(
                    change("update", row("2024-01-" + days[i] + " 00:00:00"), row("2024-01-" + days[i + 1] + " 00:00:00"))
                            + "{\"event\":\"transaction_commit\",\"xid\":9}\n");
        Path raw = capture(fixture, transactions);
        audit(config, raw, row("2024-01-06 00:00:00"));
        Path output = fixture.resolve("compiled");
        var manifest = compile(config, raw, output);
        var files = (List<?>) manifest.get("incremental_files");
        assertEquals(3, files.size());
        var ids = new HashSet<Long>();
        var rows = new ArrayList<Long>();
        var names = new ArrayList<String>();
        for (Object item : files) {
            var file = (Map<?, ?>) item;
            assertEquals("loantrans", file.get("table"));
            ids.add((Long) file.get("file_id"));
            rows.add((Long) file.get("rows"));
            names.add((String) file.get("path"));
            assertTrue(Files.exists(output.resolve((String) file.get("path"))));
            assertEquals(
                    BaselineGenerator.sha256(output.resolve((String) file.get("path"))),
                    file.get("sha256"));
        }
        assertEquals(List.of(2L, 2L, 1L), rows);
        assertEquals(
                List.of(
                        "loantrans-versions.parquet",
                        "loantrans-versions-000002.parquet",
                        "loantrans-versions-000003.parquet"),
                names);
        assertEquals(3, ids.size());
        // The first file keeps its FileID (baseline's last plus the table id); loantrans is the
        // last table, so the rollover files continue right after it.
        long firstId = (Long) ((Map<?, ?>) files.get(0)).get("file_id");
        assertEquals(firstId + 1, ((Map<?, ?>) files.get(1)).get("file_id"));
        assertEquals(firstId + 2, ((Map<?, ?>) files.get(2)).get("file_id"));
        for (boolean secondary : new boolean[] {true, false}) {
            var replay = new ReplayConfig(secondary, "none", 0, 0, 0, 0, "operation");
            assertEquals(
                    "passed",
                    ReferenceReplay.run(output, replay, fixture.resolve("replay-" + secondary + ".json"))
                            .get("status"));
            assertEquals(
                    "passed",
                    SnapshotLookupValidation.run(
                                    output, replay, fixture.resolve("snapshots-" + secondary + ".json"))
                            .get("status"));
        }
        RelocationCheck.validate(config.layout(), output, fixture.resolve("relocated"));
    }

    @Test
    public void spillingRowIdentitiesChangeNothingInTheCompiledTrace() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig config = baseline(fixture);
        String first = row("2024-01-01 00:00:00"), second = row("2024-01-02 00:00:00");
        // An update, a delete, a reinsert of the same key and an insert, over four transactions.
        Path raw =
                capture(
                        fixture,
                        List.of(
                                change("update", first, second)
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n",
                                change("delete", second, "null")
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n",
                                change("insert", "null", first)
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n",
                                change("update", first, second)
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n"));
        audit(config, raw, second);
        Path reference = fixture.resolve("reference"), spilled = fixture.resolve("spilled");
        compile(config, raw, reference);
        IndexTraceCompiler.compile(
                SourceSchema.hybench(config.source()),
                config.layout(),
                raw,
                spilled,
                RowStateAudit.defaultReport(raw),
                1,
                1_000);
        try (var files = Files.list(reference)) {
            for (Path file : files.toList()) {
                String name = file.getFileName().toString();
                if (name.equals("manifest.json")) continue;
                assertEquals(name, -1, Files.mismatch(file, spilled.resolve(name)));
            }
        }
        // The manifest records the configured limits; everything else in it must agree.
        var expected = JsonParser.parseString(Files.readString(reference.resolve("manifest.json"))).getAsJsonObject();
        var actual = JsonParser.parseString(Files.readString(spilled.resolve("manifest.json"))).getAsJsonObject();
        assertEquals(4_000_000, expected.getAsJsonObject("snapshot_crosscheck").getAsJsonObject("commit_window").get("limit").getAsInt());
        assertEquals(1_000, actual.getAsJsonObject("snapshot_crosscheck").getAsJsonObject("commit_window").get("limit").getAsInt());
        expected.getAsJsonObject("snapshot_crosscheck").remove("commit_window");
        actual.getAsJsonObject("snapshot_crosscheck").remove("commit_window");
        assertEquals(expected, actual);
        try (var names = Files.list(spilled)) {
            assertEquals(Files.list(reference).count(), names.count());
        }
        assertFalse(Files.exists(spilled.resolve("state")));
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
        compile(config, raw, output);
        // Compilation reuses the audit report and never rewrites it.
        assertEquals(auditHash, BaselineGenerator.sha256(sourceAudit));
        compile(config, raw, repeat);
        var manifest =
                JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                        .getAsJsonObject();
        assertEquals(9, manifest.get("operation_trace_version").getAsInt());
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
                        BaselineOperations.of(output).read(load);
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
        var outside =
                assertThrows(
                        java.io.IOException.class,
                        () -> ReferenceReplay.check(output, ReplayConfig.defaults()));
        assertEquals("Declared index catalog differs from pinned source schema", outside.getMessage());
        Files.writeString(output.resolve("manifest.json"), new Gson().toJson(manifest));
        // A maintenance unit missing one secondary entry, with matching digest and count, must be
        // refused by the coverage check that runs in the supply pass.
        Path workload = output.resolve("workload.ops"), saved = fixture.resolve("workload.saved");
        Files.copy(workload, saved);
        var operations = new java.util.ArrayList<IndexOperationTrace.Operation>();
        IndexOperationTrace.read(saved, operations::add);
        int dropped = -1;
        for (int i = 0; i < operations.size() && dropped < 0; i++)
            if (operations.get(i).index() > 0
                    && operations.get(i).kind() != IndexOperationTrace.SEARCH
                    && !IndexOperationTrace.isControl(operations.get(i)))
                dropped = i;
        assertTrue(dropped > 0);
        operations.remove(dropped);
        Files.delete(workload);
        try (var writer = new IndexOperationTrace.Writer(workload)) {
            for (var operation : operations) writer.append(operation);
        }
        var truncated = manifest.deepCopy();
        truncated.addProperty("workload_trace_sha256", IndexOperationTrace.sha256(workload));
        truncated.addProperty("workload_operations", operations.size());
        Files.writeString(output.resolve("manifest.json"), new Gson().toJson(truncated));
        var partial =
                assertThrows(
                        java.io.IOException.class,
                        () -> ReferenceReplay.check(output, ReplayConfig.defaults()));
        assertTrue(
                partial.getMessage(),
                partial.getMessage().matches(
                        "Partial source index maintenance is not supported|Truncated index maintenance unit"
                                + "|Interrupted index maintenance unit|Invalid maintenance unit identity"
                                + "|Invalid source index maintenance order"));
        Files.move(saved, workload, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Files.writeString(output.resolve("manifest.json"), new Gson().toJson(manifest));
    }

    @Test
    public void rejectsMissingBeforeImagesAndUnsupportedPredicates() throws Exception {
        for (String request :
                List.of(
                        "{\"event\":\"row_change\",\"xid\":9,\"relation\":\"loantrans\",\"operation\":\"update\","
                            + "\"new\":"
                                + row("2024-01-02 00:00:00")
                                + "}\n",
                        // An array key (= ANY) whose elements were not captured.
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
        assertEquals(Map.of("range", 2L), manifest.get("searched_accesses_by_shape"));
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

    private static String event(String type, String detail) {
        return "{\"event\":\"" + type + "\"," + detail + "}\n";
    }

    private static String timestampKey(String value) {
        return "{\"attribute\":1,\"strategy\":3,\"flags\":0,\"type_oid\":1114,\"value\":\""
                + value
                + "\"}";
    }

    private static final String APPLICANT_KEY =
            "{\"attribute\":1,\"strategy\":3,\"flags\":0,\"type_oid\":23,\"value\":\"1\"}";

    /** An index scan of idx_loantrans_2 that returned one row and was never asked for more. */
    private static String stoppedSecondary(String scan, String value, String pk) {
        return event("plan_scan", "\"scan\":\"" + scan + "\",\"relation\":\"idx_loantrans_2\",\"plan_node\":1,\"kind\":\"index\"")
                + event("scan_rescan", "\"scan\":\"" + scan + "\",\"relation\":\"idx_loantrans_2\",\"keys\":[" + timestampKey(value) + "]")
                + event("scan_next", "\"scan\":\"" + scan + "\",\"relation\":\"idx_loantrans_2\",\"direction\":1,\"found\":true,\"recheck\":false")
                + event("heap_visibility", "\"scan\":\"" + scan + "\",\"relation\":\"idx_loantrans_2\",\"found\":true,\"row\":{\"indexed\":[\"" + value + "\"],\"pk\":[\"" + pk + "\"]}")
                + event("scan_end", "\"scan\":\"" + scan + "\",\"relation\":\"idx_loantrans_2\"");
    }

    private static String bitmap(String scan, String index, int node) {
        return event("plan_scan", "\"scan\":\"" + scan + "\",\"relation\":\"" + index + "\",\"plan_node\":" + node + ",\"kind\":\"bitmap\"");
    }

    private static String rescan(String scan, String index, String keys) {
        return event("scan_rescan", "\"scan\":\"" + scan + "\",\"relation\":\"" + index + "\",\"keys\":[" + keys + "]");
    }

    private static String candidates(String scan, String index, String tid) {
        return event("bitmap_candidate", "\"scan\":\"" + scan + "\",\"relation\":\"" + index + "\",\"found\":true,\"tid\":\"" + tid + "\"")
                + event("bitmap_end", "\"scan\":\"" + scan + "\",\"relation\":\"" + index + "\",\"found\":false");
    }

    /**
     * applicantid = 1 AND timestamp = value, intersected by a BitmapAnd, in PostgreSQL's order:
     * constant keys reach the index while the plan is initialized, before the heap node binds it.
     */
    private static String bitmapAnd(String value) {
        return bitmap("a", "idx_loantrans_1", 2)
                + rescan("a", "idx_loantrans_1", APPLICANT_KEY)
                + bitmap("b", "idx_loantrans_2", 3)
                + rescan("b", "idx_loantrans_2", timestampKey(value))
                + event("plan_bitmap_heap", "\"heap\":\"h\",\"relation\":\"loantrans\",\"plan_node\":4,\"tree\":{\"and\":[\"a\",\"b\"]},\"scans\":[\"a\",\"b\"]")
                + candidates("a", "idx_loantrans_1", "(0,1)")
                + candidates("b", "idx_loantrans_2", "(0,1)")
                + event("bitmap_heap_candidate", "\"heap\":\"h\",\"relation\":\"loantrans\",\"tid\":\"(0,1)\",\"found\":true")
                + event("bitmap_verdicts", "\"heap\":\"h\",\"relation\":\"loantrans\",\"lossy_pages\":0,\"verdicts\":[{\"inputs\":[0,1],\"visible\":1,\"invisible\":0}]")
                + event("scan_end", "\"scan\":\"a\",\"relation\":\"idx_loantrans_1\"")
                + event("scan_end", "\"scan\":\"b\",\"relation\":\"idx_loantrans_2\"");
    }

    /** applicantid = ANY('{7,1,NULL}') through one bitmap scan. */
    private static String bitmapArray() {
        String key =
                "{\"attribute\":1,\"strategy\":3,\"flags\":32,\"type_oid\":23,\"value\":null,"
                        + "\"array_type_oid\":23,\"array\":[\"7\",\"1\",null]}";
        return bitmap("c", "idx_loantrans_1", 5)
                + rescan("c", "idx_loantrans_1", key)
                + event("plan_bitmap_heap", "\"heap\":\"g\",\"relation\":\"loantrans\",\"plan_node\":6,\"tree\":\"c\",\"scans\":[\"c\"]")
                + candidates("c", "idx_loantrans_1", "(0,1)")
                + event("bitmap_heap_candidate", "\"heap\":\"g\",\"relation\":\"loantrans\",\"tid\":\"(0,1)\",\"found\":true")
                + event("bitmap_verdicts", "\"heap\":\"g\",\"relation\":\"loantrans\",\"lossy_pages\":0,\"verdicts\":[{\"inputs\":[0],\"visible\":1,\"invisible\":0}]")
                + event("scan_end", "\"scan\":\"c\",\"relation\":\"idx_loantrans_1\"");
    }

    private Path nonPrefixCapture(Path fixture, CaptureConfig config, String pinnedPk) throws Exception {
        String first = row("2024-01-01 00:00:00"), second = row("2024-01-02 00:00:00");
        String commit = "{\"event\":\"transaction_commit\",\"xid\":0}\n";
        Path raw =
                capture(
                        fixture,
                        List.of(
                                change("update", first, second)
                                        + "{\"event\":\"transaction_commit\",\"xid\":9}\n",
                                stoppedSecondary("s", "2024-01-02 00:00:00", pinnedPk) + commit,
                                bitmapAnd("2024-01-02 00:00:00") + commit,
                                bitmapArray() + commit));
        audit(config, raw, second);
        return raw;
    }

    /**
     * The three accesses whose native result is not a KV-ordered prefix of one interval: a
     * secondary scan stopped among equal values, a BitmapAnd and an = ANY array. Each compiles to
     * searches whose counts are native facts, and the reference replay checks them.
     */
    @Test
    public void nonPrefixAccessesCompileToSearchesThatReplay() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        var config = baseline(fixture);
        Path raw = nonPrefixCapture(fixture, config, "1");
        Path output = fixture.resolve("compiled");
        var manifest = compile(config, raw, output);
        assertEquals(
                Map.of("stopped_equality", 1L, "intersection", 1L, "union", 1L),
                manifest.get("searched_accesses_by_shape"));
        var searches = new ArrayList<IndexOperationTrace.Operation>();
        IndexOperationTrace.read(
                output.resolve("workload.ops"),
                o -> {
                    if (o.kind() == IndexOperationTrace.SEARCH) searches.add(o);
                });
        assertEquals(5, searches.size());
        // The stopped scan: no fully consumed prefix, one pinned (value, PK) search.
        var pinned = searches.get(0);
        assertEquals(IndexKey.ofInt(1), pinned.key());
        assertEquals(timestamp("2024-01-02 00:00:00"), pinned.range().lower());
        assertTrue(pinned.range().point());
        assertEquals(1, pinned.expectedCount());
        // The BitmapAnd: two indexes, one group whose count is the intersection's.
        var intersection = searches.subList(1, 3);
        assertEquals(List.of(1, 2), intersection.stream().map(o -> o.index()).toList());
        for (var member : intersection) {
            assertTrue((member.flags() & IndexOperationTrace.INTERSECT_GROUP) != 0);
            assertEquals(2, member.expansionSize());
            assertEquals(1, member.expectedCount());
        }
        // The array: its two distinct non-null elements in KV order, one union group.
        var union = searches.subList(3, 5);
        assertEquals(
                List.of(IndexKey.ofInt(1), IndexKey.ofInt(7)),
                union.stream().map(o -> o.range().lower()).toList());
        for (var member : union) assertTrue((member.flags() & IndexOperationTrace.UNION_GROUP) != 0);
        for (boolean secondary : new boolean[] {true, false})
            for (String mode : List.of("operation", "transaction"))
                assertEquals(
                        "passed",
                        ReferenceReplay.run(
                                        output,
                                        new ReplayConfig(secondary, "none", 0, 0, 0, 0, mode),
                                        fixture.resolve("replay-" + secondary + "-" + mode + ".json"))
                                .get("status"));
    }

    /** A pinned search names one entry; a row the scan never returned is a replay failure. */
    @Test
    public void aPinnedSearchForARowThatDoesNotExistFailsReplay() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        var config = baseline(fixture);
        Path raw = nonPrefixCapture(fixture, config, "2");
        Path output = fixture.resolve("compiled");
        compile(config, raw, output);
        var failure =
                assertThrows(
                        java.io.IOException.class,
                        () ->
                                ReferenceReplay.run(
                                        output, ReplayConfig.defaults(), fixture.resolve("replay.json")));
        assertTrue(failure.getMessage(), failure.getMessage().contains("Search count mismatch"));
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
        assertEquals(2, manifest.get("search_operations").getAsLong());
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

    private static JsonObject json(Path file) throws java.io.IOException {
        return JsonParser.parseString(Files.readString(file)).getAsJsonObject();
    }

    /** Two concurrent sessions that search, update and search again, as a full capture audits them. */
    private Path auditedConcurrentCapture(Path fixture, CaptureConfig config, boolean summary) throws Exception {
        String before = row("2024-01-01 00:00:00"), after = row("2024-01-02 00:00:00");
        var sessions = new ArrayList<>(overlappingSessions(before, after, 200));
        var writer = new ArrayList<>(sessions.get(0));
        writer.addAll(request(101, 3, "SELECT 1", snapshotSearch("a3", "2024-01-02 00:00:00", 260, "[]", 52, false)));
        writer.add(commit(101, 3, 0, 300));
        sessions.set(0, writer);
        Path raw =
                concurrentCapture(
                        fixture,
                        summary ? List.of(summarized(101, sessions.get(0)), summarized(102, sessions.get(1))) : sessions);
        // A packaged run records its build; the fixture names one so it can be certified.
        Path jar = runJar(fixture.resolve("artifacts"), "compiler", false);
        recordBuild(raw, jar);
        audit(config, raw, after);
        return raw;
    }

    private static void recordBuild(Path raw, Path jar) throws Exception {
        Path run = raw.resolve("capture-run.json");
        JsonObject manifest = json(run);
        JsonObject build = new JsonObject();
        build.addProperty("sibench_jar_sha256", FileDigests.sha256(jar));
        build.addProperty("postgres_sha256", "postgres");
        manifest.add("build", build);
        Files.writeString(run, manifest.toString());
    }

    /**
     * The run jar of a capture under {@code artifacts}: the real capture roots, one compiler class
     * with the given content, and a library entry. {@code changedDriver} alters the capture
     * driver's class file after its constant pool, which changes its digest but not its references.
     */
    private static Path runJar(Path artifacts, String compiler, boolean changedDriver) throws Exception {
        Path jar = Certification.runJar(artifacts);
        Files.createDirectories(jar.getParent());
        try (var out = new java.util.zip.ZipOutputStream(Files.newOutputStream(jar))) {
            for (Class<?> type : CaptureCode.ROOTS) {
                String name = type.getName().replace('.', '/') + ".class";
                byte[] bytes;
                try (var in = type.getClassLoader().getResourceAsStream(name)) {
                    bytes = in.readAllBytes();
                }
                if (changedDriver && type == CaptureDriver.class) bytes = Arrays.copyOf(bytes, bytes.length + 1);
                out.putNextEntry(new java.util.zip.ZipEntry(name));
                out.write(bytes);
            }
            out.putNextEntry(new java.util.zip.ZipEntry("org/sibench/capture/IndexTraceCompiler.class"));
            out.write(compiler.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            out.putNextEntry(new java.util.zip.ZipEntry("com/example/Library.class"));
            out.write(new byte[] {1, 2, 3});
        }
        return jar;
    }

    /** PG counter snapshots that agree with a ledger: zero before, the ledger's tallies after. */
    private static Path[] counters(Path directory, JsonObject ledger, long extraCandidates) throws Exception {
        var before = new JsonArray();
        var after = new JsonArray();
        boolean first = true;
        for (JsonElement item : ledger.getAsJsonArray("indexes")) {
            JsonObject index = item.getAsJsonObject();
            for (JsonArray target : List.of(before, after)) {
                JsonObject counter = new JsonObject();
                counter.addProperty("schemaname", "public");
                counter.addProperty("relname", index.get("table").getAsString());
                counter.addProperty("indexrelname", index.get("index").getAsString());
                boolean end = target == after;
                counter.addProperty("idx_scan", end ? index.get("tree_descents_min").getAsLong() : 0);
                counter.addProperty(
                        "idx_tup_read",
                        end ? index.get("candidates").getAsLong() + (first ? extraCandidates : 0) : 0);
                counter.addProperty("idx_tup_fetch", end ? index.get("heap_fetch_visible").getAsLong() : 0);
                target.add(counter);
            }
            first = false;
        }
        var files = new Path[2];
        for (int i = 0; i < 2; i++) {
            JsonObject snapshot = new JsonObject();
            snapshot.addProperty("schema_version", CaptureCompleteness.VERSION);
            snapshot.addProperty("source", "pg_stat_user_indexes");
            snapshot.addProperty("profile", "fixture.yaml");
            snapshot.addProperty("database", "fixture");
            snapshot.addProperty("track_counts", "on");
            snapshot.add("indexes", i == 0 ? before : after);
            files[i] = directory.resolve((i == 0 ? "before-" : "after-") + extraCandidates + ".json");
            Files.writeString(files[i], snapshot.toString());
        }
        return files;
    }

    @Test
    public void compilationBuildsTheStandaloneLedgerInTheSamePass() throws Exception {
        for (boolean summary : new boolean[] {false, true}) {
            Path fixture = temporary.newFolder().toPath();
            CaptureConfig config = baseline(fixture);
            Path raw = auditedConcurrentCapture(fixture, config, summary);
            Path output = fixture.resolve("compiled");
            compile(config, raw, output);
            JsonObject manifest = json(output.resolve("manifest.json"));
            var standalone =
                    AccessLedger.analyze(
                            SourceSchema.hybench(config.source()), raw, manifest.get("text_intervals_ordered").getAsBoolean());
            assertEquals(
                    JsonParser.parseString(new GsonBuilder().serializeNulls().create().toJson(standalone)),
                    json(output.resolve("access-ledger.json")));
            assertEquals(
                    BaselineGenerator.sha256(output.resolve("access-ledger.json")),
                    manifest.get("access_ledger_sha256").getAsString());
            // The traces are hashed as they are written; the digest is that of the file.
            assertEquals(
                    IndexOperationTrace.sha256(output.resolve("workload.ops")),
                    manifest.get("workload_trace_sha256").getAsString());
            // The baseline load is not stored; deriving it again yields the recorded stream.
            assertFalse(Files.exists(output.resolve("baseline.ops")));
            assertEquals(
                    manifest.get("baseline_operations").getAsLong(),
                    BaselineOperations.of(output).read(operation -> {}));
            JsonObject altered = manifest.deepCopy();
            altered.addProperty("baseline_trace_sha256", "0".repeat(64));
            Files.writeString(output.resolve("manifest.json"), altered.toString());
            var failure = assertThrows(java.io.IOException.class, () -> BaselineOperations.of(output).read(operation -> {}));
            assertTrue(failure.getMessage(), failure.getMessage().contains("differs from the compiled trace"));
            Files.writeString(output.resolve("manifest.json"), manifest.toString());
            assertEquals("row_audit", manifest.getAsJsonObject("verification").get("mode").getAsString());
            assertEquals("not_checked", manifest.get("capture_completeness").getAsString());
        }
    }

    @Test
    public void indexCountersThatDisagreeWithTheLedgerLeaveTheTraceUncommitted() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig config = baseline(fixture);
        Path raw = auditedConcurrentCapture(fixture, config, true);
        compile(config, raw, fixture.resolve("first"));
        JsonObject ledger = json(fixture.resolve("first/access-ledger.json"));
        var schema = SourceSchema.hybench(config.source());
        Path[] agree = counters(fixture, ledger, 0), disagree = counters(fixture, ledger, 1);
        Path output = fixture.resolve("checked");
        IndexTraceCompiler.compile(
                schema, config.layout(), raw, output,
                new IndexTraceCompiler.Checks(RowStateAudit.defaultReport(raw), null, agree[0], agree[1]));
        assertEquals("consistent", json(output.resolve("manifest.json")).get("capture_completeness").getAsString());
        Path rejected = fixture.resolve("rejected");
        var failure =
                assertThrows(
                        java.io.IOException.class,
                        () -> IndexTraceCompiler.compile(
                                schema, config.layout(), raw, rejected,
                                new IndexTraceCompiler.Checks(RowStateAudit.defaultReport(raw), null, disagree[0], disagree[1])));
        assertTrue(failure.getMessage(), failure.getMessage().contains("disagree"));
        assertTrue(Files.exists(rejected.resolve("INCOMPLETE")));
        assertEquals("inconsistent", json(rejected.resolve("capture-completeness.json")).get("status").getAsString());
    }

    @Test
    public void aCertificationReplacesTheRowAuditOnlyForTheSameBuildAndCoverage() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig config = baseline(fixture);
        var schema = SourceSchema.hybench(config.source());
        Path raw = auditedConcurrentCapture(fixture, config, true);
        Path artifacts = fixture.resolve("artifacts");
        Files.createDirectories(artifacts);
        compile(config, raw, fixture.resolve("first"));
        Path[] agree = counters(fixture, json(fixture.resolve("first/access-ledger.json")), 0);
        Path trace = artifacts.resolve("trace");
        IndexTraceCompiler.compile(
                schema, config.layout(), raw, trace,
                new IndexTraceCompiler.Checks(RowStateAudit.defaultReport(raw), null, agree[0], agree[1]));
        var profile = new CaptureProfile(schema, config.layout(), raw, artifacts);
        var certificate = Certification.certify(profile);
        assertEquals("certified", certificate.get("status"));
        Path certification = artifacts.resolve(Certification.FILE);
        assertThrows(java.io.IOException.class, () -> Certification.certify(profile));

        assertFalse(json(certification).getAsJsonObject("build").has("sibench_jar_sha256"));
        assertTrue(json(certification).getAsJsonObject("build").getAsJsonObject("capture_code")
                .getAsJsonObject("classes").has("org/sibench/capture/CaptureDriver.class"));

        // The same capture, relying on the certificate instead of its row audit; a run with
        // another compiler in its jar is still the certified capture build.
        Path jar = runJar(fixture.resolve("recompiled"), "another compiler", false);
        recordBuild(raw, jar);
        Path certified = fixture.resolve("certified");
        IndexTraceCompiler.compile(
                schema, config.layout(), raw, certified,
                new IndexTraceCompiler.Checks(null, certification, agree[0], agree[1], jar));
        JsonObject manifest = json(certified.resolve("manifest.json"));
        assertEquals("certified", manifest.getAsJsonObject("verification").get("mode").getAsString());
        assertEquals(
                json(trace.resolve("manifest.json")).get("workload_trace_sha256"),
                manifest.get("workload_trace_sha256"));

        // An access path the certifying capture never exercised.
        JsonObject narrowed = json(certification);
        var paths = narrowed.getAsJsonObject("coverage").getAsJsonArray("access_paths");
        String dropped = paths.remove(0).getAsString();
        Path narrow = fixture.resolve("narrow.json");
        Files.writeString(narrow, narrowed.toString());
        Path outside = fixture.resolve("outside");
        var failure =
                assertThrows(
                        java.io.IOException.class,
                        () -> IndexTraceCompiler.compile(
                                schema, config.layout(), raw, outside,
                                new IndexTraceCompiler.Checks(null, narrow, agree[0], agree[1], jar)));
        assertTrue(failure.getMessage(), failure.getMessage().contains("outside its certification"));
        assertTrue(failure.getMessage(), failure.getMessage().contains(dropped));
        assertTrue(Files.exists(outside.resolve("INCOMPLETE")));

        // Another build is not vouched for, whatever it exercised.
        JsonObject rebuilt = json(certification);
        rebuilt.getAsJsonObject("build").addProperty("postgres_sha256", "another");
        Path other = fixture.resolve("other-build.json");
        Files.writeString(other, rebuilt.toString());
        failure =
                assertThrows(
                        java.io.IOException.class,
                        () -> IndexTraceCompiler.compile(
                                schema, config.layout(), raw, fixture.resolve("other"),
                                new IndexTraceCompiler.Checks(null, other, agree[0], agree[1], jar)));
        assertTrue(failure.getMessage(), failure.getMessage().contains("postgres_sha256"));
        assertFalse(Files.exists(fixture.resolve("other")));

        // A changed capture driver is another capture build, and the message names it.
        Path changed = runJar(fixture.resolve("driver"), "another compiler", true);
        recordBuild(raw, changed);
        failure =
                assertThrows(
                        java.io.IOException.class,
                        () -> IndexTraceCompiler.compile(
                                schema, config.layout(), raw, fixture.resolve("changed"),
                                new IndexTraceCompiler.Checks(null, certification, agree[0], agree[1], changed)));
        assertTrue(failure.getMessage(), failure.getMessage().contains("org/sibench/capture/CaptureDriver.class"));
        // A jar that is not the one the capture recorded is refused before it is compared.
        failure =
                assertThrows(
                        java.io.IOException.class,
                        () -> IndexTraceCompiler.compile(
                                schema, config.layout(), raw, fixture.resolve("unrecorded"),
                                new IndexTraceCompiler.Checks(null, certification, agree[0], agree[1], jar)));
        assertTrue(failure.getMessage(), failure.getMessage().contains("not the one the capture ran"));
    }
}
