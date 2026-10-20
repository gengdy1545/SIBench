package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.generation.BaselineGenerator;
import org.sibench.generation.GenerationConfig;
import org.sibench.generation.ParquetDataFile;
import org.sibench.generation.TableData;
import org.sibench.replay.ReferenceReplay;
import org.sibench.replay.ReplayConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndexTraceCompilerTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static CaptureConfig createBaseline(Path fixture) throws Exception {
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
        var capture =
                new CaptureConfig(
                        root,
                        original.source(),
                        fixture,
                        fixture,
                        original.jdbc(),
                        original.user(),
                        original.seconds(),
                        original.clients(),
                        original.percentages(),
                        layout,
                        original.document());
        var saving =
                MessageTypeParser.parseMessageType(
                        "message savingaccount { required int64 accountid; required double"
                                + " balance; required int64 _sib_row_id; required int64"
                                + " _sib_timestamp; }");
        var transfer =
                MessageTypeParser.parseMessageType(
                        "message transfer { required int64 id; required int64 sourceid;"
                                + " required int64 targetid; required double amount; required int64"
                                + " _sib_row_id; required int64 _sib_timestamp; }");
        Path accountFile = baseline.resolve("account.parquet");
        Path transferFile = baseline.resolve("transfer.parquet");
        try (var writer =
                new ParquetDataFile(
                        accountFile,
                        new TableData("savingaccount", 1, 1, "accountid", saving, null),
                        layout,
                        Map.of())) {
            writer.write(
                    new SimpleGroupFactory(saving)
                            .newGroup()
                            .append("accountid", 1L)
                            .append("balance", 10.0)
                            .append("_sib_row_id", 1L)
                            .append("_sib_timestamp", 0L));
            writer.finish();
        }
        try (var writer =
                new ParquetDataFile(
                        transferFile,
                        new TableData("transfer", 1, 2, "id", transfer, null),
                        layout,
                        Map.of())) {
            writer.write(
                    new SimpleGroupFactory(transfer)
                            .newGroup()
                            .append("id", 1L)
                            .append("sourceid", 10L)
                            .append("targetid", 20L)
                            .append("amount", 1.0)
                            .append("_sib_row_id", 2L)
                            .append("_sib_timestamp", 0L));
            writer.finish();
        }
        Gson gson = new Gson();
        Files.writeString(
                baseline.resolve("files.jsonl"),
                gson.toJson(
                                Map.of(
                                        "table",
                                        "savingaccount",
                                        "path",
                                        "account.parquet",
                                        "file_id",
                                        1,
                                        "rows",
                                        1))
                        + "\n"
                        + gson.toJson(
                                Map.of(
                                        "table",
                                        "transfer",
                                        "path",
                                        "transfer.parquet",
                                        "file_id",
                                        2,
                                        "rows",
                                        1))
                        + "\n");
        Files.writeString(baseline.resolve("locators.tsv"), "");
        Files.writeString(
                baseline.resolve("verified-manifest.json"),
                gson.toJson(
                        Map.of(
                                "status",
                                "verified_complete",
                                "rows",
                                2,
                                "files_manifest_sha256",
                                BaselineGenerator.sha256(baseline.resolve("files.jsonl")),
                                "locators_sha256",
                                BaselineGenerator.sha256(baseline.resolve("locators.tsv")),
                                "files",
                                List.of(
                                        Map.of(
                                                "path",
                                                "account.parquet",
                                                "sha256",
                                                BaselineGenerator.sha256(accountFile)),
                                        Map.of(
                                                "path",
                                                "transfer.parquet",
                                                "sha256",
                                                BaselineGenerator.sha256(transferFile))))));
        return capture;
    }

    @Test
    public void compilesExplicitIndexesAndObservedDeleteWithoutChangingCaptureDistribution()
            throws Exception {
        Path fixture = temporary.newFolder().toPath();
        var capture = createBaseline(fixture);
        Path raw = fixture.resolve("runtime.jsonl");
        Files.writeString(
                raw,
                """
{"pid":1,"event":"row_change","xid":4,"relation":"savingaccount","operation":"update","new":{"accountid":1,"balance":8}}
{"pid":1,"event":"row_change","xid":4,"relation":"savingaccount","operation":"update","new":{"accountid":1,"balance":7}}
{"pid":1,"event":"row_change","xid":4,"relation":"transfer","operation":"insert","new":{"id":2,"sourceid":10,"targetid":20,"amount":2}}
{"pid":1,"event":"transaction_commit","xid":4}
{"pid":1,"event":"scan_rescan","scan":"s","relation":"idx_transfer_1","keys":[{"strategy":3,"flags":0,"value":10}]}
{"pid":1,"event":"bitmap_candidate","scan":"s","found":true}
{"pid":1,"event":"bitmap_candidate","scan":"s","found":true}
{"pid":1,"event":"scan_end","scan":"s"}
{"pid":1,"event":"row_change","xid":2,"relation":"savingaccount","operation":"delete","old":{"accountid":1,"balance":8}}
{"pid":1,"event":"transaction_commit","xid":2}
{"pid":1,"event":"scan_rescan","scan":"a","relation":"savingaccount_pkey","keys":[{"strategy":3,"flags":0,"value":1}]}
{"pid":1,"event":"scan_end","scan":"a"}
{"pid":1,"event":"transaction_commit","xid":0}
{"pid":1,"event":"scan_rescan","scan":"b","relation":"savingaccount_pkey","keys":[{"strategy":3,"flags":0,"value":1}]}
{"pid":1,"event":"scan_end","scan":"b"}
{"pid":1,"event":"transaction_abort","xid":0}
""");
        List<String> correlated = new ArrayList<>();
        for (String line : Files.readAllLines(raw)) {
            var event = JsonParser.parseString(line).getAsJsonObject();
            event.addProperty("application", "fixture");
            correlated.add(event.toString());
        }
        correlated.add(
                0,
                "{\"pid\":1,\"application\":\"fixture\",\"event\":\"executor_start\",\"sql\":\"SELECT"
                    + " 1\"}");
        correlated.add("{\"pid\":1,\"application\":\"fixture\",\"event\":\"executor_end\"}");
        Files.write(raw, correlated);
        Files.writeString(
                fixture.resolve("jdbc-fixture.jsonl"),
                """
{"event":"connection","pid":1}
{"event":"request_start","application":"fixture","sql":"SELECT 1","parameters":{"1":1,"2":10}}
{"event":"request_return","application":"fixture"}
{"event":"commit","application":"fixture"}
{"event":"commit","application":"fixture"}
{"event":"commit","application":"fixture"}
{"event":"rollback","application":"fixture"}
{"event":"close","application":"fixture"}
""");
        Path output = fixture.resolve("compiled");
        IndexTraceCompiler.compile(capture, raw, output);
        var manifest =
                JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                        .getAsJsonObject();
        assertEquals(5, manifest.get("operation_trace_version").getAsInt());
        assertEquals(4, manifest.get("baseline_operations").getAsLong());
        assertEquals(15, manifest.get("workload_operations").getAsLong());
        assertEquals(1, manifest.getAsJsonObject("business_changes").get("delete").getAsLong());
        List<IndexOperationTrace.Operation> events = new ArrayList<>();
        IndexOperationTrace.readExpanded(output.resolve("workload.ops"), events::add);
        assertEquals(1, events.get(0).version());
        assertEquals(IndexOperationTrace.TRANSACTION_BEGIN, events.get(0).kind());
        assertEquals(IndexOperationTrace.BUSINESS_UPDATE, events.get(1).flags());
        assertEquals(IndexOperationTrace.BUSINESS_INSERT, events.get(3).flags());
        assertEquals(
                List.of(0, 1, 2),
                events.subList(3, 6).stream().map(IndexOperationTrace.Operation::index).toList());
        assertTrue(
                events.subList(1, 6).stream()
                        .allMatch(o -> o.version() == 1 && o.transactionId() == 5));
        assertEquals(1, events.get(1).mutationOrdinal());
        assertEquals(2, events.get(2).mutationOrdinal());
        assertEquals(3, events.get(3).mutationOrdinal());
        assertEquals(IndexOperationTrace.LOGICAL_DELETE, events.get(9).kind());
        assertEquals(2, events.get(9).version());
        assertEquals(1, events.get(9).rowId());
        assertEquals(0, events.get(8).version());
        assertEquals(1, events.get(8).readTimestamp());
        assertEquals(2, events.get(12).readTimestamp());
        assertEquals(3, events.get(14).readTimestamp());
        assertEquals(3, manifest.get("committed_transactions").getAsLong());
        assertEquals(14, events.get(11).transactionId());
        assertEquals(0, manifest.get("snapshot_searches").getAsLong());
        try (var reader =
                org.apache.parquet.hadoop.ParquetReader.builder(
                                new org.apache.parquet.hadoop.example.GroupReadSupport(),
                                new org.apache.hadoop.fs.Path(
                                        output.resolve("account-versions.parquet").toUri()))
                        .build()) {
            var first = reader.read();
            var second = reader.read();
            assertEquals(8.0, first.getDouble("balance", 0), 0);
            assertEquals(7.0, second.getDouble("balance", 0), 0);
            assertEquals(1, first.getLong("_sib_timestamp", 0));
            assertEquals(1, second.getLong("_sib_timestamp", 0));
            assertEquals(1, first.getLong("_sib_mutation_ordinal", 0));
            assertEquals(2, second.getLong("_sib_mutation_ordinal", 0));
            assertNull(reader.read());
        }
        for (boolean secondary : new boolean[] {true, false})
            for (String dispatchMode : new String[] {"operation", "transaction"}) {
                Path report = fixture.resolve("replay-" + secondary + "-" + dispatchMode + ".json");
                assertEquals(
                        "passed",
                        ReferenceReplay.run(
                                        output,
                                        new ReplayConfig(
                                                secondary, "none", 0, 0, 0, 0, dispatchMode),
                                        report)
                                .get("status"));
            }
        assertEquals(
                CaptureConfig.read(
                                Path.of("").toAbsolutePath(),
                                Path.of("config/hybench_tp_subset_sf1.yaml"))
                        .percentages(),
                capture.percentages());
    }

    @Test
    public void discardsRolledBackWritesWithoutChangingVersionsIdentitiesOrQueries()
            throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig capture = createBaseline(fixture);
        Path raw =
                writeCapture(
                        fixture,
                        List.of(
                                """
{"event":"scan_rescan","scan":"a","relation":"savingaccount_pkey","keys":[{"strategy":3,"flags":0,"value":1}]}
{"event":"heap_visibility","scan":"a","found":true}
{"event":"scan_end","scan":"a"}
{"event":"row_change","xid":9,"relation":"savingaccount","operation":"update","new":{"accountid":1,"balance":8}}
{"event":"row_change","xid":9,"relation":"transfer","operation":"insert","new":{"id":2,"sourceid":77,"targetid":88,"amount":2}}
{"event":"index_insert_attempt"}
{"event":"row_change","xid":9,"relation":"savingaccount","operation":"delete","old":{"accountid":1,"balance":8}}
{"event":"transaction_abort","xid":9}
""",
                                """
{"event":"row_change","xid":4,"relation":"savingaccount","operation":"update","new":{"accountid":1,"balance":7}}
{"event":"row_change","xid":4,"relation":"transfer","operation":"insert","new":{"id":3,"sourceid":10,"targetid":20,"amount":3}}
{"event":"transaction_commit","xid":4}
""",
                                """
{"event":"scan_rescan","scan":"b","relation":"idx_transfer_1","keys":[{"strategy":3,"flags":0,"value":77}]}
{"event":"scan_end","scan":"b"}
{"event":"transaction_abort","xid":0}
""",
                                accountSearch("c")
                                        + "{\"event\":\"transaction_commit\",\"xid\":0}\n",
                                accountSearch("d")
                                        + "{\"event\":\"transaction_abort\",\"xid\":0}\n"));
        String rawHash = BaselineGenerator.sha256(raw);
        Path output = fixture.resolve("compiled");
        IndexTraceCompiler.compile(capture, raw, output);
        var manifest =
                JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                        .getAsJsonObject();
        assertEquals("committed_only", manifest.get("change_delivery").getAsString());
        assertEquals(5, manifest.get("raw_row_changes").getAsLong());
        assertEquals(3, manifest.get("discarded_aborted_row_changes").getAsLong());
        assertEquals(3, manifest.get("aborted_transactions").getAsLong());
        assertEquals(1, manifest.get("discarded_aborted_index_insert_attempts").getAsLong());
        assertEquals(2, manifest.get("logical_row_version_writes").getAsLong());
        assertEquals(2, manifest.get("committed_transactions").getAsLong());
        assertEquals(4, manifest.get("searches").getAsLong());
        assertEquals(12, manifest.get("workload_operations").getAsLong());
        assertEquals(4, manifest.get("index_version_writes").getAsLong());
        List<IndexOperationTrace.Operation> operations = new ArrayList<>();
        IndexOperationTrace.readExpanded(output.resolve("workload.ops"), operations::add);
        assertEquals(IndexOperationTrace.SEARCH, operations.get(0).kind());
        assertEquals(0, operations.get(0).readTimestamp());
        assertEquals(1, operations.get(0).expectedCount());
        assertEquals(1, operations.get(2).rowId());
        assertEquals(3, operations.get(3).rowId());
        assertEquals(1, operations.get(3).version());
        assertEquals(3, operations.get(3).key());
        assertEquals(0, operations.get(7).expectedCount());
        assertEquals(1, operations.get(7).readTimestamp());
        assertEquals(2, operations.get(11).readTimestamp());
        assertTrue(
                operations.stream().noneMatch(o -> o.kind() == IndexOperationTrace.LOGICAL_DELETE));
        for (String table : List.of("account", "transfer")) {
            try (var reader =
                    org.apache.parquet.hadoop.ParquetReader.builder(
                                    new org.apache.parquet.hadoop.example.GroupReadSupport(),
                                    new org.apache.hadoop.fs.Path(
                                            output.resolve(table + "-versions.parquet").toUri()))
                            .build()) {
                var row = reader.read();
                assertEquals(1, row.getLong("_sib_timestamp", 0));
                assertEquals(table.equals("account") ? 1 : 3, row.getLong("_sib_row_id", 0));
                assertNull(reader.read());
            }
        }
        for (boolean secondary : new boolean[] {true, false})
            for (String dispatchMode : new String[] {"operation", "transaction"}) {
                var result =
                        ReferenceReplay.run(
                                output,
                                new ReplayConfig(secondary, "none", 0, 0, 0, 0, dispatchMode),
                                fixture.resolve(
                                        "replay-" + secondary + "-" + dispatchMode + ".json"));
                assertEquals("passed", result.get("status"));
            }
        Path repeated = fixture.resolve("compiled-repeat");
        IndexTraceCompiler.compile(capture, raw, repeated);
        for (String file :
                List.of(
                        "baseline.ops",
                        "workload.ops",
                        "account-versions.parquet",
                        "transfer-versions.parquet",
                        "manifest.json"))
            assertEquals(file, -1, Files.mismatch(output.resolve(file), repeated.resolve(file)));
        assertEquals(rawHash, BaselineGenerator.sha256(raw));
    }

    @Test
    public void rejectsAbortXidMismatchAndUnfinishedTransactions() throws Exception {
        for (String tail : List.of("{\"event\":\"transaction_abort\",\"xid\":8}\n", "")) {
            Path fixture = temporary.newFolder().toPath();
            CaptureConfig capture = createBaseline(fixture);
            Path raw =
                    writeCapture(
                            fixture,
                            List.of(
                                    """
{"event":"row_change","xid":9,"relation":"savingaccount","operation":"update","new":{"accountid":1,"balance":8}}
"""
                                            + tail));
            assertThrows(
                    java.io.IOException.class,
                    () -> IndexTraceCompiler.compile(capture, raw, fixture.resolve("compiled")));
        }
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig capture = createBaseline(fixture);
        Path raw = writeCapture(fixture, List.of(accountSearch("a")));
        assertThrows(
                java.io.IOException.class,
                () -> IndexTraceCompiler.compile(capture, raw, fixture.resolve("compiled")));
        assertFalse(Files.exists(fixture.resolve("compiled")));

        Path unfinishedScan = temporary.newFolder().toPath();
        CaptureConfig scanCapture = createBaseline(unfinishedScan);
        Path scanRaw =
                writeCapture(
                        unfinishedScan,
                        List.of(
                                """
{"event":"scan_rescan","scan":"a","relation":"savingaccount_pkey","keys":[{"strategy":3,"flags":0,"value":1}]}
{"event":"transaction_abort","xid":0}
"""));
        assertThrows(
                java.io.IOException.class,
                () ->
                        IndexTraceCompiler.compile(
                                scanCapture, scanRaw, unfinishedScan.resolve("compiled")));
        assertTrue(Files.exists(unfinishedScan.resolve("compiled/INCOMPLETE")));
        assertFalse(Files.exists(unfinishedScan.resolve("compiled/manifest.json")));
    }

    @Test
    public void closeAbortWithWritesProducesNoCommittedDataOrCompensation() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig capture = createBaseline(fixture);
        Path raw =
                writeCapture(
                        fixture,
                        List.of(
                                """
{"event":"row_change","xid":9,"relation":"savingaccount","operation":"update","new":{"accountid":1,"balance":8}}
{"event":"row_change","xid":9,"relation":"transfer","operation":"insert","new":{"id":2,"sourceid":10,"targetid":20,"amount":2}}
{"event":"row_change","xid":9,"relation":"savingaccount","operation":"delete","old":{"accountid":1,"balance":8}}
{"event":"transaction_abort","xid":9}
"""));
        Path client = fixture.resolve("jdbc-fixture.jsonl");
        Files.write(
                client,
                Files.readAllLines(client).stream()
                        .filter(line -> !line.contains("\"event\":\"rollback\""))
                        .toList());
        assertEquals(1L, RequestCaptureAudit.audit(client, raw).get("close_aborts"));
        Path output = fixture.resolve("compiled");
        IndexTraceCompiler.compile(capture, raw, output);
        var manifest =
                JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                        .getAsJsonObject();
        assertEquals(0, manifest.get("workload_operations").getAsLong());
        assertEquals(0, manifest.get("logical_row_version_writes").getAsLong());
        assertEquals(0, manifest.get("committed_transactions").getAsLong());
        assertEquals(3, manifest.get("discarded_aborted_row_changes").getAsLong());
        for (var file : manifest.getAsJsonArray("incremental_files"))
            assertEquals(0, file.getAsJsonObject().get("rows").getAsLong());
        assertEquals(
                "passed",
                ReferenceReplay.run(
                                output,
                                new ReplayConfig(true, "none", 0, 0, 0, 0),
                                fixture.resolve("replay.json"))
                        .get("status"));
    }

    @Test
    public void excludesAbortedWriteDependentSearchWithoutRejectingCapture() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig capture = createBaseline(fixture);
        Path raw =
                writeCapture(
                        fixture,
                        List.of(
                                """
{"event":"row_change","xid":9,"relation":"savingaccount","operation":"update","new":{"accountid":1,"balance":8}}
""",
                                accountSearch("a")
                                        + "{\"event\":\"transaction_abort\",\"xid\":9}\n"));
        Path output = fixture.resolve("compiled");
        IndexTraceCompiler.compile(capture, raw, output);
        var manifest =
                JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                        .getAsJsonObject();
        assertEquals(0, manifest.get("workload_operations").getAsLong());
        assertEquals(1, manifest.get("discarded_aborted_transaction_local_searches").getAsLong());
        assertEquals(1, manifest.get("discarded_aborted_row_changes").getAsLong());
    }

    @Test
    public void compilesCommittedWriteReadWriteAndInsertSearchInOriginalOrder() throws Exception {
        Path fixture = temporary.newFolder().toPath();
        CaptureConfig capture = createBaseline(fixture);
        Path raw =
                writeCapture(
                        fixture,
                        List.of(
                                accountSearch("before"),
                                """
{"event":"row_change","xid":9,"relation":"savingaccount","operation":"update","new":{"accountid":1,"balance":8}}
""",
                                accountSearch("first"),
                                """
{"event":"row_change","xid":9,"relation":"savingaccount","operation":"update","new":{"accountid":1,"balance":7}}
""",
                                accountSearch("second"),
                                """
{"event":"row_change","xid":9,"relation":"transfer","operation":"insert","new":{"id":2,"sourceid":77,"targetid":88,"amount":2}}
""",
                                """
{"event":"scan_rescan","scan":"inserted","relation":"idx_transfer_1","keys":[{"strategy":3,"flags":0,"value":77}]}
{"event":"bitmap_candidate","scan":"inserted","found":true}
{"event":"scan_end","scan":"inserted"}
""",
                                """
{"event":"row_change","xid":9,"relation":"savingaccount","operation":"delete","old":{"accountid":1,"balance":7}}
""",
                                """
{"event":"scan_rescan","scan":"deleted","relation":"savingaccount_pkey","keys":[{"strategy":3,"flags":0,"value":1}]}
{"event":"scan_end","scan":"deleted"}
{"event":"transaction_commit","xid":9}
"""));
        String rawChecksum = BaselineGenerator.sha256(raw);
        Path output = fixture.resolve("compiled");
        IndexTraceCompiler.compile(capture, raw, output);
        Path repeat = fixture.resolve("compiled-repeat");
        IndexTraceCompiler.compile(capture, raw, repeat);
        for (String file :
                List.of(
                        "baseline.ops",
                        "workload.ops",
                        "account-versions.parquet",
                        "transfer-versions.parquet",
                        "manifest.json"))
            assertEquals(
                    BaselineGenerator.sha256(output.resolve(file)),
                    BaselineGenerator.sha256(repeat.resolve(file)));
        assertEquals(rawChecksum, BaselineGenerator.sha256(raw));
        List<IndexOperationTrace.Operation> operations = new ArrayList<>();
        IndexOperationTrace.read(output.resolve("workload.ops"), operations::add);
        assertEquals(
                List.of(6, 2, 3, 2, 3, 2, 3, 3, 3, 2, 5, 2, 7),
                operations.stream().map(IndexOperationTrace.Operation::kind).toList());
        assertEquals(0, operations.get(1).readTimestamp());
        assertEquals(0, operations.get(1).mutationOrdinal());
        assertEquals(1, operations.get(3).readTimestamp());
        assertEquals(1, operations.get(3).mutationOrdinal());
        assertEquals(2, operations.get(5).mutationOrdinal());
        assertEquals(3, operations.get(9).mutationOrdinal());
        assertEquals(4, operations.get(11).mutationOrdinal());
        long previousEvent = 0;
        for (var operation : operations) {
            if (IndexOperationTrace.isControl(operation)) continue;
            assertTrue(operation.sourceEvent() >= previousEvent);
            previousEvent = operation.sourceEvent();
            assertEquals(operations.get(0).transactionId(), operation.transactionId());
        }
        for (String mode : new String[] {"operation", "transaction"})
            for (boolean secondary : new boolean[] {true, false})
                for (boolean rowIds : new boolean[] {true, false}) {
                    var model = new org.sibench.capture.IndexReferenceReplay(rowIds, secondary);
                    IndexOperationTrace.readExpanded(
                            output.resolve("baseline.ops"),
                            new org.sibench.replay.ReplayProjection(secondary, model::accept));
                    List<Long> seen = new ArrayList<>();
                    var replay =
                            new org.sibench.replay.ReplayProjection(
                                    new ReplayConfig(secondary, "none", 0, 0, 0, 0, mode),
                                    true,
                                    operation -> {
                                        model.accept(operation);
                                        if (operation.kind() == IndexOperationTrace.SEARCH
                                                && operation.table() == 3
                                                && operation.expectedCount() == 1)
                                            seen.add(
                                                    model.lookupPrimary(
                                                                    3, 1, operation.readTimestamp())
                                                            .value());
                                    });
                    IndexOperationTrace.readExpanded(output.resolve("workload.ops"), replay);
                    replay.finish();
                    assertEquals(
                            rowIds
                                    ? List.of(1L, 1L, 1L)
                                    : List.of(
                                            org.sibench.capture.IndexReferenceReplay.locator(1, 0),
                                            org.sibench.capture.IndexReferenceReplay.locator(3, 0),
                                            org.sibench.capture.IndexReferenceReplay.locator(3, 1)),
                            seen);
                    assertNull(model.lookupPrimary(3, 1, 1));
                    assertNotNull(model.lookupPrimary(3, 1, 0));
                    assertEquals(secondary ? 5 : 4, model.queries());
                }
    }

    private static String accountSearch(String scan) {
        return """
{"event":"scan_rescan","scan":"%s","relation":"savingaccount_pkey","keys":[{"strategy":3,"flags":0,"value":1}]}
{"event":"heap_visibility","scan":"%s","found":true}
{"event":"scan_end","scan":"%s"}
"""
                .formatted(scan, scan, scan);
    }

    private static Path writeCapture(Path fixture, List<String> requests) throws Exception {
        List<String> client = new ArrayList<>(List.of("{\"event\":\"connection\",\"pid\":1}"));
        List<String> server = new ArrayList<>();
        for (int i = 0; i < requests.size(); i++) {
            String app = "fixture-" + i;
            String sql =
                    requests.get(i).contains("\"row_change\"")
                            ? "UPDATE savingaccount SET balance=8"
                            : "SELECT 1";
            client.add(
                    new Gson()
                            .toJson(
                                    Map.of(
                                            "event",
                                            "request_start",
                                            "application",
                                            app,
                                            "sql",
                                            sql,
                                            "parameters",
                                            Map.of("1", 1, "2", 77))));
            client.add(new Gson().toJson(Map.of("event", "request_return", "application", app)));
            server.add(
                    new Gson()
                            .toJson(
                                    Map.of(
                                            "event",
                                            "executor_start",
                                            "application",
                                            app,
                                            "pid",
                                            1,
                                            "sql",
                                            sql)));
            String boundary = null;
            for (String line : requests.get(i).lines().toList()) {
                var event = JsonParser.parseString(line).getAsJsonObject();
                event.addProperty("pid", 1);
                event.addProperty("application", app);
                String kind = event.get("event").getAsString();
                if (kind.equals("transaction_commit") || kind.equals("transaction_abort")) {
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
                                                    app)));
                } else server.add(event.toString());
            }
            server.add(
                    new Gson()
                            .toJson(Map.of("event", "executor_end", "pid", 1, "application", app)));
            if (boundary != null) server.add(boundary);
        }
        client.add(
                new Gson()
                        .toJson(
                                Map.of(
                                        "event",
                                        "close",
                                        "application",
                                        "fixture-" + (requests.size() - 1))));
        Files.write(fixture.resolve("jdbc-fixture.jsonl"), client);
        Path raw = fixture.resolve("runtime.jsonl");
        Files.write(raw, server);
        return raw;
    }
}
