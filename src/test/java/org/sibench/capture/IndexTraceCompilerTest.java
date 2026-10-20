package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;
import org.sibench.generation.BaselineGenerator;
import org.sibench.generation.GenerationConfig;
import org.sibench.generation.ParquetDataFile;
import org.sibench.generation.TableData;
import org.sibench.replay.ReferenceReplay;
import org.sibench.replay.ReplayConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class IndexTraceCompilerTest {
    @Test
    public void compilesExplicitIndexesAndObservedDeleteWithoutChangingCaptureDistribution()
            throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var original = CaptureConfig.read(root, Path.of("config/hybench_sf1.yaml"));
        Path fixture = Files.createTempDirectory("sibench-compiler-");
        try {
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
""");
            List<String> correlated = new ArrayList<>();
            for (String line : Files.readAllLines(raw)) {
                var event = JsonParser.parseString(line).getAsJsonObject();
                event.addProperty("application", "fixture");
                correlated.add(event.toString());
            }
            correlated.add(
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
{"event":"close","application":"fixture"}
""");
            Path output = fixture.resolve("compiled");
            IndexTraceCompiler.compile(capture, raw, output);
            var manifest =
                    JsonParser.parseString(Files.readString(output.resolve("manifest.json")))
                            .getAsJsonObject();
            assertEquals(4, manifest.get("operation_trace_version").getAsInt());
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
                    events.subList(3, 6).stream()
                            .map(IndexOperationTrace.Operation::index)
                            .toList());
            assertTrue(
                    events.subList(1, 6).stream()
                            .allMatch(o -> o.version() == 1 && o.transactionId() == 4));
            assertEquals(1, events.get(1).mutationOrdinal());
            assertEquals(2, events.get(2).mutationOrdinal());
            assertEquals(3, events.get(3).mutationOrdinal());
            assertEquals(IndexOperationTrace.LOGICAL_DELETE, events.get(9).kind());
            assertEquals(2, events.get(9).version());
            assertEquals(1, events.get(9).rowId());
            assertEquals(0, events.get(7).version());
            assertEquals(1, events.get(7).readTimestamp());
            assertEquals(2, events.get(11).readTimestamp());
            assertEquals(3, events.get(14).readTimestamp());
            assertEquals(3, manifest.get("committed_transactions").getAsLong());
            assertEquals(13, events.get(12).transactionId());
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
                for (boolean coalesce : new boolean[] {true, false}) {
                    Path report = fixture.resolve("replay-" + secondary + "-" + coalesce + ".json");
                    assertEquals(
                            "passed",
                            ReferenceReplay.run(
                                            output,
                                            new ReplayConfig(
                                                    secondary, "none", 0, 0, 0, 0, coalesce),
                                            report)
                                    .get("status"));
                }
            assertEquals(original.percentages(), capture.percentages());
        } finally {
            try (var paths = Files.walk(fixture)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList())
                    Files.delete(path);
            }
        }
    }
}
