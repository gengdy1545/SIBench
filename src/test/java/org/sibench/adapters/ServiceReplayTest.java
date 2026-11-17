package org.sibench.adapters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;
import com.google.protobuf.ByteString;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.ProtoTrace;
import org.sibench.trace.IndexKey;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.replay.ReplayConfig;

public class ServiceReplayTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void deploymentDescriptionRejectsWrongValueModeAndMissingEvidence() throws Exception {
        var description = new IndexDescription("fixture",ValueKind.ROW_ID,
                Acknowledgement.ACK_MEMORY,false,"async persistence","fixture");
        description.require(ValueKind.ROW_ID);
        assertThrows(IOException.class, () -> description.require(ValueKind.LOCATOR));
        assertThrows(IOException.class, () -> new IndexDescription("fixture",ValueKind.ROW_ID,
                null,false,"async persistence","fixture").require(ValueKind.ROW_ID));
        assertThrows(IOException.class, () -> description.require(null));
        var json = new com.google.gson.Gson();
        assertEquals(description, json.fromJson(json.toJson(description), IndexDescription.class));
        for (String field : java.util.List.of("valueKind", "writeAck")) {
            var metadata = json.toJsonTree(description).getAsJsonObject();
            metadata.addProperty(field, "UNKNOWN");
            assertThrows(IOException.class, () -> json.fromJson(metadata, IndexDescription.class).require(ValueKind.ROW_ID));
            metadata.remove(field);
            assertThrows(IOException.class, () -> json.fromJson(metadata, IndexDescription.class).require(ValueKind.ROW_ID));
        }
    }

    /** Nothing listens on port 1: a supply failure must surface before any service call. */
    private static final ServiceTarget UNREACHABLE = new ServiceTarget("127.0.0.1:1", ValueKind.ROW_ID);

    @Test
    public void missingTraceFailsBeforeContactingTheService() {
        Path root = temporary.getRoot().toPath();
        Path report = root.resolve("report.json");
        var error = assertThrows(IOException.class, () -> ServiceReplay.run(
                root.resolve("missing-trace"), new ReplayConfig(true, 0, 0, 0, 0, "operation", 1, 4096, 60, 1), UNREACHABLE, report));
        assertTrue(error.getMessage().contains("Incomplete Trace"));
        assertFalse(Files.exists(report));
        assertEquals(0, temporary.getRoot().list().length);
    }

    static Path trace(Path directory) throws Exception {
        var baseline = new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"), false);
        try (baseline) {
            for (int key = 1; key <= 2; key++) baseline.append(IndexOperationTrace.write(0,
                    IndexOperationTrace.INSERT, 1, 0, IndexKey.ofInt(key), 0, 0, 0, key)
                    .withWorkUnitId(1).withExpansion(0, 1));
        }
        var workload = new ProtoTrace.Writer(directory.resolve("workload.pb.zst"), false);
        try (workload) {
            workload.append(IndexOperationTrace.search(1, 1, 0,
                    IndexOperationTrace.Range.equal(IndexKey.ofInt(1)), 0, 1,
                    IndexOperationTrace.SNAPSHOT_SEARCH).withWorkUnitId(1));
        }
        byte[] units = "{\"work_unit_id\":1,\"source_event\":1}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(directory.resolve("work-units.jsonl.zst"), com.github.luben.zstd.Zstd.compress(units));
        var schema = org.sibench.schema.SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), Map.of("tables", 1));
        var manifest = org.sibench.capture.IndexCatalog.fromSchema(schema).provenance();
        manifest.put("secondary_index_coverage", "primary_only");
        manifest.put("output", "proto");
        manifest.put("value_kinds", List.of("row_id"));
        manifest.put("baseline_rows", 2);
        manifest.put("source_work_units", 1);
        manifest.put("work_units_sha256", java.util.HexFormat.of().formatHex(
                java.security.MessageDigest.getInstance("SHA-256").digest(units)));
        manifest.put("baseline_operations", baseline.count());
        manifest.put("workload_operations", workload.count());
        manifest.put("last_resolved_version", 0);
        var gson = new com.google.gson.Gson();
        manifest.put("proto_files", Map.of("baseline", ProtoTrace.file(baseline), "workload", ProtoTrace.file(workload)));
        manifest.put("baseline_trace_sha256", ProtoTrace.identity(baseline.logicalSha256(), baseline.count()));
        manifest.put("workload_trace_sha256", ProtoTrace.identity(workload.logicalSha256(), workload.count()));
        Files.writeString(directory.resolve("manifest.json"), gson.toJson(manifest));
        return directory;
    }

    @Test public void completedBaselineSurvivesWorkloadFailureWithoutClaimingFullReplay() throws Exception {
        for (String outcome : List.of("passed", "wrong-result", "failed-write")) {
            var backend = new IndexBackend() {
                int writes;
                public IndexDescription describe() { return new IndexDescription("test", ValueKind.ROW_ID,
                        Acknowledgement.ACK_MEMORY, false, "test memory", "test"); }
                public void write(Mutation mutation) throws IOException {
                    if (++writes == 2 && outcome.equals("failed-write")) throw new IOException("Injected write failure");
                }
                public ByteString get(GetRequest request) {
                    return outcome.equals("wrong-result") ? null : org.sibench.trace.IndexEntry.rowId(1);
                }
                public void scan(ScanRequest request, ValueSink sink) { throw new AssertionError("Unexpected Scan"); }
                public void bind(String binding) {}
                public void flush() {}
                public void close() {}
            };
            Path input = trace(temporary.newFolder().toPath()), report = input.resolve("replay.json");
            try (var server = IndexServer.start(0, backend)) {
                var service = new ServiceTarget("127.0.0.1:" + server.port(), ValueKind.ROW_ID);
                var config = new ReplayConfig(false, 0, 0, 0, 0, "operation", 1, 2, 5, 1);
                if (outcome.equals("passed")) assertEquals("passed", ServiceReplay.run(input, config, service, report).get("status"));
                else assertThrows(IOException.class, () -> ServiceReplay.run(input, config, service, report));
            }
            Path completed = input.resolve("replay.json.baseline.json");
            assertEquals(!outcome.equals("failed-write"), Files.exists(completed));
            assertEquals(outcome.equals("passed"), Files.exists(report));
            assertEquals(outcome.equals("passed"), Files.exists(input.resolve("replay.json.index-manifest.json")));
            if (Files.exists(completed)) {
                var saved = com.google.gson.JsonParser.parseString(Files.readString(completed)).getAsJsonObject();
                assertEquals("baseline_passed", saved.get("status").getAsString());
                assertEquals(2, saved.getAsJsonObject("result").get("operations").getAsInt());
                assertEquals(2, saved.getAsJsonObject("result").getAsJsonObject("dispatch")
                        .get("completed_operations").getAsInt());
                assertTrue(saved.get("scope").getAsString().contains("not certified"));
                var trace = com.google.gson.JsonParser.parseString(Files.readString(input.resolve("manifest.json"))).getAsJsonObject();
                assertEquals("workload=" + trace.get("workload_trace_sha256").getAsString() + " baseline="
                        + trace.get("baseline_trace_sha256").getAsString() + " secondary=false",
                        saved.get("index_binding").getAsString());
                assertEquals("trace_integrity_and_supply_valid", saved.getAsJsonObject("preflight").get("status").getAsString());
            }
        }
    }

    @Test public void existingBaselineReportRejectsReplayBeforeReadingTheTrace() throws Exception {
        Path root = temporary.getRoot().toPath(), report = root.resolve("replay.json");
        Path completed = root.resolve("replay.json.baseline.json");
        Files.writeString(completed, "retained evidence");
        var error = assertThrows(IOException.class, () -> ServiceReplay.run(root.resolve("missing"),
                new ReplayConfig(false, 0, 0, 0, 0, "operation", 1, 2, 5, 1), UNREACHABLE, report));
        assertTrue(error.getMessage().contains("Baseline report already exists"));
        assertEquals("retained evidence", Files.readString(completed));
    }
}
