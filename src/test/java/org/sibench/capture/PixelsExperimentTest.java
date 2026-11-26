package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.time.Duration;
import java.nio.file.*;
import java.util.*;
import com.google.gson.*;
import java.io.DataOutputStream;
import io.pixelsdb.pixels.sink.SinkProto;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import java.util.concurrent.*;
import org.junit.Test;

public class PixelsExperimentTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void externalQueriesAndFreshnessRequireFixedPortsButManagedAndDisabledQueriesDoNot() throws Exception {
        var settings = org.sibench.config.ConfigValues.object(org.sibench.config.SibenchConfig.read(Path.of("config/sibench.yaml")), "pixels_experiment");
        PixelsExperiment.requireQueryPorts(settings, Map.of());
        for (String enabled : List.of("queries", "freshness")) {
            var section = org.sibench.config.ConfigValues.object(settings, enabled);
            section.put("enabled", true);
            settings.put(enabled, section);
            assertThrows(IllegalArgumentException.class, () -> PixelsExperiment.requireQueryPorts(settings, Map.of("properties", Map.of())));
            var ports = Map.of("etcd.port", "18801", "metadata.server.port", "18802", "trans.server.port", "18803",
                    "retina.server.port", "18804", "node.server.port", "18805", "query.schedule.server.port", "18806",
                    "index.server.port", "18807", "scaling.metrics.server.port", "18808", "worker.coordinate.server.port", "18809");
            PixelsExperiment.requireQueryPorts(settings, Map.of("properties", ports));
            var trino = org.sibench.config.ConfigValues.object(settings, "trino");
            trino.put("deployment_directory", "/opt/trino-deployment");
            settings.put("trino", trino);
            PixelsExperiment.requireQueryPorts(settings, Map.of());
            trino.put("deployment_directory", "");
            section.put("enabled", false);
        }
    }

    @Test public void plainSqlRunsWithoutReferenceAnswersAndStillConsumesAllResults() throws Exception {
        Path root = temporary.getRoot().toPath();
        Path sql = Files.writeString(root.resolve("query.sql"), "SELECT 1");
        assertNull(PixelsExperiment.answers(root, Map.of("updates", false), null));
        assertNull(PixelsExperiment.answers(root, Map.of("updates", true), null));
        var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        var statements = new java.util.ArrayList<String>();
        server.createContext("/", request -> {
            String body;
            if (request.getRequestMethod().equals("POST")) {
                String statement = new String(request.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                statements.add(statement);
                body = statement.startsWith("EXPLAIN ") ? "{\"id\":\"plan\",\"data\":[[\"plan\"]]}" : "{\"id\":\"query\",\"data\":[[999],[1000]]}";
            } else body = "{\"queryStats\":{}}";
            byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            request.sendResponseHeaders(200, bytes.length);
            request.getResponseBody().write(bytes);
            request.close();
        });
        server.start();
        try {
            PixelsExperiment.query(List.of(sql), Map.of("rounds", 1), Map.of("server", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "user", "test", "catalog", "pixels", "schema", "postgres", "timeout_seconds", 3), root, null);
            assertEquals(List.of("EXPLAIN SELECT 1", "SELECT 1"), statements);
            var record = JsonParser.parseString(Files.readString(root.resolve("queries/0-0-result.json"))).getAsJsonObject();
            assertFalse(record.has("oracle"));
            assertEquals(2, record.getAsJsonObject("result").get("rows").getAsInt());
        } finally { server.stop(0); }
    }

    @Test public void staticInputNeedsOnlyTheSealedBaselineAndStillRejectsTampering() throws Exception {
        Path root = temporary.getRoot().toPath();
        Path csv = Files.createDirectory(root.resolve("csv"));
        Path baseline = PixelsBaselineTest.fixture(csv, Files.createDirectory(root.resolve("baseline")));
        Generation.deleteTree(csv);
        var inputs = PixelsExperiment.inputs(root, null, baseline, Map.of("updates", false));
        assertNull(inputs.cdc());
        assertEquals(8, inputs.source().tables().size());
        assertEquals(8, PixelsBaseline.rows(inputs.baseline().manifest()).size());
        Files.writeString(baseline.resolve("schema.sql"), "changed");
        assertThrows(IOException.class, () -> PixelsExperiment.inputs(root, null, baseline, Map.of("updates", false)));
    }

    @Test public void experimentModeRejectsMissingOrUnwantedCdcBeforeOpeningFiles() {
        Path root = temporary.getRoot().toPath(), absent = root.resolve("absent");
        assertEquals("Updates require a CDC input directory", assertThrows(IllegalArgumentException.class,
                () -> PixelsExperiment.inputs(root, null, absent, Map.of("updates", true))).getMessage());
        assertEquals("Static experiments must omit the CDC input directory", assertThrows(IllegalArgumentException.class,
                () -> PixelsExperiment.inputs(root, absent, absent, Map.of("updates", false))).getMessage());
    }

    @Test public void updatesStillRequireMatchingCompleteCdcAndAnAvailablePrefix() throws Exception {
        Path root = temporary.getRoot().toPath();
        Path csv = Files.createDirectory(root.resolve("csv"));
        Path baseline = PixelsBaselineTest.fixture(csv, Files.createDirectory(root.resolve("baseline")));
        var bound = PixelsBaseline.open(baseline);
        var manifest = JsonParser.parseString(Files.readString(csv.resolve("manifest.json"))).getAsJsonObject();
        manifest.addProperty("kind", "pixels_cdc_input");
        manifest.getAsJsonObject("source_evidence").addProperty("csv_manifest_sha256", bound.manifest().get("csv_manifest_sha256").getAsString());
        Path file = Files.createDirectory(csv.resolve("cdc")).resolve("00000000.proto");
        String schema = Files.readString(csv.resolve(manifest.getAsJsonObject("schema").get("path").getAsString()));
        int marker = schema.lines().toList().indexOf("sibench_marker\tid\tid:bigint") + 1;
        assertTrue(marker > 0);
        var begin = SinkProto.TransactionMetadata.newBuilder().setId("71").setStatus(SinkProto.TransactionStatus.BEGIN).build();
        var row = SinkProto.RowRecord.newBuilder().setOp(SinkProto.OperationType.INSERT)
                .setSource(SinkProto.SourceInfo.newBuilder().setDb("postgres").setSchema("public").setTable("sibench_marker"))
                .setTransaction(SinkProto.TransactionInfo.newBuilder().setId("71").setTotalOrder(1).setDataCollectionOrder(1))
                .setAfter(SinkProto.RowValue.newBuilder().addValues(SinkProto.ColumnValue.newBuilder()
                        .setValue(com.google.protobuf.ByteString.copyFrom(new byte[] {0,0,0,0,0,0,0,1})))).build();
        var end = begin.toBuilder().setStatus(SinkProto.TransactionStatus.END).setEventCount(1)
                .addDataCollections(SinkProto.DataCollection.newBuilder().setDataCollection("public.sibench_marker").setEventCount(1)).build();
        try (var stream = new DataOutputStream(Files.newOutputStream(file))) {
            for (var frame : List.of(Map.entry(-1, begin), Map.entry(marker, row), Map.entry(-1, end))) {
                stream.writeInt(frame.getKey()); stream.writeInt(frame.getValue().getSerializedSize());
                stream.write(frame.getValue().toByteArray());
            }
        }
        var files = new JsonArray(); var descriptor = InputFiles.file(csv, file);
        descriptor.addProperty("transactions", 1); files.add(descriptor); manifest.add("cdc_files", files);
        manifest.add("counts", new Gson().toJsonTree(new CdcInput.Counts(1, 1, 0, 1)));
        Files.writeString(csv.resolve("manifest.json"), manifest.toString());
        var settings = Map.<String, Object>of("updates", true, "max_transactions", 1);
        assertEquals(new CdcInput.Counts(1, 1, 0, 1), PixelsExperiment.inputs(root, csv, baseline, settings).cdc().counts());
        assertThrows(IllegalArgumentException.class, () -> PixelsExperiment.inputs(root, csv, baseline,
                Map.of("updates", true, "max_transactions", 2)));
        manifest.getAsJsonObject("source_evidence").addProperty("csv_manifest_sha256", "f".repeat(64));
        Files.writeString(csv.resolve("manifest.json"), manifest.toString());
        assertThrows(IOException.class, () -> PixelsExperiment.inputs(root, csv, baseline, settings));
        manifest.getAsJsonObject("source_evidence").addProperty("csv_manifest_sha256", bound.manifest().get("csv_manifest_sha256").getAsString());
        Files.writeString(csv.resolve("manifest.json"), manifest.toString());
        Files.write(file, new byte[0]);
        assertThrows(IOException.class, () -> PixelsExperiment.inputs(root, csv, baseline, settings));
    }

    @Test public void failedConcurrentQueryCancelsAnIncompleteApplication() {
        var application = new CompletableFuture<Void>();
        var query = CompletableFuture.failedFuture(new IOException("deliberate query failure"));
        var error = assertThrows(IOException.class, () -> PixelsExperiment.await(application, query, Duration.ofSeconds(2)));
        assertEquals("deliberate query failure", error.getMessage());
        assertTrue(application.isCancelled());
    }
}
