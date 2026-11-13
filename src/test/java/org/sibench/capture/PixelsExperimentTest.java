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
