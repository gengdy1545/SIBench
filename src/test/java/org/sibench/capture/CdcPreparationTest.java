package org.sibench.capture;

import static org.junit.Assert.*;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.sink.SinkProto;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CdcPreparationTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void rejectsMissingHybenchSelectionBeforeLookingForNativeBuilds() throws Exception {
        Path root = Path.of("").toRealPath(), config = temporary.newFile().toPath();
        var entry=org.sibench.config.BenchmarkFixtures.entry(Path.of("config/benchmark/ycsb.yaml"),"rocksdb",temporary.getRoot().toPath().resolve("trace"));
        entry.put("workload","workloada");
        Files.writeString(config, new org.yaml.snakeyaml.Yaml().dump(Map.of("schema_version", 1, "purpose", "sibench",
                "benchmarks", Map.of("ycsb-a",entry),
                "engines", Map.of("rocksdb", "/missing/engine-config.yaml"))));
        var failure = assertThrows(IllegalArgumentException.class, () -> CdcPreparation.run(root, config, Path.of("/missing/csv-input")));
        assertTrue(failure.getMessage(), failure.getMessage().contains("requires one HyBench benchmark selection"));
    }

    /** Preparation binds the CSV without hashing it; the single post-load pass must still stop a changed copy. */
    @Test public void aCsvCopyChangedAfterMaterializationIsNeverPublished() throws Exception {
        for (boolean change : new boolean[] {false, true}) {
            Path source = temporary.newFolder().toPath(), output = temporary.newFolder().toPath();
            CsvInputTest.fixture(source);
            var csv = CsvInput.bind(source);
            CsvInput.materialize(csv, output);
            int marker = Files.readAllLines(output.resolve("pixels-schema.tsv")).size();
            Path file = Files.createDirectory(output.resolve("cdc")).resolve("00000000.proto");
            try (var stream = new DataOutputStream(Files.newOutputStream(file))) {
                frame(stream, -1, SinkProto.TransactionMetadata.newBuilder().setId("7").setStatus(SinkProto.TransactionStatus.BEGIN).build());
                frame(stream, marker, SinkProto.RowRecord.newBuilder().setOp(SinkProto.OperationType.INSERT)
                        .setSource(SinkProto.SourceInfo.newBuilder().setDb("postgres").setSchema("public").setTable("sibench_marker"))
                        .setTransaction(SinkProto.TransactionInfo.newBuilder().setId("7").setTotalOrder(1).setDataCollectionOrder(1))
                        .setAfter(SinkProto.RowValue.newBuilder().addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(new byte[8]))))
                        .build());
                frame(stream, -1, SinkProto.TransactionMetadata.newBuilder().setId("7").setStatus(SinkProto.TransactionStatus.END).setEventCount(1)
                        .addDataCollections(SinkProto.DataCollection.newBuilder().setDataCollection("public.sibench_marker").setEventCount(1)).build());
            }
            var entry = InputFiles.file(output, file);
            Files.writeString(output.resolve("cdc/files.json"), "[{\"name\":\"00000000.proto\",\"bytes\":" + entry.get("bytes")
                    + ",\"sha256\":\"" + entry.get("sha256").getAsString() + "\",\"transactions\":1}]");
            var counts = new CdcInput.Counts(1, 1, 0, 1);
            if (!change) {
                var manifest = CdcInput.publish(output, csv, counts, Map.of());
                assertEquals(csv.sha256(), ((Map<?, ?>) manifest.get("source_evidence")).get("csv_manifest_sha256"));
                assertEquals("passed", CdcInput.check(output).get("status"));
                continue;
            }
            // Same size, one byte: the hard-linked original changes too, and size-only binding still accepts it.
            Path copy = output.resolve("native/Data_1x/customer.csv");
            byte[] bytes = Files.readAllBytes(copy);
            bytes[0] = (byte) (bytes[0] == '1' ? '2' : '1');
            Files.write(copy, bytes);
            CsvInput.bind(source);
            assertThrows(java.io.IOException.class, () -> CdcInput.publish(output, csv, counts, Map.of()));
            assertFalse(Files.exists(output.resolve("manifest.json")));
        }
    }

    private static void frame(DataOutputStream output, int key, com.google.protobuf.MessageLite message) throws java.io.IOException {
        output.writeInt(key); output.writeInt(message.getSerializedSize()); output.write(message.toByteArray());
    }

    @Test public void protoFileLimitIsAValidatedEngineOverride() {
        assertEquals(Map.of("sink.proto.maxRecords", "2"), org.sibench.config.PixelsConfig.sinkProperties(Map.of("sink.proto.maxRecords", "2")));
        for (String value : List.of("0", "-1", "2147483648"))
            assertThrows(IllegalArgumentException.class,
                    () -> org.sibench.config.PixelsConfig.sinkProperties(Map.of("sink.proto.maxRecords", value)));
    }
}
