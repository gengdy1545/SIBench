package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.sink.SinkProto;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CdcInputTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void portableInputChecksWithoutAnEngineOrSourceCheckout() throws Exception {
        Path root = fixture(1, 1);
        Path relocated = root.resolveSibling("relocated");
        Files.move(root, relocated);
        assertEquals("passed", CdcInput.check(relocated).get("status"));
    }

    @Test public void replayForwardsWholeTransactionsAndRechecksMutableFileBytes() throws Exception {
        Path root = fixture(1, 1);
        CdcInput.check(root);
        var frames = new java.util.ArrayList<Integer>();
        assertEquals(new CdcInput.Counts(1, 1, 1, 0), CdcInput.stream(root, read(root), 1, (table, bytes) -> frames.add(table)));
        assertEquals(java.util.List.of(-1, 1, -1), frames);
        assertThrows(IOException.class, () -> CdcInput.stream(root, read(root), 2, (table, bytes) -> {}));
        var document = read(root);
        byte[] changed = Files.readAllBytes(root.resolve("cdc/00000000.proto"));
        changed[changed.length - 1] ^= 1;
        Files.write(root.resolve("cdc/00000000.proto"), changed);
        assertThrows(IOException.class, () -> CdcInput.stream(root, document, 0, (table, bytes) -> {}));
    }

    @Test public void structurallyWrongFilesAreRejectedEvenWithMatchingHashes() throws Exception {
        for (int[] wrong : new int[][] {{2, 1}, {1, 2}}) {
            Path root = fixture(wrong[0], wrong[1]);
            assertThrows(IOException.class, () -> CdcInput.check(root));
        }
    }

    @Test public void truncationAndMutationAreRejected() throws Exception {
        Path root = fixture(1, 1), file = root.resolve("cdc/00000000.proto");
        byte[] data = Files.readAllBytes(file);
        Files.write(file, java.util.Arrays.copyOf(data, data.length - 1));
        var document = read(root);
        var item = InputFiles.file(root, file);
        item.addProperty("transactions", 1);
        var files = new JsonArray(); files.add(item); document.add("cdc_files", files); write(root, document);
        assertThrows(IOException.class, () -> CdcInput.check(root));
        Path second = fixture(1, 1);
        Files.writeString(second.resolve("baseline/a.csv"), "2\n");
        assertThrows(IOException.class, () -> CdcInput.check(second));
    }

    @Test public void missingExtraAndUnorderedFilesAreRejected() throws Exception {
        Path missing = fixture(1, 1);
        Files.delete(missing.resolve("cdc/00000000.proto"));
        assertThrows(IOException.class, () -> CdcInput.check(missing));
        Path extra = fixture(1, 1);
        Files.copy(extra.resolve("cdc/00000000.proto"), extra.resolve("cdc/00000001.proto"));
        assertThrows(IOException.class, () -> CdcInput.check(extra));
        Path wrongOrder = fixture(1, 1);
        Files.move(wrongOrder.resolve("cdc/00000000.proto"), wrongOrder.resolve("cdc/00000001.proto"));
        var doc = read(wrongOrder);
        doc.getAsJsonArray("cdc_files").get(0).getAsJsonObject().addProperty("path", "cdc/00000001.proto");
        write(wrongOrder, doc);
        assertThrows(IOException.class, () -> CdcInput.check(wrongOrder));
    }

    @Test public void pathEscapeAndWrongBaselineCountAreRejected() throws Exception {
        Path root = fixture(1, 1);
        var doc = read(root);
        doc.getAsJsonObject("baseline_rows").addProperty("a", 2);
        write(root, doc);
        assertThrows(IOException.class, () -> CdcInput.check(root));
        Path linked = fixture(1, 1);
        Files.delete(linked.resolve("baseline/a.csv"));
        Files.createSymbolicLink(linked.resolve("baseline/a.csv"), root.resolve("baseline/a.csv"));
        assertThrows(IOException.class, () -> CdcInput.check(linked));
        Path aliased = fixture(1, 1);
        doc = read(aliased);doc.getAsJsonObject("baseline_files").getAsJsonObject("a.csv").addProperty("path", "baseline//a.csv");write(aliased, doc);
        assertThrows(IOException.class, () -> CdcInput.check(aliased));
        Path fractional = fixture(1, 1);
        doc = read(fractional);doc.getAsJsonArray("cdc_files").get(0).getAsJsonObject().addProperty("transactions", 1.5);write(fractional, doc);
        assertThrows(IOException.class, () -> CdcInput.check(fractional));
    }

    private Path fixture(int rowOrder, long endCount) throws Exception {
        Path root = temporary.newFolder().toPath();
        Files.createDirectory(root.resolve("baseline")); Files.createDirectory(root.resolve("cdc"));
        Path csv = Files.writeString(root.resolve("baseline/a.csv"), "1\n");
        Path schema = Files.writeString(root.resolve("schema.tsv"), "a\tid\tid:int\nsibench_marker\tid\tid:bigint\n");
        Path ddl = Files.writeString(root.resolve("schema.sql"), "CREATE TABLE a(id int PRIMARY KEY);\n");
        Path file = root.resolve("cdc/00000000.proto");
        try (var stream = new DataOutputStream(Files.newOutputStream(file))) {
            frame(stream, -1, SinkProto.TransactionMetadata.newBuilder().setId("71").setStatus(SinkProto.TransactionStatus.BEGIN).build());
            frame(stream, 1, SinkProto.RowRecord.newBuilder().setOp(SinkProto.OperationType.INSERT)
                    .setSource(SinkProto.SourceInfo.newBuilder().setDb("postgres").setSchema("public").setTable("a"))
                    .setTransaction(SinkProto.TransactionInfo.newBuilder().setId("71").setTotalOrder(rowOrder).setDataCollectionOrder(1))
                    .setAfter(SinkProto.RowValue.newBuilder().addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(new byte[] {0,0,0,1}))))
                    .build());
            frame(stream, -1, SinkProto.TransactionMetadata.newBuilder().setId("71").setStatus(SinkProto.TransactionStatus.END)
                    .setEventCount(endCount).addDataCollections(SinkProto.DataCollection.newBuilder().setDataCollection("public.a").setEventCount(endCount)).build());
        }
        var document = new JsonObject(); document.addProperty("kind", "pixels_cdc_input"); document.addProperty("status", "complete");
        document.add("schema", InputFiles.file(root, schema)); document.add("source_ddl", InputFiles.file(root, ddl));
        document.add("baseline_rows", new Gson().toJsonTree(Map.of("a", 1)));
        var baselines = new JsonObject();var entry = InputFiles.file(root, csv); entry.addProperty("table", "a");entry.addProperty("rows", 1);
        baselines.add("a.csv", entry);document.add("baseline_files", baselines);
        var files = new JsonArray(); var value = InputFiles.file(root, file);value.addProperty("transactions", 1);files.add(value);document.add("cdc_files", files);
        document.add("counts", new Gson().toJsonTree(new CdcInput.Counts(1, 1, 1, 0)));write(root, document);return root;
    }

    private static void frame(DataOutputStream output, int key, com.google.protobuf.MessageLite message) throws IOException {
        output.writeInt(key);output.writeInt(message.getSerializedSize());output.write(message.toByteArray());
    }
    private static JsonObject read(Path root) throws IOException { return com.google.gson.JsonParser.parseString(Files.readString(root.resolve("manifest.json"))).getAsJsonObject(); }
    private static void write(Path root, JsonObject document) throws IOException { Files.writeString(root.resolve("manifest.json"), new Gson().toJson(document)); }
}
