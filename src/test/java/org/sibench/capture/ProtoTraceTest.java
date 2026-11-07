package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.trace.IndexKey;
import org.sibench.adapters.proto.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class ProtoTraceTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static Operation write(int kind, long rowId, long version, long ordinal) {
        return IndexOperationTrace.write(12, kind, 1, 0, IndexKey.ofInt(1), version,
                kind == IndexOperationTrace.LOGICAL_DELETE ? 0 : 7, 0, rowId)
                .withTransaction(version == 0 ? 0 : 91, ordinal).withWorkUnitId(1);
    }

    @Test public void limitedSearchCanExhaustItsRangeBeforeTheLimit() throws Exception {
        var original=List.of(write(IndexOperationTrace.INSERT,1,0,0).withExpansion(0,1),
                IndexOperationTrace.search(21,1,0,new IndexOperationTrace.Range(IndexKey.ofInt(1),true,null,false,false,3),
                        0,1,IndexOperationTrace.SNAPSHOT_SEARCH),
                IndexOperationTrace.search(22,1,0,new IndexOperationTrace.Range(IndexKey.ofInt(2),true,null,false,false,3),
                        0,0,IndexOperationTrace.SNAPSHOT_SEARCH));
        Path file=temporary.getRoot().toPath().resolve("short-scans.pb.zst");
        var writer=new ProtoTrace.Writer(file);
        try(writer){for(var operation:original)writer.append(operation);}
        var decoded=new ArrayList<Operation>();
        ProtoTrace.read(file,writer.sha256(),writer.logicalSha256(),decoded::add);
        assertEquals(original,decoded);
        try(var model=new IndexReferenceReplay(true,true,new IndexReferenceReplay.Settings(null,1,1,1024))){
            for(var operation:decoded)model.accept(operation);
        }
    }

    @Test
    public void abortedWritesAndTheirLocalReadsSurviveEncoding() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("abort.pb.zst");
        var original = List.of(
                IndexOperationTrace.transaction(10, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0),
                write(IndexOperationTrace.UPSERT, 2, 1, 1).withExpansion(0, 1),
                IndexOperationTrace.search(13, 1, 0, IndexOperationTrace.Range.equal(IndexKey.ofInt(1)),
                        0, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
                        .withTransaction(91, 1).withVersion(1).withWorkUnitId(1),
                IndexOperationTrace.transaction(14, IndexOperationTrace.TRANSACTION_ABORT, 91, 1, 1));
        var writer = new ProtoTrace.Writer(file);
        try (writer) { for (var operation : original) writer.append(operation); }
        var decoded = new ArrayList<Operation>();
        ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add);
        assertEquals(original, decoded);
    }

    @Test
    public void serviceRequestsPreserveLocalReadsGroupsLocatorsAndDeleteReinsertIdentity() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("trace.pb.zst");
        var original = new ArrayList<Operation>();
        original.add(write(IndexOperationTrace.INSERT, 1, 0, 0).withExpansion(0, 1));
        original.add(IndexOperationTrace.transaction(10, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0));
        original.add(write(IndexOperationTrace.LOGICAL_DELETE, 1, 1, 1).withExpansion(0, 1));
        original.add(write(IndexOperationTrace.UPSERT, 2, 1, 2).withExpansion(0, 1));
        original.add(IndexOperationTrace.search(13, 1, 0, IndexOperationTrace.Range.equal(IndexKey.ofInt(1)),
                0, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
                .withTransaction(91, 2).withVersion(1).withWorkUnitId(1));
        original.add(IndexOperationTrace.transaction(14, IndexOperationTrace.TRANSACTION_COMMIT, 91, 1, 2));
        for (int i = 0; i < 2; i++) original.add(IndexOperationTrace.search(15, 1, 0,
                IndexOperationTrace.Range.equal(IndexKey.ofInt(i + 1)), null, 1, 1,
                IndexOperationTrace.UNION_GROUP, i, 2).withWorkUnitId(2));
        var writer = new ProtoTrace.Writer(file);
        try (writer) { for (Operation operation : original) writer.append(operation); }
        var decoded = new ArrayList<Operation>();
        assertEquals(original.size(), ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add));
        assertEquals(original, decoded);
        try (var model = new IndexReferenceReplay(true, true, new IndexReferenceReplay.Settings(null, 1, 1, 1024))) {
            for (Operation operation : decoded) if (!IndexOperationTrace.isControl(operation)) model.accept(operation);
            assertEquals(1, model.lookupPrimary(1, IndexKey.ofInt(1), 0).value());
            assertEquals(2, model.lookupPrimary(1, IndexKey.ofInt(1), 1).value());
        }
    }

    @Test
    public void corruptTruncatedAndMisboundFilesAreRejected() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("trace.pb.zst");
        var writer = new ProtoTrace.Writer(file);
        try (writer) { writer.append(write(IndexOperationTrace.INSERT, 1, 0, 0)); }
        byte[] complete = Files.readAllBytes(file);
        for (int length : List.of(0, 3, complete.length - 1, complete.length - 16)) {
            Files.write(file, Arrays.copyOf(complete, length));
            assertThrows(IOException.class, () -> ProtoTrace.read(file, null, null, operation -> {}));
        }
        Files.write(file, complete);
        assertThrows(IOException.class, () -> ProtoTrace.read(file, "wrong", writer.logicalSha256(), operation -> {}));
        assertThrows(IOException.class, () -> ProtoTrace.read(file, writer.sha256(), "wrong", operation -> {}));
        byte[] corrupt = complete.clone();
        corrupt[16] ^= 1;
        Files.write(file, corrupt);
        assertThrows(IOException.class, () -> ProtoTrace.read(file, null, null, operation -> {}));
        Files.write(file, Arrays.copyOf(complete, complete.length + 1));
        assertThrows(IOException.class, () -> ProtoTrace.read(file, null, null, operation -> {}));
    }

    @Test
    public void decodedDataAndControlsAreValidatedBeforeTheyReachTheConsumer() throws Exception {
        var position = TracePosition.newBuilder().setSourceEvent(12).setWorkUnitId(1).build();
        var key = EntryKey.newBuilder().setTable(0)
                .setPrimaryKey(com.google.protobuf.ByteString.copyFrom(IndexKey.ofInt(1).bytes()));
        var write = TraceRecord.newBuilder().addPositions(position).setRequest(Request.newBuilder()
                .setWrite(Write.newBuilder().setKind(Write.Kind.INSERT).setKey(key)
                        .setValue(Value.newBuilder().setRowId(1)))).build();
        var search = Search.newBuilder().setTable(0);
        var badSearch = TraceRecord.newBuilder().addPositions(position)
                .setRequest(Request.newBuilder().setSearch(search)).build();
        var group = TraceRecord.newBuilder().addPositions(position).addPositions(position)
                .setRequest(Request.newBuilder().setGroup(SearchGroup.newBuilder()
                        .addMembers(search).addMembers(search))).build();
        var control = TracePosition.newBuilder().setSourceEvent(12).build();
        var begin = TraceRecord.newBuilder().addPositions(control)
                .setBegin(TraceBegin.newBuilder().setTransactionId(1).setVersion(1)).build();
        var end = TraceRecord.newBuilder().addPositions(control)
                .setRequest(Request.newBuilder().setTransactionId(1).setCommit(Commit.newBuilder().setVersion(1))).build();
        var cases = List.of(List.of(write), List.of(badSearch), List.of(group),
                List.of(begin.toBuilder().setBegin(begin.getBegin().toBuilder().setTransactionId(0)).build()),
                List.of(begin, end.toBuilder().setRequest(end.getRequest().toBuilder()
                        .setCommit(end.getRequest().getCommit().toBuilder().setOperations(1))).build()));
        for (var records : cases) {
            Path file = temporary.newFile().toPath();
            unchecked(file, records);
            var seen = new ArrayList<Operation>();
            assertThrows(IOException.class, () -> ProtoTrace.read(file, null, null, seen::add));
            assertTrue(seen.stream().allMatch(o -> o.kind() == IndexOperationTrace.TRANSACTION_BEGIN && o.sourceEvent() == 12));
        }
    }

    /** Frame deliberately invalid messages independently of the production writer's checks. */
    private static void unchecked(Path file, List<TraceRecord> records) throws IOException {
        var bytes = new java.io.ByteArrayOutputStream();
        for (var record : records) record.writeDelimitedTo(bytes);
        framed(file, bytes.toByteArray(), records.size());
    }

    @Test
    public void messageLengthsAndRecordCountCannotEscapeTheirBlock() throws Exception {
        Path file = temporary.newFile().toPath();
        for (byte[] plain : List.of(new byte[0], new byte[] {0}, new byte[] {10, 8, 1},
                new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 15})) {
            framed(file, plain, 1);
            assertThrows(IOException.class, () -> ProtoTrace.read(file, null, null, operation -> {}));
        }
    }

    private static void framed(Path file, byte[] plain, int records) throws IOException {
        byte[] compressed = com.github.luben.zstd.Zstd.compress(plain);
        var crc = new java.util.zip.CRC32C();
        crc.update(plain);
        try (var output = new java.io.DataOutputStream(Files.newOutputStream(file))) {
            output.writeInt(0x53495042);
            output.writeInt(records);
            output.writeInt(compressed.length);
            output.writeInt(plain.length);
            output.writeInt((int) crc.getValue());
            output.write(compressed);
            for (int i = 0; i < 4; i++) output.writeInt(0);
        }
    }

    @Test
    public void inventoryRequiresCanonicalFileAndBindsItsCountAndIdentity() throws Exception {
        Path root = temporary.getRoot().toPath();
        var writer = new ProtoTrace.Writer(root.resolve("baseline.pb.zst"));
        try (writer) { writer.append(write(IndexOperationTrace.INSERT, 1, 0, 0)); }
        var files = new com.google.gson.JsonArray();
        files.add(new com.google.gson.Gson().toJsonTree(ProtoTrace.file("baseline.pb.zst", writer)));
        var manifest = new com.google.gson.JsonObject();
        var inventory = new com.google.gson.JsonObject();
        inventory.add("baseline", files);
        manifest.add("proto_files", inventory);
        manifest.addProperty("baseline_trace_sha256", ProtoTrace.identity(files));
        assertEquals(1, ProtoTrace.source(root, manifest, "baseline").read(operation -> {}));
        var file = files.get(0).getAsJsonObject();
        file.addProperty("operations", 2);
        assertThrows(IOException.class, () -> ProtoTrace.source(root, manifest, "baseline"));
        manifest.addProperty("baseline_trace_sha256", ProtoTrace.identity(files));
        assertThrows(IOException.class, () -> ProtoTrace.source(root, manifest, "baseline").read(operation -> {}));
        for (String name : List.of("../outside.pb.zst", "baseline/table1.pb.zst")) {
            file.addProperty("path", name);
            assertThrows(IOException.class, () -> ProtoTrace.source(root, manifest, "baseline"));
        }
        file.addProperty("path", "baseline.pb.zst");
        files.add(file.deepCopy());
        manifest.addProperty("baseline_trace_sha256", ProtoTrace.identity(files));
        assertThrows(IOException.class, () -> ProtoTrace.source(root, manifest, "baseline"));
    }

    @Test
    public void unfinishedTransactionsAndGroupsCannotBePublished() throws Exception {
        Path root = temporary.getRoot().toPath();
        var transaction = new ProtoTrace.Writer(root.resolve("transaction.pb.zst"));
        transaction.append(IndexOperationTrace.transaction(1, IndexOperationTrace.TRANSACTION_BEGIN, 1, 1, 0));
        assertThrows(IOException.class, transaction::close);
        var group = new ProtoTrace.Writer(root.resolve("group.pb.zst"));
        group.append(IndexOperationTrace.search(1, 1, 0, IndexOperationTrace.Range.equal(IndexKey.ofInt(1)),
                null, 0, 0, IndexOperationTrace.UNION_GROUP, 0, 2).withWorkUnitId(1));
        assertThrows(IOException.class, group::close);
    }

    @Test
    public void multipleBlocksPreserveEveryMemberAndRequireTheirFinalTerminator() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("blocks.pb.zst");
        var writer = new ProtoTrace.Writer(file);
        try (writer) {
            for (int i = 1; i <= 9000; i++) writer.append(IndexOperationTrace.write(0,
                    IndexOperationTrace.INSERT, 1, 0, IndexKey.ofInt(i), 0, 0, 0, i).withWorkUnitId(1));
        }
        long[] expected = {1};
        assertEquals(9000, ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), operation -> {
            assertEquals(IndexKey.ofInt(expected[0]), operation.key());
            assertEquals(expected[0]++, operation.rowId());
        }));
        assertEquals(9001, expected[0]);
    }
}
