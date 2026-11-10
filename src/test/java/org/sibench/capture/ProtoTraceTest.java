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

    @Test public void decodingRejectsMissingValuesAndUnknownNestedFieldsBeforeLosingTheirPresence() throws Exception {
        var unknown=com.google.protobuf.UnknownFieldSet.newBuilder().addField(99,
                com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var key=EntryKey.newBuilder().setTable(1).setPrimaryKey(com.google.protobuf.ByteString.copyFrom(IndexKey.ofInt(1).bytes())).build();
        var value=Value.newBuilder().setRowId(0).build();
        var write=Write.newBuilder().setKind(Write.Kind.INSERT).setKey(key).setValue(value).build();
        var position=TracePosition.newBuilder().setSourceEvent(1).setWorkUnitId(1).build();
        var valid=TraceRecord.newBuilder().setRequest(Request.newBuilder().setWrite(write)).addPositions(position).build();
        var cases=new LinkedHashMap<String,TraceRecord>();
        cases.put("missing value",valid.toBuilder().setRequest(Request.newBuilder().setWrite(write.toBuilder().clearValue())).build());
        cases.put("unknown write",valid.toBuilder().setRequest(Request.newBuilder().setWrite(write.toBuilder().setUnknownFields(unknown))).build());
        cases.put("unknown key",valid.toBuilder().setRequest(Request.newBuilder().setWrite(write.toBuilder().setKey(key.toBuilder().setUnknownFields(unknown)))).build());
        cases.put("unknown value",valid.toBuilder().setRequest(Request.newBuilder().setWrite(write.toBuilder().setValue(value.toBuilder().setUnknownFields(unknown)))).build());
        cases.put("unknown position",valid.toBuilder().setPositions(0,position.toBuilder().setUnknownFields(unknown)).build());
        var bound=Bound.newBuilder().setKey(key.getPrimaryKey()).setInclusive(true).build();
        cases.put("unknown bound",TraceRecord.newBuilder().addPositions(position.toBuilder().setFlags(IndexOperationTrace.LOWER_INCLUSIVE))
                .setRequest(Request.newBuilder().setSearch(Search.newBuilder().setTable(1).setLower(bound.toBuilder().setUnknownFields(unknown)))).build());
        var accepted=new ArrayList<String>();
        for (var entry:cases.entrySet()) {
            var file=temporary.getRoot().toPath().resolve(entry.getKey()+".pb.zst");
            unchecked(file,List.of(entry.getValue()));
            try { ProtoTrace.read(file,null,null,ignored->{}); accepted.add(entry.getKey()); }
            catch (IOException expected) { }
        }
        assertEquals("Malformed nested messages were silently projected",List.of(),accepted);
        var zero=temporary.getRoot().toPath().resolve("valid-zero.pb.zst");
        unchecked(zero,List.of(valid));
        var decoded=new ArrayList<Operation>();
        ProtoTrace.read(zero,null,null,decoded::add);
        assertEquals(0,decoded.get(0).rowId());
    }

    @Test public void logicalDigestMatchesHandSpecifiedMessageAndRejectsDifferentValidTrace() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("identity.pb.zst");
        var operation = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(42), 0, 7, 0, 11).withWorkUnitId(1);
        var expected = TraceRecord.newBuilder()
                .addPositions(TracePosition.newBuilder().setSourceEvent(1).setWorkUnitId(1))
                .setRequest(Request.newBuilder().setWrite(Write.newBuilder().setKind(Write.Kind.INSERT)
                        .setKey(EntryKey.newBuilder().setTable(1).setPrimaryKey(
                                com.google.protobuf.ByteString.copyFrom(HexFormat.of().parseHex("01800000000000002a"))))
                        .setValue(Value.newBuilder().setRowId(11).setFile(7)))).build();
        var plain = new java.io.ByteArrayOutputStream();
        expected.writeDelimitedTo(plain);
        String digest = HexFormat.of().formatHex(org.sibench.config.Digests.digest().digest(plain.toByteArray()));
        var writer = new ProtoTrace.Writer(file);
        try (writer) { writer.append(operation); }
        assertEquals(digest, writer.logicalSha256());
        var decoded = new ArrayList<Operation>();
        assertEquals(1, ProtoTrace.read(file, writer.sha256(), digest, decoded::add));
        assertEquals(List.of(operation), decoded);
        Files.delete(file);
        try (var different = new ProtoTrace.Writer(file)) { different.append(operation.withWorkUnitId(2)); }
        assertTrue(assertThrows(IOException.class, () -> ProtoTrace.read(file, null, digest, ignored -> {}))
                .getMessage().contains("logical digest mismatch"));
    }

    private static Operation write(int kind, long rowId, long version, long ordinal) {
        return IndexOperationTrace.write(12, kind, 1, 0, IndexKey.ofInt(1), version,
                kind == IndexOperationTrace.LOGICAL_DELETE ? 0 : 7, 0, rowId)
                .withTransaction(version == 0 ? 0 : 91, ordinal).withWorkUnitId(1);
    }

    @Test public void readOnlySourceTransactionsStreamSearchesButWritesBoundTheBuffer() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("read-only.pb.zst");
        var search = IndexOperationTrace.search(2, 1, 0, IndexOperationTrace.Range.equal(IndexKey.ofInt(1)), 0, 0, 0)
                .withTransaction(91, 0).withWorkUnitId(1);
        int searches = ProtoTrace.MAX_TRANSACTION_OPERATIONS + 1;
        var writer = new ProtoTrace.Writer(file);
        try (writer) {
            writer.accept(IndexOperationTrace.transaction(1, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0));
            for (int i = 0; i < searches; i++) writer.accept(search);
            writer.accept(IndexOperationTrace.transaction(3, IndexOperationTrace.TRANSACTION_COMMIT, 91, 1, 0));
        }
        long[] read = {0};
        ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), operation -> read[0]++);
        assertEquals(searches + 2, read[0]);
        var buffered = new ProtoTrace.Writer(file.resolveSibling("buffered.pb.zst"));
        buffered.accept(IndexOperationTrace.transaction(1, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0));
        buffered.accept(write(IndexOperationTrace.UPSERT, 1, 1, 1).withExpansion(0, 1));
        var after = IndexOperationTrace.search(13, 1, 0, IndexOperationTrace.Range.equal(IndexKey.ofInt(1)), 0, 0, 0)
                .withTransaction(91, 1).withWorkUnitId(1);
        assertTrue(assertThrows(IOException.class, () -> {
            for (int i = 0; i < ProtoTrace.MAX_TRANSACTION_OPERATIONS; i++) buffered.accept(after);
        }).getMessage().contains("operation buffer limit"));
    }

    @Test public void countsEachSearchMemberWithoutSplittingItsRequestOrBudgetBoundary() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("primitive-count.pb.zst");
        Path stop = file.resolveSibling("primitive-count.stop");
        var budget = new RequestBudget(4, java.time.Duration.ofSeconds(10));
        budget.begin("mixed", 1, stop);
        var writer = new ProtoTrace.Writer(file, false, budget);
        try (writer) {
            writer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0));
            for (int ordinal = 1; ordinal <= 2; ordinal++) {
                writer.accept(IndexOperationTrace.write(ordinal, IndexOperationTrace.UPSERT, 1, 0,
                        IndexKey.ofInt(ordinal), 1, 7, ordinal, ordinal)
                        .withTransaction(91, ordinal).withWorkUnitId(1).withExpansion(0, 1));
            }
            writer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_COMMIT, 91, 1, 2));
            for (int member = 0; member < 3; member++) {
                writer.accept(IndexOperationTrace.search(13, 1, 0,
                        IndexOperationTrace.Range.equal(IndexKey.ofInt(member + 1)), null, 1, 2,
                        IndexOperationTrace.UNION_GROUP, member, 3).withWorkUnitId(2));
                if (member < 2) {
                    assertEquals(2, budget.count());
                    assertFalse(Files.exists(stop));
                }
            }
        }
        // Two writes and three searches are five operations, although the searches share one request.
        assertEquals(5L, writer.statistics().get("effective_requests"));
        assertEquals(Map.of("upsert", 2L, "union_search", 1L), writer.statistics().get("request_types"));
        assertEquals(Map.of("begin", 1L, "commit", 1L), writer.statistics().get("transaction_markers"));
        assertEquals(7, writer.count());
        assertEquals(7, ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), ignored -> {}));
        assertEquals(5, budget.count());
        assertTrue(Files.isRegularFile(stop));
        budget.end();
        assertEquals(1L, budget.finish().get("overshoot_requests"));
    }

    @Test public void primaryExportKeepsTransactionsAndWholeGroupsWithSeparateCounts() throws Exception {
        for (boolean secondary : List.of(false, true)) {
            Path file = temporary.getRoot().toPath().resolve("scope-" + secondary + ".pb.zst");
            var budget = new RequestBudget(2, java.time.Duration.ofSeconds(10));
            Path stop = file.resolveSibling("stop-" + secondary);
            budget.begin("mixed", 1, stop);
            var writer = new ProtoTrace.Writer(file, secondary, budget);
            try (writer) {
                writer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0));
                for (int i = 0; i < 2; i++) writer.accept(IndexOperationTrace.search(11, 1, i,
                        IndexOperationTrace.Range.equal(IndexKey.ofInt(1)), null, 0, 1,
                        IndexOperationTrace.UNION_GROUP, i, 2).withTransaction(91, 0).withWorkUnitId(1));
                IndexMutation.emit(write(IndexOperationTrace.UPSERT, 1, 1, 1),
                        Map.of(1, IndexKey.ofInt(2)), Map.of(1, IndexKey.ofInt(3)), writer);
                writer.accept(IndexOperationTrace.search(13, 1, 0, IndexOperationTrace.Range.equal(IndexKey.ofInt(1)),
                        0, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH).withTransaction(91, 1).withVersion(1).withWorkUnitId(1));
                writer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_COMMIT, 91, 1, 1));
                writer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 92, 2, 0));
                writer.accept(IndexOperationTrace.search(15, 1, 1, IndexOperationTrace.Range.equal(IndexKey.ofInt(3)),
                        1, 1, IndexOperationTrace.SNAPSHOT_SEARCH).withTransaction(92, 0).withWorkUnitId(2));
                writer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_ABORT, 92, 2, 0));
                writer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 93, 3, 0));
                for (int i = 0; i < 2; i++) writer.accept(IndexOperationTrace.search(16, 1, 0,
                        IndexOperationTrace.Range.equal(IndexKey.ofInt(i + 1)), null, 2, 1,
                        IndexOperationTrace.UNION_GROUP, i, 2).withTransaction(93, 0).withWorkUnitId(3));
                writer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_COMMIT, 93, 3, 0));
            }
            assertTrue(Files.isRegularFile(stop));
            assertEquals(secondary ? 8 : 3, budget.count());
            budget.end();
            assertEquals(secondary ? 6L : 1L, budget.finish().get("overshoot_requests"));
            var decoded = new ArrayList<Operation>();
            assertEquals(secondary ? 14 : 9, ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add));
            assertEquals(secondary ? 14 : 9, writer.count());
            assertEquals(Files.size(file), writer.bytesWritten());
            var stats = writer.statistics();
            assertEquals(secondary ? 8L : 3L, stats.get("index_entries"));
            assertEquals(secondary ? 8L : 3L, stats.get("effective_requests"));
            assertEquals(secondary ? 4L : 3L, stats.get("primary_entries"));
            assertEquals(secondary ? 4L : 0L, stats.get("secondary_entries"));
            assertEquals(Map.of("begin", 3L, "commit", 2L, "abort", 1L), stats.get("transaction_markers"));
            assertEquals(secondary
                    ? Map.of("upsert", 2L, "logical_delete", 1L, "point_search", 1L, "union_search", 2L)
                    : Map.of("upsert", 1L, "union_search", 1L), stats.get("request_types"));
            if (!secondary) {
                assertTrue(decoded.stream().allMatch(o -> o.index() == 0));
                assertEquals(1, decoded.get(1).expansionSize());
                assertTrue(decoded.stream().noneMatch(o -> (o.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0));
                assertEquals(IndexOperationTrace.TRANSACTION_ABORT, decoded.get(4).kind());
            }
        }
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
    public void abortedWritesAndLocalReadsAreExcludedBeforeCounting() throws Exception {
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
        assertEquals(List.of(original.get(0), original.get(3)), decoded);
        assertEquals(0L, writer.statistics().get("effective_requests"));
        var filtered = (Map<?, ?>) writer.statistics().get("source_projection");
        assertEquals(1L, filtered.get("excluded_aborted_writes"));
        assertEquals(1L, filtered.get("excluded_local_search_members"));
    }

    @Test
    public void committedRequestsPreserveGroupsLocatorsAndFinalDeleteReinsertIdentity() throws Exception {
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
        assertEquals(6, ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add));
        assertEquals(List.of(original.get(0), original.get(1), original.get(3), original.get(5),
                original.get(6), original.get(7)), decoded);
        try (var model = new IndexReferenceReplay(true, true, new IndexReferenceReplay.Settings(null, 1, 1, 1024))) {
            for (Operation operation : decoded) if (!IndexOperationTrace.isControl(operation)) model.accept(operation);
            assertEquals(1, model.lookupPrimary(1, IndexKey.ofInt(1), 0).value());
            assertEquals(2, model.lookupPrimary(1, IndexKey.ofInt(1), 1).value());
        }
    }

    @Test
    public void onlyFinalPhysicalWritesSurviveRepeatedPrimaryAndSecondaryUpdates() throws Exception {
        var file = temporary.getRoot().toPath().resolve("net-state.pb.zst");
        var writer = new ProtoTrace.Writer(file);
        try (writer) {
            writer.append(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0));
            IndexMutation.emit(write(IndexOperationTrace.UPSERT, 2, 1, 1),
                    Map.of(1, IndexKey.ofInt(10)), Map.of(1, IndexKey.ofInt(20)), writer);
            IndexMutation.emit(write(IndexOperationTrace.UPSERT, 3, 1, 2),
                    Map.of(1, IndexKey.ofInt(20)), Map.of(1, IndexKey.ofInt(30)), writer);
            writer.append(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_COMMIT, 91, 1, 2));
        }
        var decoded = new ArrayList<Operation>();
        assertEquals(6, ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add));
        var writes = decoded.stream().filter(IndexOperationTrace::isMutation).toList();
        assertEquals(4, writes.size());
        assertEquals(1, writes.stream().filter(o -> o.index() == 0).count());
        assertEquals(3, writes.stream().filter(o -> o.index() == 0).findFirst().orElseThrow().rowId());
        assertEquals(Set.of(IndexKey.ofInt(10), IndexKey.ofInt(20)), writes.stream()
                .filter(o -> o.kind() == IndexOperationTrace.LOGICAL_DELETE).map(Operation::indexedValue)
                .collect(java.util.stream.Collectors.toSet()));
        assertEquals(List.of(IndexKey.ofInt(30)), writes.stream().filter(o -> o.index() == 1
                && o.kind() == IndexOperationTrace.UPSERT).map(Operation::indexedValue).toList());
        assertTrue(writes.stream().allMatch(o -> o.expansionPosition() == 0 && o.expansionSize() == 1));
        assertEquals(4L, writer.statistics().get("effective_requests"));
        assertEquals(2L, ((Map<?, ?>) writer.statistics().get("source_projection")).get("coalesced_writes"));
        assertEquals(2, decoded.get(decoded.size() - 1).expectedCount());
        try (var raw = new IndexReferenceReplay(true, true);
                var committed = new IndexReferenceReplay(true, true)) {
            var baseline = write(IndexOperationTrace.INSERT, 1, 0, 0);
            IndexMutation.emit(baseline, Map.of(), Map.of(1, IndexKey.ofInt(10)), raw::accept);
            IndexMutation.emit(baseline, Map.of(), Map.of(1, IndexKey.ofInt(10)), committed::acceptCommitted);
            for (Operation operation : writes) committed.acceptCommitted(operation);
            assertEquals(3, committed.lookupPrimary(1, IndexKey.ofInt(1), 1).value());
            assertTrue(committed.search(1, 1, IndexOperationTrace.Range.equal(IndexKey.ofInt(20)), 1).isEmpty());
            assertEquals(3, committed.search(1, 1, IndexOperationTrace.Range.equal(IndexKey.ofInt(30)), 1).get(0).value());
            assertTrue(assertThrows(IOException.class, () -> {
                for (Operation operation : writes) raw.accept(operation);
            }).getMessage().contains("Deletion of absent index entry"));
        }
    }

    @Test
    public void interleavedSupersededWritesLeaveFinalWritesInTheirLastSourceOrder() throws Exception {
        // Keys 1,2,1,3,2: the survivors are ordinals 3, 4 and 5, in that order.
        int[] keys = {1, 2, 1, 3, 2};
        for (boolean aborted : List.of(false, true)) {
            Path file = temporary.getRoot().toPath().resolve("superseded-" + aborted + ".pb.zst");
            var writer = new ProtoTrace.Writer(file);
            try (writer) {
                writer.append(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0));
                for (int i = 0; i < keys.length; i++)
                    writer.append(IndexOperationTrace.write(10 + i, IndexOperationTrace.UPSERT, 1, 0,
                            IndexKey.ofInt(keys[i]), 1, 7, i, 100 + i).withTransaction(91, i + 1)
                            .withWorkUnitId(1).withExpansion(0, 1));
                writer.append(IndexOperationTrace.transaction(0, aborted ? IndexOperationTrace.TRANSACTION_ABORT
                        : IndexOperationTrace.TRANSACTION_COMMIT, 91, 1, keys.length));
            }
            var decoded = new ArrayList<Operation>();
            ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add);
            var writes = decoded.stream().filter(IndexOperationTrace::isMutation).toList();
            assertEquals(aborted ? List.of() : List.of(102L, 103L, 104L), writes.stream().map(Operation::rowId).toList());
            assertEquals(aborted ? List.of() : List.of(3L, 4L, 5L), writes.stream().map(Operation::mutationOrdinal).toList());
            assertEquals(aborted ? List.of() : List.of(IndexKey.ofInt(1), IndexKey.ofInt(3), IndexKey.ofInt(2)),
                    writes.stream().map(Operation::key).toList());
            var filtered = (Map<?, ?>) writer.statistics().get("source_projection");
            assertEquals(aborted ? 5L : 0L, filtered.get("excluded_aborted_writes"));
            assertEquals(aborted ? 0L : 2L, filtered.get("coalesced_writes"));
            assertEquals(5L, filtered.get("peak_buffered_operations"));
        }
    }

    @Test
    public void sourceSnapshotValidationPrecedesScopeAndOutcomeFiltering() throws Exception {
        var file = temporary.getRoot().toPath().resolve("future-snapshot.pb.zst");
        try (var writer = new ProtoTrace.Writer(file, false)) {
            assertThrows(IOException.class, () -> writer.append(IndexOperationTrace.search(1, 1, 1,
                    IndexOperationTrace.Range.equal(IndexKey.ofInt(1)), 1, 0, IndexOperationTrace.SNAPSHOT_SEARCH)));
        }
        var aborted = new ProtoTrace.Writer(file.resolveSibling("invalid-aborted-snapshot.pb.zst"));
        aborted.append(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 1, 1, 0));
        assertThrows(IOException.class, () -> aborted.append(IndexOperationTrace.search(1, 1, 0,
                IndexOperationTrace.Range.equal(IndexKey.ofInt(1)), 1, 0, IndexOperationTrace.SNAPSHOT_SEARCH)
                .withTransaction(1, 0)));
        assertThrows(IOException.class, aborted::close);
    }

    @Test
    public void abortedSourceRetainsItsSnapshotButDropsItsWholeLocalGroup() throws Exception {
        var file = temporary.getRoot().toPath().resolve("aborted-snapshots.pb.zst");
        var budget = new RequestBudget(1, java.time.Duration.ofSeconds(10));
        budget.begin("snapshot", 1, file.resolveSibling("stop"));
        var writer = new ProtoTrace.Writer(file, true, budget);
        try (writer) {
            writer.append(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0));
            writer.append(IndexOperationTrace.search(1, 1, 0, IndexOperationTrace.Range.equal(IndexKey.ofInt(1)),
                    0, 1, IndexOperationTrace.SNAPSHOT_SEARCH).withTransaction(91, 0).withWorkUnitId(1));
            writer.append(write(IndexOperationTrace.UPSERT, 2, 1, 1).withExpansion(0, 1));
            for (int member = 0; member < 2; member++)
                writer.append(IndexOperationTrace.search(13, 1, 0,
                        IndexOperationTrace.Range.equal(IndexKey.ofInt(member + 1)), null, 0, 1,
                        IndexOperationTrace.UNION_GROUP | IndexOperationTrace.TRANSACTION_LOCAL_SEARCH, member, 2)
                        .withTransaction(91, 1).withVersion(1).withWorkUnitId(1));
            // The search before the first write is already exported; the local group waits for the outcome.
            assertEquals(1, budget.count());
            writer.append(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_ABORT, 91, 1, 1));
        }
        var decoded = new ArrayList<Operation>();
        assertEquals(3, ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add));
        assertEquals(IndexOperationTrace.SEARCH, decoded.get(1).kind());
        assertEquals(0, decoded.get(1).readTimestamp());
        assertEquals(1, decoded.get(1).expectedCount());
        assertEquals(1, budget.count());
        budget.end();
        assertEquals(2L, ((Map<?, ?>) writer.statistics().get("source_projection")).get("excluded_local_search_members"));
    }

    @Test
    public void malformedAbortedSourceIsRejectedBeforeItsWritesCanBeFiltered() throws Exception {
        var file = temporary.getRoot().toPath().resolve("malformed-source.pb.zst");
        var writer = new ProtoTrace.Writer(file);
        writer.append(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 91, 1, 0));
        writer.append(write(IndexOperationTrace.UPSERT, 2, 1, 1).withExpansion(0, 2));
        assertThrows(IOException.class, () -> writer.append(IndexOperationTrace.transaction(
                0, IndexOperationTrace.TRANSACTION_ABORT, 91, 1, 1)));
        assertThrows(IOException.class, writer::close);
    }

    @Test
    public void corruptTruncatedAndMisboundFilesAreRejected() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("trace.pb.zst");
        var writer = new ProtoTrace.Writer(file);
        try (writer) { writer.append(write(IndexOperationTrace.INSERT, 1, 0, 0)); }
        byte[] complete = Files.readAllBytes(file);
        for (int magic : new int[] {0, 0x53494f5a}) {
            byte[] invalid = complete.clone();
            java.nio.ByteBuffer.wrap(invalid).putInt(magic);
            Files.write(file, invalid);
            assertThrows(IOException.class, () -> ProtoTrace.read(file, null, null, operation -> fail()));
        }
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
                .setEnd(TraceEnd.newBuilder().setTransactionId(1).setVersion(1)).build();
        var cases = List.of(List.of(write), List.of(badSearch), List.of(group),
                // A valid envelope and CRC cannot make an unterminated encoded text key valid.
                List.of(write.toBuilder().setRequest(write.getRequest().toBuilder().setWrite(
                        write.getRequest().getWrite().toBuilder().setKey(key.setTable(1).setPrimaryKey(
                                com.google.protobuf.ByteString.copyFrom(new byte[] {1, 'x'}))))).build()),
                List.of(begin.toBuilder().setBegin(begin.getBegin().toBuilder().setTransactionId(0)).build()),
                List.of(begin, end.toBuilder().setEnd(end.getEnd().toBuilder().setOperations(1)).build()));
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
