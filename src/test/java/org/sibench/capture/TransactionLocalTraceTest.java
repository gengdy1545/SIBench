package org.sibench.capture;

import org.sibench.adapters.IndexTestSupport;

import static org.junit.Assert.*;
import static org.sibench.capture.IndexOperationTrace.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.IndexExecution;
import org.sibench.adapters.IndexStatus;
import org.sibench.adapters.TraceRequests;
import org.sibench.adapters.ValueKind;
import org.sibench.adapters.VersionedIndexBackend;
import org.sibench.trace.IndexKey;

public class TransactionLocalTraceTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static Operation change(long source, int kind, int key, long row, long ordinal) {
        return write(source, kind, 1, 0, IndexKey.ofInt(key), 1, 0, 0, row)
                .withTransaction(1, ordinal).withWorkUnitId(1).withExpansion(0, 1);
    }

    private static Operation local(long source, int key, long ordinal, long count) {
        return search(source, 1, 0, Range.equal(IndexKey.ofInt(key)), 0, count, TRANSACTION_LOCAL_SEARCH)
                .withTransaction(1, ordinal).withWorkUnitId(1);
    }

    @Test public void abortedReadsKeepTheirOwnWritePrefixesWithoutPublishingThoseWrites() throws Exception {
        var file = temporary.getRoot().toPath().resolve("local.pb.zst");
        var writer = new ProtoTrace.Writer(file, true);
        try (writer) {
            writer.accept(write(0, INSERT, 1, 0, IndexKey.ofInt(1), 0, 0, 0, 10).withExpansion(0, 1));
            writer.accept(transaction(0, TRANSACTION_BEGIN, 1, 1, 0));
            writer.accept(change(1, UPSERT, 2, 20, 1));
            writer.accept(local(2, 2, 1, 1));
            writer.accept(change(3, UPSERT, 2, 30, 2));
            writer.accept(local(4, 2, 2, 1));
            writer.accept(change(5, LOGICAL_DELETE, 1, 10, 3));
            writer.accept(local(6, 1, 3, 0));
            writer.accept(search(7, 1, 0, new Range(IndexKey.ofInt(1), true, IndexKey.ofInt(3), true, true, 1),
                    0, 1, TRANSACTION_LOCAL_SEARCH).withTransaction(1, 3).withWorkUnitId(1));
            writer.accept(transaction(0, TRANSACTION_ABORT, 1, 1, 3));
            writer.accept(search(8, 1, 0, Range.equal(IndexKey.ofInt(2)), 0, 0, SNAPSHOT_SEARCH).withWorkUnitId(2));
            writer.accept(search(9, 1, 0, Range.equal(IndexKey.ofInt(1)), 0, 1, SNAPSHOT_SEARCH).withWorkUnitId(3));
        }
        var answers = new ArrayList<List<Long>>();
        var requests = new TraceRequests(ValueKind.ROW_ID);
        try (var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("index"), ValueKind.ROW_ID)) {
            ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), new Consumer() {
                @Override public void accept(Operation operation) { fail("Missing stored request"); }
                @Override public void accept(org.sibench.adapters.proto.TraceRecord record, List<Operation> views) throws java.io.IOException {
                    if (!record.hasOperation()) return;
                    var operation = views.get(0);
                    var request = requests.forward(record.getOperation(), operation);
                    var response = IndexTestSupport.execute(backend, request);
                    if (request instanceof org.sibench.adapters.proto.GetRequest get && get.getLocalWritesCount() > 0)
                        assertThrows(java.io.IOException.class, () -> requests.check(operation,
                                IndexTestSupport.execute(backend, get.toBuilder().clearLocalWrites().build())));
                    assertEquals(response.message(), IndexStatus.OK, response.status());
                    requests.check(operation, response);
                    if (operation.kind() == SEARCH)
                        answers.add(response.values().stream().map(value -> value.asReadOnlyByteBuffer().getLong()).toList());
                }
            });
        }
        assertEquals(List.of(List.of(20L), List.of(30L), List.of(), List.of(30L), List.of(), List.of(10L)), answers);
    }

    @Test public void queryGroupsUsePrivateSecondaryEntriesInBothValueModesAcrossEitherOutcome() throws Exception {
        for (boolean commit : List.of(false, true)) for (var mode : ValueKind.values()) {
            var directory = temporary.newFolder().toPath();
            var baseline = directory.resolve("baseline.pb.zst");
            var base = new ProtoTrace.Writer(baseline, true);
            try (base) {
                IndexMutation.emit(write(0, INSERT, 1, 0, IndexKey.ofInt(1), 0, 11, 1, 10).withWorkUnitId(1),
                        java.util.Map.of(), java.util.Map.of(1, IndexKey.ofInt(5)), base);
            }
            var file = directory.resolve("workload.pb.zst");
            var writer = new ProtoTrace.Writer(file, true);
            try (writer) {
                if (!commit) writer.expectAbort();
                writer.accept(transaction(0, TRANSACTION_BEGIN, 1, 1, 0));
                IndexMutation.emit(write(1, UPSERT, 1, BUSINESS_INSERT, IndexKey.ofInt(2), 1, 0, 0, 20)
                        .withTransaction(1, 1).withWorkUnitId(1), java.util.Map.of(), java.util.Map.of(1, IndexKey.ofInt(5)), writer);
                for (int index = 0; index < 2; index++) writer.accept(search(2, 1, index,
                        Range.equal(IndexKey.ofInt(index == 0 ? 2 : 5)), null, 0, 1,
                        TRANSACTION_LOCAL_SEARCH | INTERSECT_GROUP, index, 2)
                        .withTransaction(1, 1).withWorkUnitId(1));
                IndexMutation.emit(write(3, UPSERT, 1, BUSINESS_UPDATE, IndexKey.ofInt(2), 1, 33, 3, 30)
                        .withTransaction(1, 2).withWorkUnitId(1), java.util.Map.of(1, IndexKey.ofInt(5)),
                        java.util.Map.of(1, IndexKey.ofInt(7)), writer);
                writer.accept(local(4, 2, 2, 1));
                writer.accept(transaction(0, commit ? TRANSACTION_COMMIT : TRANSACTION_ABORT, 1, 1, 2));
                writer.accept(search(5, 1, 0, Range.equal(IndexKey.ofInt(2)), 1, commit ? 1 : 0, SNAPSHOT_SEARCH)
                        .withWorkUnitId(2));
            }
            var answers = new ArrayList<List<com.google.protobuf.ByteString>>();
            var requests = new TraceRequests(mode);
            try (var backend = VersionedIndexBackend.create(directory.resolve("index"), mode)) {
                org.sibench.adapters.InProcessService.Executor executor = (request, partial) -> {
                    var response = IndexExecution.execute(backend, request, partial);
                    if (request instanceof org.sibench.adapters.proto.GetRequest || request instanceof org.sibench.adapters.proto.ScanRequest)
                        answers.add(response.values());
                    return java.util.concurrent.CompletableFuture.completedFuture(response);
                };
                var factory = org.sibench.adapters.InProcessService.factory(executor, requests);
                var config = new org.sibench.replay.ReplayConfig(true, 0, 0, 0, 0, "operation", 1, 4096, 60, 1);
                try (var load = org.sibench.replay.ReplayProjection.withClients(config, false, factory)) {
                    ProtoTrace.read(baseline, base.sha256(), base.logicalSha256(), load); load.finish();
                }
                try (var runtime = org.sibench.replay.ReplayProjection.withClients(config, true, factory)) {
                    ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), runtime); runtime.finish();
                    assertEquals(3L, runtime.describe().get("private_context_reads"));
                }
                var value20 = mode == ValueKind.ROW_ID ? org.sibench.trace.IndexEntry.rowId(20) : org.sibench.trace.IndexEntry.locator(0, 0);
                var value30 = mode == ValueKind.ROW_ID ? org.sibench.trace.IndexEntry.rowId(30) : org.sibench.trace.IndexEntry.locator(33, 3);
                var value10 = mode == ValueKind.ROW_ID ? org.sibench.trace.IndexEntry.rowId(10) : org.sibench.trace.IndexEntry.locator(11, 1);
                assertEquals(List.of(List.of(value20), List.of(value10, value20), List.of(value30),
                        commit ? List.of(value30) : List.of()), answers);
                try (var oracle = new IndexReferenceReplay(mode == ValueKind.ROW_ID, true)) {
                    ProtoTrace.read(baseline, base.sha256(), base.logicalSha256(), operation -> { if (!isControl(operation)) oracle.acceptCommitted(operation); });
                    ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), operation -> { if (!isControl(operation)) oracle.acceptCommitted(operation); });
                    assertEquals(3, oracle.queries());
                }
            }
        }
    }
}
