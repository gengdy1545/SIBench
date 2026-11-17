package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.capture.IndexOperationTrace.*;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayProjection;
import org.sibench.trace.IndexKey;

public class CommittedTraceTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static Operation read(long tx, long snapshot, long ordinal) {
        return search(10, 1, 0, Range.equal(IndexKey.ofInt(1)), snapshot, 1, SNAPSHOT_SEARCH)
                .withTransaction(tx, ordinal).withWorkUnitId(tx);
    }

    private static Operation write(long tx, long ordinal, long row) {
        return IndexOperationTrace.write(10, UPSERT, 1, 0, IndexKey.ofInt(1), tx, 0, 0, row)
                .withTransaction(tx, ordinal).withWorkUnitId(tx).withExpansion(0, 1);
    }

    @Test public void allCompletedSnapshotReadsContributeToTheRequestBudget() throws Exception {
        var file=temporary.getRoot().toPath().resolve("reads.pb.zst");
        var budget=new RequestBudget(7,Duration.ofSeconds(10));
        budget.begin(null);
        var writer=new ProtoTrace.Writer(file,true,budget);
        try(writer) {
            writer.accept(transaction(0,TRANSACTION_BEGIN,1,1,0));
            for(int i=0;i<2;i++)writer.accept(search(10,1,0,Range.equal(IndexKey.ofInt(i+1)),null,0,1,
                    SNAPSHOT_SEARCH|UNION_GROUP,i,2).withTransaction(1,0).withWorkUnitId(1));
            writer.accept(transaction(0,TRANSACTION_COMMIT,1,1,0));
            assertEquals(2,budget.count());
            writer.accept(transaction(0,TRANSACTION_BEGIN,2,2,0));
            writer.accept(read(2,1,0));
            writer.accept(write(2,1,20));
            writer.accept(write(2,2,30));
            writer.accept(transaction(0,TRANSACTION_COMMIT,2,2,2));
            writer.accept(transaction(0,TRANSACTION_BEGIN,3,3,0));
            writer.accept(read(3,2,0));
            writer.accept(transaction(0,TRANSACTION_COMMIT,3,3,0));
            writer.expectAbort();
            writer.accept(transaction(0,TRANSACTION_BEGIN,4,4,0));
            writer.accept(read(4,3,0));
            writer.accept(transaction(0,TRANSACTION_ABORT,4,4,0));
            writer.accept(transaction(0,TRANSACTION_BEGIN,5,5,0));
            writer.accept(read(5,3,0));
            writer.accept(transaction(0,TRANSACTION_ABORT,5,5,0));
        }
        budget.end();
        assertEquals(7,budget.count());
        assertEquals(0L,budget.finish().get("overshoot_requests"));
        var decoded=new ArrayList<Operation>();
        ProtoTrace.read(file,writer.sha256(),writer.logicalSha256(),decoded::add);
        assertEquals(6,decoded.stream().filter(o->o.kind()==SEARCH).count());
        assertEquals(1,decoded.stream().filter(IndexOperationTrace::isMutation).count());
        assertEquals(30,decoded.stream().filter(IndexOperationTrace::isMutation).findFirst().orElseThrow().rowId());
        var statistics=(Map<?,?>)writer.statistics().get("source_projection");
        assertEquals(2L,statistics.get("retained_rollback_search_members"));
        assertEquals(2L,statistics.get("aborted_transactions"));
        assertEquals(1L,statistics.get("coalesced_writes"));
    }

    @Test public void incompleteTransactionsAndBrokenGroupsAreRejected() throws Exception {
        var projection=new CommittedOperations(ignored->{});
        projection.accept(transaction(0,TRANSACTION_BEGIN,1,1,0));
        assertThrows(IOException.class,projection::finish);
        projection.accept(search(10,1,0,Range.equal(IndexKey.ofInt(1)),null,0,1,
                SNAPSHOT_SEARCH|INTERSECT_GROUP,0,2).withTransaction(1,0).withWorkUnitId(1));
        assertThrows(IOException.class,()->projection.accept(transaction(0,TRANSACTION_COMMIT,1,1,0)));
    }

    @Test public void committedWritesAndRollbackReadsReachTheTraceAndRequestBudget() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("committed.pb.zst");
        var budget = new RequestBudget(6, Duration.ofSeconds(10));
        budget.begin(file.resolveSibling("stop"));
        var writer = new ProtoTrace.Writer(file, true, budget);
        try (writer) {
            writer.accept(transaction(0, TRANSACTION_BEGIN, 1, 1, 0));
            writer.accept(read(1, 0, 0));
            assertEquals("Unresolved reads must not consume supply", 0, budget.count());
            writer.accept(transaction(0, TRANSACTION_ABORT, 1, 1, 0));
            writer.accept(transaction(0, TRANSACTION_BEGIN, 2, 2, 0));
            writer.accept(read(2, 1, 0));
            writer.accept(write(2, 1, 99));
            writer.accept(read(2, 1, 1));
            writer.accept(transaction(0, TRANSACTION_ABORT, 2, 2, 1));
            assertEquals(3, writer.count());
            writer.accept(transaction(0, TRANSACTION_BEGIN, 3, 3, 0));
            writer.accept(read(3, 2, 0));
            writer.accept(write(3, 1, 20));
            writer.accept(write(3, 2, 30));
            writer.accept(transaction(0, TRANSACTION_COMMIT, 3, 3, 2));
            writer.accept(transaction(0, TRANSACTION_BEGIN, 4, 4, 0));
            writer.accept(read(4, 3, 0));
            writer.accept(transaction(0, TRANSACTION_COMMIT, 4, 4, 0));
        }
        assertEquals(6, budget.count());
        budget.end();
        var decoded = new ArrayList<Operation>();
        ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add);
        assertEquals(List.of(SEARCH, SEARCH, SEARCH, TRANSACTION_BEGIN, SEARCH, UPSERT, TRANSACTION_COMMIT,
                TRANSACTION_BEGIN, SEARCH, TRANSACTION_COMMIT), decoded.stream().map(Operation::kind).toList());
        assertEquals(List.of(3L, 4L), decoded.stream().filter(o -> o.kind() == TRANSACTION_BEGIN)
                .map(Operation::version).toList());
        assertEquals(0, decoded.get(4).readTimestamp());
        assertEquals(30, decoded.get(5).rowId());
        assertEquals(2, decoded.get(5).mutationOrdinal());
        assertEquals(0, decoded.get(6).expectedCount());
        try (var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0, "operation", 1, 4096, 60, 1), true, ignored -> {})) {
            for (var operation : decoded) replay.accept(operation);
            replay.finish();
            assertEquals(6L, replay.describe().get("effective_requests"));
        }
        try (var oracle = new IndexReferenceReplay(true, true)) {
            oracle.accept(IndexOperationTrace.write(0, INSERT, 1, 0, IndexKey.ofInt(1), 0, 0, 0, 10)
                    .withExpansion(0, 1));
            for (var operation : decoded) if (!isControl(operation)) oracle.acceptCommitted(operation);
            assertEquals(5, oracle.queries());
        }
    }

    @Test public void malformedSourceCountIsRejectedEvenWhenTheTransactionAborts() throws Exception {
        var writer = new ProtoTrace.Writer(temporary.getRoot().toPath().resolve("bad-source.pb.zst"), true);
        writer.accept(transaction(0, TRANSACTION_BEGIN, 1, 1, 0));
        writer.accept(write(1, 1, 99));
        assertThrows(IOException.class, () -> writer.accept(transaction(0, TRANSACTION_ABORT, 1, 1, 2)));
        assertThrows(IOException.class, writer::close);
    }

    @Test public void abortedTailDoesNotAdvanceThePublishedWatermark() throws Exception {
        var schema = org.sibench.schema.SourceSchema.of("ycsb", Path.of("benchmark/YCSB"), Map.of("fieldcount", 1));
        for (boolean commit : List.of(false, true)) {
            Path directory = temporary.getRoot().toPath().resolve("tail-" + commit);
            Map<String, Object> manifest;
            try (var trace = new RocksTrace(directory, schema, true, null)) {
                trace.baseline(new org.sibench.generation.RocksDbRows.Row(10, "key", new byte[][] {{1}}));
                trace.finishBaseline(1);
                trace.event(new org.sibench.generation.RocksDbRows.Event("read", "key", 10, 1, 0, 1, 0, commit));
                trace.event(new org.sibench.generation.RocksDbRows.Event("insert", "key", 10, 2, 1, 0, 0, false));
                trace.close();
                manifest = trace.publish(Map.of("input", "hand-specified committed and aborted events"));
            }
            assertEquals(commit ? 1L : 0L, manifest.get("last_resolved_version"));
            assertEquals(commit ? 1L : 2L, manifest.get("aborted_transactions"));
            var checked = org.sibench.replay.ReplayInput.check(directory, new ReplayConfig(true, 0, 0, 0, 0, "operation", 1, 4096, 60, 1));
            assertEquals(1L, ((Map<?, ?>) checked.get("workload")).get("effective_requests"));
        }
    }
}
