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

    @Test public void onlyCommittedTransactionsReachTheTraceAndRequestBudget() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("committed.pb.zst");
        var budget = new RequestBudget(3, Duration.ofSeconds(10));
        budget.begin("committed", 1, file.resolveSibling("stop"));
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
            assertEquals(0, writer.count());
            writer.accept(transaction(0, TRANSACTION_BEGIN, 3, 3, 0));
            writer.accept(read(3, 2, 0));
            writer.accept(write(3, 1, 20));
            writer.accept(write(3, 2, 30));
            writer.accept(transaction(0, TRANSACTION_COMMIT, 3, 3, 2));
            writer.accept(transaction(0, TRANSACTION_BEGIN, 4, 4, 0));
            writer.accept(read(4, 3, 0));
            writer.accept(transaction(0, TRANSACTION_COMMIT, 4, 4, 0));
        }
        assertEquals(3, budget.count());
        budget.end();
        var decoded = new ArrayList<Operation>();
        ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add);
        assertEquals(List.of(TRANSACTION_BEGIN, SEARCH, UPSERT, TRANSACTION_COMMIT,
                TRANSACTION_BEGIN, SEARCH, TRANSACTION_COMMIT), decoded.stream().map(Operation::kind).toList());
        assertEquals(List.of(3L, 4L), decoded.stream().filter(o -> o.kind() == TRANSACTION_BEGIN)
                .map(Operation::version).toList());
        assertEquals(0, decoded.get(1).readTimestamp());
        assertEquals(30, decoded.get(2).rowId());
        assertEquals(2, decoded.get(2).mutationOrdinal());
        assertEquals(0, decoded.get(3).expectedCount());
        try (var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0), true, ignored -> {})) {
            for (var operation : decoded) replay.accept(operation);
            replay.finish();
            assertEquals(3L, replay.describe().get("effective_requests"));
        }
        try (var oracle = new IndexReferenceReplay(true, true)) {
            oracle.accept(IndexOperationTrace.write(0, INSERT, 1, 0, IndexKey.ofInt(1), 0, 0, 0, 10)
                    .withExpansion(0, 1));
            for (var operation : decoded) if (!isControl(operation)) oracle.acceptCommitted(operation);
            assertEquals(2, oracle.queries());
        }
    }

    @Test public void malformedSourceCountIsRejectedEvenWhenTheTransactionAborts() throws Exception {
        var writer = new ProtoTrace.Writer(temporary.getRoot().toPath().resolve("bad-source.pb.zst"));
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
            try (var trace = new RocksTrace(directory, schema, true)) {
                trace.baseline(new org.sibench.generation.RocksDbRows.Row(10, "key", new byte[][] {{1}}));
                trace.finishBaseline(1);
                trace.event(new org.sibench.generation.RocksDbRows.Event("read", "key", 10, 1, 0, 1, 0, commit));
                trace.event(new org.sibench.generation.RocksDbRows.Event("insert", "key", 10, 2, 1, 0, 0, false));
                trace.close();
                manifest = trace.publish(Map.of("input", "hand-specified committed and aborted events"));
            }
            assertEquals(commit ? 1L : 0L, manifest.get("last_resolved_version"));
            assertEquals(commit ? 1L : 2L, manifest.get("aborted_transactions"));
            var checked = org.sibench.replay.ReplayInput.check(directory, new ReplayConfig(true, 0, 0, 0, 0));
            assertEquals(commit ? 1L : 0L, ((Map<?, ?>) checked.get("workload")).get("effective_requests"));
        }
    }
}
