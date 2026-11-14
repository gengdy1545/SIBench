package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.capture.IndexOperationTrace.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.trace.IndexKey;

public class TransactionConsistentTraceTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void rollbackRetainsCompletedReadsAtTheirIndividualSnapshots() throws Exception {
        for (boolean knownAbort : List.of(false, true)) {
            var file = temporary.getRoot().toPath().resolve("snapshots-" + knownAbort + ".pb.zst");
            var writer = new ProtoTrace.Writer(file, true);
            try (writer) {
                writer.accept(transaction(0, TRANSACTION_BEGIN, 1, 1, 0));
                writer.accept(write(1, UPSERT, 1, BUSINESS_INSERT, IndexKey.ofInt(2), 1, 0, 0, 20)
                        .withTransaction(1, 1).withWorkUnitId(1).withExpansion(0, 1));
                writer.accept(transaction(0, TRANSACTION_COMMIT, 1, 1, 1));
                if (knownAbort) writer.expectAbort();
                writer.accept(transaction(0, TRANSACTION_BEGIN, 2, 2, 0));
                writer.accept(search(2, 1, 0, Range.equal(IndexKey.ofInt(2)), 0, 0, SNAPSHOT_SEARCH)
                        .withTransaction(2, 0).withWorkUnitId(2));
                writer.accept(search(3, 1, 0, Range.equal(IndexKey.ofInt(2)), 1, 1, SNAPSHOT_SEARCH)
                        .withTransaction(2, 0).withWorkUnitId(3));
                writer.accept(transaction(0, TRANSACTION_ABORT, 2, 2, 0));
            }
            var decoded = new ArrayList<Operation>();
            ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add);
            var reads = decoded.stream().filter(o -> o.kind() == SEARCH).toList();
            assertEquals(List.of(0L, 1L), reads.stream().map(Operation::readTimestamp).toList());
            assertEquals(List.of(0L, 1L), reads.stream().map(Operation::expectedCount).toList());
            assertEquals(List.of(2L, 3L), reads.stream().map(Operation::workUnitId).toList());
            assertTrue(reads.stream().allMatch(o -> o.transactionId() == 0 && o.mutationOrdinal() == 0));
            assertEquals(1, writer.lastCommittedVersion());
            try (var oracle = new IndexReferenceReplay(true, true)) {
                for (var operation : decoded) if (!isControl(operation)) oracle.acceptCommitted(operation);
                assertEquals(2, oracle.queries());
            }
        }
    }

    @Test public void transactionLocalReadsRetainPrivateWritesAcrossRollback() throws Exception {
        for (boolean knownAbort : List.of(false, true)) {
            var file = temporary.getRoot().toPath().resolve("local-" + knownAbort + ".pb.zst");
            var writer = new ProtoTrace.Writer(file, true);
            try (writer) {
                if (knownAbort) writer.expectAbort();
                writer.accept(transaction(0, TRANSACTION_BEGIN, 1, 1, 0));
                writer.accept(write(1, UPSERT, 1, BUSINESS_INSERT, IndexKey.ofInt(2), 1, 0, 0, 20)
                        .withTransaction(1, 1).withWorkUnitId(1).withExpansion(0, 1));
                writer.accept(search(2, 1, 0, Range.equal(IndexKey.ofInt(2)), 0, 1, TRANSACTION_LOCAL_SEARCH)
                        .withTransaction(1, 1).withVersion(1).withWorkUnitId(2));
                writer.accept(transaction(0, TRANSACTION_ABORT, 1, 1, 1));
            }
            var decoded = new ArrayList<Operation>();
            ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add);
            assertEquals(1, decoded.size());
            assertEquals(0, decoded.get(0).transactionId());
            assertEquals(1, decoded.get(0).localWrites().size());
            assertEquals(20, decoded.get(0).localWrites().get(0).rowId());
            try (var oracle = new IndexReferenceReplay(true, true)) {
                oracle.acceptCommitted(decoded.get(0));
                assertEquals(1, oracle.queries());
                assertNull(oracle.lookupPrimary(1, IndexKey.ofInt(2), 0));
            }
        }
    }

    @Test public void exporterRejectsTheRemovedQueryFilter() {
        for (boolean value : List.of(false, true)) {
            var failure = assertThrows(IllegalArgumentException.class, () -> Generation.secondaryIndexes(
                    Map.of("trace", Map.of("secondary_indexes", false, "export_queries", value))));
            assertTrue(failure.getMessage().contains("export_queries"));
        }
    }

    @Test public void sourceSnapshotContextCanSelectAnEarlierWriteOrExcludeAllWrites() throws Exception {
        for (boolean commit : List.of(false, true)) {
            var file = temporary.getRoot().toPath().resolve("command-" + commit + ".pb.zst");
            var writer = new ProtoTrace.Writer(file, true);
            var put = write(1, UPSERT, 1, BUSINESS_INSERT, IndexKey.ofInt(2), 1, 0, 0, 20)
                    .withTransaction(1, 1).withWorkUnitId(1).withExpansion(0, 1);
            try (writer) {
                writer.accept(transaction(0, TRANSACTION_BEGIN, 1, 1, 0));
                writer.accept(put);
                writer.accept(write(1, LOGICAL_DELETE, 1, 0, IndexKey.ofInt(2), 1, 0, 0, 20)
                        .withTransaction(1, 2).withWorkUnitId(1).withExpansion(0, 1));
                writer.accept(search(2, 1, 0, Range.equal(IndexKey.ofInt(2)), 0, 1, SNAPSHOT_SEARCH)
                        .withTransaction(1, 2).withWorkUnitId(2).withLocalWrites(List.of(privateWrite(put))));
                writer.accept(search(3, 1, 0, Range.equal(IndexKey.ofInt(2)), 0, 0, SNAPSHOT_SEARCH)
                        .withTransaction(1, 2).withWorkUnitId(2));
                writer.accept(transaction(0, commit ? TRANSACTION_COMMIT : TRANSACTION_ABORT, 1, 1, 2));
            }
            var decoded = new ArrayList<Operation>();
            ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), decoded::add);
            var reads = decoded.stream().filter(o -> o.kind() == SEARCH).toList();
            assertEquals(20, reads.get(0).localWrites().get(0).rowId());
            assertTrue(reads.get(1).localWrites().isEmpty());
            try (var oracle = new IndexReferenceReplay(true, true)) {
                for (var operation : decoded) if (!isControl(operation)) oracle.acceptCommitted(operation);
                assertEquals(2, oracle.queries());
            }
            try (var oracle = new IndexReferenceReplay(true, true)) {
                assertThrows(IOException.class, () -> oracle.acceptCommitted(reads.get(0).withLocalWrites(List.of())));
            }
        }
    }

    @Test public void privateWritesCannotHideAnUnavailableCommittedSnapshot() throws Exception {
        for (boolean knownAbort : List.of(false, true)) {
            var writer = new ProtoTrace.Writer(temporary.getRoot().toPath().resolve("future-local-" + knownAbort + ".pb.zst"), true);
            try {
                if (knownAbort) writer.expectAbort();
                writer.accept(transaction(0, TRANSACTION_BEGIN, 2, 2, 0));
                writer.accept(write(1, UPSERT, 1, 0, IndexKey.ofInt(2), 2, 0, 0, 20)
                        .withTransaction(2, 1).withWorkUnitId(1).withExpansion(0, 1));
                var failure = assertThrows(IOException.class, () -> writer.accept(
                        search(2, 1, 0, Range.equal(IndexKey.ofInt(2)), 1, 1, TRANSACTION_LOCAL_SEARCH)
                                .withTransaction(2, 1).withVersion(2).withWorkUnitId(1)));
                assertTrue(failure.getMessage().contains("available source snapshot"));
            } finally { assertThrows(IOException.class, writer::close); }
        }
    }
}
