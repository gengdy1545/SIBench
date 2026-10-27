package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;
import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.TestOps;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReplayProjectionTest {
    /** A search of one source transaction, at its position after the preceding mutations. */
    private static Operation local(Operation search, long transaction, long preceding) {
        return search.withTransaction(transaction, preceding);
    }

    private static Operation mutation(
            int table, long key, long version, long tx, long ordinal, int kind) {
        return TestOps.unit(
                TestOps.op(
                        ordinal,
                        kind,
                        table,
                        0,
                        0,
                        key,
                        version,
                        kind == 5 ? 0 : 9,
                        ordinal,
                        key + 100,
                        10,
                        20,
                        0,
                        tx,
                        ordinal));
    }

    @Test
    public void dispatchesCommittedGroupsInOneThreadWithoutControlsCoalescingOrReordering()
            throws Exception {
        for (String mode : new String[] {"operation", "transaction"}) {
            for (boolean secondary : new boolean[] {true, false}) {
                List<Operation> received = new ArrayList<>();
                var config = new ReplayConfig(secondary, "none", 0, 0, 0, 0, mode);
                var projection = new ReplayProjection(config, true, received::add);
                projection.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
                org.sibench.capture.TransferIndexFixtures.expandRow(
                        mutation(5, 1, 1, 10, 1, 3), projection);
                org.sibench.capture.TransferIndexFixtures.expandRow(
                        mutation(5, 2, 1, 10, 2, 3), projection);
                org.sibench.capture.TransferIndexFixtures.expandRow(
                        mutation(5, 1, 1, 10, 3, 3), projection);
                projection.accept(mutation(3, 1, 1, 10, 4, 3));
                projection.accept(mutation(3, 1, 1, 10, 5, 5));
                assertTrue(received.isEmpty());
                projection.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 5));
                projection.finish();
                var writes = received.stream().filter(IndexOperationTrace::isMutation).toList();
                int transferWidth = secondary ? 3 : 1;
                assertEquals(transferWidth * 3 + 2, writes.size());
                assertEquals(TestOps.key(1), writes.get(0).key());
                assertEquals(TestOps.key(2), writes.get(transferWidth).key());
                assertEquals(TestOps.key(1), writes.get(transferWidth * 2).key());
                assertTrue(received.stream().noneMatch(IndexOperationTrace::isControl));
                assertEquals((long) writes.size(), projection.describe().get("target_calls"));
                projection.accept(IndexOperationTrace.transaction(11, 6, 11, 2, 0));
                projection.accept(mutation(3, 1, 2, 11, 1, 3));
                projection.accept(IndexOperationTrace.transaction(11, 7, 11, 2, 1));
                assertEquals(writes.size() + 1L, projection.dispatched());
                assertEquals(2L, projection.describe().get("committed_transactions"));
                assertEquals("committed_only", projection.describe().get("change_delivery"));
            }
        }
    }

    @Test
    public void publicationMarkersCountOnlySentOperationsAndResetAfterEachTransaction() throws Exception {
        for (String mode : List.of("operation", "transaction")) {
            for (boolean secondary : new boolean[] {false, true}) {
                var sent = new ArrayList<org.sibench.adapters.proto.Request>();
                var requests = new org.sibench.adapters.TraceRequests(org.sibench.adapters.proto.ValueKind.ROW_ID);
                ReplayTarget.Factory target = clientId -> new ReplayTarget() {
                    @Override
                    public SendHandle send(long sequence, Operation operation) throws IOException {
                        sent.add(requests.request(sequence, operation));
                        return SendHandle.completed();
                    }

                    @Override
                    public SendHandle sendCommit(long sequence, long transactionId, long version) throws IOException {
                        sent.add(requests.commit(sequence, transactionId, version));
                        return SendHandle.completed();
                    }
                };
                try (var replay = ReplayProjection.withClients(
                        new ReplayConfig(secondary, "none", 0, 0, 0, 0, mode), true, target, true)) {
                    replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
                    org.sibench.capture.TransferIndexFixtures.expandRow(mutation(5, 1, 1, 10, 1, 3), replay);
                    for (int index : new int[] {0, 1}) {
                        replay.accept(IndexOperationTrace.search(2 + index, 5, index,
                                Range.equal(IndexKey.ofInt(1)), 0, 1,
                                IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
                                .withTransaction(10, 1).withVersion(1));
                    }
                    assertTrue("operations cannot escape before the source commit", sent.isEmpty());
                    replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 1));
                    int operations = secondary ? 5 : 2;
                    assertEquals(operations + 1, sent.size());
                    for (int i = 0; i < operations; i++) {
                        assertFalse(sent.get(i).hasCommit());
                        assertEquals(i + 1L, sent.get(i).getTransactionOperation());
                        assertEquals(10, sent.get(i).getTransactionId());
                        if (sent.get(i).hasSearch()) {
                            assertEquals(0, sent.get(i).getSearch().getReadVersion());
                            assertEquals(1, sent.get(i).getSearch().getLocalVersion());
                        }
                    }
                    var marker = sent.get(operations);
                    assertTrue(marker.hasCommit());
                    assertEquals(1, marker.getCommit().getVersion());
                    assertEquals(secondary ? 3 : 1, marker.getCommit().getWrites());
                    assertEquals(operations, marker.getCommit().getOperations());
                    // A second read-only transaction may become empty after secondary filtering.
                    replay.accept(IndexOperationTrace.transaction(11, 6, 11, 2, 0));
                    replay.accept(IndexOperationTrace.search(4, 5, 1,
                            Range.equal(IndexKey.ofInt(10)), 1, 0, 0).withTransaction(11, 0));
                    replay.accept(IndexOperationTrace.transaction(11, 7, 11, 2, 0));
                    replay.finish();
                    var next = sent.get(sent.size() - 1);
                    assertTrue(next.hasCommit());
                    assertEquals(11, next.getTransactionId());
                    assertEquals(2, next.getCommit().getVersion());
                    assertEquals(0, next.getCommit().getWrites());
                    assertEquals(secondary ? 1 : 0, next.getCommit().getOperations());
                }
            }
        }
    }

    @Test
    public void rejectsMalformedBoundariesEvenWhenSecondaryWritesAreFiltered() throws Exception {
        var config = new ReplayConfig(false, "none", 0, 0, 0, 0);
        var projection = new ReplayProjection(config, true, operation -> {});
        assertThrows(IOException.class, () -> projection.accept(mutation(3, 1, 1, 10, 1, 3)));
        var opened = new ReplayProjection(config, true, operation -> {});
        opened.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        assertThrows(IOException.class, opened::finish);
        final var valid = new ReplayProjection(config, true, operation -> {});
        valid.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        assertThrows(
                IOException.class,
                () -> valid.accept(TestOps.op(2, 2, 3, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1)));
    }

    @Test
    public void sourceGroupsPreserveQueriesAndRejectThreadSwitching() throws Exception {
        for (String mode : new String[] {"operation", "transaction"}) {
            List<Operation> received = new ArrayList<>();
            Thread owner = Thread.currentThread();
            ReplayTarget.Factory target =
                    clientId ->
                            (sequence, o) -> {
                                assertSame(owner, Thread.currentThread());
                                received.add(o);
                                return ReplayTarget.SendHandle.completed();
                            };
            var replay =
                    ReplayProjection.withClients(
                            new ReplayConfig(false, "none", 0, 0, 0, 0, mode), true, target);
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            Operation write = mutation(3, 1, 1, 10, 1, 3);
            Operation search = local(IndexOperationTrace.search(2, 3, 0, Range.equal(IndexKey.ofInt(1)), 1, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH), 10, 1);
            replay.accept(write);
            replay.accept(search);
            assertTrue(received.isEmpty());
            replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 1));
            replay.finish();
            assertEquals(List.of(write, search), received);
            assertEquals(
                    mode.equals("transaction") ? 1L : 0L,
                    replay.describe().get("source_dispatch_groups"));
            var error = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            Thread other =
                    new Thread(
                            () -> {
                                try {
                                    replay.accept(IndexOperationTrace.search(3, 3, 0, Range.equal(IndexKey.ofInt(1)), 1, 1, 0));
                                } catch (Throwable e) {
                                    error.set(e);
                                }
                            });
            other.start();
            other.join();
            assertTrue(error.get() instanceof IOException);
            assertEquals(2, received.size());
        }
    }

    @Test
    public void rejectsWrongTransactionVersionOrdinalAndSourceOrderBeforeSending()
            throws Exception {
        for (Operation invalid :
                List.of(
                        mutation(3, 1, 1, 11, 1, 3),
                        mutation(3, 1, 2, 10, 1, 3),
                        mutation(3, 1, 1, 10, 2, 3))) {
            List<Operation> received = new ArrayList<>();
            var replay = new ReplayProjection(ReplayConfig.defaults(), true, received::add);
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            assertThrows(IOException.class, () -> replay.accept(invalid));
            assertTrue(received.isEmpty());
        }
        var replay = new ReplayProjection(ReplayConfig.defaults(), true, operation -> fail());
        replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        replay.accept(mutation(3, 1, 1, 10, 1, 3));
        assertThrows(
                IOException.class,
                () ->
                        replay.accept(
                                local(IndexOperationTrace.search(0, 3, 0, Range.equal(IndexKey.ofInt(1)), 1, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH), 10, 1)));
    }

    @Test
    public void rejectsWrongLocalReadPointsAndIncompleteFilteredIndexExpansion() throws Exception {
        for (Operation invalid :
                List.of(
                        local(IndexOperationTrace.search(2, 3, 0, Range.equal(IndexKey.ofInt(1)), 0, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH), 10, 1),
                        local(IndexOperationTrace.search(2, 3, 0, Range.equal(IndexKey.ofInt(1)), 1, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH), 11, 1),
                        local(IndexOperationTrace.search(2, 3, 0, Range.equal(IndexKey.ofInt(1)), 1, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH), 10, 2))) {
            var replay = new ReplayProjection(ReplayConfig.defaults(), true, operation -> fail());
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(mutation(3, 1, 1, 10, 1, 3));
            assertThrows(IOException.class, () -> replay.accept(invalid));
        }
        var replay =
                new ReplayProjection(
                        new ReplayConfig(false, "none", 0, 0, 0, 0), true, operation -> fail());
        replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        // The unit declares three index writes; filtering secondaries must not hide the gap.
        replay.accept(mutation(5, 1, 1, 10, 1, 3).withExpansion(0, 3));
        assertThrows(
                IOException.class,
                () ->
                        replay.accept(
                                local(IndexOperationTrace.search(2, 5, 1, Range.equal(IndexKey.ofInt(10)), 1, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH), 10, 1)));
    }

    @Test
    public void stopsAfterTargetFailureInEitherModeWithoutSendingLaterOperations()
            throws Exception {
        for (String mode : new String[] {"operation", "transaction"}) {
            List<Operation> received = new ArrayList<>();
            var replay =
                    new ReplayProjection(
                            new ReplayConfig(true, "none", 0, 0, 0, 0, mode),
                            true,
                            operation -> {
                                received.add(operation);
                                throw new IOException("Target failed");
                            });
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(mutation(3, 1, 1, 10, 1, 3));
            replay.accept(mutation(3, 1, 1, 10, 2, 3));
            assertThrows(
                    IOException.class,
                    () -> replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 2)));
            assertThrows(
                    IOException.class,
                    () -> replay.accept(IndexOperationTrace.transaction(11, 6, 11, 2, 0)));
            assertThrows(IOException.class, replay::finish);
            assertEquals(1, received.size());
        }
    }

    @Test
    public void emptyFilteredGroupAdvancesReadPointWithoutCallingTarget() throws Exception {
        var replay =
                new ReplayProjection(
                        new ReplayConfig(false, "none", 0, 0, 0, 0, "transaction"),
                        true,
                        operation -> fail());
        replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        replay.accept(local(IndexOperationTrace.search(1, 5, 1, Range.equal(IndexKey.ofInt(10)), 0, 0, 0), 10, 0));
        replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 0));
        replay.accept(IndexOperationTrace.search(20, 5, 1, Range.equal(IndexKey.ofInt(10)), 1, 0, 0));
        replay.finish();
        assertEquals(1L, replay.describe().get("committed_transactions"));
        assertEquals(0L, replay.describe().get("target_calls"));
        assertEquals(2L, replay.describe().get("filtered_secondary_operations"));
    }

    @Test
    public void rejectsCommitMismatchAndNonIncreasingSourceTransactions() throws Exception {
        for (Operation invalid :
                List.of(
                        IndexOperationTrace.transaction(10, 7, 11, 1, 1),
                        IndexOperationTrace.transaction(10, 7, 10, 2, 1),
                        IndexOperationTrace.transaction(10, 7, 10, 1, 0),
                        IndexOperationTrace.transaction(11, 6, 11, 2, 0))) {
            var replay = new ReplayProjection(ReplayConfig.defaults(), true, operation -> fail());
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(mutation(3, 1, 1, 10, 1, 3));
            assertThrows(IOException.class, () -> replay.accept(invalid));
        }
        for (Operation invalid :
                List.of(
                        IndexOperationTrace.transaction(11, 6, 11, 1, 0),
                        IndexOperationTrace.transaction(9, 6, 9, 2, 0))) {
            var replay = new ReplayProjection(ReplayConfig.defaults(), true, operation -> fail());
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 0));
            assertThrows(IOException.class, () -> replay.accept(invalid));
        }
    }

    @Test
    public void bufferLimitIncludesFilteredQueries() throws Exception {
        var replay =
                new ReplayProjection(
                        new ReplayConfig(false, "none", 0, 0, 0, 0), true, operation -> fail());
        replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        Operation query = local(IndexOperationTrace.search(1, 5, 1, Range.equal(IndexKey.ofInt(10)), 0, 0, 0), 10, 0);
        for (int i = 0; i < ReplayProjection.MAX_TRANSACTION_OPERATIONS; i++) replay.accept(query);
        assertThrows(IOException.class, () -> replay.accept(query));
        assertEquals(0, replay.dispatched());
    }
}
