package org.sibench.replay;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReplayProjectionTest {
    @Test
    public void disablesAllSecondaryKindsWithoutChangingPrimaryIdentityOrOrder() throws Exception {
        List<Operation> primary = new ArrayList<>(), received = new ArrayList<>();
        var projection = new ReplayProjection(false, received::add);
        for (int kind : new int[] {1, 2, 3, 4, 5}) {
            var row = new Operation(9, kind, 5, 0, 0, 7, 4, 8, 12, 20, 3, 6, 1);
            primary.add(row);
            if (kind == IndexOperationTrace.SEARCH) {
                projection.accept(row);
                projection.accept(new Operation(9, kind, 5, 1, 0, 3, 4, 0, 0, 0, 0, 0, 1));
                projection.accept(new Operation(9, kind, 5, 2, 0, 6, 4, 0, 0, 0, 0, 0, 1));
            } else org.sibench.capture.LegacyIndexFixtures.expandRow(row, projection);
        }
        assertEquals(primary, received);
        assertEquals(15L, projection.describe().get("source_operations"));
        assertEquals(10L, projection.describe().get("filtered_secondary_operations"));
        assertEquals(5, projection.dispatched());
        var all = new ReplayProjection(true, operation -> {});
        for (Operation row : primary) all.accept(row);
        assertEquals(0L, all.describe().get("filtered_secondary_operations"));
    }

    private static Operation mutation(
            int table, long key, long version, long tx, long ordinal, int kind) {
        return new Operation(
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
                ordinal);
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
                org.sibench.capture.LegacyIndexFixtures.expandRow(
                        mutation(5, 1, 1, 10, 1, 3), projection);
                org.sibench.capture.LegacyIndexFixtures.expandRow(
                        mutation(5, 2, 1, 10, 2, 3), projection);
                org.sibench.capture.LegacyIndexFixtures.expandRow(
                        mutation(5, 1, 1, 10, 3, 3), projection);
                projection.accept(mutation(3, 1, 1, 10, 4, 3));
                projection.accept(mutation(3, 1, 1, 10, 5, 5));
                assertTrue(received.isEmpty());
                projection.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 5));
                projection.finish();
                var writes = received.stream().filter(IndexOperationTrace::isMutation).toList();
                int transferWidth = secondary ? 3 : 1;
                assertEquals(transferWidth * 3 + 2, writes.size());
                assertEquals(1, writes.get(0).key());
                assertEquals(2, writes.get(transferWidth).key());
                assertEquals(1, writes.get(transferWidth * 2).key());
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
                () -> valid.accept(new Operation(2, 2, 3, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1)));
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
            Operation search = IndexOperationTrace.transactionSearch(2, 3, 0, 1, 1, 1, 10, 1, true);
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
                                    replay.accept(IndexOperationTrace.search(3, 3, 0, 1, 1, 1, 0));
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
                                IndexOperationTrace.transactionSearch(
                                        0, 3, 0, 1, 1, 1, 10, 1, true)));
    }

    @Test
    public void rejectsWrongLocalReadPointsAndIncompleteFilteredIndexExpansion() throws Exception {
        for (Operation invalid :
                List.of(
                        IndexOperationTrace.transactionSearch(2, 3, 0, 1, 0, 1, 10, 1, true),
                        IndexOperationTrace.transactionSearch(2, 3, 0, 1, 1, 1, 11, 1, true),
                        IndexOperationTrace.transactionSearch(2, 3, 0, 1, 1, 1, 10, 2, true))) {
            var replay = new ReplayProjection(ReplayConfig.defaults(), true, operation -> fail());
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(mutation(3, 1, 1, 10, 1, 3));
            assertThrows(IOException.class, () -> replay.accept(invalid));
        }
        var replay =
                new ReplayProjection(
                        new ReplayConfig(false, "none", 0, 0, 0, 0), true, operation -> fail());
        replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        replay.accept(mutation(5, 1, 1, 10, 1, 3));
        assertThrows(
                IOException.class,
                () ->
                        replay.accept(
                                IndexOperationTrace.transactionSearch(
                                        2, 5, 1, 10, 1, 1, 10, 1, true)));
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
        replay.accept(IndexOperationTrace.transactionSearch(1, 5, 1, 10, 0, 0, 10, 0, false));
        replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 0));
        replay.accept(IndexOperationTrace.search(20, 5, 1, 10, 1, 0, 0));
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
        Operation query = IndexOperationTrace.transactionSearch(1, 5, 1, 10, 0, 0, 10, 0, false);
        for (int i = 0; i < ReplayProjection.MAX_TRANSACTION_OPERATIONS; i++) replay.accept(query);
        assertThrows(IOException.class, () -> replay.accept(query));
        assertEquals(0, replay.dispatched());
    }
}
