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
    @Test
    public void supplyBudgetCountsEveryBasicSearchInAGroup() throws Exception {
        var operations = new ArrayList<Operation>();
        for (int i = 0; i < 2; i++) operations.add(IndexOperationTrace.search(1, 1, 0,
                Range.equal(IndexKey.ofInt(i + 1)), null, 0, 0,
                IndexOperationTrace.UNION_GROUP, i, 2).withWorkUnitId(1));
        IndexOperationTrace.Source source = consumer -> {
            for (var operation : operations) consumer.accept(operation);
            return operations.size();
        };
        var report = ReplayInput.checkStream(source, new ReplayConfig(false, 1, 1, 0, 0), true);
        assertEquals(2L, report.get("dispatched_operations"));
        assertEquals(2L, report.get("effective_requests"));
        assertEquals(1L, report.get("search_groups"));
        ReplayInput.checkStream(source, new ReplayConfig(false, 1, 2, 0, 0), true);
        assertThrows(IOException.class,
                () -> ReplayInput.checkStream(source, new ReplayConfig(false, 1, 3, 0, 0), true));
    }

    @Test
    public void abortedSourceRetainsSnapshotQueriesButRejectsExportedWrites() throws Exception {
        var query = IndexOperationTrace.search(1, 3, 0, Range.equal(IndexKey.ofInt(1)), 0, 0, 0)
                .withTransaction(10, 0);
        var operations = List.of(
                IndexOperationTrace.transaction(1, IndexOperationTrace.TRANSACTION_BEGIN, 10, 1, 0),
                query,
                IndexOperationTrace.transaction(2, IndexOperationTrace.TRANSACTION_ABORT, 10, 1, 2));
        var report = ReplayInput.checkStream(consumer -> {
            for (var operation : operations) consumer.accept(operation);
            return operations.size();
        }, new ReplayConfig(true, 0, 0, 0, 0), true);
        assertEquals(1L, report.get("dispatched_operations"));
        assertEquals(1L, report.get("aborted_transactions"));
        assertEquals(0L, report.get("committed_transactions"));
        assertEquals(1L, report.get("last_resolved_version"));
        try (var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0), true, op -> fail())) {
            replay.accept(operations.get(0));
            replay.accept(mutation(3, 1, 1, 10, 1, IndexOperationTrace.UPSERT));
            assertThrows(IOException.class, () -> replay.accept(operations.get(2)));
        }
    }

    /** A search of one source transaction, at its position after the preceding mutations. */
    private static Operation sourceSearch(Operation search, long transaction, long preceding) {
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
    public void finalWritesKeepSourceOrderAndGappedProvenanceWithoutServiceControls() throws Exception {
        for (String mode : List.of("operation", "transaction")) {
            var received = new ArrayList<Operation>();
            try (var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0, mode), true, received::add)) {
                replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
                var first = mutation(3, 1, 1, 10, 2, IndexOperationTrace.UPSERT);
                var last = mutation(3, 2, 1, 10, 5, IndexOperationTrace.LOGICAL_DELETE);
                replay.accept(first);
                replay.accept(last);
                assertTrue(received.isEmpty());
                replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 5));
                replay.finish();
                assertEquals(List.of(first, last), received);
                assertEquals(2L, replay.describe().get("basic_operations"));
                assertFalse(replay.describe().containsKey("control_calls"));
                assertEquals(1L, replay.describe().get("last_resolved_version"));
            }
        }
    }

    @Test
    public void repeatedPhysicalEntriesAreRejectedBeforeSending() throws Exception {
        try (var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0), true, op -> fail())) {
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(mutation(3, 1, 1, 10, 1, IndexOperationTrace.UPSERT));
            assertThrows(IOException.class, () -> replay.accept(mutation(3, 1, 1, 10, 3, IndexOperationTrace.LOGICAL_DELETE)));
        }
    }

    @Test
    public void snapshotVerificationSendsOnlySnapshotQueriesInTraceOrder() throws Exception {
        var events = new ArrayList<Long>();
        try (var replay = ReplayProjection.snapshotSearches(new ReplayConfig(true, 0, 0, 0, 0),
                ReplayTarget.immediate(op -> events.add(op.sourceEvent())))) {
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(mutation(3, 1, 1, 10, 1, IndexOperationTrace.UPSERT));
            replay.accept(IndexOperationTrace.search(2, 3, 0, Range.equal(IndexKey.ofInt(1)), 0, 1, 0)
                    .withTransaction(10, 1));
            replay.accept(IndexOperationTrace.search(4, 3, 0, Range.equal(IndexKey.ofInt(2)), 0, 0, 0)
                    .withTransaction(10, 3));
            replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 3));
            replay.finish();
            assertEquals(List.of(2L, 4L), events);
            assertEquals(1L, replay.describe().get("unsent_writes"));
        }
    }

    @Test
    public void traceBoundariesDoNotConsumeWarmupOrCreateRequests() throws Exception {
        var config = new ReplayConfig(true, 0, 0, 1, 0);
        try (var replay = new ReplayProjection(config, true, op -> {})) {
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(mutation(3, 1, 1, 10, 1, IndexOperationTrace.UPSERT));
            replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 1));
            replay.accept(IndexOperationTrace.transaction(11, 6, 11, 2, 0));
            replay.accept(IndexOperationTrace.search(2, 3, 0, Range.equal(IndexKey.ofInt(1)), 1, 1, 0)
                    .withTransaction(11, 0));
            replay.accept(IndexOperationTrace.transaction(11, 7, 11, 2, 0));
            replay.finish();
            var report = replay.describe();
            assertEquals(4L, report.get("transaction_control_records"));
            assertEquals(2L, report.get("target_calls"));
            var measurement = (java.util.Map<?, ?>) report.get("measurement");
            assertEquals(1L, measurement.get("warmup_operations"));
            assertEquals(1L, measurement.get("measured_operations"));
        }
    }

    @Test
    public void rejectsMalformedBoundariesEvenWhenSecondaryWritesAreFiltered() throws Exception {
        var config = new ReplayConfig(false, 0, 0, 0, 0);
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
                            new ReplayConfig(false, 0, 0, 0, 0, mode), true, target);
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            Operation write = mutation(3, 1, 1, 10, 1, 3);
            Operation search = sourceSearch(IndexOperationTrace.search(2, 3, 0, Range.equal(IndexKey.ofInt(1)), 0, 1, 0), 10, 1);
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
                        mutation(3, 1, 2, 10, 1, 3))) {
            List<Operation> received = new ArrayList<>();
            var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0), true, received::add);
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            assertThrows(IOException.class, () -> replay.accept(invalid));
            assertTrue(received.isEmpty());
        }
        var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0), true, operation -> fail());
        replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        replay.accept(mutation(3, 1, 1, 10, 1, 3));
        assertThrows(
                IOException.class,
                () ->
                        replay.accept(
                                sourceSearch(IndexOperationTrace.search(0, 3, 0, Range.equal(IndexKey.ofInt(1)), 1, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH), 10, 1)));
    }

    @Test
    public void rejectsLocalQueriesAndUncollapsedExpansionEvenWhenFiltered() throws Exception {
        var invalid = sourceSearch(IndexOperationTrace.search(2, 3, 0, Range.equal(IndexKey.ofInt(1)),
                0, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH).withVersion(1), 10, 1);
        try (var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0), true, op -> fail())) {
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            assertThrows(IOException.class, () -> replay.accept(invalid));
        }
        try (var replay = new ReplayProjection(new ReplayConfig(false, 0, 0, 0, 0), true, op -> fail())) {
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            assertThrows(IOException.class, () -> replay.accept(mutation(5, 1, 1, 10, 1, 3).withExpansion(0, 3)));
        }
    }

    @Test
    public void stopsAfterTargetFailureInEitherModeWithoutSendingLaterOperations()
            throws Exception {
        for (String mode : new String[] {"operation", "transaction"}) {
            List<Operation> received = new ArrayList<>();
            var replay =
                    new ReplayProjection(
                            new ReplayConfig(true, 0, 0, 0, 0, mode),
                            true,
                            operation -> {
                                received.add(operation);
                                throw new IOException("Target failed");
                            });
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(mutation(3, 1, 1, 10, 1, 3));
            replay.accept(mutation(3, 2, 1, 10, 2, 3));
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
                        new ReplayConfig(false, 0, 0, 0, 0, "transaction"),
                        true,
                        operation -> fail());
        replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        replay.accept(sourceSearch(IndexOperationTrace.search(1, 5, 1, Range.equal(IndexKey.ofInt(10)), 0, 0, 0), 10, 0));
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
            var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0), true, operation -> fail());
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(mutation(3, 1, 1, 10, 1, 3));
            assertThrows(IOException.class, () -> replay.accept(invalid));
        }
        for (Operation invalid :
                List.of(
                        IndexOperationTrace.transaction(11, 6, 11, 1, 0),
                        IndexOperationTrace.transaction(9, 6, 9, 2, 0))) {
            var replay = new ReplayProjection(new ReplayConfig(true, 0, 0, 0, 0), true, operation -> fail());
            replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
            replay.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 0));
            assertThrows(IOException.class, () -> replay.accept(invalid));
        }
    }

    @Test
    public void bufferLimitIncludesFilteredQueries() throws Exception {
        var replay =
                new ReplayProjection(
                        new ReplayConfig(false, 0, 0, 0, 0), true, operation -> fail());
        replay.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        Operation query = sourceSearch(IndexOperationTrace.search(1, 5, 1, Range.equal(IndexKey.ofInt(10)), 0, 0, 0), 10, 0);
        for (int i = 0; i < org.sibench.capture.ProtoTrace.MAX_TRANSACTION_OPERATIONS; i++) replay.accept(query);
        assertThrows(IOException.class, () -> replay.accept(query));
        assertEquals(0, replay.dispatched());
    }
}
