package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;
import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;

public class ReplayDispatcherTest {
    private static ReplayConfig config(int clients, int inFlight) {
        return new ReplayConfig(true, 0, 0, 0, 0, "operation", clients, inFlight, 2);
    }

    private static Operation search(long unit) {
        return IndexOperationTrace.search(unit, 3, 0, Range.equal(IndexKey.ofInt(1)), 0, 0, 0).withWorkUnitId(unit);
    }

    private static Operation baseline(long event) {
        return IndexOperationTrace.write(event, IndexOperationTrace.INSERT, 3, 0,
                IndexKey.ofInt(event), 0, 1, event, event).withWorkUnitId(1);
    }

    private static Operation groupMember(int position, int size) {
        return IndexOperationTrace.search(3, 3, 0, Range.equal(IndexKey.ofInt(position)),
                null, 0, 0, IndexOperationTrace.UNION_GROUP, position, size).withWorkUnitId(3);
    }

    @Test
    public void subMillisecondQuantilesAreUpperBoundsWithoutClippingLongSamples() {
        var latency = new ReplayDispatcher.Latency();
        for (int i = 0; i < 99; i++) latency.add(100_000);
        latency.add(900_000);
        var report = latency.describe();
        assertEquals(0.108, (double) report.get("mean_ms"), 1e-12);
        assertEquals(0.9, (double) report.get("max_ms"), 0);
        for (String quantile : List.of("p50_ms", "p95_ms", "p99_ms")) {
            double actual = (double) report.get(quantile);
            assertTrue(actual >= 0.1);
            assertTrue(actual < 0.1 * (1 + 1.0 / 1024));
        }
        assertEquals("upper", report.get("percentile_bound"));
        var exact = new ReplayDispatcher.Latency();
        exact.add(137);
        assertEquals(0.000137, (double) exact.describe().get("p99_ms"), 0);
        var longSamples = new ReplayDispatcher.Latency();
        longSamples.add(Long.MAX_VALUE);
        longSamples.add(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE / 1e6, (double) longSamples.describe().get("mean_ms"), 0);
        assertEquals(Long.MAX_VALUE / 1e6, (double) longSamples.describe().get("p99_ms"), 0);
    }

    /** A client's last baseline batch is sent when dispatch leaves its work unit, not when its next unit arrives. */
    @Test
    public void leavingAClientFlushesItsPartialBaselineBatch() throws Exception {
        var events = new ArrayList<String>();
        ReplayTarget.Factory factory = client -> new ReplayTarget() {
            @Override public SendHandle send(long sequence, Operation operation, org.sibench.adapters.proto.TraceOperation stored) {
                events.add("send-" + client + "-" + operation.workUnitId());
                return SendHandle.completed();
            }
            @Override public void flush() { events.add("flush-" + client); }
        };
        try (var dispatcher = new ReplayDispatcher(config(2, 10), factory, 0, System::nanoTime)) {
            for (long unit : new long[] {1, 1, 2, 3}) dispatcher.send(baseline(unit).withWorkUnitId(unit));
            assertEquals(List.of("send-0-1", "send-0-1", "flush-0", "send-1-2", "flush-1", "send-0-3"), events);
        }
    }

    @Test
    public void onlyTheFinalBusinessResponseCompletesLatency() throws Exception {
        var now = new AtomicLong(1_000_000);
        var response = new CompletableFuture<Void>();
        try (var dispatcher = new ReplayDispatcher(config(1, 10), client -> (seq, op, stored) -> {
            now.addAndGet(10_000); // Client serialization and local buffering belong in latency.
            return new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response);
        }, 0, now::get)) {
            dispatcher.send(search(1));
            assertEquals(0L, dispatcher.describe().get("completed_operations"));
            assertEquals(0L, ((Map<?, ?>) dispatcher.describe().get("search_latency")).get("count"));
            now.set(1_050_000);
            response.complete(null);
            dispatcher.drain();
            var latency = (Map<?, ?>) dispatcher.describe().get("search_latency");
            assertEquals(1L, latency.get("count"));
            assertEquals(0.05, (double) latency.get("mean_ms"), 0);
        }
    }

    @Test
    public void admissionWaitIsSeparateFromClientCompletionLatency() throws Exception {
        var now = new AtomicLong(1_000_000);
        var responses = List.of(new CompletableFuture<Void>(), new CompletableFuture<Void>());
        var firstSent = new CountDownLatch(1);
        var secondSent = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();
        try (var dispatcher = new ReplayDispatcher(config(1, 1), client -> (seq, op, stored) -> {
            if (seq == 2) secondSent.countDown();
            return new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), responses.get((int) seq - 1));
        }, 0, now::get)) {
            var sender = new Thread(() -> {
                try {
                    dispatcher.send(search(1));
                    firstSent.countDown();
                    dispatcher.send(search(2));
                    dispatcher.drain();
                } catch (Throwable failure) { error.set(failure); }
            });
            sender.start();
            try {
                assertTrue(firstSent.await(1, TimeUnit.SECONDS));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (sender.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.sleep(1);
                assertEquals(Thread.State.TIMED_WAITING, sender.getState());
                assertEquals(1, secondSent.getCount());
                now.set(21_000_000);
                responses.get(0).complete(null);
                assertTrue(secondSent.await(1, TimeUnit.SECONDS));
                now.set(21_050_000);
                responses.get(1).complete(null);
            } finally {
                responses.forEach(response -> response.complete(null));
                sender.join(3000);
            }
            assertNull(error.get());
            assertFalse(sender.isAlive());
            var report = dispatcher.describe();
            assertEquals(0.02, (double) report.get("backpressure_seconds"), 0);
            assertEquals(10.025, (double) ((Map<?, ?>) report.get("search_latency")).get("mean_ms"), 1e-12);
        }
    }

    @Test
    public void groupValidationWaitDoesNotReplaceIndividualBusinessResponseTimes() throws Exception {
        var now=new AtomicLong(1_000_000);
        var validated=new CompletableFuture<Void>();
        var replies=List.of(new CompletableFuture<Void>(),new CompletableFuture<Void>());
        try(var dispatcher=new ReplayDispatcher(config(1,4),client -> (sequence, operation, stored) ->
                new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null),validated,replies.get((int)sequence-1)),0,now::get)) {
            dispatcher.send(groupMember(0,2)); dispatcher.send(groupMember(1,2));
            now.set(1_050_000); replies.get(0).complete(null);
            now.set(1_150_000); replies.get(1).complete(null);
            assertEquals(0L,dispatcher.describe().get("completed_operations"));
            now.set(2_000_000); validated.complete(null); dispatcher.drain();
            var latency=(Map<?,?>)dispatcher.describe().get("search_latency");
            assertEquals(2L,latency.get("count"));
            assertEquals(0.1,(double)latency.get("mean_ms"),1e-12);
            assertEquals(0.15,(double)latency.get("max_ms"),1e-12);
        }
    }

    @Test
    public void deferredQueriesKeepTheirOriginalWarmupMembership() throws Exception {
        var now=new AtomicLong(1_000_000);
        var writes=List.of(new CompletableFuture<Void>(),new CompletableFuture<Void>());
        var order=new ArrayList<Long>();
        try(var dispatcher=new ReplayDispatcher(config(1,4),client -> (sequence, operation, stored) -> {
            order.add(operation.sourceEvent());
            return new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null),operation.kind()==IndexOperationTrace.SEARCH
                    ?CompletableFuture.completedFuture(null):writes.get((int)operation.version()-1));
        },2,now::get)) {
            dispatcher.send(IndexOperationTrace.write(1,IndexOperationTrace.UPSERT,3,0,IndexKey.ofInt(1),1,0,0,1).withWorkUnitId(1));
            dispatcher.send(IndexOperationTrace.search(2,3,0,Range.equal(IndexKey.ofInt(1)),1,1,0).withWorkUnitId(2));
            dispatcher.send(IndexOperationTrace.write(3,IndexOperationTrace.UPSERT,3,0,IndexKey.ofInt(1),2,0,0,2).withWorkUnitId(3));
            dispatcher.send(IndexOperationTrace.search(4,3,0,Range.equal(IndexKey.ofInt(1)),2,1,0).withWorkUnitId(4));
            now.set(1_050_000); writes.forEach(reply -> reply.complete(null)); dispatcher.drain();
            assertEquals(List.of(1L,3L,2L,4L),order);
            assertEquals(1L,((Map<?,?>)dispatcher.describe().get("write_latency")).get("count"));
            assertEquals(1L,((Map<?,?>)dispatcher.describe().get("search_latency")).get("count"));
            assertEquals(2L,((Map<?,?>)dispatcher.describe().get("measurement")).get("completed_operations"));
        }
    }

    @Test
    public void bufferedReadGroupWaitsForRoomBeforeItsFirstMember() throws Exception {
        var now = new AtomicLong(1_000_000);
        var ordinary = List.of(new CompletableFuture<Void>(), new CompletableFuture<Void>());
        var grouped = List.of(new CompletableFuture<Void>(), new CompletableFuture<Void>(),
                new CompletableFuture<Void>());
        var ordinarySent = new CountDownLatch(1);
        var groupSent = new CountDownLatch(1);
        var groupCalls = new AtomicLong();
        var error = new AtomicReference<Throwable>();
        try (var dispatcher = new ReplayDispatcher(config(1, 4), client -> (seq, op, stored) -> {
            CompletableFuture<Void> response;
            if (op.grouped()) {
                response = grouped.get(op.expansionPosition());
                if (groupCalls.incrementAndGet() == 3) groupSent.countDown();
            } else response = ordinary.get((int) seq - 1);
            return new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response);
        }, 2, now::get)) {
            var sender = new Thread(() -> {
                try {
                    dispatcher.send(search(1));
                    dispatcher.send(search(2));
                    ordinarySent.countDown();
                    for (int i = 0; i < 3; i++) dispatcher.send(groupMember(i, 3));
                    dispatcher.drain();
                } catch (Throwable failure) { error.set(failure); }
            });
            sender.start();
            try {
                assertTrue(ordinarySent.await(1, TimeUnit.SECONDS));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (sender.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.sleep(1);
                assertEquals(Thread.State.TIMED_WAITING, sender.getState());
                assertEquals("The first group member must wait for the whole group", 0, groupCalls.get());
                now.set(21_000_000);
                ordinary.get(0).complete(null);
                assertTrue("The group must reach its final member without a group response",
                        groupSent.await(1, TimeUnit.SECONDS));
                now.set(21_050_000);
                grouped.forEach(response -> response.complete(null));
                ordinary.get(1).complete(null);
            } finally {
                ordinary.forEach(response -> response.complete(null));
                grouped.forEach(response -> response.complete(null));
                sender.join(3000);
                if (sender.isAlive()) { sender.interrupt(); sender.join(1000); }
            }
            assertNull(error.get());
            assertFalse(sender.isAlive());
            var report = dispatcher.describe();
            assertEquals(5L, report.get("completed_operations"));
            assertEquals(0.02, (double) report.get("backpressure_seconds"), 0);
            assertEquals(0.05, (double) ((Map<?, ?>) report.get("search_latency")).get("mean_ms"), 1e-12);
        }
    }

    @Test
    public void oversizedReadGroupFailsBeforeOpeningAClient() throws Exception {
        try (var dispatcher = new ReplayDispatcher(config(1, 2), client -> {
            fail("An oversized group must be rejected before opening a client");
            return null;
        })) {
            IOException error = assertThrows(IOException.class, () -> dispatcher.send(groupMember(0, 3)));
            assertTrue(error.getMessage().contains("3 members, exceeding max_in_flight=2"));
            assertEquals(0L, dispatcher.describe().get("basic_operations"));
            assertSame(error, assertThrows(IOException.class, dispatcher::drain));
        }
    }

    @Test
    public void snapshotWaitsOnlyForRequiredWritesAndExcludesDependencyTime() throws Exception {
        var now = new AtomicLong(1_000_000);
        var oldWrite = new CompletableFuture<Void>();
        var newWrite = new CompletableFuture<Void>();
        var readResponse = new CompletableFuture<Void>();
        var independentReadSent = new CountDownLatch(1);
        var dependentReadSent = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();
        try (var dispatcher = new ReplayDispatcher(config(2, 8), client -> (seq, op, stored) -> {
            CompletableFuture<Void> response;
            if (op.kind() != IndexOperationTrace.SEARCH)
                response = op.version() == 1 ? oldWrite : newWrite;
            else if (op.readTimestamp() == 0) {
                independentReadSent.countDown();
                response = CompletableFuture.completedFuture(null);
            } else {
                dependentReadSent.countDown();
                response = readResponse;
            }
            return new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response);
        }, 3, now::get)) {
            var sender = new Thread(() -> {
                try {
                    dispatcher.send(baseline(1).withVersion(1).withWorkUnitId(1));
                    dispatcher.send(baseline(2).withVersion(2).withWorkUnitId(2));
                    dispatcher.send(search(3)); // Timestamp zero is independent of both writes.
                    dispatcher.send(IndexOperationTrace.search(4, 3, 0,
                            Range.equal(IndexKey.ofInt(1)), 1, 0, 0).withWorkUnitId(4));
                    dispatcher.drain();
                } catch (Throwable failure) { error.set(failure); }
            });
            sender.start();
            try {
                assertTrue(independentReadSent.await(1, TimeUnit.SECONDS));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (sender.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.sleep(1);
                assertEquals(Thread.State.TIMED_WAITING, sender.getState());
                assertEquals("Snapshot must wait for its required write response", 1, dependentReadSent.getCount());
                now.set(21_000_000);
                oldWrite.complete(null);
                assertTrue("A newer pending write must not block the older snapshot",
                        dependentReadSent.await(1, TimeUnit.SECONDS));
                assertFalse(newWrite.isDone());
                now.set(21_050_000);
                readResponse.complete(null);
                newWrite.complete(null);
            } finally {
                oldWrite.complete(null);
                newWrite.complete(null);
                readResponse.complete(null);
                sender.join(3000);
                if (sender.isAlive()) { sender.interrupt(); sender.join(1000); }
            }
            assertNull(error.get());
            assertFalse(sender.isAlive());
            var report = dispatcher.describe();
            assertEquals(0.02, (double) report.get("dependency_wait_seconds"), 0);
            assertEquals(0.0, (double) report.get("backpressure_seconds"), 0);
            assertEquals(0.05, (double) ((Map<?, ?>) report.get("search_latency")).get("mean_ms"), 1e-12);
        }
    }

    private static Operation write(long unit, long version) {
        return baseline(unit).withVersion(version).withWorkUnitId(unit);
    }

    private static Operation read(long unit, long readTimestamp) {
        return IndexOperationTrace.search(unit, 3, 0, Range.equal(IndexKey.ofInt(1)), readTimestamp, 0, 0)
                .withWorkUnitId(unit);
    }

    private static Operation groupMember(long unit, long readTimestamp, int position, int size) {
        return IndexOperationTrace.search(unit, 3, 0, Range.equal(IndexKey.ofInt(position)),
                null, readTimestamp, 0, IndexOperationTrace.UNION_GROUP, position, size).withWorkUnitId(unit);
    }

    /** Records the global send order; writes at the given versions stay pending until completed. */
    private static ReplayTarget.Factory recording(List<String> order, Map<Long, CompletableFuture<Void>> pending) {
        return client -> (seq, op, stored) -> {
            synchronized (order) {
                order.add(op.kind() == IndexOperationTrace.SEARCH
                        ? "S" + op.readTimestamp() + (op.grouped() ? "." + op.expansionPosition() : "") : "W" + op.version());
            }
            var response = op.kind() == IndexOperationTrace.SEARCH ? CompletableFuture.<Void>completedFuture(null)
                    : pending.getOrDefault(op.version(), CompletableFuture.completedFuture(null));
            return new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response);
        };
    }

    @Test
    public void laterWritesProceedWhileASnapshotWaitsForItsOlderWrite() throws Exception {
        var order = new ArrayList<String>();
        var first = new CompletableFuture<Void>();
        try (var dispatcher = new ReplayDispatcher(config(4, 8), recording(order, Map.of(1L, first)))) {
            dispatcher.send(write(1, 1));
            dispatcher.send(read(2, 1));
            // The previous single blocking sender could not reach these writes before first completed.
            dispatcher.send(write(3, 2));
            dispatcher.send(write(4, 3));
            assertEquals(List.of("W1", "W2", "W3"), order);
            first.complete(null);
            dispatcher.drain();
            assertEquals(List.of("W1", "W2", "W3", "S1"), order);
            var report = dispatcher.describe();
            assertEquals(1L, report.get("deferred_search_operations"));
            assertEquals(4L, report.get("completed_operations"));
        }
    }

    @Test
    public void writeAtOrBelowADeferredSnapshotWaitsForThatSnapshot() throws Exception {
        var order = new ArrayList<String>();
        var first = new CompletableFuture<Void>();
        var error = new AtomicReference<Throwable>();
        try (var dispatcher = new ReplayDispatcher(config(4, 8), recording(order, Map.of(1L, first)))) {
            var sender = new Thread(() -> {
                try {
                    dispatcher.send(write(1, 1));
                    dispatcher.send(read(2, 5));
                    dispatcher.send(write(3, 5));
                    dispatcher.drain();
                } catch (Throwable failure) { error.set(failure); }
            });
            sender.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (sender.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) Thread.sleep(1);
            synchronized (order) { assertEquals(List.of("W1"), order); }
            first.complete(null);
            sender.join(3000);
            assertNull(error.get());
            assertEquals(List.of("W1", "S5", "W5"), order);
        }
    }

    @Test
    public void deferredGroupsLeaveWholeAndNeverInterleaveASentGroup() throws Exception {
        var order = new ArrayList<String>();
        var first = new CompletableFuture<Void>();
        try (var dispatcher = new ReplayDispatcher(config(4, 8), recording(order, Map.of(2L, first)))) {
            dispatcher.send(write(1, 2));
            dispatcher.send(groupMember(2, 2, 0, 2));
            first.complete(null);
            dispatcher.send(groupMember(2, 2, 1, 2));
            // The deferred group became ready mid-group; it must not split itself or this sent group.
            dispatcher.send(groupMember(3, 0, 0, 2));
            dispatcher.send(groupMember(3, 0, 1, 2));
            dispatcher.drain();
            assertEquals(List.of("W2", "S2.0", "S2.1", "S0.0", "S0.1"), order);
            assertEquals(2L, dispatcher.describe().get("deferred_search_operations"));
        }
    }

    @Test
    public void dependencyWaitFlushesBufferedWritesAndPreservesTheirFailures() throws Exception {
        for (boolean rejected : List.of(false, true)) {
            var response = new CompletableFuture<Void>();
            var readCalls = new AtomicLong();
            ReplayTarget.Factory factory = client -> new ReplayTarget() {
                public SendHandle send(long seq, Operation op, org.sibench.adapters.proto.TraceOperation stored) {
                    if (op.kind() == IndexOperationTrace.SEARCH) {
                        readCalls.incrementAndGet();
                        return SendHandle.completed();
                    }
                    return new SendHandle(CompletableFuture.completedFuture(null), response);
                }
                public void flush() {
                    if (rejected) response.completeExceptionally(new IOException("write rejected"));
                    else response.complete(null);
                }
            };
            try (var dispatcher = new ReplayDispatcher(config(1, 8), factory)) {
                dispatcher.send(baseline(1));
                if (rejected) {
                    IOException failure = assertThrows(IOException.class, () -> dispatcher.send(search(2)));
                    assertSame(failure, assertThrows(IOException.class, dispatcher::drain));
                    assertEquals(0, readCalls.get());
                    assertEquals(1L, dispatcher.describe().get("failed_response_operations"));
                } else {
                    dispatcher.send(search(2));
                    dispatcher.drain();
                    assertEquals(1, readCalls.get());
                }
            }
        }
    }

    @Test
    public void sharedBatchKeepsEachMembersClientEntryTime() throws Exception {
        var now = new AtomicLong(1_000_000);
        var response = new CompletableFuture<Void>();
        try (var dispatcher = new ReplayDispatcher(config(1, 10), client -> (seq, op, stored) ->
                new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response), 0, now::get)) {
            dispatcher.send(baseline(1));
            now.addAndGet(4_000);
            dispatcher.send(baseline(2));
            now.addAndGet(4_000);
            response.complete(null);
            dispatcher.drain();
            var latency = (Map<?, ?>) dispatcher.describe().get("write_latency");
            assertEquals(2L, latency.get("count"));
            assertEquals(0.006, (double) latency.get("mean_ms"), 0);
            assertEquals(0.008, (double) latency.get("max_ms"), 0);
        }
    }

    @Test
    public void sharedBaselineCompletionStillBoundsAndCountsEveryMember() throws Exception {
        var batch = new CompletableFuture<Void>();
        var next = new CompletableFuture<Void>();
        var twoReturned = new CountDownLatch(1);
        var thirdCalled = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();
        try (var dispatcher = new ReplayDispatcher(config(1, 2), client -> (sequence, operation, stored) -> {
            if (sequence == 3) thirdCalled.countDown();
            return new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null),
                    sequence <= 2 ? batch : next);
        })) {
            var sender = new Thread(() -> {
                try {
                    dispatcher.send(baseline(1));
                    dispatcher.send(baseline(2));
                    twoReturned.countDown();
                    dispatcher.send(baseline(3));
                    dispatcher.drain();
                } catch (Throwable failure) { error.set(failure); }
            });
            sender.start();
            try {
                assertTrue(twoReturned.await(1, TimeUnit.SECONDS));
                assertEquals(2L, dispatcher.describe().get("pending_responses"));
                assertEquals(1, dispatcher.describe().get("pending_completion_groups"));
                assertFalse(thirdCalled.await(50, TimeUnit.MILLISECONDS));
                batch.complete(null);
                assertTrue(thirdCalled.await(1, TimeUnit.SECONDS));
                next.complete(null);
            } finally {
                batch.complete(null); next.complete(null); sender.join(3000);
            }
            assertFalse(sender.isAlive());
            assertNull(error.get());
            var report = dispatcher.describe();
            assertEquals(3L, report.get("target_calls"));
            assertEquals(3L, report.get("completed_responses"));
            assertEquals(0L, report.get("pending_responses"));
            assertEquals(2L, report.get("peak_in_flight"));
            assertEquals(1L, report.get("peak_completion_groups"));
            assertEquals(3L, ((Map<?, ?>) report.get("write_latency")).get("count"));
        }
    }

    @Test
    public void aBatchMayCompleteInsideItsLastSendBeforeRegistration() throws Exception {
        var response = new CompletableFuture<Void>();
        try (var dispatcher = new ReplayDispatcher(config(1, 4), client -> (sequence, operation, stored) -> {
            if (sequence == 2) response.complete(null);
            return new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response);
        })) {
            dispatcher.send(baseline(1));
            dispatcher.send(baseline(2));
            dispatcher.drain();
            var report = dispatcher.describe();
            assertEquals(2L, report.get("completed_responses"));
            assertEquals(0L, report.get("pending_responses"));
            assertEquals(0, report.get("pending_completion_groups"));
            assertEquals(2L, ((Map<?, ?>) report.get("write_latency")).get("count"));
        }
    }

    @Test
    public void sharedBaselineFailureDoesNotCountAnyMemberAsSuccessful() throws Exception {
        var response = new CompletableFuture<Void>();
        try (var dispatcher = new ReplayDispatcher(config(1, 4), client -> (sequence, operation, stored) ->
                new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response))) {
            dispatcher.send(baseline(1));
            dispatcher.send(baseline(2));
            response.completeExceptionally(new IOException("injected batch failure"));
            var failure = assertThrows(IOException.class, dispatcher::drain);
            assertTrue(failure.getMessage(), failure.getMessage().contains("dispatch 1"));
            assertEquals("injected batch failure", failure.getCause().getMessage());
            assertEquals(0L, dispatcher.describe().get("completed_responses"));
            assertEquals(2L, dispatcher.describe().get("failed_response_operations"));
            assertTrue(dispatcher.describe().containsKey("failure"));
            assertEquals(0L, dispatcher.describe().get("pending_responses"));
        }
    }

    @Test
    public void closingSharedBaselineCompletionCancelsAndClearsAllMembers() throws Exception {
        var response = new CompletableFuture<Void>();
        var dispatcher = new ReplayDispatcher(config(1, 4), client -> (sequence, operation, stored) ->
                new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response));
        dispatcher.send(baseline(1));
        dispatcher.send(baseline(2));
        dispatcher.close();
        assertTrue(response.isCancelled());
        assertEquals(0L, dispatcher.describe().get("pending_responses"));
        assertEquals(0, dispatcher.describe().get("pending_completion_groups"));
    }

    @Test
    public void delayedWarmupResponsesStayOutsideTheMeasuredWindow() throws Exception {
        for (boolean batch : new boolean[] {false, true}) {
            var response = new CompletableFuture<Void>();
            try (var dispatcher = new ReplayDispatcher(config(1, 10), client -> (sequence, operation, stored) ->
                    new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response), 1)) {
                dispatcher.send(batch ? baseline(1) : search(1));
                dispatcher.send(batch ? baseline(2) : search(2));
                response.complete(null);
                dispatcher.drain();
                var report = dispatcher.describe();
                assertEquals(1L, ((Map<?, ?>) report.get(batch ? "write_latency" : "search_latency")).get("count"));
                var measurement = (Map<?, ?>) report.get("measurement");
                assertEquals(1.0, (double) measurement.get("completion_rate")
                        * (double) measurement.get("completion_seconds"), 1e-9);
            }
        }
    }

    @Test
    public void measuredWindowStartsAfterTheWarmupCalls() throws Exception {
        try (var dispatcher =
                new ReplayDispatcher(
                        config(1, 10),
                        client -> (sequence, operation, stored) -> ReplayTarget.SendHandle.completed(),
                        2)) {
            for (long unit = 1; unit <= 5; unit++) dispatcher.send(search(unit));
            dispatcher.drain();
            var report = dispatcher.describe();
            var measurement = (Map<?, ?>) report.get("measurement");
            assertEquals(2L, measurement.get("warmup_operations"));
            assertEquals(3L, measurement.get("measured_operations"));
            assertEquals("N/A", measurement.get("target_arrival_rate"));
            assertEquals(3L, ((Map<?, ?>) report.get("search_latency")).get("count"));
            assertEquals(5L, report.get("completed_responses"));
        }
    }

    @Test
    public void serializesLocalSendsWithoutWaitingForServiceResponses() throws Exception {
        List<Long> sends = new ArrayList<>();
        List<Integer> clients = new ArrayList<>();
        List<CompletableFuture<Void>> responses = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> firstSent = new CompletableFuture<>();
        CountDownLatch firstCall = new CountDownLatch(1), allSent = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (var dispatcher =
                new ReplayDispatcher(
                        config(3, 10),
                        client ->
                                (sequence, operation, stored) -> {
                                    synchronized (sends) {
                                        sends.add(sequence);
                                        clients.add(client);
                                        responses.add(new CompletableFuture<>());
                                    }
                                    if (sequence == 1) firstCall.countDown();
                                    if (sequence == 4) allSent.countDown();
                                    return new ReplayTarget.SendHandle(
                                            sequence == 1
                                                    ? firstSent
                                                    : CompletableFuture.completedFuture(null),
                                            responses.get((int) sequence - 1));
                                })) {
            Thread sender =
                    new Thread(
                            () -> {
                                try {
                                    dispatcher.send(search(1));
                                    dispatcher.send(search(2));
                                    dispatcher.send(search(2));
                                    dispatcher.send(search(3));
                                    dispatcher.drain();
                                } catch (Throwable error) {
                                    failure.set(error);
                                }
                            });
            sender.start();
            try {
                assertTrue(firstCall.await(1, TimeUnit.SECONDS));
                synchronized (sends) {
                    assertEquals(List.of(1L), sends);
                }
                firstSent.complete(null);
                assertTrue(allSent.await(1, TimeUnit.SECONDS));
                assertEquals(List.of(1L, 2L, 3L, 4L), sends);
                assertEquals(List.of(0, 1, 1, 2), clients);
                for (int i = responses.size() - 1; i >= 0; i--) responses.get(i).complete(null);
            } finally {
                firstSent.complete(null);
                for (var response : responses) response.complete(null);
                sender.join(3000);
            }
            assertFalse(sender.isAlive());
            assertNull(failure.get());
            assertEquals(4L, dispatcher.describe().get("completed_responses"));
            assertEquals(3L, dispatcher.describe().get("work_units"));
            dispatcher.requireClientCoverage();
        }
    }

    @Test
    public void backpressureBoundsResponsesWithoutChangingRouting() throws Exception {
        List<CompletableFuture<Void>> responses = new CopyOnWriteArrayList<>();
        CountDownLatch first = new CountDownLatch(1), second = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        try (var dispatcher =
                new ReplayDispatcher(
                        config(2, 1),
                        client ->
                                (seq, op, stored) -> {
                                    CompletableFuture<Void> response = new CompletableFuture<>();
                                    responses.add(response);
                                    if (seq == 1) first.countDown();
                                    else second.countDown();
                                    return new ReplayTarget.SendHandle(
                                            CompletableFuture.completedFuture(null), response);
                                })) {
            Thread sender =
                    new Thread(
                            () -> {
                                try {
                                    dispatcher.send(search(1));
                                    dispatcher.send(search(2));
                                    dispatcher.drain();
                                } catch (Throwable failure) {
                                    error.set(failure);
                                }
                            });
            sender.start();
            try {
                assertTrue(first.await(1, TimeUnit.SECONDS));
                assertFalse(second.await(50, TimeUnit.MILLISECONDS));
                responses.get(0).complete(null);
                assertTrue(second.await(1, TimeUnit.SECONDS));
                responses.get(1).complete(null);
            } finally {
                for (var response : responses) response.complete(null);
                sender.join(3000);
            }
            assertNull(error.get());
            assertFalse(sender.isAlive());
            assertEquals(1L, dispatcher.describe().get("peak_in_flight"));
        }
    }

    @Test
    public void failuresStopSendingAndClientsAreClosed() throws Exception {
        CompletableFuture<Void> response = new CompletableFuture<>();
        List<Integer> closed = new ArrayList<>();
        var dispatcher =
                new ReplayDispatcher(
                        config(2, 10),
                        client ->
                                new ReplayTarget() {
                                    @Override
                                    public SendHandle send(long seq, Operation op, org.sibench.adapters.proto.TraceOperation stored) {
                                        return new SendHandle(
                                                CompletableFuture.completedFuture(null), response);
                                    }

                                    @Override
                                    public void close() {
                                        closed.add(client);
                                    }
                                });
        dispatcher.send(search(1));
        response.completeExceptionally(new IOException("Rejected"));
        assertThrows(IOException.class, () -> dispatcher.send(search(2)));
        assertThrows(IOException.class, dispatcher::drain);
        dispatcher.close();
        assertEquals(List.of(0), closed);
        assertThrows(IOException.class, () -> dispatcher.send(search(1)));
        try (var failedSend =
                new ReplayDispatcher(
                        config(1, 10),
                        client ->
                                (seq, op, stored) ->
                                        new ReplayTarget.SendHandle(
                                                CompletableFuture.failedFuture(
                                                        new IOException("send")),
                                                CompletableFuture.completedFuture(null)))) {
            assertThrows(IOException.class, () -> failedSend.send(search(1)));
            assertThrows(IOException.class, () -> failedSend.send(search(2)));
        }
    }

    @Test
    public void supportsManyClientsAndRejectsMissingIdentityOrCoverage() throws Exception {
        // 128 is an experiment setting, not a limit: 256 clients work when units cover them.
        for (int clients : new int[] {128, 256}) {
            try (var dispatcher =
                    new ReplayDispatcher(config(clients, 10), ReplayTarget.immediate(op -> {}))) {
                for (int i = 1; i <= clients; i++) dispatcher.send(search(i));
                dispatcher.drain();
                dispatcher.requireClientCoverage();
                assertEquals(clients, dispatcher.describe().get("used_clients"));
                assertEquals(0, dispatcher.route(search(clients + 1)));
            }
        }
        try (var dispatcher =
                new ReplayDispatcher(config(128, 10), ReplayTarget.immediate(op -> {}))) {
            dispatcher.send(search(1));
            assertThrows(IOException.class, dispatcher::requireClientCoverage);
            assertThrows(IOException.class, () -> dispatcher.send(search(0)));
        }
    }

    @Test
    public void responseAndLocalSendTimeoutsStopReplayAndCancelOutstandingRequests()
            throws Exception {
        for (boolean localTimeout : new boolean[] {true, false}) {
            var sent =
                    localTimeout
                            ? new CompletableFuture<Void>()
                            : CompletableFuture.<Void>completedFuture(null);
            var response = new CompletableFuture<Void>();
            var dispatcher =
                    new ReplayDispatcher(
                            new ReplayConfig(true, 0, 0, 0, 0, "operation", 1, 1, 1),
                            client -> (seq, op, stored) -> new ReplayTarget.SendHandle(sent, response));
            try {
                if (localTimeout) assertThrows(IOException.class, () -> dispatcher.send(search(1)));
                else {
                    dispatcher.send(search(1));
                    var error = assertThrows(IOException.class, dispatcher::drain);
                    assertTrue(error.getMessage(), error.getMessage().contains("source event 1, search, transaction 0, version 0"));
                }
                assertThrows(IOException.class, () -> dispatcher.send(search(2)));
            } finally {
                dispatcher.close();
            }
            assertTrue(response.isCancelled());
            if (localTimeout) assertTrue(sent.isCancelled());
        }
    }

    @Test
    public void coverageIsCheckedAfterSecondaryFilteringWithoutRenumberingWorkUnits()
            throws Exception {
        var operations =
                List.of(
                        search(1),
                        IndexOperationTrace.search(2, 5, 1, Range.equal(IndexKey.ofInt(10)), 0, 0, 0).withWorkUnitId(2),
                        search(3));
        for (boolean secondary : new boolean[] {false, true}) {
            List<Integer> routes = new ArrayList<>();
            var config = new ReplayConfig(secondary, 0, 0, 0, 0, "operation", 2, 10, 2);
            try (var projection =
                    ReplayProjection.withClients(
                            config,
                            false,
                            client ->
                                    (seq, op, stored) -> {
                                        routes.add(client);
                                        return ReplayTarget.SendHandle.completed();
                                    })) {
                for (var operation : operations) projection.accept(operation);
                projection.finish();
                assertEquals(secondary ? List.of(0, 1, 0) : List.of(0, 0), routes);
                if (secondary) projection.requireClientCoverage();
                else assertThrows(IOException.class, projection::requireClientCoverage);
            }
        }
    }

    @Test
    public void interruptionPreservesFlagAndPendingResponseIsCancelledOnClose() throws Exception {
        CompletableFuture<Void> response = new CompletableFuture<>();
        CountDownLatch sent = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicBoolean interrupted =
                new java.util.concurrent.atomic.AtomicBoolean();
        try (var dispatcher =
                new ReplayDispatcher(
                        config(1, 10),
                        client ->
                                (seq, op, stored) -> {
                                    sent.countDown();
                                    return new ReplayTarget.SendHandle(
                                            CompletableFuture.completedFuture(null), response);
                                })) {
            Thread sender =
                    new Thread(
                            () -> {
                                try {
                                    dispatcher.send(search(1));
                                    dispatcher.drain();
                                } catch (IOException failure) {
                                    error.set(failure);
                                    interrupted.set(Thread.currentThread().isInterrupted());
                                }
                            });
            sender.start();
            try {
                assertTrue(sent.await(1, TimeUnit.SECONDS));
                sender.interrupt();
            } finally {
                sender.join(3000);
                if (sender.isAlive()) {
                    response.complete(null);
                    sender.interrupt();
                    sender.join(3000);
                }
            }
            assertFalse(sender.isAlive());
            assertTrue(interrupted.get());
            assertTrue(error.get() instanceof IOException);
        }
        assertTrue(response.isCancelled());
    }

    @Test
    public void completedResponseCannotHideAnExpiredRequestOrUnfinishedLocalSend()
            throws Exception {
        var response = new CompletableFuture<Void>();
        try (var dispatcher =
                new ReplayDispatcher(
                        new ReplayConfig(true, 0, 0, 0, 0, "operation", 1, 10, 1),
                        client ->
                                (seq, op, stored) ->
                                        new ReplayTarget.SendHandle(
                                                CompletableFuture.completedFuture(null),
                                                response))) {
            dispatcher.send(search(1));
            Thread.sleep(1100);
            response.complete(null);
            assertThrows(IOException.class, dispatcher::drain);
            assertEquals(1L, dispatcher.describe().get("timed_out_operations"));
            assertThrows(IOException.class, () -> dispatcher.send(search(2)));
        }
        var localSend = new CompletableFuture<Void>();
        var dispatcher =
                new ReplayDispatcher(
                        new ReplayConfig(true, 0, 0, 0, 0, "operation", 1, 10, 1),
                        client ->
                                (seq, op, stored) ->
                                        new ReplayTarget.SendHandle(
                                                localSend,
                                                CompletableFuture.completedFuture(null)));
        try {
            assertThrows(IOException.class, () -> dispatcher.send(search(1)));
        } finally {
            dispatcher.close();
        }
        assertTrue(localSend.isCancelled());
    }
}
