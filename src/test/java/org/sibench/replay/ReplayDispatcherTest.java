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

public class ReplayDispatcherTest {
    private static ReplayConfig config(int clients, int inFlight) {
        return new ReplayConfig(true, "none", 0, 0, 0, 0, "operation", clients, inFlight, 2);
    }

    private static Operation search(long unit) {
        return IndexOperationTrace.search(unit, 3, 0, Range.equal(IndexKey.ofInt(1)), 0, 0, 0).withWorkUnitId(unit);
    }

    private static Operation baseline(long event) {
        return IndexOperationTrace.write(event, IndexOperationTrace.INSERT, 3, 0,
                IndexKey.ofInt(event), 0, 1, event, event).withWorkUnitId(1);
    }

    @Test
    public void sharedBaselineCompletionStillBoundsAndCountsEveryMember() throws Exception {
        var batch = new CompletableFuture<Void>();
        var next = new CompletableFuture<Void>();
        var twoReturned = new CountDownLatch(1);
        var thirdCalled = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();
        try (var dispatcher = new ReplayDispatcher(config(1, 2), client -> (sequence, operation) -> {
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
        try (var dispatcher = new ReplayDispatcher(config(1, 4), client -> (sequence, operation) -> {
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
        try (var dispatcher = new ReplayDispatcher(config(1, 4), client -> (sequence, operation) ->
                new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response))) {
            dispatcher.send(baseline(1));
            dispatcher.send(baseline(2));
            response.completeExceptionally(new IOException("injected batch failure"));
            var failure = assertThrows(IOException.class, dispatcher::drain);
            assertTrue(failure.getMessage(), failure.getMessage().contains("dispatch 1"));
            assertEquals("injected batch failure", failure.getCause().getMessage());
            assertEquals(0L, dispatcher.describe().get("completed_responses"));
            assertEquals(0L, dispatcher.describe().get("pending_responses"));
        }
    }

    @Test
    public void closingSharedBaselineCompletionCancelsAndClearsAllMembers() throws Exception {
        var response = new CompletableFuture<Void>();
        var dispatcher = new ReplayDispatcher(config(1, 4), client -> (sequence, operation) ->
                new ReplayTarget.SendHandle(CompletableFuture.completedFuture(null), response));
        dispatcher.send(baseline(1));
        dispatcher.send(baseline(2));
        dispatcher.close();
        assertTrue(response.isCancelled());
        assertEquals(0L, dispatcher.describe().get("pending_responses"));
        assertEquals(0, dispatcher.describe().get("pending_completion_groups"));
    }

    @Test
    public void measuredWindowStartsAfterTheWarmupCalls() throws Exception {
        try (var dispatcher =
                new ReplayDispatcher(
                        config(1, 10),
                        client -> (sequence, operation) -> ReplayTarget.SendHandle.completed(),
                        2)) {
            for (long unit = 1; unit <= 5; unit++) dispatcher.send(search(unit));
            dispatcher.drain();
            var report = dispatcher.describe();
            var measurement = (Map<?, ?>) report.get("measurement");
            assertEquals(2L, measurement.get("warmup_calls"));
            assertEquals(3L, measurement.get("measured_calls"));
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
                                (sequence, operation) -> {
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
                                (seq, op) -> {
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
                                    public SendHandle send(long seq, Operation op) {
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
                                (seq, op) ->
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
                            new ReplayConfig(true, "none", 0, 0, 0, 0, "operation", 1, 1, 1),
                            client -> (seq, op) -> new ReplayTarget.SendHandle(sent, response));
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
            var config = new ReplayConfig(secondary, "none", 0, 0, 0, 0, "operation", 2, 10, 2);
            try (var projection =
                    ReplayProjection.withClients(
                            config,
                            false,
                            client ->
                                    (seq, op) -> {
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
                                (seq, op) -> {
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
                        new ReplayConfig(true, "none", 0, 0, 0, 0, "operation", 1, 10, 1),
                        client ->
                                (seq, op) ->
                                        new ReplayTarget.SendHandle(
                                                CompletableFuture.completedFuture(null),
                                                response))) {
            dispatcher.send(search(1));
            Thread.sleep(1100);
            response.complete(null);
            assertThrows(IOException.class, dispatcher::drain);
            assertThrows(IOException.class, () -> dispatcher.send(search(2)));
        }
        var localSend = new CompletableFuture<Void>();
        var dispatcher =
                new ReplayDispatcher(
                        new ReplayConfig(true, "none", 0, 0, 0, 0, "operation", 1, 10, 1),
                        client ->
                                (seq, op) ->
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
