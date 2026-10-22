package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/** One sender, deterministic work-unit routing, and bounded independently completed responses. */
public final class ReplayDispatcher implements AutoCloseable {
    private record Pending(long deadline, CompletableFuture<Void> response) {}

    private final ReplayConfig config;
    private final ReplayTarget.Factory factory;
    private final Map<Integer, ReplayTarget> clients = new LinkedHashMap<>();
    private final Set<Long> workUnits = new HashSet<>();
    private final Map<Integer, long[]> counts = new TreeMap<>();
    private final Map<Long, Pending> inFlight = new LinkedHashMap<>();
    private long sent, completed, peakInFlight, orderChecksum, backpressureNanos;
    private IOException failure;
    private CompletableFuture<Void> localSend;
    private volatile boolean closed;
    private Thread owner;

    public ReplayDispatcher(ReplayConfig config, ReplayTarget.Factory factory) {
        this.config = config;
        this.factory = Objects.requireNonNull(factory);
    }

    public int route(Operation operation) throws IOException {
        long id = operation.workUnitId();
        if (id < 0 || id == 0 && config.clientCount() > 1)
            throw new IOException(
                    "Multi-client replay requires source work-unit identities; recompile the"
                            + " capture");
        return id == 0 ? 0 : (int) ((id - 1) % config.clientCount());
    }

    private synchronized void checkSender() throws IOException {
        if (owner == null) owner = Thread.currentThread();
        if (owner != Thread.currentThread())
            throw new IOException("Only one sending thread is allowed");
        if (closed) throw new IOException("Dispatcher is closed");
        synchronized (inFlight) {
            checkFailure();
        }
    }

    // Response callbacks and the sender share failure/deadline state under the in-flight monitor.
    private void checkFailure() throws IOException {
        if (failure != null) throw failure;
        if (!inFlight.isEmpty()
                && inFlight.values().iterator().next().deadline() <= System.nanoTime())
            throw fail(new IOException("Service response timeout"));
    }

    private IOException fail(IOException error) {
        if (failure == null) failure = error;
        inFlight.notifyAll();
        return failure;
    }

    public void send(Operation operation) throws IOException {
        checkSender();
        int clientId = route(operation);
        synchronized (inFlight) {
            long start = inFlight.size() >= config.maxInFlight() ? System.nanoTime() : 0;
            while (inFlight.size() >= config.maxInFlight()) {
                checkFailure();
                waitForResponse();
            }
            if (start != 0) backpressureNanos += System.nanoTime() - start;
            checkFailure();
        }
        try {
            ReplayTarget client = clients.get(clientId);
            if (client == null) {
                client = Objects.requireNonNull(factory.open(clientId));
                clients.put(clientId, client);
            }
            long sequence = sent + 1;
            long deadline =
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(config.requestTimeoutSeconds());
            ReplayTarget.SendHandle handle =
                    Objects.requireNonNull(client.send(sequence, operation));
            localSend = handle.sent().toCompletableFuture();
            CompletableFuture<Void> response = handle.response().toCompletableFuture();
            synchronized (inFlight) {
                inFlight.put(sequence, new Pending(deadline, response));
                peakInFlight = Math.max(peakInFlight, inFlight.size());
            }
            response.whenComplete(
                    (unused, error) -> {
                        synchronized (inFlight) {
                            inFlight.remove(sequence);
                            if (error != null && !closed)
                                fail(
                                        new IOException(
                                                "Service response failed at dispatch "
                                                        + sequence
                                                        + ", client "
                                                        + clientId,
                                                error));
                            else if (error == null && !closed) {
                                if (deadline <= System.nanoTime())
                                    fail(
                                            new IOException(
                                                    "Service response timeout at dispatch "
                                                            + sequence
                                                            + ", client "
                                                            + clientId));
                                else completed++;
                            }
                            inFlight.notifyAll();
                        }
                    });
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Local send timeout");
            localSend.get(remaining, TimeUnit.NANOSECONDS);
            localSend = null;
            sent++;
            long[] clientCounts = counts.computeIfAbsent(clientId, key -> new long[4]);
            clientCounts[0]++;
            if (operation.kind() == IndexOperationTrace.SEARCH) clientCounts[1]++;
            else clientCounts[2]++;
            if (operation.workUnitId() != 0 && workUnits.add(operation.workUnitId()))
                clientCounts[3]++;
            orderChecksum =
                    Long.rotateLeft(orderChecksum, 7)
                            ^ operation.sourceEvent()
                            ^ Long.rotateLeft(java.util.Objects.hashCode(operation.key()), 13)
                            ^ operation.kind()
                            ^ ((long) operation.table() << 48)
                            ^ ((long) operation.index() << 40)
                            ^ Long.rotateLeft(operation.version(), 19)
                            ^ Long.rotateLeft(operation.readTimestamp(), 23)
                            ^ Long.rotateLeft(operation.mutationOrdinal(), 29)
                            ^ Long.rotateLeft(operation.workUnitId(), 31)
                            ^ Long.rotateLeft(operation.file(), 37)
                            ^ Long.rotateLeft(operation.position(), 41)
                            ^ Long.rotateLeft(operation.rowId(), 43)
                            ^ Long.rotateLeft(java.util.Objects.hashCode(operation.indexedValue()), 47)
                            ^ Long.rotateLeft(
                                    java.util.Objects.hash(
                                            operation.lower(), operation.upper(), operation.limit()),
                                    53)
                            ^ Long.rotateLeft(operation.expectedCount(), 59)
                            ^ Long.rotateLeft(operation.transactionId(), 61)
                            ^ Long.rotateLeft(operation.flags(), 3);
            synchronized (inFlight) {
                checkFailure();
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            synchronized (inFlight) {
                throw fail(new IOException("Replay interrupted", error));
            }
        } catch (ExecutionException | TimeoutException | RuntimeException error) {
            synchronized (inFlight) {
                throw fail(new IOException("Local send failed", error));
            }
        } catch (IOException error) {
            synchronized (inFlight) {
                throw fail(error);
            }
        }
    }

    private void waitForResponse() throws IOException {
        long nanos = inFlight.values().iterator().next().deadline() - System.nanoTime();
        if (nanos <= 0) {
            checkFailure();
            return;
        }
        try {
            TimeUnit.NANOSECONDS.timedWait(inFlight, nanos);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw fail(new IOException("Replay interrupted while draining responses", error));
        }
    }

    public void drain() throws IOException {
        checkSender();
        synchronized (inFlight) {
            while (!inFlight.isEmpty()) {
                checkFailure();
                waitForResponse();
            }
            checkFailure();
        }
    }

    public void requireClientCoverage() throws IOException {
        if (config.clientCount() > 1 && counts.size() != config.clientCount())
            throw new IOException(
                    "Insufficient work-unit coverage: configured "
                            + config.clientCount()
                            + " clients, used "
                            + counts.size()
                            + "; continue native capture");
    }

    public Map<String, Object> describe() {
        var report = new LinkedHashMap<String, Object>();
        report.put("configured_clients", config.clientCount());
        report.put("used_clients", counts.size());
        report.put("work_units", workUnits.size());
        report.put("client_routing", "source_work_unit_round_robin");
        report.put("target_calls", sent);
        report.put("operation_order_checksum", orderChecksum);
        report.put("local_send_boundary", "complete_local_transmission_not_service_execution");
        report.put("service_visibility_owner", "index_service");
        report.put("backpressure_seconds", backpressureNanos / 1e9);
        synchronized (inFlight) {
            report.put("completed_responses", completed);
            report.put("pending_responses", inFlight.size());
            report.put("peak_in_flight", peakInFlight);
        }
        report.put(
                "per_client",
                counts.entrySet().stream()
                        .map(
                                entry -> {
                                    long[] c = entry.getValue();
                                    return Map.of(
                                            "client_id",
                                            entry.getKey(),
                                            "operations",
                                            c[0],
                                            "queries",
                                            c[1],
                                            "writes",
                                            c[2],
                                            "work_units",
                                            c[3]);
                                })
                        .toList());
        return report;
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        if (localSend != null) localSend.cancel(false);
        synchronized (inFlight) {
            for (Pending pending : List.copyOf(inFlight.values())) {
                pending.response().cancel(false);
            }
            inFlight.clear();
            inFlight.notifyAll();
        }
        IOException error = null;
        for (ReplayTarget client : clients.values()) {
            try {
                client.close();
            } catch (IOException failure) {
                if (error == null) error = failure;
                else error.addSuppressed(failure);
            }
        }
        if (error != null) throw error;
    }
}
