package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/** One sender, deterministic work-unit routing, and bounded independently completed responses. */
public final class ReplayDispatcher implements AutoCloseable {
    private static final class Pending {
        private final long deadline, started;
        private final CompletableFuture<Void> response;
        private final String commitDescription;
        private final int clientId, kind;
        private final Operation operation;
        private int members = 1;
        private long[] memberStarts;

        Pending(long deadline, CompletableFuture<Void> response, String commitDescription,
                int clientId, Operation operation, long started, int kind) {
            this.deadline = deadline;
            this.response = response;
            this.commitDescription = commitDescription;
            this.clientId = clientId;
            this.operation = operation;
            this.started = started;
            this.kind = kind;
        }

        long deadline() { return deadline; }
        CompletableFuture<Void> response() { return response; }

        // The sender and response callback hold inFlight while adding or completing members.
        void addMember(long start) {
            if (memberStarts == null) {
                memberStarts = new long[8];
                memberStarts[0] = started;
            }
            if (members == memberStarts.length)
                memberStarts = Arrays.copyOf(memberStarts, members * 2);
            memberStarts[members++] = start;
        }

        void recordLatency(Latency latency, long completed) {
            if (memberStarts == null) latency.add(completed - started);
            else for (int i = 0; i < members; i++) latency.add(completed - memberStarts[i]);
        }

        String description() {
            if (operation == null) return commitDescription;
            return "client " + clientId + ", source event " + operation.sourceEvent()
                    + ", " + IndexOperationTrace.kindName(operation.kind())
                    + ", transaction " + operation.transactionId() + ", version " + operation.version();
        }
    }

    /** Millisecond response latency from send call (queue included) to response. */
    private static final class Latency {
        private static final int MAX_MILLIS = 60_000;
        private final long[] buckets = new long[MAX_MILLIS + 1];
        private long count, totalNanos, maximumNanos, overflow;

        void reset() {
            java.util.Arrays.fill(buckets, 0);
            count = totalNanos = maximumNanos = overflow = 0;
        }

        void add(long nanos) {
            count++;
            totalNanos += nanos;
            maximumNanos = Math.max(maximumNanos, nanos);
            long millis = nanos / 1_000_000L;
            if (millis > MAX_MILLIS) {
                overflow++;
                buckets[MAX_MILLIS]++;
            } else buckets[(int) millis]++;
        }

        Map<String, Object> describe() {
            var report = new LinkedHashMap<String, Object>();
            report.put("count", count);
            report.put("mean_ms", count == 0 ? 0.0 : totalNanos / 1e6 / count);
            report.put("p50_ms", percentile(50));
            report.put("p95_ms", percentile(95));
            report.put("p99_ms", percentile(99));
            report.put("max_ms", maximumNanos / 1e6);
            report.put("resolution_ms", 1);
            report.put("overflow_ms", overflow);
            return report;
        }

        /** The bucket at or above the requested share; bucket values are upper bounds. */
        private long percentile(int percent) {
            if (count == 0) return 0;
            long target = Math.max(1, (count * percent + 99) / 100);
            long seen = 0;
            for (int millis = 0; millis <= MAX_MILLIS; millis++) {
                seen += buckets[millis];
                if (seen >= target) return millis;
            }
            return MAX_MILLIS;
        }
    }

    private final ReplayConfig config;
    private final ReplayTarget.Factory factory;
    /** Calls sent before the measured window starts; their latencies are not reported. */
    private final long warmup;
    private long firstSend, measuredStart, lastSend, lastCompletion, completedAtMeasuredStart, drainNanos;
    private final Map<Integer, ReplayTarget> clients = new LinkedHashMap<>();
    private final BitSet workUnits = new BitSet();
    private final Map<Integer, long[]> counts = new TreeMap<>();
    private final Map<Long, Pending> inFlight = new LinkedHashMap<>();
    private final Map<CompletableFuture<Void>, Pending> baselineResponses = new IdentityHashMap<>();
    private final Latency writeLatency = new Latency(), searchLatency = new Latency(), commitLatency = new Latency();
    private long sent, completed, workUnitCount, peakInFlight, orderChecksum, backpressureNanos, commitMarkers;
    private long inFlightMembers, peakCompletionGroups;
    private IOException failure;
    private CompletableFuture<Void> localSend;
    private volatile boolean closed;
    private Thread owner;

    public ReplayDispatcher(ReplayConfig config, ReplayTarget.Factory factory) {
        this(config, factory, 0);
    }

    /** {@code warmup}: target calls before the measured window, 0 to measure from the start. */
    public ReplayDispatcher(ReplayConfig config, ReplayTarget.Factory factory, long warmup) {
        this.config = config;
        this.factory = Objects.requireNonNull(factory);
        this.warmup = warmup;
    }

    /**
     * Before a call is handed to its client: the first send, and the measured window's start
     * before its first call, whose response may complete within the send itself.
     */
    private void beginCall(long now) {
        if (sent == 0) firstSend = now;
        if (sent == warmup)
            synchronized (inFlight) {
                measuredStart = now;
                completedAtMeasuredStart = completed;
                writeLatency.reset();
                searchLatency.reset();
                commitLatency.reset();
            }
    }

    public int route(Operation operation) throws IOException {
        long id = operation.workUnitId();
        if (id < 0 || id == 0 && config.clientCount() > 1)
            throw new IOException(
                    "Multi-client replay requires source work-unit identities; recompile the"
                            + " capture");
        if (id > Integer.MAX_VALUE)
            throw new IOException("Source work-unit identity exceeds the driver index range");
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
                && inFlight.values().iterator().next().deadline() <= System.nanoTime()) {
            var oldest = inFlight.entrySet().iterator().next();
            throw fail(
                    new IOException(
                            "Service response timeout at dispatch "
                                    + oldest.getKey()
                                    + ": "
                                    + oldest.getValue().description()));
        }
    }

    private IOException fail(IOException error) {
        if (failure == null) failure = error;
        inFlight.notifyAll();
        return failure;
    }

    public void send(Operation operation) throws IOException {
        checkSender();
        int clientId = route(operation);
        long started = System.nanoTime();
        synchronized (inFlight) {
            long wait = inFlightMembers >= config.maxInFlight() ? System.nanoTime() : 0;
            while (inFlightMembers >= config.maxInFlight()) {
                checkFailure();
                waitForResponse();
            }
            if (wait != 0) backpressureNanos += System.nanoTime() - wait;
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
            beginCall(System.nanoTime());
            ReplayTarget.SendHandle handle =
                    Objects.requireNonNull(client.send(sequence, operation));
            localSend = handle.sent().toCompletableFuture();
            CompletableFuture<Void> response = handle.response().toCompletableFuture();
            int latencyKind = operation.kind() == IndexOperationTrace.SEARCH ? 1 : 0;
            boolean baseline = operation.kind() == IndexOperationTrace.INSERT
                    && operation.version() == 0 && operation.transactionId() == 0;
            Pending added = null;
            synchronized (inFlight) {
                Pending shared = baseline ? baselineResponses.get(response) : null;
                if (shared == null) {
                    added = new Pending(deadline, response, null, clientId, operation, started, latencyKind);
                    inFlight.put(sequence, added);
                    if (baseline) baselineResponses.put(response, added);
                } else shared.addMember(started);
                inFlightMembers++;
                peakInFlight = Math.max(peakInFlight, inFlightMembers);
                peakCompletionGroups = Math.max(peakCompletionGroups, inFlight.size());
            }
            Pending pending = added;
            if (pending != null) response.whenComplete(
                    (unused, error) -> {
                        synchronized (inFlight) {
                            Pending removed = inFlight.remove(sequence);
                            if (removed != null) inFlightMembers -= removed.members;
                            baselineResponses.remove(response, pending);
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
                                else {
                                    completed += pending.members;
                                    lastCompletion = System.nanoTime();
                                    pending.recordLatency(latencyKind == 1 ? searchLatency : writeLatency,
                                            lastCompletion);
                                }
                            }
                            inFlight.notifyAll();
                        }
                    });
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Local send timeout");
            localSend.get(remaining, TimeUnit.NANOSECONDS);
            localSend = null;
            sent++;
            lastSend = System.nanoTime();
            long[] clientCounts = counts.computeIfAbsent(clientId, key -> new long[4]);
            clientCounts[0]++;
            if (operation.kind() == IndexOperationTrace.SEARCH) clientCounts[1]++;
            else clientCounts[2]++;
            if (operation.workUnitId() != 0 && !workUnits.get((int) operation.workUnitId())) {
                workUnits.set((int) operation.workUnitId());
                workUnitCount++;
                clientCounts[3]++;
            }
            orderChecksum =
                    Long.rotateLeft(orderChecksum, 7)
                            ^ operation.sourceEvent()
                            ^ Long.rotateLeft(word(operation.key()), 13)
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
                            ^ Long.rotateLeft(word(operation.indexedValue()), 47)
                            // Objects.hash(lower, upper, limit) as it was defined.
                            ^ Long.rotateLeft(
                                    31 * (31 * (31 + word(operation.lower())) + word(operation.upper()))
                                            + Long.hashCode(operation.limit()),
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

    /** Sends one complete-commit marker after its transaction's operations, on the control client. */
    public void sendCommit(long transactionId, long version) throws IOException {
        checkSender();
        int clientId = 0;
        long started = System.nanoTime();
        synchronized (inFlight) {
            while (inFlightMembers >= config.maxInFlight()) {
                checkFailure();
                waitForResponse();
            }
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
            beginCall(System.nanoTime());
            ReplayTarget.SendHandle handle =
                    Objects.requireNonNull(client.sendCommit(sequence, transactionId, version));
            localSend = handle.sent().toCompletableFuture();
            CompletableFuture<Void> response = handle.response().toCompletableFuture();
            synchronized (inFlight) {
                inFlight.put(
                        sequence,
                        new Pending(
                                deadline,
                                response,
                                "client " + clientId + ", commit marker, transaction "
                                        + transactionId + ", version " + version,
                                clientId,
                                null,
                                started,
                                2));
                inFlightMembers++;
                peakInFlight = Math.max(peakInFlight, inFlightMembers);
                peakCompletionGroups = Math.max(peakCompletionGroups, inFlight.size());
            }
            response.whenComplete(
                    (unused, error) -> {
                        synchronized (inFlight) {
                            Pending removed = inFlight.remove(sequence);
                            if (removed != null) inFlightMembers -= removed.members;
                            if (error != null && !closed)
                                fail(
                                        new IOException(
                                                "Commit response failed at dispatch "
                                                        + sequence
                                                        + ", client "
                                                        + clientId,
                                                error));
                            else if (error == null && !closed) {
                                if (deadline <= System.nanoTime())
                                    fail(
                                            new IOException(
                                                    "Commit response timeout at dispatch "
                                                            + sequence
                                                            + ", client "
                                                            + clientId));
                                else {
                                    completed++;
                                    lastCompletion = System.nanoTime();
                                    commitLatency.add(lastCompletion - started);
                                }
                            }
                            inFlight.notifyAll();
                        }
                    });
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Local send timeout");
            localSend.get(remaining, TimeUnit.NANOSECONDS);
            localSend = null;
            sent++;
            lastSend = System.nanoTime();
            commitMarkers++;
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
        long start = System.nanoTime();
        try {
            for (ReplayTarget client : clients.values()) client.flush();
        } catch (IOException | RuntimeException error) {
            synchronized (inFlight) {
                throw fail(error instanceof IOException io ? io : new IOException("Client flush failed", error));
            }
        }
        synchronized (inFlight) {
            while (!inFlight.isEmpty()) {
                checkFailure();
                waitForResponse();
            }
            checkFailure();
        }
        drainNanos += System.nanoTime() - start;
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
        report.put("work_units", workUnitCount);
        report.put("client_routing", "source_work_unit_round_robin");
        report.put("target_calls", sent);
        report.put("commit_markers", commitMarkers);
        report.put("operation_order_checksum", orderChecksum);
        report.put("local_send_boundary", "complete_local_transmission_not_service_execution");
        report.put("service_visibility_owner", "index_service");
        report.put("backpressure_seconds", backpressureNanos / 1e9);
        synchronized (inFlight) {
            report.put("completed_responses", completed);
            report.put("pending_responses", inFlightMembers);
            report.put("pending_completion_groups", inFlight.size());
            report.put("peak_completion_groups", peakCompletionGroups);
            report.put("peak_in_flight", peakInFlight);
            report.put("measurement", measurement());
            report.put("search_latency", searchLatency.describe());
            report.put("write_latency", writeLatency.describe());
            report.put("commit_latency", commitLatency.describe());
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
            baselineResponses.clear();
            inFlightMembers = 0;
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

    /**
     * The measured window: calls after the warmup, their local send rate and the rate responses
     * completed at, and the final drain. Latency histograms cover the same calls. There is no
     * rate schedule, so a target arrival rate does not exist (N/A).
     */
    private Map<String, Object> measurement() {
        var report = new LinkedHashMap<String, Object>();
        report.put("warmup_calls", Math.min(warmup, sent));
        long calls = Math.max(0, sent - warmup);
        report.put("measured_calls", calls);
        report.put("target_arrival_rate", "N/A");
        if (calls > 0) {
            double sending = (lastSend - measuredStart) / 1e9;
            double completing = (lastCompletion - measuredStart) / 1e9;
            report.put("send_seconds", sending);
            report.put("send_rate", sending > 0 ? calls / sending : 0.0);
            report.put("completion_seconds", completing);
            report.put(
                    "completion_rate",
                    completing > 0 ? (completed - completedAtMeasuredStart) / completing : 0.0);
        }
        report.put("first_send_to_last_completion_seconds", Math.max(0, lastCompletion - firstSend) / 1e9);
        report.put("drain_seconds", drainNanos / 1e9);
        return report;
    }

    /** A key's word in the order checksum: its checksum word, 0 for none. */
    private static int word(org.sibench.trace.IndexKey key) {
        return key == null ? 0 : key.checksumWord();
    }
}
