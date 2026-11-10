package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongSupplier;

/**
 * One sender, deterministic work-unit routing, and bounded independently completed responses.
 * A snapshot search whose older writes are still unacknowledged is deferred, not sent, so later
 * writes keep flowing; deferred searches leave in arrival order once those writes complete.
 */
public final class ReplayDispatcher implements AutoCloseable {
    private record Deferred(Operation operation, org.sibench.adapters.proto.Request stored, long since, boolean measured) {}

    private static final class Pending {
        private final long deadline, started;
        private final CompletableFuture<Void> response;
        private final int clientId;
        private final Operation operation;
        private int members = 1;
        private int warmupMembers;
        private long[] memberStarts;
        private boolean failed;
        private long businessCompleted = -1;

        Pending(long deadline, CompletableFuture<Void> response,
                int clientId, Operation operation, long started, boolean measured) {
            this.deadline = deadline;
            this.response = response;
            this.clientId = clientId;
            this.operation = operation;
            this.started = started;
            this.warmupMembers = measured ? 0 : 1;
        }

        long deadline() { return deadline; }
        CompletableFuture<Void> response() { return response; }

        // The sender and response callback hold inFlight while adding or completing members.
        void addMember(long start, boolean measured) {
            if (memberStarts == null) {
                memberStarts = new long[8];
                memberStarts[0] = started;
            }
            if (members == memberStarts.length)
                memberStarts = Arrays.copyOf(memberStarts, members * 2);
            memberStarts[members++] = start;
            if (!measured) warmupMembers++;
        }

        void recordLatency(Latency latency, long completed) {
            if (memberStarts == null) {
                if (warmupMembers == 0) latency.add(completed - started);
            } else for (int i = warmupMembers; i < members; i++) latency.add(completed - memberStarts[i]);
        }

        String description() {
            return "client " + clientId + ", source event " + operation.sourceEvent()
                    + ", " + IndexOperationTrace.kindName(operation.kind())
                    + ", transaction " + operation.transactionId() + ", version " + operation.version();
        }
    }

    /** Exact samples below 2048 ns; larger buckets have less than 1/1024 relative width. */
    static final class Latency {
        private final long[] buckets = new long[54 * 1024];
        private long count, totalNanos, maximumNanos;
        private BigInteger overflowTotal;

        void add(long nanos) {
            if (nanos < 0) throw new IllegalArgumentException("Negative response latency");
            count++;
            if (overflowTotal != null) overflowTotal = overflowTotal.add(BigInteger.valueOf(nanos));
            else if (Long.MAX_VALUE - totalNanos < nanos)
                overflowTotal = BigInteger.valueOf(totalNanos).add(BigInteger.valueOf(nanos));
            else totalNanos += nanos;
            maximumNanos = Math.max(maximumNanos, nanos);
            int shift = Math.max(0, 63 - Long.numberOfLeadingZeros(nanos) - 10);
            buckets[(shift << 10) + (int) (nanos >>> shift)]++;
        }

        Map<String, Object> describe() {
            var report = new LinkedHashMap<String, Object>();
            report.put("count", count);
            double total = overflowTotal == null ? totalNanos : overflowTotal.doubleValue();
            report.put("mean_ms", count == 0 ? 0.0 : total / 1e6 / count);
            report.put("p50_ms", percentile(50));
            report.put("p95_ms", percentile(95));
            report.put("p99_ms", percentile(99));
            report.put("max_ms", maximumNanos / 1e6);
            report.put("percentile_bound", "upper");
            report.put("percentile_relative_error_bound", 1.0 / 1024);
            report.put("exact_below_ns", 2048);
            report.put("mean_and_max", "unquantized");
            return report;
        }

        /** The bucket at or above the requested share; bucket values are upper bounds. */
        private double percentile(int percent) {
            if (count == 0) return 0;
            long target = Math.max(1, (count * percent + 99) / 100);
            long seen = 0;
            for (int bucket = 0; bucket < buckets.length; bucket++) {
                seen += buckets[bucket];
                if (seen >= target) {
                    int shift = Math.max(0, (bucket >>> 10) - 1);
                    long upper = bucket < 2048 ? bucket
                            : ((long) (1024 + (bucket & 1023)) << shift) | ((1L << shift) - 1);
                    return Math.min(upper, maximumNanos) / 1e6;
                }
            }
            throw new IllegalStateException("Latency histogram count mismatch");
        }
    }

    private final ReplayConfig config;
    private final ReplayTarget.Factory factory;
    private final LongSupplier nanoTime;
    /** Basic operations sent before the measured window starts. */
    private final long warmup;
    private long firstSend, measuredStart, lastSend, lastCompletion, measuredLastCompletion, measuredCompleted, drainNanos;
    private long admitted, measuredLastSend;
    private boolean measurementStarted;
    private final Map<Integer, ReplayTarget> clients = new LinkedHashMap<>();
    private final BitSet workUnits = new BitSet();
    private final Map<Integer, long[]> counts = new TreeMap<>();
    private final Map<Long, Pending> inFlight = new LinkedHashMap<>();
    private final Map<CompletableFuture<Void>, Pending> baselineResponses = new IdentityHashMap<>();
    private final Latency writeLatency = new Latency(), searchLatency = new Latency();
    private long sent, operationsSent, completed, completedOperations, workUnitCount, peakInFlight,
            orderChecksum, backpressureNanos, dependencyWaitNanos, deferredNanos, deferredOperations,
            peakDeferred;
    private final ArrayDeque<Deferred> deferred = new ArrayDeque<>();
    /** Members still expected for the deferred group at the queue tail, or for a sent group. */
    private int deferringGroup, directGroup;
    private long deferredReadLimit = -1;
    private long inFlightMembers, peakCompletionGroups;
    private int pendingWriteResponses;
    private long failedOperations, timedOutOperations;
    private IOException failure;
    private CompletableFuture<Void> localSend;
    private volatile boolean closed;
    private Thread owner;

    public ReplayDispatcher(ReplayConfig config, ReplayTarget.Factory factory) {
        this(config, factory, 0);
    }

    /** {@code warmup}: basic operations before the measured window, 0 to measure from the start. */
    public ReplayDispatcher(ReplayConfig config, ReplayTarget.Factory factory, long warmup) {
        this(config, factory, warmup, System::nanoTime);
    }

    ReplayDispatcher(ReplayConfig config, ReplayTarget.Factory factory, long warmup, LongSupplier nanoTime) {
        this.config = config;
        this.factory = Objects.requireNonNull(factory);
        this.warmup = warmup;
        this.nanoTime = Objects.requireNonNull(nanoTime);
    }

    /**
     * Before a call is handed to its client: the first send, and the measured window's start
     * before its first call, whose response may complete within the send itself.
     */
    private void beginCall(long now, boolean measured) {
        if (operationsSent == 0) firstSend = now;
        if (measured && !measurementStarted)
            synchronized (inFlight) {
                measuredStart = now;
                measurementStarted = true;
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
                && inFlight.values().iterator().next().deadline() <= nanoTime.getAsLong()) {
            var oldest = inFlight.entrySet().iterator().next();
            recordResponseFailure(oldest.getValue(), true);
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

    private void recordResponseFailure(Pending pending, boolean timeout) {
        if (pending.failed) return;
        pending.failed = true;
        if (timeout) timedOutOperations += pending.members;
        else failedOperations += pending.members;
    }

    public void send(Operation operation) throws IOException {
        send(operation, null);
    }

    public void send(Operation operation, org.sibench.adapters.proto.Request stored) throws IOException {
        checkSender();
        route(operation);
        boolean measured = admitted++ >= warmup;
        boolean member = operation.grouped() && operation.expansionPosition() > 0;
        if (operation.kind() == IndexOperationTrace.SEARCH && member) {
            if (deferringGroup > 0) defer(operation, stored, false, measured);
            else {
                directGroup--;
                dispatch(operation, stored, measured);
            }
            return;
        }
        if (directGroup != 0 || deferringGroup != 0) throw new IOException("Incomplete search group");
        releaseDeferred();
        if (operation.kind() == IndexOperationTrace.SEARCH) {
            if (needsDeferral(operation)) defer(operation, stored, true, measured);
            else {
                directGroup = operation.grouped() ? operation.expansionSize() - 1 : 0;
                dispatch(operation, stored, measured);
            }
            return;
        }
        // A deferred snapshot at or after this version precedes it in source order.
        if (operation.version() <= deferredReadLimit)
            awaitDeferred(() -> deferred.stream().noneMatch(d -> d.operation().readTimestamp() >= operation.version()));
        dispatch(operation, stored, measured);
    }

    private boolean needsDeferral(Operation operation) {
        synchronized (inFlight) {
            return hasPendingWrites(operation.readTimestamp());
        }
    }

    private void defer(Operation operation, org.sibench.adapters.proto.Request stored, boolean head, boolean measured)
            throws IOException {
        if (head) {
            int size = operation.grouped() ? operation.expansionSize() : 1;
            if (size > config.maxInFlight())
                synchronized (inFlight) {
                    throw fail(new IOException("Search group has " + size
                            + " members, exceeding max_in_flight=" + config.maxInFlight()));
                }
            // Deferred members are bounded like in-flight ones; the sender waits for room.
            if (deferred.size() + size > config.maxInFlight())
                awaitDeferred(() -> deferred.size() + size <= config.maxInFlight());
            // Held writes, such as a partial batch, must leave before a deferred search can.
            if (deferred.isEmpty()) {
                flushClients();
                synchronized (inFlight) {
                    checkFailure();
                }
            }
            deferringGroup = size;
            deferredReadLimit = Math.max(deferredReadLimit, operation.readTimestamp());
        }
        deferred.addLast(new Deferred(operation, stored, nanoTime.getAsLong(), measured));
        deferringGroup--;
        peakDeferred = Math.max(peakDeferred, deferred.size());
    }

    /** Sends complete deferred groups from the queue head whose required writes are acknowledged. */
    private void releaseDeferred() throws IOException {
        while (!deferred.isEmpty()) {
            var first = deferred.peekFirst().operation();
            int size = first.grouped() ? first.expansionSize() : 1;
            if (deferred.size() < size || needsDeferral(first)) return;
            long now = nanoTime.getAsLong();
            for (int i = 0; i < size; i++) {
                var item = deferred.pollFirst();
                deferredNanos += now - item.since();
                deferredOperations++;
                dispatch(item.operation(), item.stored(), item.measured());
            }
        }
        deferredReadLimit = -1;
    }

    /** Blocks the sender, releasing deferred searches as writes complete, until {@code done}. */
    private void awaitDeferred(java.util.function.BooleanSupplier done) throws IOException {
        if (deferred.isEmpty()) return;
        long start = nanoTime.getAsLong();
        flushClients();
        dependencyWaitNanos += nanoTime.getAsLong() - start;
        while (true) {
            releaseDeferred();
            if (deferred.isEmpty() || done.getAsBoolean()) return;
            synchronized (inFlight) {
                checkFailure();
                // A complete head group whose writes finished is released by the next pass.
                if (!needsDeferral(deferred.peekFirst().operation())) continue;
                start = nanoTime.getAsLong();
                waitForResponse();
                dependencyWaitNanos += nanoTime.getAsLong() - start;
            }
        }
    }

    private void dispatch(Operation operation, org.sibench.adapters.proto.Request stored, boolean measured) throws IOException {
        int clientId = route(operation);
        synchronized (inFlight) {
            // A buffered search group cannot produce responses until its last member arrives.
            // Projection delivers complete groups contiguously on this single sender.
            int admissionSize = operation.grouped() && operation.expansionPosition() == 0
                    ? operation.expansionSize() : 1;
            if (admissionSize > config.maxInFlight())
                throw fail(new IOException("Search group has " + admissionSize
                        + " members, exceeding max_in_flight=" + config.maxInFlight()));
            long wait = inFlightMembers > config.maxInFlight() - admissionSize ? nanoTime.getAsLong() : 0;
            while (inFlightMembers > config.maxInFlight() - admissionSize) {
                checkFailure();
                waitForResponse();
            }
            if (wait != 0) backpressureNanos += nanoTime.getAsLong() - wait;
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
                    nanoTime.getAsLong() + TimeUnit.SECONDS.toNanos(config.requestTimeoutSeconds());
            long started = nanoTime.getAsLong();
            beginCall(started, measured);
            ReplayTarget.SendHandle handle =
                    Objects.requireNonNull(client.send(sequence, operation, stored));
            localSend = handle.sent().toCompletableFuture();
            CompletableFuture<Void> response = handle.response().toCompletableFuture();
            int latencyKind = operation.kind() == IndexOperationTrace.SEARCH ? 1 : 0;
            boolean baseline = operation.kind() == IndexOperationTrace.INSERT
                    && operation.version() == 0 && operation.transactionId() == 0;
            Pending added = null;
            synchronized (inFlight) {
                Pending shared = baseline ? baselineResponses.get(response) : null;
                if (shared == null) {
                    added = new Pending(deadline, response, clientId, operation, started, measured);
                    inFlight.put(sequence, added);
                    if (operation.kind() != IndexOperationTrace.SEARCH) pendingWriteResponses++;
                    if (baseline) baselineResponses.put(response, added);
                } else shared.addMember(started, measured);
                inFlightMembers++;
                peakInFlight = Math.max(peakInFlight, inFlightMembers);
                peakCompletionGroups = Math.max(peakCompletionGroups, inFlight.size());
            }
            Pending pending = added;
            CompletableFuture<Void> validated = response;
            if (pending != null && handle.businessResponse() != handle.response()) {
                var timed = handle.businessResponse().handle((unused, error) -> {
                    synchronized (inFlight) { pending.businessCompleted = nanoTime.getAsLong(); }
                    return null;
                });
                validated = response.thenCombine(timed, (unused, time) -> null);
            }
            if (pending != null) validated.whenComplete(
                    (unused, error) -> {
                        synchronized (inFlight) {
                            Pending removed = inFlight.remove(sequence);
                            if (removed != null) inFlightMembers -= removed.members;
                            if (removed != null && operation.kind() != IndexOperationTrace.SEARCH) pendingWriteResponses--;
                            baselineResponses.remove(response, pending);
                            if (error != null && !closed) {
                                recordResponseFailure(pending, false);
                                fail(
                                        new IOException(
                                                "Service response failed at dispatch "
                                                        + sequence
                                                        + ", client "
                                                        + clientId,
                                                error));
                            }
                            else if (error == null && !closed) {
                                if (pending.businessCompleted < 0) pending.businessCompleted = nanoTime.getAsLong();
                                if (deadline <= nanoTime.getAsLong()) {
                                    recordResponseFailure(pending, true);
                                    fail(
                                            new IOException(
                                                    "Service response timeout at dispatch "
                                                            + sequence
                                                            + ", client "
                                                            + clientId));
                                }
                                else {
                                    completed += pending.members;
                                    completedOperations += pending.members;
                                    measuredCompleted += pending.members - pending.warmupMembers;
                                    lastCompletion = nanoTime.getAsLong();
                                    if (pending.members > pending.warmupMembers) measuredLastCompletion = lastCompletion;
                                    pending.recordLatency(latencyKind == 1 ? searchLatency : writeLatency,
                                            pending.businessCompleted);
                                }
                            }
                            inFlight.notifyAll();
                        }
                    });
            long remaining = deadline - nanoTime.getAsLong();
            if (remaining <= 0) throw new TimeoutException("Local send timeout");
            localSend.get(remaining, TimeUnit.NANOSECONDS);
            localSend = null;
            sent++;
            operationsSent++;
            lastSend = nanoTime.getAsLong();
            if (measured) measuredLastSend = lastSend;
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

    // Snapshot reads depend on acknowledged writes at or before their read timestamp.
    // Called only at query boundaries under inFlight; bounded by max_in_flight.
    private boolean hasPendingWrites(long readTimestamp) {
        if (pendingWriteResponses == 0) return false;
        for (Pending pending : inFlight.values())
            if (pending.operation.kind() != IndexOperationTrace.SEARCH
                    && pending.operation.version() <= readTimestamp) return true;
        return false;
    }

    private void flushClients() throws IOException {
        try {
            for (ReplayTarget client : clients.values()) client.flush();
        } catch (IOException | RuntimeException error) {
            synchronized (inFlight) {
                throw fail(error instanceof IOException io ? io : new IOException("Client flush failed", error));
            }
        }
    }

    private void waitForResponse() throws IOException {
        long nanos = inFlight.values().iterator().next().deadline() - nanoTime.getAsLong();
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
        if (directGroup != 0 || deferringGroup != 0) throw new IOException("Incomplete search group");
        awaitDeferred(() -> false);
        long start = nanoTime.getAsLong();
        flushClients();
        synchronized (inFlight) {
            while (!inFlight.isEmpty()) {
                checkFailure();
                waitForResponse();
            }
            checkFailure();
        }
        drainNanos += nanoTime.getAsLong() - start;
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
        report.put("basic_operations", operationsSent);
        report.put("operation_order_checksum", orderChecksum);
        report.put("local_send_boundary", "transport_acceptance_not_service_execution");
        report.put("service_visibility_owner", "index_service");
        report.put("backpressure_seconds", backpressureNanos / 1e9);
        report.put("dependency_wait_seconds", dependencyWaitNanos / 1e9);
        report.put("deferred_search_operations", deferredOperations);
        report.put("search_deferral_seconds", deferredNanos / 1e9);
        report.put("peak_deferred_operations", peakDeferred);
        report.put("latency_boundary", "client_api_entry_to_final_response_excluding_replay_waits");
        synchronized (inFlight) {
            report.put("completed_responses", completed);
            report.put("completed_operations", completedOperations);
            report.put("failed_response_operations", failedOperations);
            report.put("timed_out_operations", timedOutOperations);
            if (failure != null) report.put("failure", failure.getMessage());
            report.put("pending_responses", inFlightMembers);
            report.put("pending_completion_groups", inFlight.size());
            report.put("peak_completion_groups", peakCompletionGroups);
            report.put("peak_in_flight", peakInFlight);
            report.put("measurement", measurement());
            report.put("search_latency", searchLatency.describe());
            report.put("write_latency", writeLatency.describe());
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
            deferred.clear();
            baselineResponses.clear();
            inFlightMembers = 0;
            pendingWriteResponses = 0;
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
     * The measured window: basic operations after warmup, their local send rate and response
     * completion rate, and the final drain. There is no
     * rate schedule, so a target arrival rate does not exist (N/A).
     */
    private Map<String, Object> measurement() {
        var report = new LinkedHashMap<String, Object>();
        report.put("unit", "basic_index_operation");
        report.put("warmup_operations", Math.min(warmup, operationsSent));
        long calls = Math.max(0, operationsSent - warmup);
        report.put("measured_operations", calls);
        report.put("completed_operations", measuredCompleted);
        report.put("target_arrival_rate", "N/A");
        if (calls > 0) {
            double sending = (measuredLastSend - measuredStart) / 1e9;
            double completing = measuredLastCompletion == 0 ? 0 : (measuredLastCompletion - measuredStart) / 1e9;
            report.put("send_seconds", sending);
            report.put("send_rate", sending > 0 ? calls / sending : 0.0);
            report.put("completion_seconds", completing);
            report.put(
                    "completion_rate",
                    completing > 0 ? measuredCompleted / completing : 0.0);
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
