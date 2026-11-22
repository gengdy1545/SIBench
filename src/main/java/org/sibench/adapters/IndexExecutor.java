package org.sibench.adapters;

import com.google.protobuf.Message;
import org.sibench.adapters.proto.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Concurrent index calls; a successful response follows visible application, without transactions. */
public final class IndexExecutor implements AutoCloseable {
    private final IndexBackend backend;
    private final ServiceConfig.Execution execution;
    private final java.io.BufferedWriter statistics;
    private final ExecutorService writers, readers;
    private final java.util.concurrent.ScheduledExecutorService maintenance;
    private long changes, persisted;
    private IOException maintenanceFailure;
    private final Set<CompletableFuture<IndexResponse>> pending = new HashSet<>();
    private boolean closed;
    private int activeCalls;
    // Queue: submit to pool-thread start; execute: pool-thread start to backend completion; flush: explicit persistence calls.
    private final CallTimes readQueue = new CallTimes(), readExecute = new CallTimes(),
            writeQueue = new CallTimes(), writeExecute = new CallTimes(), flushCalls = new CallTimes();

    public IndexExecutor(IndexBackend backend, ServiceConfig.Execution execution) throws IOException {
        this.backend = backend;
        this.execution = execution;
        var path = execution.statistics().toAbsolutePath();
        java.nio.file.Files.createDirectories(path.getParent());
        statistics = java.nio.file.Files.newBufferedWriter(path, java.nio.file.StandardOpenOption.CREATE_NEW,
                java.nio.file.StandardOpenOption.WRITE);
        writers = pool(execution.writeThreads(), "sibench-write");
        readers = pool(execution.readThreads(), "sibench-read");
        maintenance = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(work -> {
            var thread = new Thread(work, "sibench-persistence"); thread.setDaemon(true); return thread;
        });
        maintenance.scheduleWithFixedDelay(this::persist, 5, 5, TimeUnit.SECONDS);
    }

    private static ExecutorService pool(int count, String name) {
        return Executors.newFixedThreadPool(count, work -> {
            var thread = new Thread(work, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    public CompletableFuture<IndexResponse> submit(Message request) {
        return submit(request, chunk -> { throw new IllegalStateException("Unexpected partial response"); });
    }

    public CompletableFuture<IndexResponse> submit(Message request, Consumer<IndexResponse> partial) {
        var response = new CompletableFuture<IndexResponse>();
        synchronized (this) {
            if (closed) return CompletableFuture.completedFuture(unavailable("Index executor is closed"));
            if (maintenanceFailure != null) return CompletableFuture.completedFuture(unavailable(maintenanceFailure.toString()));
            pending.add(response);
            boolean write = request instanceof PutRequest || request instanceof DeleteRequest || request instanceof WriteBatchRequest;
            var pool = write ? writers : readers;
            long submitted = System.nanoTime();
            // Submission and shutdown share this monitor, so an accepted task cannot be lost.
            pool.execute(() -> {
                synchronized (IndexExecutor.this) { activeCalls++; }
                long started = System.nanoTime();
                (write ? writeQueue : readQueue).record(started - submitted, true);
                IndexExecution.executeAsync(backend, request, partial).thenAccept(result -> {
                    (write ? writeExecute : readExecute).record(System.nanoTime() - started, result.status() == IndexStatus.OK);
                    synchronized (IndexExecutor.this) {
                        // A failed write may be partially applied; only validation rejections leave state untouched.
                        if (write && result.status() != IndexStatus.INVALID) changes++;
                    }
                    response.complete(result);
                    synchronized (IndexExecutor.this) {
                        pending.remove(response); activeCalls--; IndexExecutor.this.notifyAll();
                    }
                });
            });
        }
        return response;
    }

    /** Background persistence on the single maintenance thread; it requires no client barrier. Samples statistics every cycle. */
    private void persist() {
        try {
            long through;
            synchronized (this) {
                if (maintenanceFailure != null) return;
                through = changes;
            }
            if (through != persisted) {
                long start = System.nanoTime();
                boolean success = false;
                try { backend.flush(); success = true; }
                finally { flushCalls.record(System.nanoTime() - start, success); }
                synchronized (this) { persisted = through; }
            }
        } catch (IOException | RuntimeException error) {
            synchronized (this) { maintenanceFailure = new IOException("Background persistence failed", error); }
        }
        try {
            var service = new java.util.LinkedHashMap<String, Object>();
            for (var entry : java.util.Map.of("read_queue", readQueue, "read_execute", readExecute, "write_queue", writeQueue,
                    "write_execute", writeExecute, "flush_call", flushCalls).entrySet())
                service.put(entry.getKey(), CallTimes.describe(entry.getValue().snapshot()));
            var line = new java.util.LinkedHashMap<String, Object>();
            line.put("time_millis", System.currentTimeMillis());
            line.put("backend", backend.describe());
            line.put("execution", java.util.Map.of("read_threads", execution.readThreads(), "write_threads", execution.writeThreads()));
            line.put("service", service);
            line.put("engine", backend.statistics());
            synchronized (this) { if (maintenanceFailure != null) line.put("error", maintenanceFailure.toString()); }
            statistics.write(new com.google.gson.Gson().toJson(line));
            statistics.newLine(); statistics.flush();
        } catch (IOException | RuntimeException error) {
            synchronized (this) {
                if (maintenanceFailure == null) maintenanceFailure = new IOException("Statistics output failed", error);
                else maintenanceFailure.addSuppressed(error);
            }
        }
    }

    synchronized void requireHealthy() throws IOException {
        if (closed) throw new IOException("Index executor is closed");
        if (maintenanceFailure != null) throw maintenanceFailure;
    }

    /** Backend handles may be closed only after workers and active persistence have exited. */
    @Override
    public void close() throws IOException {
        Set<CompletableFuture<IndexResponse>> accepted;
        synchronized (this) {
            closed = true;
            accepted = Set.copyOf(pending);
            pending.clear();
            writers.shutdownNow();
            readers.shutdownNow();
        }
        accepted.forEach(response -> response.complete(unavailable("Index executor closed")));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try {
            for (var pool : List.of(writers, readers))
                if (!pool.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                    throw new IOException("Index shutdown timed out; backend must remain open");
            synchronized (this) {
                while (activeCalls != 0) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) throw new IOException("Index shutdown timed out; backend must remain open");
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                }
                if (!maintenance.isShutdown()) { maintenance.execute(this::persist); maintenance.shutdown(); }
            }
            if (!maintenance.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new IOException("Persistence shutdown timed out; backend must remain open");
            synchronized (this) {
                try { statistics.close(); }
                catch (IOException error) {
                    if (maintenanceFailure == null) throw error;
                    maintenanceFailure.addSuppressed(error);
                }
                if (maintenanceFailure != null) throw maintenanceFailure;
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("Index shutdown interrupted; backend must remain open", error);
        }
    }

    private static IndexResponse unavailable(String message) {
        return IndexResponse.error(IndexStatus.UNAVAILABLE, message);
    }
}
