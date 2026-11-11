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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/** Concurrent index calls; a successful response follows visible application, without transactions. */
public final class IndexExecutor implements AutoCloseable, IndexExecution.Executor {
    static final long FLUSH_TIMEOUT_SECONDS = 3600;
    private final IndexBackend backend;
    private final ExecutorService writers, readers;
    private final java.util.concurrent.ScheduledExecutorService maintenance;
    private long changes, persisted;
    private IOException maintenanceFailure;
    private final Set<CompletableFuture<IndexResponse>> pending = new HashSet<>();
    private final ReentrantLock flushLock = new ReentrantLock();
    private boolean closed;
    private IOException failure;

    public IndexExecutor(IndexBackend backend) {
        this(backend, 5000);
    }

    IndexExecutor(IndexBackend backend, long intervalMillis) {
        this.backend = backend;
        int processors = Runtime.getRuntime().availableProcessors();
        writers = pool(processors, "sibench-write");
        readers = pool(2 * processors, "sibench-read");
        if (intervalMillis < 1) throw new IllegalArgumentException("Invalid maintenance interval");
        maintenance = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(work -> {
            var thread = new Thread(work, "sibench-persistence"); thread.setDaemon(true); return thread;
        });
        maintenance.scheduleWithFixedDelay(this::persist, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
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

    @Override
    public CompletableFuture<IndexResponse> submit(Message request, Consumer<IndexResponse> partial) {
        var response = new CompletableFuture<IndexResponse>();
        synchronized (this) {
            if (closed) return CompletableFuture.completedFuture(unavailable("Index executor is closed"));
            if (maintenanceFailure != null) return CompletableFuture.completedFuture(unavailable(maintenanceFailure.toString()));
            pending.add(response);
            boolean write = request instanceof PutRequest || request instanceof DeleteRequest || request instanceof WriteBatchRequest;
            var pool = write ? writers : readers;
            // Submission and shutdown share this monitor, so an accepted task cannot be lost.
            pool.execute(() -> {
                IndexResponse result = IndexExecution.execute(backend, request, partial);
                synchronized (IndexExecutor.this) {
                    if (result.status() != IndexStatus.OK && failure == null)
                        failure = new IOException("Accepted request failed: " + result.message());
                    if (result.status() == IndexStatus.OK && write) changes++;
                }
                response.complete(result);
                synchronized (IndexExecutor.this) { pending.remove(response); }
            });
        }
        return response;
    }

    /** Background persistence does not wait for unrelated queries or require a client barrier. */
    private void persist() {
        flushLock.lock();
        try {
            long through;
            synchronized (this) {
                if (maintenanceFailure != null || changes == persisted) return;
                through = changes;
            }
            backend.flush();
            synchronized (this) { persisted = through; }
        } catch (IOException | RuntimeException error) {
            synchronized (this) { maintenanceFailure = new IOException("Background persistence failed", error); }
        } finally { flushLock.unlock(); }
    }

    synchronized void requireHealthy() throws IOException {
        if (closed) throw new IOException("Index executor is closed");
        if (maintenanceFailure != null) throw maintenanceFailure;
    }

    /** Drains calls already accepted, then persists them at the backend's declared durability. */
    public void flush() throws IOException {
        flushLock.lock();
        try {
            List<CompletableFuture<IndexResponse>> accepted;
            synchronized (this) {
                if (closed) throw new IOException("Index executor is closed");
                accepted = List.copyOf(pending);
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(FLUSH_TIMEOUT_SECONDS);
            for (var response : accepted) {
                try {
                    response.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Flush interrupted", error);
                } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException error) {
                    throw new IOException("Flush could not complete accepted requests", error);
                }
            }
            synchronized (this) {
                if (closed) throw new IOException("Index executor is closed");
                if (failure != null) throw failure;
                if (maintenanceFailure != null) throw maintenanceFailure;
            }
            backend.flush();
        } finally { flushLock.unlock(); }
    }

    /** Backend handles may be closed only after workers and an active Flush have exited. */
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
                if (!maintenance.isShutdown()) { maintenance.execute(this::persist); maintenance.shutdown(); }
            }
            if (!maintenance.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new IOException("Persistence shutdown timed out; backend must remain open");
            if (!flushLock.tryLock(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new IOException("Flush shutdown timed out; backend must remain open");
            flushLock.unlock();
            synchronized (this) { if (maintenanceFailure != null) throw maintenanceFailure; }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("Index shutdown interrupted; backend must remain open", error);
        }
    }

    private static IndexResponse unavailable(String message) {
        return IndexResponse.error(IndexStatus.UNAVAILABLE, message);
    }
}
