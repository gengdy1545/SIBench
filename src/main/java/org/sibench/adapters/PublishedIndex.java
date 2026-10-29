package org.sibench.adapters;

import org.sibench.adapters.proto.Commit;
import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
import org.sibench.adapters.proto.Result;
import org.sibench.adapters.proto.Search;
import org.sibench.adapters.proto.SearchGroup;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.Write;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/**
 * The complete-commit publication mode of the reference index service. Requests of one source
 * transaction execute in {@code Request.transaction_operation} order, while different
 * transactions run independently. A read runs once its {@code read_version} is published; a
 * version is published only after its Commit marker was received and every declared write was
 * applied. The highest version merely seen is never used as a watermark.
 *
 * <p>The watermark is recorded in the backend at every {@link #flush()} and resumed from it when
 * the index is reopened; versions above it were never published and stay invisible.
 *
 * <p>Requests never hold a thread while they wait: a transaction's next operation, or a read
 * whose version is not yet published, is parked and dispatched to a fixed worker pool when its
 * predecessor completes or its version is published. All ordering state is guarded by this
 * object's monitor, which is never held during a backend call or a response callback.
 */
public final class PublishedIndex implements AutoCloseable, IndexExecution.Executor {
    private final IndexBackend backend;
    private final ExecutorService workers;
    /**
     * Searches run apart from writes and commits: a search streaming a large result waits for the
     * client under flow control, which must not hold back the writes that publish versions.
     */
    private final ExecutorService readers;
    private final Map<Long, Lane> lanes = new HashMap<>();
    /** Committed source transactions, kept so a late request can still be refused. */
    private final BitSet completedTransactions = new BitSet();
    /** Requests not yet answered, failed on close. */
    private final Set<Task> open = ConcurrentHashMap.newKeySet();
    /** Actions waiting for a version to be published, by that version. */
    private final TreeMap<Long, List<Consumer<Effects>>> waiting = new TreeMap<>();
    private final TreeMap<Long, Commit> commits = new TreeMap<>();
    private final Map<Long, Long> applied = new HashMap<>();
    private final Map<Long, Long> active = new HashMap<>();
    private final Map<Long, Long> owners = new HashMap<>();
    private long published, nextVersion = 1;
    private boolean closed;

    public PublishedIndex(IndexBackend backend) throws IOException {
        this.backend = backend;
        this.published = backend.publishedVersion();
        this.nextVersion = published + 1;
        int processors = Runtime.getRuntime().availableProcessors();
        this.workers = pool(processors, "sibench-published");
        this.readers = pool(2 * processors, "sibench-published-search");
    }

    private static ExecutorService pool(int threads, String name) {
        return Executors.newFixedThreadPool(
                threads,
                runnable -> {
                    Thread thread = new Thread(runnable, name);
                    thread.setDaemon(true);
                    return thread;
                });
    }

    private static final class Task {
        final Request request;
        /** Full chunks of a large search result, sent before the final response. */
        final java.util.function.Consumer<Response> partial;
        final CompletableFuture<Response> response = new CompletableFuture<>();

        Task(Request request, java.util.function.Consumer<Response> partial) {
            this.request = request;
            this.partial = partial;
        }
    }

    /** Work found under the monitor and run after leaving it: responses and backend calls. */
    private final class Effects {
        private final List<Runnable> actions = new ArrayList<>();

        void respond(Task task, Response response) {
            actions.add(
                    () -> {
                        open.remove(task);
                        task.response.complete(response);
                    });
        }

        void dispatch(Request request, Runnable call) {
            var pool = request.getOperationCase() == Request.OperationCase.WRITE ? workers : readers;
            actions.add(
                    () -> {
                        try {
                            pool.execute(call);
                        } catch (RejectedExecutionException closing) {
                            // close() answers every open request.
                        }
                    });
        }

        void apply() {
            for (Runnable action : actions) action.run();
        }
    }

    /** Submits a request whose results, if any, fit one message. */
    public CompletableFuture<Response> submit(Request request) {
        return submit(request, chunk -> {
            throw new IllegalStateException("Unexpected partial response");
        });
    }

    @Override
    public CompletableFuture<Response> submit(
            Request request, java.util.function.Consumer<Response> partial) {
        // Baseline entries (version 0) need no ordering or publication; they load in parallel.
        if (request.getOperationCase() == Request.OperationCase.BATCH
                || request.getOperationCase() == Request.OperationCase.WRITE
                        && request.getWrite().getVersion() == 0) {
            try {
                return CompletableFuture.supplyAsync(
                        () -> IndexExecution.execute(backend, request), workers);
            } catch (RejectedExecutionException closing) {
                return CompletableFuture.completedFuture(closedResponse(request));
            }
        }
        var task = new Task(request, partial);
        open.add(task);
        var effects = new Effects();
        synchronized (this) {
            try {
                if (closed) throw new IOException("Published index is closed");
                accept(task, effects);
            } catch (IOException | RuntimeException error) {
                effects.respond(task, failure(request, error));
            }
        }
        effects.apply();
        return task.response;
    }

    private void accept(Task task, Effects effects) throws IOException {
        Request request = task.request;
        switch (request.getOperationCase()) {
            case WRITE -> {
                requireTransaction(request);
                lane(request.getTransactionId()).add(task, request.getWrite().getVersion(), effects);
            }
            case SEARCH -> {
                Search search = request.getSearch();
                if (search.getLocalVersion() == 0 && request.getTransactionId() == 0)
                    whenPublished(
                            search.getReadVersion(),
                            ready -> ready.dispatch(task.request, () -> run(null, task)),
                            effects);
                else {
                    requireTransaction(request);
                    lane(request.getTransactionId()).add(task, search.getLocalVersion(), effects);
                }
            }
            case GROUP -> {
                SearchGroup group = request.getGroup();
                if (group.getMembersCount() == 0) throw new IllegalArgumentException("Empty group");
                Search first = group.getMembers(0);
                if (first.getLocalVersion() == 0 && request.getTransactionId() == 0)
                    whenPublished(
                            first.getReadVersion(),
                            ready -> ready.dispatch(task.request, () -> run(null, task)),
                            effects);
                else {
                    requireTransaction(request);
                    lane(request.getTransactionId()).add(task, first.getLocalVersion(), effects);
                }
            }
            case COMMIT -> {
                requireTransaction(request);
                lane(request.getTransactionId()).commit(task, effects);
            }
            default -> throw new IllegalArgumentException("Request without an operation");
        }
    }

    /** Runs one ready request on a worker; a lane's request then advances its lane. */
    private void run(Lane lane, Task task) {
        Request request = task.request;
        List<Result> found = null;
        Throwable error = null;
        try {
            switch (request.getOperationCase()) {
                case WRITE -> backend.write(request.getWrite());
                case SEARCH ->
                        found = IndexExecution.search(backend, request, task.partial).getResultsList();
                case GROUP -> found = IndexExecution.group(backend, request.getGroup());
                default -> throw new IOException("Unsupported transaction lane operation");
            }
        } catch (IOException | RuntimeException failure) {
            error = failure;
        }
        var effects = new Effects();
        synchronized (this) {
            if (request.getOperationCase() == Request.OperationCase.WRITE) {
                long version = request.getWrite().getVersion();
                if (error != null) release(version);
                else {
                    try {
                        applied(version, effects);
                    } catch (IOException failure) {
                        error = failure;
                    }
                }
            }
            if (error == null) effects.respond(task, ok(request, found));
            else effects.respond(task, failure(request, error));
            if (lane != null) lane.completed(error, effects);
        }
        effects.apply();
    }

    private Lane lane(long transaction) throws IOException {
        if (transaction < 1 || transaction > Integer.MAX_VALUE - 8)
            throw new IOException("Transaction id is outside the supported range");
        if (completedTransactions.get((int) transaction))
            throw new IOException("Transaction already committed");
        return lanes.computeIfAbsent(transaction, Lane::new);
    }

    /** Lanes still holding an incomplete transaction; completed ones are removed after commit. */
    synchronized int laneCount() {
        return lanes.size();
    }

    private static void requireTransaction(Request request) {
        if (request.getTransactionId() == 0)
            throw new IllegalArgumentException("Complete-publication request needs a transaction id");
    }

    public synchronized long publishedVersion() {
        return published;
    }

    /** Records the current watermark and flushes; returns the watermark now durable. */
    public long flush() throws IOException {
        long version = publishedVersion();
        backend.recordPublishedVersion(version);
        backend.flush();
        return version;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) return;
            closed = true;
            waiting.clear();
        }
        for (Task task : List.copyOf(open)) {
            open.remove(task);
            task.response.complete(closedResponse(task.request));
        }
        workers.shutdownNow();
        readers.shutdownNow();
    }

    private static Response ok(Request request, List<Result> results) {
        var response = Response.newBuilder().setId(request.getId()).setStatus(Status.OK);
        if (results != null) response.addAllResults(results);
        return response.build();
    }

    private static Response failure(Request request, Throwable error) {
        var response = Response.newBuilder().setId(request.getId());
        if (error instanceof IllegalArgumentException)
            return response.setStatus(Status.INVALID)
                    .setMessage(String.valueOf(error.getMessage()))
                    .build();
        return response.setStatus(Status.UNAVAILABLE).setMessage(String.valueOf(error)).build();
    }

    private static Response closedResponse(Request request) {
        return failure(request, new IOException("Publication closed"));
    }

    /** One source transaction's ordered operations and its commit marker. */
    private final class Lane {
        private final long transaction;
        private final TreeMap<Long, Task> pending = new TreeMap<>();
        private long version;
        private long total = -1;
        private long declaredWrites = -1;
        /** The next operation to execute; every lower one has been applied. */
        private long next = 1;
        private boolean executing, parked, committing;
        private Task commit;
        private IOException failure;

        Lane(long transaction) {
            this.transaction = transaction;
        }

        void add(Task task, long requestVersion, Effects effects) throws IOException {
            if (failure != null) throw failure;
            if (requestVersion > 0) {
                if (version != 0 && version != requestVersion)
                    throw new IOException("One transaction spans versions");
            }
            long operation = task.request.getTransactionOperation();
            if (operation < 1) throw new IOException("Transaction operation must be positive");
            if (total >= 0 && operation > total)
                throw new IOException("Operation exceeds its committed transaction");
            if (operation < next || pending.containsKey(operation) || executing && operation == next)
                throw new IOException("Repeated transaction operation");
            if (requestVersion > 0) version = requestVersion;
            pending.put(operation, task);
            pump(effects);
        }

        void commit(Task task, Effects effects) throws IOException {
            if (failure != null) throw failure;
            if (commit != null) throw new IOException("Repeated commit marker for the transaction");
            Commit marker = task.request.getCommit();
            if (version != 0 && version != marker.getVersion())
                throw new IOException("Transaction version differs from its writes");
            if (marker.getOperations() < next - 1)
                throw new IOException("Commit has fewer operations than already applied");
            if (marker.getOperations() < marker.getWrites())
                throw new IOException("More writes than operations in a transaction");
            if (!pending.isEmpty() && pending.lastKey() > marker.getOperations())
                throw new IOException("Operation beyond the committed transaction count");
            version = marker.getVersion();
            total = marker.getOperations();
            declaredWrites = marker.getWrites();
            commit = task;
            pump(effects);
        }

        /** Dispatches the next operation if it is present and its read version is published. */
        private void pump(Effects effects) {
            if (executing || parked || failure != null || closed) return;
            Task head = pending.get(next);
            if (head == null) {
                if (commit != null && !committing && next - 1 >= total) publish(effects);
                return;
            }
            long readVersion = readVersion(head.request);
            if (readVersion > published) {
                parked = true;
                whenPublished(
                        readVersion,
                        ready -> {
                            parked = false;
                            pump(ready);
                        },
                        effects);
                return;
            }
            pending.remove(next);
            if (head.request.getOperationCase() == Request.OperationCase.WRITE) {
                try {
                    reserve(head.request.getWrite().getVersion(), transaction);
                } catch (IOException refused) {
                    effects.respond(head, failure(head.request, refused));
                    fail(refused, effects);
                    return;
                }
            }
            executing = true;
            effects.dispatch(head.request, () -> run(this, head));
        }

        void completed(Throwable error, Effects effects) {
            executing = false;
            if (error != null) {
                fail(
                        error instanceof IOException io
                                ? io
                                : new IOException("Transaction lane failed", error),
                        effects);
                return;
            }
            next++;
            pump(effects);
        }

        /** Every operation is applied: register the marker and answer it once published. */
        private void publish(Effects effects) {
            committing = true;
            try {
                registerCommit(version, transaction, declaredWrites, total);
            } catch (IOException refused) {
                effects.respond(commit, failure(commit.request, refused));
                return;
            }
            whenPublished(
                    version,
                    ready -> {
                        completedTransactions.set((int) transaction);
                        lanes.remove(transaction);
                        ready.respond(commit, ok(commit.request, null));
                    },
                    effects);
            publishReady(effects);
        }

        private void fail(IOException error, Effects effects) {
            failure = error;
            for (Task task : pending.values()) effects.respond(task, failure(task.request, error));
            pending.clear();
            if (commit != null && !committing) effects.respond(commit, failure(commit.request, error));
        }
    }

    private static long readVersion(Request request) {
        return switch (request.getOperationCase()) {
            case SEARCH -> request.getSearch().getReadVersion();
            case GROUP -> request.getGroup().getMembers(0).getReadVersion();
            default -> 0;
        };
    }

    private void whenPublished(long version, Consumer<Effects> action, Effects effects) {
        if (published >= version) action.accept(effects);
        else waiting.computeIfAbsent(version, ignored -> new ArrayList<>()).add(action);
    }

    // ---- publication: commit markers, write ownership and applied-write counts ----

    private void registerCommit(long version, long transaction, long writes, long operations)
            throws IOException {
        if (version < 1) throw new IOException("Commit version must be positive");
        if (commits.containsKey(version))
            throw new IOException("Repeated commit marker for version " + version);
        if (version < nextVersion)
            throw new IOException("Commit marker for an already published version");
        Long owner = owners.putIfAbsent(version, transaction);
        if (owner != null && owner != transaction)
            throw new IOException("Commit marker changes the version's transaction");
        long used = applied.getOrDefault(version, 0L) + active.getOrDefault(version, 0L);
        if (used > writes) throw new IOException("Commit declares fewer writes than were accepted");
        commits.put(
                version,
                Commit.newBuilder()
                        .setVersion(version)
                        .setWrites(writes)
                        .setOperations(operations)
                        .build());
    }

    /**
     * Reserves a write slot before touching the backend. A write for an already published
     * version, another transaction's version, or a version whose declared write count is already
     * covered is refused before it can mutate state.
     */
    private void reserve(long version, long transaction) throws IOException {
        if (version < 1) throw new IOException("Write without a version");
        if (version <= published) throw new IOException("Write for an already published version");
        Long owner = owners.putIfAbsent(version, transaction);
        if (owner != null && owner != transaction)
            throw new IOException("Write belongs to another transaction's version");
        Commit commit = commits.get(version);
        if (commit != null
                && applied.getOrDefault(version, 0L) + active.getOrDefault(version, 0L)
                        >= commit.getWrites())
            throw new IOException("Write exceeds the committed write count");
        active.merge(version, 1L, Long::sum);
    }

    private void applied(long version, Effects effects) throws IOException {
        if (version <= published)
            throw new IOException("Write applied after its version was published");
        long activeWrites = active.getOrDefault(version, 0L);
        if (activeWrites <= 0) throw new IOException("Write was not reserved");
        if (activeWrites == 1) active.remove(version);
        else active.put(version, activeWrites - 1);
        applied.merge(version, 1L, Long::sum);
        publishReady(effects);
    }

    private void release(long version) {
        long activeWrites = active.getOrDefault(version, 0L);
        if (activeWrites <= 1) active.remove(version);
        else active.put(version, activeWrites - 1);
    }

    /** Advances the watermark over every complete version and wakes what waited for it. */
    private void publishReady(Effects effects) {
        boolean advanced = false;
        while (active.getOrDefault(nextVersion, 0L) == 0) {
            Commit commit = commits.get(nextVersion);
            if (commit == null) break;
            long writes = applied.getOrDefault(nextVersion, 0L);
            // A count above the declaration is refused by reserve(); equality publishes.
            if (writes < commit.getWrites()) break;
            published = nextVersion;
            commits.remove(nextVersion);
            applied.remove(nextVersion);
            owners.remove(nextVersion);
            nextVersion++;
            advanced = true;
        }
        if (!advanced) return;
        while (!waiting.isEmpty() && waiting.firstKey() <= published)
            for (Consumer<Effects> action : waiting.pollFirstEntry().getValue())
                action.accept(effects);
    }
}
