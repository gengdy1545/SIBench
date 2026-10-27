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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The complete-commit publication mode of the reference index service. Requests of one source
 * transaction execute in {@code Request.transaction_operation} order, while different
 * transactions run independently. A read waits for its {@code read_version} to be published; a
 * version is published only after its Commit marker was received and every declared write was
 * applied. The highest version merely seen is never used as a watermark.
 *
 * <p>This is a correctness reference for the protocol, not a throughput-optimized service. Each
 * lane executes its transaction in order, and a waiting read occupies its caller thread.
 */
public final class PublishedIndex implements AutoCloseable, IndexExecution.Executor {
    private final IndexBackend backend;
    private final Publication publication = new Publication();
    private final Map<Long, Lane> lanes = new HashMap<>();
    private volatile boolean closed;

    public PublishedIndex(IndexBackend backend) {
        this.backend = backend;
    }

    private Lane lane(long transaction) {
        synchronized (lanes) {
            if (closed) throw new IllegalStateException("Published index is closed");
            return lanes.computeIfAbsent(transaction, ignored -> new Lane());
        }
    }

    @Override
    public Response execute(Request request) {
        var response = Response.newBuilder().setId(request.getId());
        try {
            switch (request.getOperationCase()) {
                case WRITE -> {
                    Write write = request.getWrite();
                    if (write.getVersion() == 0) backend.write(write);
                    else {
                        requireTransaction(request);
                        lane(request.getTransactionId()).write(request, write);
                    }
                    return response.setStatus(Status.OK).build();
                }
                case SEARCH -> {
                    Search search = request.getSearch();
                    List<Result> results;
                    if (search.getLocalVersion() == 0 && request.getTransactionId() == 0) {
                        publication.await(search.getReadVersion());
                        results = backend.search(search);
                    } else {
                        requireTransaction(request);
                        results = lane(request.getTransactionId()).search(request, search);
                    }
                    return response.addAllResults(results).setStatus(Status.OK).build();
                }
                case GROUP -> {
                    SearchGroup group = request.getGroup();
                    if (group.getMembersCount() == 0) throw new IllegalArgumentException("Empty group");
                    Search first = group.getMembers(0);
                    List<Result> results;
                    if (first.getLocalVersion() == 0 && request.getTransactionId() == 0) {
                        publication.await(first.getReadVersion());
                        results = IndexExecution.group(backend, group);
                    } else {
                        requireTransaction(request);
                        results = lane(request.getTransactionId()).group(request, group);
                    }
                    return response.addAllResults(results).setStatus(Status.OK).build();
                }
                case COMMIT -> {
                    requireTransaction(request);
                    Commit commit = request.getCommit();
                    Lane lane = lane(request.getTransactionId());
                    lane.commit(
                            commit.getVersion(),
                            commit.getWrites(),
                            commit.getOperations());
                    publication.commit(
                            commit.getVersion(),
                            request.getTransactionId(),
                            commit.getWrites(),
                            commit.getOperations());
                    publication.await(commit.getVersion());
                    lane.complete();
                    return response.setStatus(Status.OK).build();
                }
                default -> throw new IllegalArgumentException("Request without an operation");
            }
        } catch (IllegalArgumentException invalid) {
            return response.setStatus(Status.INVALID)
                    .setMessage(String.valueOf(invalid.getMessage()))
                    .build();
        } catch (IOException | InterruptedException error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            return response.setStatus(Status.UNAVAILABLE)
                    .setMessage(String.valueOf(error))
                    .build();
        } catch (RuntimeException error) {
            return response.setStatus(Status.UNAVAILABLE)
                    .setMessage(String.valueOf(error))
                    .build();
        }
    }

    private static void requireTransaction(Request request) {
        if (request.getTransactionId() == 0)
            throw new IllegalArgumentException("Complete-publication request needs a transaction id");
    }

    public long publishedVersion() {
        return publication.publishedVersion();
    }

    public long flush() throws IOException {
        backend.flush();
        return publication.publishedVersion();
    }

    @Override
    public void close() {
        closed = true;
        publication.close();
        synchronized (lanes) {
            for (Lane lane : lanes.values()) lane.close();
        }
    }

    /** One source transaction's ordered operations. */
    private final class Lane {
        private final TreeMap<Long, Request> pending = new TreeMap<>();
        private final Map<Long, List<Result>> results = new HashMap<>();
        private long version;
        private long total = -1;
        private long declaredWrites = -1;
        private long next = 1, lastApplied;
        private boolean executing;
        private boolean closed;
        private boolean complete;
        private IOException failure;

        void write(Request request, Write write) throws IOException, InterruptedException {
            submit(request, write.getVersion());
        }

        List<Result> search(Request request, Search search) throws IOException, InterruptedException {
            return submit(request, search.getLocalVersion());
        }

        List<Result> group(Request request, SearchGroup group) throws IOException, InterruptedException {
            return submit(request, group.getMembers(0).getLocalVersion());
        }

        private List<Result> submit(Request request, long requestVersion)
                throws IOException, InterruptedException {
            long operation;
            synchronized (this) {
                prepare(request, requestVersion);
                operation = request.getTransactionOperation();
            }
            while (true) {
                synchronized (this) {
                    check();
                    if (lastApplied >= operation) return results.remove(operation);
                }
                step();
            }
        }

        void commit(long commitVersion, long writes, long operations)
                throws IOException, InterruptedException {
            synchronized (this) {
                check();
                if (complete) throw new IOException("Transaction already committed");
                if (version != 0 && version != commitVersion)
                    throw new IOException("Transaction version differs from its writes");
                version = commitVersion;
                if (declaredWrites >= 0 && declaredWrites != writes)
                    throw new IOException("Repeated commit declares another write count");
                declaredWrites = writes;
                if (total >= 0 && total != operations) throw new IOException("Repeated commit count");
                if (operations < lastApplied)
                    throw new IOException("Commit has fewer operations than already applied");
                total = operations;
                if (total < declaredWrites)
                    throw new IOException("More writes than operations in a transaction");
                if (!pending.isEmpty() && pending.lastKey() > total)
                    throw new IOException("Operation beyond the committed transaction count");
            }
            while (true) {
                synchronized (this) {
                    check();
                    if (lastApplied >= total) return;
                }
                step();
            }
        }

        private void prepare(Request request, long requestVersion) throws IOException {
            if (closed) throw new IOException("Transaction lane closed");
            if (complete) throw new IOException("Transaction already committed");
            if (failure != null) throw failure;
            if (requestVersion > 0) {
                if (version != 0 && version != requestVersion)
                    throw new IOException("One transaction spans versions");
                version = requestVersion;
            }
            long operation = request.getTransactionOperation();
            if (operation < 1) throw new IOException("Transaction operation must be positive");
            if (total >= 0 && operation > total)
                throw new IOException("Operation exceeds its committed transaction");
            if (operation < next) throw new IOException("Repeated transaction operation");
            if (pending.containsKey(operation) || (executing && operation == next))
                throw new IOException("Repeated transaction operation");
            pending.put(operation, request);
        }

        private void check() throws IOException {
            if (closed) throw new IOException("Transaction lane closed");
            if (failure != null) throw failure;
        }

        /**
         * Executes at most one contiguous operation. The lane monitor is never held while the
         * backend runs or while a read waits for publication, so an earlier operation arriving on
         * another stream can make progress.
         */
        private void step() throws IOException, InterruptedException {
            Request request;
            long operation;
            synchronized (this) {
                check();
                if (executing || !pending.containsKey(next)) {
                    wait();
                    return;
                }
                executing = true;
                operation = next;
                request = pending.remove(operation);
            }
            try {
                List<Result> found = execute(request);
                synchronized (this) {
                    if (found != null) results.put(operation, found);
                    lastApplied = operation;
                    next = operation + 1;
                    executing = false;
                    notifyAll();
                }
            } catch (IOException | InterruptedException | RuntimeException error) {
                synchronized (this) {
                    executing = false;
                    if (error instanceof IOException failure) this.failure = failure;
                    else if (error instanceof InterruptedException)
                        this.failure = new IOException("Transaction lane interrupted", error);
                    else this.failure = new IOException("Transaction lane failed", error);
                    notifyAll();
                }
                throw error;
            }
        }

        private List<Result> execute(Request request) throws IOException, InterruptedException {
            switch (request.getOperationCase()) {
                case WRITE -> {
                    long version = request.getWrite().getVersion();
                    publication.reserve(version, request.getTransactionId());
                    boolean applied = false;
                    try {
                        backend.write(request.getWrite());
                        publication.applied(version);
                        applied = true;
                    } finally {
                        if (!applied) publication.release(version);
                    }
                    return null;
                }
                case SEARCH -> {
                    publication.await(request.getSearch().getReadVersion());
                    return backend.search(request.getSearch());
                }
                case GROUP -> {
                    publication.await(request.getGroup().getMembers(0).getReadVersion());
                    return IndexExecution.group(backend, request.getGroup());
                }
                default -> throw new IOException("Unsupported transaction lane operation");
            }
        }

        synchronized void complete() throws IOException {
            if (failure != null) throw failure;
            complete = true;
        }

        synchronized void close() {
            closed = true;
            notifyAll();
        }
    }

    /** Commit markers, write ownership and the applied-write counts that publish them. */
    private static final class Publication {
        private final TreeMap<Long, Commit> commits = new TreeMap<>();
        private final Map<Long, Long> applied = new HashMap<>();
        private final Map<Long, Long> active = new HashMap<>();
        private final Map<Long, Long> owners = new HashMap<>();
        private long published, next = 1;
        private boolean closed;

        synchronized void commit(
                long version, long transaction, long writes, long operations)
                throws IOException {
            if (version < 1) throw new IOException("Commit version must be positive");
            if (commits.containsKey(version))
                throw new IOException("Repeated commit marker for version " + version);
            if (version < next) throw new IOException("Commit marker for an already published version");
            Long owner = owners.putIfAbsent(version, transaction);
            if (owner != null && owner != transaction)
                throw new IOException("Commit marker changes the version's transaction");
            long used = applied.getOrDefault(version, 0L) + active.getOrDefault(version, 0L);
            if (used > writes)
                throw new IOException("Commit declares fewer writes than were accepted");
            commits.put(
                    version,
                    Commit.newBuilder()
                            .setVersion(version)
                            .setWrites(writes)
                            .setOperations(operations)
                            .build());
            publish();
        }

        /**
         * Reserves a write slot before touching the backend. A write for an already published
         * version, another transaction's version, or a version whose declared write count is
         * already covered is refused before it can mutate state.
         */
        synchronized void reserve(long version, long transaction) throws IOException {
            if (version < 1) throw new IOException("Write without a version");
            if (version <= published)
                throw new IOException("Write for an already published version");
            Long owner = owners.putIfAbsent(version, transaction);
            if (owner != null && owner != transaction)
                throw new IOException("Write belongs to another transaction's version");
            Commit commit = commits.get(version);
            if (commit != null
                    && applied.getOrDefault(version, 0L)
                                    + active.getOrDefault(version, 0L)
                            >= commit.getWrites())
                throw new IOException("Write exceeds the committed write count");
            active.merge(version, 1L, Long::sum);
        }

        synchronized void applied(long version) throws IOException {
            if (version < 1) throw new IOException("Applied write without a version");
            if (version <= published)
                throw new IOException("Write applied after its version was published");
            long activeWrites = active.getOrDefault(version, 0L);
            if (activeWrites <= 0) throw new IOException("Write was not reserved");
            if (activeWrites == 1) active.remove(version);
            else active.put(version, activeWrites - 1);
            applied.merge(version, 1L, Long::sum);
            publish();
        }

        synchronized void release(long version) {
            long activeWrites = active.getOrDefault(version, 0L);
            if (activeWrites <= 1) active.remove(version);
            else active.put(version, activeWrites - 1);
            notifyAll();
        }

        private void publish() throws IOException {
            while (true) {
                if (active.getOrDefault(next, 0L) > 0) return;
                Commit commit = commits.get(next);
                if (commit == null) return;
                long writes = applied.getOrDefault(next, 0L);
                if (writes > commit.getWrites())
                    throw new IOException("More applied writes than the commit declares");
                if (writes < commit.getWrites()) return;
                published = next;
                commits.remove(next);
                applied.remove(next);
                active.remove(next);
                owners.remove(next);
                next++;
                notifyAll();
            }
        }

        synchronized void await(long version) throws IOException, InterruptedException {
            while (!closed && published < version) {
                if (closed) throw new IOException("Publication closed");
                wait();
            }
            if (closed) throw new IOException("Publication closed");
        }

        synchronized long publishedVersion() {
            return published;
        }

        synchronized void close() {
            closed = true;
            notifyAll();
        }
    }
}
