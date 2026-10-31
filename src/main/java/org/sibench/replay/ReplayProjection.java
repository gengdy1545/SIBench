package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Validates committed source groups, then serializes local sends without waiting for execution. */
public final class ReplayProjection implements IndexOperationTrace.Consumer, AutoCloseable {
    public static final int MAX_TRANSACTION_OPERATIONS = 1_000_000;
    private final boolean secondaryIndexes, sourceTransactions, publication;
    private final String dispatchMode;
    private final ReplayDispatcher dispatcher;
    private final List<Operation> pending = new ArrayList<>();
    private final Map<String, Long> kinds = new LinkedHashMap<>();
    private long source, dispatched, primary, secondary, filtered, transactions, controls, groups;
    private long peakPending, peakSearchGroup, peakExpansion;
    private long activeTransaction, activeVersion, lastVersion, lastOrdinal, lastTransactionId;
    private long lastSourceEvent;
    private int transactionOperations;
    private Operation lastMutation;
    private final List<Operation> expansion = new ArrayList<>();
    /** The members of the open search group, sent or filtered together once complete. */
    private final List<Operation> searchGroup = new ArrayList<>();
    private Thread owner;
    private boolean failed;
    /** Snapshot verification: receives each transaction-local search instead of sending it. */
    private final Skipped skipped;
    private long skippedWrites, skippedLocalSearches;

    /** A search (one, or a whole group) deliberately not sent, in trace order. */
    @FunctionalInterface
    public interface Skipped {
        void skip(List<Operation> members) throws IOException;
    }

    public ReplayProjection(boolean secondaryIndexes, IndexOperationTrace.Consumer target) {
        this(new ReplayConfig(secondaryIndexes, "none", 0, 0, 0, 0), false, target);
    }

    public ReplayProjection(
            ReplayConfig config, boolean sourceTransactions, IndexOperationTrace.Consumer target) {
        this(config, sourceTransactions, ReplayTarget.immediate(target));
    }

    public static ReplayProjection withClients(
            ReplayConfig config, boolean sourceTransactions, ReplayTarget.Factory target) {
        return new ReplayProjection(config, sourceTransactions, target, false, null);
    }

    /** {@code publication}: send complete-commit markers to a service that declares them. */
    public static ReplayProjection withClients(
            ReplayConfig config,
            boolean sourceTransactions,
            ReplayTarget.Factory target,
            boolean publication) {
        return new ReplayProjection(config, sourceTransactions, target, publication, null);
    }

    /**
     * The workload validated as usual, but only its snapshot searches are sent, to an index that
     * already holds the whole trace: writes are not sent, and a transaction-local search, whose
     * answer depends on its transaction's state at that point, goes to {@code skipped}.
     */
    public static ReplayProjection snapshotSearches(
            ReplayConfig config, ReplayTarget.Factory target, Skipped skipped) {
        return new ReplayProjection(config, true, target, false, java.util.Objects.requireNonNull(skipped));
    }

    private ReplayProjection(
            ReplayConfig config, boolean sourceTransactions, ReplayTarget.Factory target) {
        this(config, sourceTransactions, target, false, null);
    }

    private ReplayProjection(
            ReplayConfig config,
            boolean sourceTransactions,
            ReplayTarget.Factory target,
            boolean publication,
            Skipped skipped) {
        this.skipped = skipped;
        this.secondaryIndexes = config.secondaryIndexes();
        this.sourceTransactions = sourceTransactions;
        this.publication = publication;
        this.dispatchMode = config.dispatchMode();
        // The warmup budget applies to the workload, never to the baseline load.
        this.dispatcher =
                new ReplayDispatcher(config, target, sourceTransactions ? config.warmupOperations() : 0);
    }

    public boolean includes(Operation operation) {
        return secondaryIndexes || operation.index() == 0;
    }

    private synchronized void checkThread() throws IOException {
        if (owner == null) owner = Thread.currentThread();
        if (owner != Thread.currentThread())
            throw new IOException("Replay must remain on one sending thread");
        if (failed) throw new IOException("Replay failed; continuation is not allowed");
    }

    @Override
    public void accept(Operation operation) throws IOException {
        checkThread();
        try {
            acceptOrdered(operation);
        } catch (IOException | RuntimeException error) {
            failed = true;
            throw error;
        }
    }

    private void acceptOrdered(Operation operation) throws IOException {
        if (IndexOperationTrace.isControl(operation)) {
            control(operation);
            return;
        }
        source++;
        dispatcher.route(operation);
        if (operation.index() == 0) primary++;
        else secondary++;
        if (activeTransaction != 0 && ++transactionOperations > MAX_TRANSACTION_OPERATIONS)
            throw new IOException("Source transaction exceeds operation buffer limit");
        if (activeTransaction != 0) {
            if (operation.sourceEvent() < lastSourceEvent)
                throw new IOException("Source operation order moved backwards");
            lastSourceEvent = operation.sourceEvent();
        }
        if (activeTransaction != 0
                && !IndexOperationTrace.isMutation(operation)
                && operation.kind() != IndexOperationTrace.SEARCH)
            throw new IOException("Only mutations and searches are allowed inside a source group");
        if (sourceTransactions && IndexOperationTrace.isMutation(operation))
            validateMutation(operation);
        else if (operation.kind() != IndexOperationTrace.SEARCH
                && (operation.transactionId() != 0 || operation.mutationOrdinal() != 0))
            throw new IOException("Unexpected source transaction metadata");
        if (!searchGroup.isEmpty() && !operation.grouped())
            throw new IOException("Search group interrupted before its last member");
        if (operation.kind() == IndexOperationTrace.SEARCH) validateSearch(operation);
        else if (operation.readTimestamp() != -1)
            throw new IOException("ReadTimestamp is only valid on searches");
        if (operation.grouped()) {
            groupMember(operation);
            return;
        }
        if (!includes(operation)) {
            filtered++;
            return;
        }
        deliver(operation);
    }

    private void deliver(Operation operation) throws IOException {
        if (skipped != null) {
            if (operation.kind() != IndexOperationTrace.SEARCH) {
                skippedWrites++;
                return;
            }
            if (local(operation)) {
                skipped.skip(List.of(operation));
                skippedLocalSearches++;
                return;
            }
            // Snapshot reads of a loaded index need no commit; sending now keeps the trace order
            // of sent and skipped searches.
            sendOperation(operation);
            return;
        }
        if (activeTransaction != 0) {
            pending.add(operation);
            peakPending = Math.max(peakPending, pending.size());
        } else sendOperation(operation);
    }

    /**
     * A group is one source access: its members arrive consecutively with one source event, read
     * point and count, and the group is sent whole or, when a member's index is filtered, not at
     * all, so a filtered secondary never leaves a partial intersection behind.
     */
    private void groupMember(Operation o) throws IOException {
        if (o.expansionPosition() != searchGroup.size())
            throw new IOException("Search group member out of order");
        if (!searchGroup.isEmpty()) {
            Operation first = searchGroup.get(0);
            if (o.sourceEvent() != first.sourceEvent()
                    || o.table() != first.table()
                    || o.workUnitId() != first.workUnitId()
                    || o.readTimestamp() != first.readTimestamp()
                    || o.version() != first.version()
                    || o.expectedCount() != first.expectedCount()
                    || o.expansionSize() != first.expansionSize()
                    || o.transactionId() != first.transactionId()
                    || o.mutationOrdinal() != first.mutationOrdinal()
                    || (o.flags() & GROUP_SHARED) != (first.flags() & GROUP_SHARED))
                throw new IOException("Inconsistent search group member");
        }
        searchGroup.add(o);
        peakSearchGroup = Math.max(peakSearchGroup, searchGroup.size());
        if (searchGroup.size() < o.expansionSize()) return;
        boolean included = true;
        for (Operation member : searchGroup) included &= includes(member);
        if (included && skipped != null && local(o)) {
            skipped.skip(List.copyOf(searchGroup));
            skippedLocalSearches++;
            searchGroup.clear();
            return;
        }
        for (Operation member : searchGroup) {
            if (included) deliver(member);
            else filtered++;
        }
        searchGroup.clear();
    }

    private static final int GROUP_SHARED =
            IndexOperationTrace.INTERSECT_GROUP
                    | IndexOperationTrace.UNION_GROUP
                    | IndexOperationTrace.SNAPSHOT_SEARCH
                    | IndexOperationTrace.TRANSACTION_LOCAL_SEARCH;

    private static boolean local(Operation o) {
        return (o.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0;
    }

    private void validateSearch(Operation o) throws IOException {
        boolean local = local(o);
        if (o.readTimestamp() < 0
                || (o.flags() & ~IndexOperationTrace.SEARCH_FLAGS) != 0
                || o.expectedCount() < 0
                || (!local && o.version() != 0))
            throw new IOException("Invalid search timestamp/flags/count");
        if (activeTransaction != 0) {
            requireCompleteExpansion();
            if (o.transactionId() != activeTransaction || o.mutationOrdinal() != lastOrdinal)
                throw new IOException("Search source transaction position mismatch");
        } else if (o.transactionId() != 0 || o.mutationOrdinal() != 0 || local)
            throw new IOException("Transaction-local search outside source group");
        if (local) {
            // A local search names its own version and keeps the statement snapshot.
            if (lastOrdinal == 0
                    || o.version() != activeVersion
                    || o.readTimestamp() >= activeVersion
                    || (o.flags() & IndexOperationTrace.SNAPSHOT_SEARCH) != 0)
                throw new IOException("Invalid transaction-local read point");
        } else if (sourceTransactions
                && (o.readTimestamp() > lastVersion
                        || (o.readTimestamp() != lastVersion
                                && (o.flags() & IndexOperationTrace.SNAPSHOT_SEARCH) == 0)))
            throw new IOException("Search requires an available read point");
    }

    private void sendOperation(Operation operation) throws IOException {
        dispatcher.send(operation);
        countDispatch(operation);
    }

    private void countDispatch(Operation operation) {
        dispatched++;
        kinds.merge(IndexOperationTrace.kindName(operation.kind()), 1L, Long::sum);
    }

    private void validateMutation(Operation o) throws IOException {
        if (activeTransaction == 0
                || o.transactionId() != activeTransaction
                || o.version() != activeVersion
                || o.mutationOrdinal() < 1)
            throw new IOException("Mutation missing matching source group/version");
        if (o.expansionSize() < 1) throw new IOException("Mutation outside a maintenance unit");
        if (o.mutationOrdinal() != lastOrdinal) {
            requireCompleteExpansion();
            if (o.mutationOrdinal() != lastOrdinal + 1
                    || o.index() != 0
                    || o.expansionPosition() != 0)
                throw new IOException("Non-contiguous source mutation");
            expansion.clear();
            lastOrdinal = o.mutationOrdinal();
            lastMutation = o;
        } else if (lastMutation == null
                || o.expansionSize() != lastMutation.expansionSize()
                || o.expansionPosition() != expansion.size()
                || o.table() != lastMutation.table()
                || !o.key().equals(lastMutation.key())
                || o.rowId() != lastMutation.rowId()
                || o.sourceEvent() != lastMutation.sourceEvent()
                || o.workUnitId() != lastMutation.workUnitId()
                || o.index() <= 0
                || o.flags() != lastMutation.flags()
                || (o.kind() != lastMutation.kind()
                        && o.kind() != IndexOperationTrace.LOGICAL_DELETE)
                || (o.kind() != IndexOperationTrace.LOGICAL_DELETE
                        && (o.file() != lastMutation.file()
                                || o.position() != lastMutation.position()))
                || (o.kind() == IndexOperationTrace.LOGICAL_DELETE
                        && (o.file() != 0 || o.position() != 0)))
            throw new IOException("Invalid index maintenance identity/position");
        if (o.expansionSize() > MAX_TRANSACTION_OPERATIONS
                || o.expansionPosition() < 0
                || o.expansionPosition() >= o.expansionSize())
            throw new IOException("Invalid index maintenance width");
        if (o.index() > 0 && !expansion.isEmpty()) {
            Operation previous = expansion.get(expansion.size() - 1);
            if (o.index() < previous.index()
                    || o.index() > previous.index() + 1
                    || (o.index() == previous.index()
                            && (previous.kind() != IndexOperationTrace.LOGICAL_DELETE
                                    || o.kind() != IndexOperationTrace.UPSERT
                                    || java.util.Objects.equals(
                                            previous.indexedValue(), o.indexedValue()))))
                throw new IOException("Invalid secondary delete/write expansion");
        }
        expansion.add(o);
        peakExpansion = Math.max(peakExpansion, expansion.size());
    }

    private void requireCompleteExpansion() throws IOException {
        if (lastMutation != null && expansion.size() != lastMutation.expansionSize())
            throw new IOException("Incomplete index maintenance unit");
    }

    private void control(Operation o) throws IOException {
        if (!sourceTransactions
                || o.transactionId() < 1
                || o.version() < 1
                || o.index() != 0
                || o.mutationOrdinal() != 0)
            throw new IOException("Unexpected source transaction control");
        controls++;
        if (o.kind() == IndexOperationTrace.TRANSACTION_BEGIN) {
            if (activeTransaction != 0
                    || o.version() <= lastVersion
                    || o.transactionId() <= lastTransactionId
                    || o.expectedCount() != 0)
                throw new IOException("Invalid source group begin/order");
            activeTransaction = o.transactionId();
            activeVersion = o.version();
            lastOrdinal = 0;
            lastMutation = null;
            expansion.clear();
            lastSourceEvent = 0;
            transactionOperations = 0;
        } else {
            if (activeTransaction == 0
                    || o.transactionId() != activeTransaction
                    || o.version() != activeVersion
                    || o.expectedCount() != lastOrdinal)
                throw new IOException("Source commit or mutation count mismatch");
            requireCompleteExpansion();
            if (!searchGroup.isEmpty()) throw new IOException("Search group crosses its commit");
            if (dispatchMode.equals("transaction") && !pending.isEmpty()) groups++;
            for (Operation operation : pending) sendOperation(operation);
            pending.clear();
            if (publication)
                dispatcher.sendCommit(activeTransaction, activeVersion);
            lastVersion = activeVersion;
            lastTransactionId = activeTransaction;
            activeTransaction = 0;
            transactions++;
        }
    }

    public void finish() throws IOException {
        checkThread();
        if (!searchGroup.isEmpty()) throw new IOException("Truncated search group");
        if (activeTransaction != 0)
            throw new IOException("Truncated source transaction without commit");
        dispatcher.drain();
    }

    public void requireClientCoverage() throws IOException {
        dispatcher.requireClientCoverage();
    }

    @Override
    public void close() throws IOException {
        dispatcher.close();
    }

    public long dispatched() {
        return dispatched;
    }

    public Map<String, Object> describe() {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("source_operations", source);
        report.put("source_primary_operations", primary);
        report.put("source_secondary_operations", secondary);
        report.put("dispatched_operations", dispatched);
        report.put("filtered_secondary_operations", filtered);
        report.put("committed_transactions", transactions);
        report.put("last_commit_version", lastVersion);
        report.put("change_delivery", sourceTransactions ? "committed_only" : "baseline");
        report.put("transaction_control_records", controls);
        report.put("dispatch_mode", dispatchMode);
        report.put("complete_commit_publication", publication);
        report.put("source_dispatch_groups", groups);
        report.put("peak_pending_operations", peakPending);
        report.put("peak_search_group_members", peakSearchGroup);
        report.put("peak_expansion_members", peakExpansion);
        report.putAll(dispatcher.describe());
        report.put("sending_threads", 1);
        report.put("ordering", "source_transaction_and_operation_local_send_order");
        report.put("downstream_transaction_api_required", false);
        report.put("dispatched_by_kind", new LinkedHashMap<>(kinds));
        if (skipped != null) {
            report.put("snapshot_verification", true);
            report.put("unsent_writes", skippedWrites);
            report.put("unsent_transaction_local_searches", skippedLocalSearches);
        }
        return report;
    }
}
