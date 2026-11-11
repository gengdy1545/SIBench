package org.sibench.replay;

import org.sibench.adapters.proto.TraceOperation;
import org.sibench.adapters.proto.TraceRecord;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Validates committed Trace writes and snapshot searches, preserving source send order. */
public final class ReplayProjection implements IndexOperationTrace.Consumer, AutoCloseable {
    private final boolean secondaryIndexes, sourceTransactions, snapshotOnly;
    private final String dispatchMode;
    private final ReplayDispatcher dispatcher;
    private record Pending(Operation operation, TraceOperation request) {}
    private final List<Pending> pending = new ArrayList<>();
    private TraceOperation storedRequest;
    private final Map<String, Long> kinds = new LinkedHashMap<>();
    private long source, dispatched, effectiveRequests, primary, secondary, filtered, transactions, controls, groups;
    private long searchGroups;
    private long peakPending, peakSearchGroup;
    private long activeTransaction, activeVersion, lastVersion, lastTransactionId;
    private long lastSourceEvent;
    private record WriteIdentity(int table, int index, org.sibench.trace.IndexKey key,
            org.sibench.trace.IndexKey indexedValue) {}
    private final java.util.Set<WriteIdentity> writes = new java.util.HashSet<>();
    /** The members of the open search group, sent or filtered together once complete. */
    private final List<Operation> searchGroup = new ArrayList<>();
    private Thread owner;
    private boolean failed;
    private long skippedWrites;

    public ReplayProjection(boolean secondaryIndexes, IndexOperationTrace.Consumer target) {
        this(new ReplayConfig(secondaryIndexes, 0, 0, 0, 0), false, target);
    }

    public ReplayProjection(
            ReplayConfig config, boolean sourceTransactions, IndexOperationTrace.Consumer target) {
        this(config, sourceTransactions, ReplayTarget.immediate(target));
    }

    public static ReplayProjection withClients(
            ReplayConfig config, boolean sourceTransactions, ReplayTarget.Factory target) {
        return new ReplayProjection(config, sourceTransactions, target, false);
    }

    /** Sends snapshot searches to an index that already holds the complete Trace. */
    public static ReplayProjection snapshotSearches(ReplayConfig config, ReplayTarget.Factory target) {
        return new ReplayProjection(config, true, target, true);
    }

    private ReplayProjection(
            ReplayConfig config, boolean sourceTransactions, ReplayTarget.Factory target) {
        this(config, sourceTransactions, target, false);
    }

    private ReplayProjection(ReplayConfig config, boolean sourceTransactions,
            ReplayTarget.Factory target, boolean snapshotOnly) {
        this.snapshotOnly = snapshotOnly;
        this.secondaryIndexes = config.secondaryIndexes();
        this.sourceTransactions = sourceTransactions;
        this.dispatchMode = config.dispatchMode();
        this.dispatcher = new ReplayDispatcher(config, target,
                sourceTransactions ? config.warmupOperations() : 0);
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

    @Override
    public void accept(TraceRecord record, List<Operation> views) throws IOException {
        storedRequest = record.hasOperation() ? record.getOperation() : null;
        try {
            for (Operation operation : views) accept(operation);
        } finally {
            storedRequest = null;
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
        if (snapshotOnly) {
            if (operation.kind() != IndexOperationTrace.SEARCH) skippedWrites++;
            else sendOperation(operation);
            return;
        }
        // Trace contains only committed transactions; prefix reads can be dispatched immediately.
        if (activeTransaction != 0 && (!pending.isEmpty() || operation.kind() != IndexOperationTrace.SEARCH)) {
            if (pending.size() >= org.sibench.capture.ProtoTrace.MAX_TRANSACTION_OPERATIONS)
                throw new IOException("Source transaction exceeds operation buffer limit");
            pending.add(new Pending(operation, storedRequest));
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
        for (Operation member : searchGroup) {
            if (included) deliver(member);
            else filtered++;
        }
        searchGroup.clear();
    }

    private static final int GROUP_SHARED =
            IndexOperationTrace.INTERSECT_GROUP
                    | IndexOperationTrace.UNION_GROUP
                    | IndexOperationTrace.SNAPSHOT_SEARCH;

    private void validateSearch(Operation o) throws IOException {
        if (o.readTimestamp() < 0 || o.version() != 0
                || (o.flags() & ~IndexOperationTrace.SEARCH_FLAGS) != 0
                || (o.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0
                || o.expectedCount() < 0)
            throw new IOException("Only snapshot searches are supported in Trace replay");
        if (activeTransaction != 0) {
            if (o.transactionId() != activeTransaction || o.mutationOrdinal() < 0)
                throw new IOException("Search source transaction identity mismatch");
        } else if (o.transactionId() != 0 || o.mutationOrdinal() != 0)
            throw new IOException("Search provenance outside source boundary");
        if (sourceTransactions && (o.readTimestamp() > lastVersion
                || o.readTimestamp() != lastVersion
                    && (o.flags() & IndexOperationTrace.SNAPSHOT_SEARCH) == 0))
            throw new IOException("Search requires an available read point");
    }

    private void sendOperation(Operation operation) throws IOException {
        dispatcher.send(operation, storedRequest);
        countDispatch(operation);
    }

    private void countDispatch(Operation operation) {
        dispatched++;
        effectiveRequests++;
        if (!operation.grouped() || operation.expansionPosition() == 0) {
            if (operation.grouped()) searchGroups++;
        }
        kinds.merge(IndexOperationTrace.kindName(operation.kind()), 1L, Long::sum);
    }

    private void validateMutation(Operation o) throws IOException {
        if (activeTransaction == 0 || o.transactionId() != activeTransaction
                || o.version() != activeVersion || o.mutationOrdinal() < 1)
            throw new IOException("Mutation missing matching source boundary/version");
        if (o.expansionSize() != 1 || o.expansionPosition() != 0)
            throw new IOException("Committed Trace writes must be singleton final entries");
        if (!writes.add(new WriteIdentity(o.table(), o.index(), o.key(), o.indexedValue())))
            throw new IOException("Repeated physical index entry in one committed version");
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
            writes.clear();
            lastSourceEvent = 0;
        } else {
            if (o.kind() != IndexOperationTrace.TRANSACTION_COMMIT || activeTransaction == 0
                    || o.transactionId() != activeTransaction
                    || o.version() != activeVersion)
                throw new IOException("Invalid committed source boundary");
            if (!searchGroup.isEmpty()) throw new IOException("Search group crosses its commit");
            if (dispatchMode.equals("transaction") && !pending.isEmpty()) groups++;
            for (Pending item : pending) {
                dispatcher.send(item.operation(), item.request());
                countDispatch(item.operation());
            }
            pending.clear();
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

    public long effectiveRequests() {
        return effectiveRequests;
    }

    public Map<String, Object> describe() {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("source_operations", source);
        report.put("source_primary_operations", primary);
        report.put("source_secondary_operations", secondary);
        report.put("dispatched_operations", dispatched);
        report.put("effective_requests", effectiveRequests);
        report.put("search_groups", searchGroups);
        report.put("filtered_secondary_operations", filtered);
        report.put("committed_transactions", transactions);
        report.put("last_resolved_version", lastVersion);
        report.put("change_delivery", sourceTransactions ? "committed_transactions" : "baseline");
        report.put("transaction_control_records", controls);
        report.put("dispatch_mode", dispatchMode);
        report.put("source_dispatch_groups", groups);
        report.put("peak_pending_operations", peakPending);
        report.put("peak_search_group_members", peakSearchGroup);
        report.putAll(dispatcher.describe());
        report.put("sending_threads", 1);
        report.put("ordering", "source_order_with_snapshot_write_dependencies");
        report.put("downstream_transaction_api_required", false);
        report.put("dispatched_by_kind", new LinkedHashMap<>(kinds));
        if (snapshotOnly) {
            report.put("snapshot_verification", true);
            report.put("unsent_writes", skippedWrites);
        }
        return report;
    }
}
