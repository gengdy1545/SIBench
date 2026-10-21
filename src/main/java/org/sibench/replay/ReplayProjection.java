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
    private final boolean secondaryIndexes, sourceTransactions;
    private final String dispatchMode;
    private final ReplayDispatcher dispatcher;
    private final List<Operation> pending = new ArrayList<>();
    private final Map<String, Long> kinds = new LinkedHashMap<>();
    private long source, dispatched, primary, secondary, filtered, transactions, controls, groups;
    private long activeTransaction, activeVersion, lastVersion, lastOrdinal, lastTransactionId;
    private long lastSourceEvent;
    private int lastIndex, transactionOperations;
    private Operation lastMutation;
    private Thread owner;
    private boolean failed;

    public ReplayProjection(boolean secondaryIndexes, IndexOperationTrace.Consumer target) {
        this(new ReplayConfig(secondaryIndexes, "none", 0, 0, 0, 0), false, target);
    }

    public ReplayProjection(
            ReplayConfig config, boolean sourceTransactions, IndexOperationTrace.Consumer target) {
        this(config, sourceTransactions, ReplayTarget.immediate(target));
    }

    public static ReplayProjection withClients(
            ReplayConfig config, boolean sourceTransactions, ReplayTarget.Factory target) {
        return new ReplayProjection(config, sourceTransactions, target);
    }

    private ReplayProjection(
            ReplayConfig config, boolean sourceTransactions, ReplayTarget.Factory target) {
        this.secondaryIndexes = config.secondaryIndexes();
        this.sourceTransactions = sourceTransactions;
        this.dispatchMode = config.dispatchMode();
        this.dispatcher = new ReplayDispatcher(config, target);
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
        if (operation.kind() == IndexOperationTrace.SEARCH) validateSearch(operation);
        else if (operation.readTimestamp() != -1)
            throw new IOException("ReadTimestamp is only valid on searches");
        if (!includes(operation)) {
            filtered++;
            return;
        }
        if (activeTransaction != 0) pending.add(operation);
        else sendOperation(operation);
    }

    private void validateSearch(Operation o) throws IOException {
        int allowed =
                IndexOperationTrace.SNAPSHOT_SEARCH
                        | IndexOperationTrace.DERIVED_SEARCH
                        | IndexOperationTrace.TRANSACTION_LOCAL_SEARCH;
        boolean local = (o.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0;
        if (o.version() != 0
                || o.readTimestamp() < 0
                || (o.flags() & ~allowed) != 0
                || o.expectedCount() < 0)
            throw new IOException("Invalid search timestamp/flags/count");
        if (activeTransaction != 0) {
            requireCompleteExpansion();
            if (o.transactionId() != activeTransaction || o.mutationOrdinal() != lastOrdinal)
                throw new IOException("Search source transaction position mismatch");
        } else if (o.transactionId() != 0 || o.mutationOrdinal() != 0 || local)
            throw new IOException("Transaction-local search outside source group");
        if (local) {
            if (lastOrdinal == 0
                    || o.readTimestamp() != activeVersion
                    || o.flags() != IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
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
        if (o.mutationOrdinal() == lastOrdinal) {
            if (lastMutation == null
                    || lastMutation.table() != 5
                    || o.table() != 5
                    || o.index() != lastIndex + 1
                    || o.key() != lastMutation.key()
                    || o.kind() != lastMutation.kind()
                    || o.rowId() != lastMutation.rowId()
                    || o.file() != lastMutation.file()
                    || o.position() != lastMutation.position()
                    || o.sourceEvent() != lastMutation.sourceEvent()
                    || o.workUnitId() != lastMutation.workUnitId()
                    || o.flags() != lastMutation.flags()
                    || o.secondarySource() != lastMutation.secondarySource()
                    || o.secondaryTarget() != lastMutation.secondaryTarget())
                throw new IOException("Invalid expanded mutation identity/order");
        } else {
            requireCompleteExpansion();
            if (o.mutationOrdinal() != lastOrdinal + 1 || o.index() != 0)
                throw new IOException("Non-contiguous source mutation ordinal");
            lastOrdinal = o.mutationOrdinal();
            lastMutation = o;
        }
        lastIndex = o.index();
    }

    private void requireCompleteExpansion() throws IOException {
        if (lastMutation != null && lastMutation.table() == 5 && lastIndex != 2)
            throw new IOException("Incomplete explicit transfer index expansion");
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
            lastSourceEvent = 0;
            transactionOperations = 0;
        } else {
            if (activeTransaction == 0
                    || o.transactionId() != activeTransaction
                    || o.version() != activeVersion
                    || o.expectedCount() != lastOrdinal)
                throw new IOException("Source commit or mutation count mismatch");
            requireCompleteExpansion();
            if (dispatchMode.equals("transaction") && !pending.isEmpty()) groups++;
            for (Operation operation : pending) sendOperation(operation);
            pending.clear();
            lastVersion = activeVersion;
            lastTransactionId = activeTransaction;
            activeTransaction = 0;
            transactions++;
        }
    }

    public void finish() throws IOException {
        checkThread();
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
        report.put("change_delivery", sourceTransactions ? "committed_only" : "baseline_or_legacy");
        report.put("transaction_control_records", controls);
        report.put("dispatch_mode", dispatchMode);
        report.put("source_dispatch_groups", groups);
        report.putAll(dispatcher.describe());
        report.put("sending_threads", 1);
        report.put("ordering", "source_transaction_and_operation_local_send_order");
        report.put("downstream_transaction_api_required", false);
        report.put("dispatched_by_kind", new LinkedHashMap<>(kinds));
        return report;
    }
}
