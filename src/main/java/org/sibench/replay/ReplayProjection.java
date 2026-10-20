package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Applies index selection at the dispatch boundary, including loading and maintenance. */
public final class ReplayProjection implements IndexOperationTrace.Consumer {
    private final boolean secondaryIndexes;
    private final IndexOperationTrace.Consumer target;
    private final boolean transactional, coalesce;
    private long source,
            dispatched,
            primary,
            secondary,
            filtered,
            coalesced,
            transactions,
            controls;
    private long activeTransaction, activeVersion, lastVersion, lastOrdinal, lastTransactionId;
    private int lastIndex;
    private Operation lastMutation;
    private final List<Operation> pending = new ArrayList<>();
    private final Map<Key, Operation> finalWrites = new LinkedHashMap<>();
    private final Map<String, Long> kinds = new LinkedHashMap<>();
    public static final int MAX_TRANSACTION_OPERATIONS = 1_000_000;
    private int transactionOperations;

    private record Key(int table, int index, long key, long indexedValue) {
        static Key of(Operation o) {
            return new Key(
                    o.table(),
                    o.index(),
                    o.key(),
                    o.index() == 1
                            ? o.secondarySource()
                            : o.index() == 2 ? o.secondaryTarget() : 0);
        }
    }

    public ReplayProjection(boolean secondaryIndexes, IndexOperationTrace.Consumer target) {
        this(secondaryIndexes, false, false, target);
    }

    public ReplayProjection(
            ReplayConfig config, boolean transactional, IndexOperationTrace.Consumer target) {
        this(config.secondaryIndexes(), transactional, config.coalesceTransactionWrites(), target);
    }

    private ReplayProjection(
            boolean secondaryIndexes,
            boolean transactional,
            boolean coalesce,
            IndexOperationTrace.Consumer target) {
        this.secondaryIndexes = secondaryIndexes;
        this.transactional = transactional;
        this.coalesce = coalesce;
        this.target = target;
    }

    public boolean includes(Operation operation) {
        return secondaryIndexes || operation.index() == 0;
    }

    @Override
    public void accept(Operation operation) throws IOException {
        if (IndexOperationTrace.isControl(operation)) {
            control(operation);
            return;
        }
        source++;
        if (operation.index() == 0) primary++;
        else secondary++;
        if (transactional && IndexOperationTrace.isMutation(operation)) validateMutation(operation);
        else if (transactional
                && (activeTransaction != 0
                        || operation.transactionId() != 0
                        || operation.mutationOrdinal() != 0))
            throw new IOException("Non-mutation inside transaction or with transaction metadata");
        if (operation.kind() == IndexOperationTrace.SEARCH) {
            if (operation.version() != 0
                    || operation.readTimestamp() < 0
                    || (operation.flags()
                                    & ~(IndexOperationTrace.SNAPSHOT_SEARCH
                                            | IndexOperationTrace.DERIVED_SEARCH))
                            != 0
                    || operation.expectedCount() < 0)
                throw new IOException("Invalid search timestamp/flags/count");
            if (transactional
                    && (operation.readTimestamp() > lastVersion
                            || (operation.readTimestamp() != lastVersion
                                    && (operation.flags() & IndexOperationTrace.SNAPSHOT_SEARCH)
                                            == 0)))
                throw new IOException(
                        "Search requires a published read point; historical reads must be explicit"
                            + " snapshots");
        } else if (operation.readTimestamp() != -1)
            throw new IOException("ReadTimestamp is only valid on searches");
        if (!includes(operation)) {
            filtered++;
            return;
        }
        if (transactional && IndexOperationTrace.isMutation(operation)) {
            if (coalesce) {
                Key key = Key.of(operation);
                if (finalWrites.remove(key) != null) coalesced++;
                finalWrites.put(key, operation);
            } else pending.add(operation);
            return;
        }
        dispatch(operation);
    }

    private void dispatch(Operation operation) throws IOException {
        target.accept(operation);
        dispatched++;
        kinds.merge(IndexOperationTrace.kindName(operation.kind()), 1L, Long::sum);
    }

    private void validateMutation(Operation o) throws IOException {
        if (activeTransaction == 0
                || o.transactionId() != activeTransaction
                || o.version() != activeVersion
                || o.mutationOrdinal() < 1)
            throw new IOException("Mutation missing matching transaction boundary/version");
        if (++transactionOperations > MAX_TRANSACTION_OPERATIONS)
            throw new IOException("Transaction exceeds reference operation buffer limit");
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
                    || o.flags() != lastMutation.flags()
                    || o.secondarySource() != lastMutation.secondarySource()
                    || o.secondaryTarget() != lastMutation.secondaryTarget())
                throw new IOException("Invalid expanded mutation identity/order");
        } else {
            requireCompleteExpansion();
            if (o.mutationOrdinal() != lastOrdinal + 1 || o.index() != 0)
                throw new IOException("Non-contiguous transaction mutation ordinal");
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
        if (!transactional
                || o.transactionId() < 1
                || o.version() < 1
                || o.index() != 0
                || o.mutationOrdinal() != 0)
            throw new IOException("Unexpected transaction control");
        controls++;
        if (o.kind() == IndexOperationTrace.TRANSACTION_BEGIN) {
            if (activeTransaction != 0
                    || o.version() <= lastVersion
                    || o.transactionId() <= lastTransactionId
                    || o.expectedCount() != 0)
                throw new IOException("Invalid transaction begin/version");
            activeTransaction = o.transactionId();
            activeVersion = o.version();
            lastOrdinal = 0;
            lastMutation = null;
            transactionOperations = 0;
        } else {
            if (activeTransaction == 0
                    || o.transactionId() != activeTransaction
                    || o.version() != activeVersion
                    || o.expectedCount() != lastOrdinal)
                throw new IOException("Commit boundary or mutation count mismatch");
            requireCompleteExpansion();
            target.accept(
                    IndexOperationTrace.transaction(
                            o.sourceEvent(),
                            IndexOperationTrace.TRANSACTION_BEGIN,
                            activeTransaction,
                            activeVersion,
                            0));
            for (Operation write : coalesce ? finalWrites.values() : pending) dispatch(write);
            target.accept(o);
            pending.clear();
            finalWrites.clear();
            lastVersion = activeVersion;
            lastTransactionId = activeTransaction;
            activeTransaction = 0;
            transactions++;
        }
    }

    public void finish() throws IOException {
        if (activeTransaction != 0) throw new IOException("Truncated transaction without commit");
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
        report.put("coalesced_operations", coalesced);
        report.put("committed_transactions", transactions);
        report.put(
                "change_delivery",
                transactional ? "committed_only" : "unavailable_legacy_or_baseline");
        report.put("transaction_control_records", controls);
        report.put(
                "transaction_coalescing",
                transactional
                        ? coalesce ? "enabled" : "disabled"
                        : "unavailable_legacy_or_baseline");
        report.put("dispatched_by_kind", new LinkedHashMap<>(kinds));
        return report;
    }
}
