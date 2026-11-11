package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.sibench.capture.IndexOperationTrace.*;

/** Validates native source operations before projecting committed state and snapshot queries. */
final class CommittedOperations {
    static final int MAX_TRANSACTION_OPERATIONS = 1_000_000;
    private final Consumer output;
    private final ArrayList<Operation> pending = new ArrayList<>();
    private record Entry(int table, int index, IndexKey indexedValue, IndexKey primaryKey) {
        Entry(Operation operation) { this(operation.table(), operation.index(), operation.indexedValue(), operation.key()); }
    }
    private final Map<Entry, Integer> lastWrites = new HashMap<>();
    private long transaction, version, lastVersion, lastTransaction, ordinal, lastSource, sourceOperations, superseded;
    private Operation mutation, previousExpansion, searchGroup;
    private int expansionMembers, searchMembers;
    private long abortedWrites, localSearches, coalescedWrites, peakBuffered;

    CommittedOperations(Consumer output) { this.output = output; }

    void accept(Operation operation) throws IOException {
        validate(operation);
        if (operation.kind() == INSERT && operation.version() != 0)
            throw new IOException("Baseline requires timestamp zero");
        if (isControl(operation)) {
            control(operation);
            return;
        }
        if (transaction != 0) {
            if (operation.sourceEvent() < lastSource)
                throw new IOException("Source operation order moved backwards");
            lastSource = operation.sourceEvent();
        }
        if (searchGroup != null && !operation.grouped()) throw new IOException("Interrupted source search group");
        if (operation.kind() == SEARCH) search(operation);
        else if (isMutation(operation)) mutation(operation);
        else if (transaction != 0 || operation.transactionId() != 0 || operation.mutationOrdinal() != 0)
            throw new IOException("Unexpected source transaction metadata");
        // Until a transaction writes, its snapshot searches cannot depend on its outcome, and
        // emitting them now keeps the output order: a read-only transaction is never buffered.
        if (transaction == 0 || pending.isEmpty() && operation.kind() == SEARCH
                && (operation.flags() & TRANSACTION_LOCAL_SEARCH) == 0) output.accept(operation);
        // Operations held for the outcome count against the limit, even when dropped or superseded early.
        else if (sourceOperations++ >= MAX_TRANSACTION_OPERATIONS)
            throw new IOException("Source transaction exceeds operation buffer limit");
        else if (operation.kind() == SEARCH && (operation.flags() & TRANSACTION_LOCAL_SEARCH) != 0) localSearches++;
        else {
            // A superseded write is never exported; its slot keeps the survivors' source order.
            if (isMutation(operation)) {
                Integer previous = lastWrites.put(new Entry(operation), pending.size());
                if (previous != null) {
                    pending.set(previous, null);
                    superseded++;
                }
            }
            pending.add(operation);
            peakBuffered = Math.max(peakBuffered, pending.size());
        }
    }

    private void control(Operation operation) throws IOException {
        if (operation.transactionId() <= 0 || operation.version() <= 0 || operation.mutationOrdinal() != 0)
            throw new IOException("Invalid source transaction control");
        if (operation.kind() == TRANSACTION_BEGIN) {
            if (transaction != 0 || searchGroup != null || operation.version() <= lastVersion
                    || operation.transactionId() <= lastTransaction || operation.expectedCount() != 0)
                throw new IOException("Invalid source group begin/order");
            transaction = operation.transactionId();
            version = operation.version();
            ordinal = lastSource = sourceOperations = superseded = 0;
            mutation = previousExpansion = null;
            expansionMembers = 0;
            output.accept(operation);
            return;
        }
        if (transaction == 0 || operation.transactionId() != transaction || operation.version() != version
                || operation.expectedCount() != ordinal || searchGroup != null)
            throw new IOException("Source outcome or mutation count mismatch");
        requireCompleteExpansion();
        boolean aborted = operation.kind() == TRANSACTION_ABORT;
        if (aborted) abortedWrites += superseded;
        else coalescedWrites += superseded;
        for (Operation member : pending) {
            if (member == null) continue;
            if (member.kind() == SEARCH) output.accept(member);
            else if (aborted) abortedWrites++;
            else output.accept(member.withExpansion(0, 1));
        }
        output.accept(operation);
        pending.clear();
        lastWrites.clear();
        lastVersion = version;
        lastTransaction = transaction;
        transaction = 0;
    }

    private void mutation(Operation operation) throws IOException {
        if (transaction == 0 || operation.transactionId() != transaction || operation.version() != version
                || operation.mutationOrdinal() < 1)
            throw new IOException("Mutation missing matching source group/version");
        if (operation.expansionSize() < 1 || operation.expansionSize() > MAX_TRANSACTION_OPERATIONS
                || operation.expansionPosition() < 0 || operation.expansionPosition() >= operation.expansionSize())
            throw new IOException("Invalid index maintenance width");
        if (operation.mutationOrdinal() != ordinal) {
            requireCompleteExpansion();
            if (operation.mutationOrdinal() != ordinal + 1 || operation.index() != 0 || operation.expansionPosition() != 0)
                throw new IOException("Non-contiguous source mutation");
            ordinal = operation.mutationOrdinal();
            mutation = operation;
            expansionMembers = 0;
        } else if (mutation == null || operation.expansionSize() != mutation.expansionSize()
                || operation.expansionPosition() != expansionMembers || operation.table() != mutation.table()
                || !operation.key().equals(mutation.key()) || operation.rowId() != mutation.rowId()
                || operation.sourceEvent() != mutation.sourceEvent() || operation.workUnitId() != mutation.workUnitId()
                || operation.index() <= 0 || operation.flags() != mutation.flags()
                || operation.kind() != mutation.kind() && operation.kind() != LOGICAL_DELETE
                || operation.kind() != LOGICAL_DELETE && (operation.file() != mutation.file() || operation.position() != mutation.position())
                || operation.kind() == LOGICAL_DELETE && (operation.file() != 0 || operation.position() != 0))
            throw new IOException("Invalid index maintenance identity/position");
        if (operation.index() > 0 && expansionMembers > 0) {
            if (operation.index() < previousExpansion.index() || operation.index() > previousExpansion.index() + 1
                    || operation.index() == previousExpansion.index()
                    && (previousExpansion.kind() != LOGICAL_DELETE || operation.kind() != UPSERT
                        || Objects.equals(previousExpansion.indexedValue(), operation.indexedValue())))
                throw new IOException("Invalid secondary delete/write expansion");
        }
        expansionMembers++;
        previousExpansion = operation;
    }

    private void search(Operation operation) throws IOException {
        boolean local = (operation.flags() & TRANSACTION_LOCAL_SEARCH) != 0;
        if (transaction != 0) {
            requireCompleteExpansion();
            if (operation.transactionId() != transaction || operation.mutationOrdinal() != ordinal)
                throw new IOException("Search source transaction position mismatch");
            if (local && (ordinal == 0 || operation.version() != version || operation.readTimestamp() >= version))
                throw new IOException("Invalid source snapshot read point");
        } else if (operation.transactionId() != 0 || operation.mutationOrdinal() != 0 || local)
            throw new IOException("Transaction-local search outside source group");
        // A standalone snapshot has the same resolved-source dependency as one inside a group.
        if (!local && (operation.readTimestamp() > lastVersion
                || operation.readTimestamp() != lastVersion && (operation.flags() & SNAPSHOT_SEARCH) == 0))
            throw new IOException("Search requires an available source snapshot");
        if (!operation.grouped()) return;
        if (operation.expansionPosition() != searchMembers) throw new IOException("Search group member out of order");
        if (searchGroup == null) searchGroup = operation;
        else {
            int shared = INTERSECT_GROUP | UNION_GROUP | SNAPSHOT_SEARCH | TRANSACTION_LOCAL_SEARCH;
            if (operation.sourceEvent() != searchGroup.sourceEvent() || operation.table() != searchGroup.table()
                    || operation.workUnitId() != searchGroup.workUnitId() || operation.readTimestamp() != searchGroup.readTimestamp()
                    || operation.version() != searchGroup.version() || operation.expectedCount() != searchGroup.expectedCount()
                    || operation.expansionSize() != searchGroup.expansionSize() || operation.transactionId() != searchGroup.transactionId()
                    || operation.mutationOrdinal() != searchGroup.mutationOrdinal()
                    || (operation.flags() & shared) != (searchGroup.flags() & shared))
                throw new IOException("Inconsistent search group member");
        }
        if (++searchMembers == operation.expansionSize()) {
            searchGroup = null;
            searchMembers = 0;
        }
    }

    private void requireCompleteExpansion() throws IOException {
        if (mutation != null && expansionMembers != mutation.expansionSize())
            throw new IOException("Incomplete index maintenance unit");
    }

    void finish() throws IOException {
        if (transaction != 0 || searchGroup != null) throw new IOException("Incomplete source transaction or search group");
    }

    Map<String, Long> statistics() {
        return Map.of("excluded_aborted_writes", abortedWrites, "excluded_local_search_members", localSearches,
                "coalesced_writes", coalescedWrites, "peak_buffered_operations", peakBuffered);
    }
}
