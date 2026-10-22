package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.*;

/** One source row modification expands to a complete, contiguous index maintenance unit. */
public final class IndexMutation {
    private IndexMutation() {}

    public static void emit(
            Operation primary,
            Map<Integer, IndexKey> before,
            Map<Integer, IndexKey> after,
            IndexOperationTrace.Consumer consumer)
            throws IOException {
        if (primary.index() != 0
                || !Set.of(
                                IndexOperationTrace.INSERT,
                                IndexOperationTrace.UPSERT,
                                IndexOperationTrace.LOGICAL_DELETE,
                                IndexOperationTrace.RELOCATE)
                        .contains(primary.kind())
                || primary.indexedValue() != null
                || before.keySet().stream().anyMatch(index -> index < 1)
                || after.keySet().stream().anyMatch(index -> index < 1))
            throw new IOException("Index maintenance requires a primary row write");
        if ((primary.kind() == IndexOperationTrace.INSERT
                                || primary.kind() == IndexOperationTrace.RELOCATE)
                        && !before.isEmpty()
                || primary.kind() == IndexOperationTrace.LOGICAL_DELETE && !after.isEmpty())
            throw new IOException("Invalid baseline/delete/relocation index values");
        var operations = new ArrayList<Operation>();
        operations.add(primary);
        var indexes = new TreeSet<Integer>();
        indexes.addAll(before.keySet());
        indexes.addAll(after.keySet());
        for (int index : indexes) {
            boolean removed =
                    before.containsKey(index)
                            && (!after.containsKey(index)
                                    || !Objects.equals(before.get(index), after.get(index)));
            if (removed)
                operations.add(
                        primary.withIndex(
                                index, before.get(index), IndexOperationTrace.LOGICAL_DELETE));
            if (after.containsKey(index))
                operations.add(primary.withIndex(index, after.get(index), primary.kind()));
        }
        for (int position = 0; position < operations.size(); position++)
            consumer.accept(operations.get(position).withExpansion(position, operations.size()));
    }
}
