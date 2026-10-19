package org.sibench.generation;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.function.LongFunction;

/** Row ordinals are zero-based within a table; primary keys retain source semantics. */
public record TableData(
        String name,
        long rows,
        long rowIdStart,
        String primaryKey,
        MessageType schema,
        LongFunction<Group> row) {
    public static long totalRows(List<TableData> tables) {
        long total = 0;
        for (TableData table : tables) total = Math.addExact(total, table.rows());
        return total;
    }
}
