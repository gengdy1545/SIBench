package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChangeKindsTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private static final Map<String, Object> SYSBENCH =
            Map.of("tables", 1, "create-secondary", true, "auto-inc", true);

    private static SourceSchema sysbench() throws IOException {
        return SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), SYSBENCH);
    }

    private static SourceSchema ycsb() throws IOException {
        return SourceSchema.of(
                "ycsb",
                Path.of("benchmark/YCSB"),
                Map.of("fieldcount", 2, "table", "usertable"));
    }

    private static JsonObject event(String type) {
        var result = new JsonObject();
        result.addProperty("event", type);
        return result;
    }

    private static JsonObject row(String table, String operation, String old, String updated) {
        var result = event("row_change");
        result.addProperty("relation", table);
        result.addProperty("operation", operation);
        result.add("old", old == null ? null : JsonParser.parseString(old));
        result.add("new", updated == null ? null : JsonParser.parseString(updated));
        return result;
    }

    private static String sbtest(long id, long k, String c) {
        return String.format(
                "{\"id\":\"%d\",\"k\":\"%d\",\"c\":\"%s\",\"pad\":\"pad\"}", id, k, c);
    }

    @Test
    public void anUpdateOfTheSecondaryIndexColumnCountsDifferentlyFromAnOrdinaryOne()
            throws Exception {
        var events = new ArrayList<JsonObject>();
        // k is the only secondary index column; c and pad are ordinary.
        events.add(
                row("sbtest1", "update", sbtest(1, 10, "aaaa"), sbtest(1, 11, "aaaa")));
        events.add(event("transaction_commit"));
        events.add(row("sbtest1", "update", sbtest(2, 10, "aaaa"), sbtest(2, 10, "bbbb")));
        events.add(event("transaction_commit"));
        // A rewritten value that equals the old one is not a change of the indexed column.
        events.add(row("sbtest1", "update", sbtest(3, 10, "aaaa"), sbtest(3, 10, "aaaa")));
        events.add(event("transaction_commit"));
        var report = ChangeKinds.count(sysbench(), events);
        assertEquals(1L, report.total().indexColumnUpdates());
        assertEquals(2L, report.total().ordinaryColumnUpdates());
        assertEquals(0L, report.total().deletes());
        assertEquals(0L, report.total().deleteThenReinsert());
        assertEquals(0L, report.total().inserts());
        assertEquals(
                1L, report.byTable().get("sbtest1").indexColumnUpdates());
    }

    @Test
    public void aDeleteFollowedByAReinsertOfTheSameKeyIsItsOwnMaintenancePath() throws Exception {
        var events = new ArrayList<JsonObject>();
        events.add(row("sbtest1", "delete", sbtest(7, 10, "aaaa"), null));
        events.add(row("sbtest1", "insert", null, sbtest(7, 12, "cccc")));
        events.add(event("transaction_commit"));
        // A delete that nothing re-inserts stays an ordinary delete.
        events.add(row("sbtest1", "delete", sbtest(8, 10, "aaaa"), null));
        events.add(event("transaction_commit"));
        events.add(row("sbtest1", "insert", null, sbtest(99, 10, "aaaa")));
        events.add(event("transaction_commit"));
        var report = ChangeKinds.count(sysbench(), events);
        assertEquals(1L, report.total().deleteThenReinsert());
        assertEquals(1L, report.total().deletes());
        assertEquals(2L, report.total().inserts());
        assertEquals(0L, report.total().indexColumnUpdates());
        assertEquals(0L, report.total().ordinaryColumnUpdates());
    }

    @Test
    public void anAbortedChangeIsNotPublished() throws Exception {
        var events = new ArrayList<JsonObject>();
        events.add(row("sbtest1", "update", sbtest(1, 10, "aaaa"), sbtest(1, 11, "aaaa")));
        events.add(event("transaction_abort"));
        events.add(row("sbtest1", "delete", sbtest(2, 10, "aaaa"), null));
        events.add(event("transaction_commit"));
        var report = ChangeKinds.count(sysbench(), events);
        assertEquals(0L, report.total().indexColumnUpdates());
        assertEquals(1L, report.total().deletes());
    }

    @Test
    public void unfinishedSourceRowChangesAreRejected() {
        var events = new ArrayList<JsonObject>();
        events.add(row("sbtest1", "update", sbtest(1, 10, "aaaa"), sbtest(1, 11, "aaaa")));
        assertThrows(IOException.class, () -> ChangeKinds.count(sysbench(), events));
    }

    @Test
    public void aPrimaryKeyRewriteIsOutsideTheStandardWorkload() {
        var events = new ArrayList<JsonObject>();
        events.add(row("sbtest1", "update", sbtest(1, 10, "aaaa"), sbtest(2, 10, "aaaa")));
        events.add(event("transaction_commit"));
        assertTrue(
                assertThrows(IOException.class, () -> ChangeKinds.count(sysbench(), events))
                        .getMessage()
                        .contains("Primary key value UPDATE"));
    }

    @Test
    public void ycsbHasNoSecondaryIndexSoEveryUpdateIsOrdinary() throws Exception {
        var events = new ArrayList<JsonObject>();
        events.add(
                row(
                        "usertable",
                        "update",
                        "{\"ycsb_key\":\"user1\",\"field0\":\"a\",\"field1\":\"b\"}",
                        "{\"ycsb_key\":\"user1\",\"field0\":\"a\",\"field1\":\"z\"}"));
        events.add(event("transaction_commit"));
        var report = ChangeKinds.count(ycsb(), events);
        assertEquals(0L, report.total().indexColumnUpdates());
        assertEquals(1L, report.total().ordinaryColumnUpdates());
        assertEquals(Map.of("usertable", report.total()), report.byTable());
    }

    @Test
    public void theReportSeparatesTablesAndDescribesEveryPath() throws Exception {
        var events = new ArrayList<JsonObject>();
        events.add(row("sbtest1", "update", sbtest(1, 10, "aaaa"), sbtest(1, 11, "aaaa")));
        events.add(row("sbtest1", "delete", sbtest(2, 10, "aaaa"), null));
        events.add(row("sbtest1", "insert", null, sbtest(2, 13, "dddd")));
        events.add(event("transaction_commit"));
        var described = ChangeKinds.count(sysbench(), events).describe();
        @SuppressWarnings("unchecked")
        var total = (Map<String, Object>) described.get("total");
        assertEquals(1L, total.get("index_column_updates"));
        assertEquals(1L, total.get("delete_then_reinsert"));
        assertEquals(0L, total.get("deletes"));
        assertEquals(1L, total.get("inserts"));
        @SuppressWarnings("unchecked")
        var byTable = (Map<String, Object>) described.get("by_table");
        assertEquals(java.util.Set.of("sbtest1"), byTable.keySet());
    }

    @Test
    public void aRawTraceIsCountedInFileOrder() throws Exception {
        Path trace = folder.newFolder("raw").toPath().resolve("pg-1.jsonl");
        var lines = new ArrayList<String>();
        lines.add(row("sbtest1", "delete", sbtest(5, 10, "aaaa"), null).toString());
        lines.add(event("transaction_commit").toString());
        // A delete that a later transaction re-inserts is paired even across transactions, but a
        // different key is not: file order is what the classifier reads.
        lines.add(row("sbtest1", "insert", null, sbtest(6, 10, "eeee")).toString());
        lines.add(event("transaction_commit").toString());
        Files.write(trace, lines);
        var report = ChangeKinds.countTraces(sysbench(), List.of(trace));
        assertEquals(1L, report.total().deletes());
        assertEquals(1L, report.total().inserts());
        assertEquals(0L, report.total().deleteThenReinsert());
    }
}
