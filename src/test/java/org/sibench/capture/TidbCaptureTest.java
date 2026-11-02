package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.capture.TidbKeysTest.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * A hand-checkable capture: table 100 with rows (pk 1, k 10) and (pk 2, k 20) at the baseline
 * snapshot 1000. Transaction A (start 1100, commit 1200) moves pk 1 to k 30. Transaction B
 * (start 1300, commit 1400) moves pk 2 to k 50 on its retried attempt; its first attempt wrote
 * k 40 and was rolled back. Reads: an index range at 1250 sees (20, 2, v0) and (30, 1, v1); a
 * full index scan at 1500 sees (30, 1, v1) and (50, 2, v2); a point get of pk 1 at 1150 sees v0.
 */
public class TidbCaptureTest {
    static final long T0 = 1000;
    @Rule public TemporaryFolder temp = new TemporaryFolder();

    static String scan(String lo, String hi, boolean exhausted, String... entries) {
        return "{\"scan\":{\"lo\":\"" + lo + "\",\"hi\":\"" + hi + "\",\"desc\":false,\"exhausted\":" + exhausted
                + ",\"entries\":[" + String.join(",", entries) + "]}}";
    }

    static String entry(String key, long commitTs) {
        return "{\"k\":\"" + key + "\",\"v\":\"00\",\"vk\":1,\"vt\":" + commitTs + "}";
    }

    static String tikv(long task, long startTs, String... reads) {
        return "{\"kind\":\"cop_dag\",\"start_ts\":" + startTs + ",\"task_id\":" + task
                + ",\"region\":1,\"store\":1,\"executors\":\"TypeIndexScan\",\"reads\":[" + String.join(",", reads) + "]}";
    }

    static String stmt(long task, String sql) {
        return "{\"ev\":\"stmt_begin\",\"conn\":7,\"task\":" + task + ",\"sql\":\"" + sql + "\",\"plan\":\"\"}";
    }

    static String mutation(String ev, long task, long start, long index, long value, long handle) {
        String extra = ev.equals("index_put") ? ",\"untouched\":false" : "";
        return "{\"ev\":\"" + ev + "\",\"conn\":7,\"task\":" + task + ",\"start_ts\":" + start + ",\"table\":100,\"index\":"
                + index + ",\"values\":[\"" + value + "\"],\"handle\":\"" + handle + "\"" + extra + ",\"err\":\"\"}";
    }

    static String row(long task, long start, long handle) {
        return "{\"ev\":\"row\",\"conn\":7,\"task\":" + task + ",\"start_ts\":" + start + ",\"op\":\"update\",\"table\":100,\"handle\":\""
                + handle + "\",\"err\":\"\"}";
    }

    static String commit(long start, long commit) {
        return "{\"ev\":\"commit\",\"conn\":7,\"start_ts\":" + start + ",\"read_only\":false,\"info\":\"{\\\"start_ts\\\":" + start
                + ",\\\"commit_ts\\\":" + commit + "}\",\"err\":\"\"}";
    }

    static final String FULL_RECORD_LO = record(100, Long.MIN_VALUE), FULL_RECORD_HI = table(100) + "5f72ffffffffffffffff00";
    static final String INDEX_LO = table(100) + "5f69" + intHex(1) + "00", INDEX_HI = table(100) + "5f69" + intHex(1) + "fb";

    static String indexBound(long value) {
        return table(100) + "5f69" + intHex(1) + "03" + intHex(value);
    }

    List<String> tidb() {
        return new ArrayList<>(List.of(
                stmt(1, "baseline"),
                stmt(2, "UPDATE t SET k = 30 WHERE id = 1"),
                mutation("index_delete", 2, 1100, 1, 10, 1),
                mutation("index_put", 2, 1100, 1, 30, 1),
                row(2, 1100, 1),
                commit(1100, 1200),
                stmt(3, "UPDATE t SET k = k + 30 WHERE id = 2"),
                mutation("index_delete", 3, 1300, 1, 20, 2),
                mutation("index_put", 3, 1300, 1, 40, 2),
                row(3, 1300, 2),
                "{\"ev\":\"stmt_rollback\",\"conn\":7,\"task\":3,\"for_retry\":true}",
                "{\"ev\":\"stmt_retry\",\"conn\":7,\"old_task\":3,\"task\":4,\"for_update_ts\":1350,\"err\":\"conflict\"}",
                mutation("index_delete", 4, 1300, 1, 20, 2),
                mutation("index_put", 4, 1300, 1, 50, 2),
                row(4, 1300, 2),
                commit(1300, 1400),
                stmt(5, "range at 1250"),
                stmt(6, "full index at 1500"),
                stmt(7, "point at 1150"),
                "{\"ev\":\"cop\",\"task\":5,\"start_ts\":1250,\"tp\":103,\"desc\":false,\"ranges\":[[\"" + indexBound(0) + "\",\"" + indexBound(100) + "\"]]}"));
    }

    List<String> tikv() {
        return new ArrayList<>(List.of(
                tikv(1, T0, scan(FULL_RECORD_LO, FULL_RECORD_HI, true, entry(record(100, 1), 500), entry(record(100, 2), 500))),
                tikv(1, T0, scan(INDEX_LO, INDEX_HI, true, entry(index(100, 1, 10, 1), 500), entry(index(100, 1, 20, 2), 500))),
                tikv(5, 1250, scan(indexBound(0), indexBound(100), true, entry(index(100, 1, 20, 2), 500), entry(index(100, 1, 30, 1), 1200))),
                tikv(6, 1500, scan(INDEX_LO, INDEX_HI, true, entry(index(100, 1, 30, 1), 1200), entry(index(100, 1, 50, 2), 1400))),
                "{\"kind\":\"kv_get\",\"start_ts\":1150,\"task_id\":7,\"region\":1,\"store\":1,\"reads\":[{\"get\":"
                        + entry(record(100, 1), 500) + "}]}"));
    }

    Map<String, Object> convert(List<String> tidb, List<String> tikv) throws IOException {
        Path capture = temp.newFolder().toPath();
        Files.write(capture.resolve("tidb1.jsonl"), tidb);
        Files.write(capture.resolve("tikv-s1.jsonl"), tikv);
        Path schema = temp.getRoot().toPath().resolve("schema.json");
        if (!Files.exists(schema))
            Files.writeString(schema, "{\"tables\":[{\"id\":100,\"name\":\"t\",\"int_handle\":true,\"pk_types\":[\"int\"],"
                    + "\"indexes\":[{\"id\":1,\"name\":\"k\",\"unique\":false,\"types\":[\"int\"]}]}]}");
        return TidbCapture.convert(capture, schema, T0, capture.resolve("out"));
    }

    @SuppressWarnings("unchecked")
    static long mismatches(Map<String, Object> report) {
        return (Long) ((Map<String, Object>) report.get("oracle")).get("mismatches");
    }

    @SuppressWarnings("unchecked")
    static Map<String, Long> counts(Map<String, Object> report) {
        return (Map<String, Long>) report.get("counts");
    }

    @Test
    public void handCheckedCaptureIsConsistent() throws IOException {
        var report = convert(tidb(), tikv());
        assertEquals(report.toString(), "consistent", report.get("status"));
        assertEquals(0, mismatches(report));
        assertEquals(Long.valueOf(3), counts(report).get("searches"));
        assertEquals(2, report.get("committed_writer_transactions"));
        assertEquals(Long.valueOf(3), counts(report).get("discarded_statement_rollback_mutations"));
        assertEquals(Long.valueOf(1), counts(report).get("cop_ranges_covered"));
        assertEquals(4, report.get("baseline_operations"));
    }

    void assertDetected(UnaryOperator<List<String>> tidb, UnaryOperator<List<String>> tikv) throws IOException {
        var report = convert(tidb.apply(tidb()), tikv.apply(tikv()));
        assertEquals("inconsistent", report.get("status"));
        assertTrue(report.toString(), mismatches(report) > 0);
    }

    @Test
    public void detectsAMissedIndexMaintenance() throws IOException {
        assertDetected(t -> { t.remove(mutation("index_delete", 2, 1100, 1, 10, 1)); return t; }, t -> t);
    }

    @Test
    public void detectsAWrongReadPoint() throws IOException {
        // The point get of pk 1 read v0; claiming 1250 puts it after transaction A's commit.
        assertDetected(t -> t, t -> { t.replaceAll(l -> l.replace("\"start_ts\":1150", "\"start_ts\":1250")); return t; });
    }

    @Test
    public void detectsAWrongRowIdentity() throws IOException {
        assertDetected(t -> t, t -> {
            t.replaceAll(l -> l.replace(entry(index(100, 1, 50, 2), 1400), entry(index(100, 1, 50, 3), 1400)));
            return t;
        });
    }

    @Test
    public void detectsACountedRolledBackAttempt() throws IOException {
        assertDetected(t -> { t.removeIf(l -> l.contains("stmt_rollback")); return t; }, t -> t);
    }

    @Test
    public void rejectsATiDBRangeTiKVNeverRead() throws IOException {
        var tikv = tikv();
        tikv.remove(2);
        var error = assertThrows(IOException.class, () -> convert(tidb(), tikv));
        assertTrue(error.getMessage().contains("TiKV recorded no read"));
    }

    @Test
    public void rejectsAMaxTimestampMissWhoseReadPointIsUnobserved() throws IOException {
        var tikv = tikv();
        tikv.set(4, "{\"kind\":\"kv_get\",\"start_ts\":18446744073709551615,\"task_id\":7,\"region\":1,\"store\":1,\"reads\":"
                + "[{\"get\":{\"k\":\"" + record(100, 9) + "\",\"v\":null,\"vk\":0,\"vt\":0}}]}");
        var report = convert(tidb(), tikv);
        assertEquals("inconsistent", report.get("status"));
        assertEquals(Map.of("max_ts_point_get_miss_read_point_unobserved", 1L), report.get("rejected"));
    }

    @Test
    public void rejectsABoundBetweenEntriesOfOneIndexedValue() throws IOException {
        var tikv = tikv();
        tikv.set(3, tikv(6, 1500, scan(index(100, 1, 30, 1) + "00", INDEX_HI, true, entry(index(100, 1, 50, 2), 1400))));
        var report = convert(tidb(), tikv);
        assertEquals(Map.of("scan_bound_inside_equal_index_values", 1L), report.get("rejected"));
    }

    @Test
    public void joinsAPagingContinuationIntoOneScan() throws IOException {
        var tikv = tikv();
        tikv.set(3, tikv(6, 1500,
                scan(INDEX_LO, INDEX_HI, false, entry(index(100, 1, 30, 1), 1200)),
                scan(index(100, 1, 30, 1) + "00", INDEX_HI, true, entry(index(100, 1, 50, 2), 1400))));
        var report = convert(tidb(), tikv);
        assertEquals(report.toString(), "consistent", report.get("status"));
        assertEquals(Long.valueOf(1), counts(report).get("scans_joined_from_pieces"));
    }

    @Test
    public void acceptsARangeLeftUnreadAfterALimitStoppedTheScan() throws IOException {
        // TiDB asked for [0, 100); TiKV stopped after one entry of a Region ending at 25.
        var tikv = tikv();
        tikv.set(2, tikv(5, 1250, scan(indexBound(0), indexBound(25), false, entry(index(100, 1, 20, 2), 500))));
        var report = convert(tidb(), tikv);
        assertEquals(report.toString(), "consistent", report.get("status"));
        assertEquals(Long.valueOf(1), counts(report).get("cop_ranges_covered_until_scan_stopped"));
    }

    @Test
    public void commitsTheTransactionTiDBRetriedAtCommit() throws IOException {
        // TiDB re-executed transaction A as start 1110: its first attempt (1100) never commits.
        var tidb = tidb();
        tidb.replaceAll(l -> l.equals(commit(1100, 1200)) ? commit(1100, 1200).replace("\\\"start_ts\\\":1100", "\\\"start_ts\\\":1110") : l);
        tidb.add(tidb.indexOf(commit(1300, 1400)) - 9, mutation("index_delete", 2, 1110, 1, 10, 1));
        tidb.add(tidb.indexOf(commit(1300, 1400)) - 9, mutation("index_put", 2, 1110, 1, 30, 1));
        tidb.add(tidb.indexOf(commit(1300, 1400)) - 9, row(2, 1110, 1));
        var report = convert(tidb, tikv());
        assertEquals(report.toString(), "consistent", report.get("status"));
        assertEquals(Long.valueOf(1), counts(report).get("transaction_level_retries"));
    }
}
