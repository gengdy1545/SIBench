package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.config.ConfigValues;
import org.sibench.schema.SourceSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class FinalStateTest {
    private static final Pattern LOW = Pattern.compile("ctid >= '\\((\\d+),0\\)'"), HIGH = Pattern.compile("ctid < '\\((\\d+),0\\)'");

    /** A table of {@code rows} spread over {@code blocks} heap blocks, row i in block i % blocks. */
    private record Table(SourceSchema.Table schema, List<Map<String, String>> rows, long blocks) {}

    private static SourceSchema.Table usertable() throws Exception {
        var document = ConfigValues.parse(Files.readString(Path.of("src/test/resources/config/ycsb_1b.yaml")));
        var nativeConfig = ConfigValues.object(document, "native");
        nativeConfig.put("fieldcount", 2);
        document.put("native", nativeConfig);
        return SourceSchema.read(Path.of(""), document).tables().get(0);
    }

    private static Table table(int rows, long blocks) throws Exception {
        var schema = usertable();
        var data = new ArrayList<Map<String, String>>();
        for (int i = 0; i < rows; i++) {
            var row = new LinkedHashMap<String, String>();
            for (var column : schema.columns())
                row.put(column.name(), column.name().equals("ycsb_key") ? "user" + (7919L * i % 100_003) : i % 7 == 0 ? null : "v" + i);
            data.add(row);
        }
        return new Table(schema, data, blocks);
    }

    /** Block ranges read in parallel give the digest of the whole table, each row once. */
    @Test
    public void parallelBlockRangesDigestEveryRowOnce() throws Exception {
        var table = table(20_000, 5_000);
        var whole = new FinalState.Digest();
        for (var row : table.rows()) whole.add(table.schema(), row);
        int[] imported = {0};
        String previous = System.getProperty("sibench.audit.scan_threads");
        System.setProperty("sibench.audit.scan_threads", "6");
        try {
            var digest = FinalState.scan(connection(table, false, imported), () -> connection(table, true, imported), table.schema());
            assertTrue(digest + " vs " + whole, digest.same(whole));
            assertEquals(20_000, digest.rows());
            assertTrue(imported[0] > 1);
            // A table of few blocks is read whole on the main connection.
            imported[0] = 0;
            var small = table(50, 3);
            var expected = new FinalState.Digest();
            for (var row : small.rows()) expected.add(small.schema(), row);
            assertTrue(FinalState.scan(connection(small, false, imported), () -> { throw new AssertionError(); }, small.schema()).same(expected));
        } finally {
            if (previous == null) System.clearProperty("sibench.audit.scan_threads");
            else System.setProperty("sibench.audit.scan_threads", previous);
        }
    }

    /** One changed value, one row more or less: a different digest. */
    @Test
    public void aDifferentMultisetOfRowsHasAnotherDigest() throws Exception {
        var table = table(100, 1);
        var a = new FinalState.Digest();
        for (var row : table.rows()) a.add(table.schema(), row);
        var b = new FinalState.Digest();
        for (int i = table.rows().size() - 1; i >= 0; i--) b.add(table.schema(), table.rows().get(i));
        assertTrue(a.same(b));
        var changed = new LinkedHashMap<>(table.rows().get(3));
        changed.put("field0", changed.get("field0") + "x");
        var c = new FinalState.Digest();
        for (int i = 0; i < table.rows().size(); i++) c.add(table.schema(), i == 3 ? changed : table.rows().get(i));
        assertFalse(a.same(c));
        var nulled = new LinkedHashMap<>(table.rows().get(3));
        nulled.put("field1", null);
        var d = new FinalState.Digest();
        for (int i = 0; i < table.rows().size(); i++) d.add(table.schema(), i == 3 ? nulled : table.rows().get(i));
        assertFalse(a.same(d));
        var e = new FinalState.Digest();
        for (int i = 1; i < table.rows().size(); i++) e.add(table.schema(), table.rows().get(i));
        assertFalse(a.same(e));
    }

    private static Connection connection(Table table, boolean worker, int[] imported) {
        boolean[] snapshot = {!worker};
        return RowAuditTestSupport.proxy(Connection.class, (p, method, args) -> switch (method.getName()) {
            case "setAutoCommit", "setReadOnly", "setTransactionIsolation", "rollback", "close", "commit" -> null;
            case "createStatement" -> RowAuditTestSupport.proxy(Statement.class, (s, m, a) -> switch (m.getName()) {
                case "close", "setFetchSize" -> null;
                case "execute" -> {
                    assertEquals("SET TRANSACTION SNAPSHOT 'exported'", a[0]);
                    snapshot[0] = true;
                    imported[0]++;
                    yield false;
                }
                case "executeQuery" -> {
                    String sql = a[0].toString();
                    if (sql.startsWith("SELECT pg_relation_size(")) yield single(table.blocks());
                    if (sql.equals("SELECT pg_export_snapshot()")) {
                        assertFalse(worker);
                        yield single("exported");
                    }
                    assertTrue("a worker reads under the exported snapshot", snapshot[0]);
                    var low = LOW.matcher(sql);
                    var high = HIGH.matcher(sql);
                    long from = low.find() ? Long.parseLong(low.group(1)) : 0;
                    long to = high.find() ? Long.parseLong(high.group(1)) : Long.MAX_VALUE;
                    var selected = new ArrayList<Map<String, String>>();
                    for (int i = 0; i < table.rows().size(); i++) {
                        long block = i % table.blocks();
                        if (block >= from && block < to) selected.add(table.rows().get(i));
                    }
                    yield rows(selected);
                }
                default -> throw new AssertionError(m);
            });
            default -> throw new AssertionError(method);
        });
    }

    private static ResultSet single(Object value) {
        int[] cursor = {-1};
        return RowAuditTestSupport.proxy(ResultSet.class, (r, m, a) -> switch (m.getName()) {
            case "next" -> ++cursor[0] < 1;
            case "close" -> null;
            case "getLong" -> (Long) value;
            case "getString" -> (String) value;
            default -> throw new AssertionError(m);
        });
    }

    private static ResultSet rows(List<Map<String, String>> data) {
        int[] cursor = {-1};
        return RowAuditTestSupport.proxy(ResultSet.class, (r, m, a) -> switch (m.getName()) {
            case "next" -> ++cursor[0] < data.size();
            case "close" -> null;
            case "getString" -> data.get(cursor[0]).get(a[0].toString());
            default -> throw new AssertionError(m);
        });
    }
}
