package org.sibench.capture;

import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

/**
 * The final rows of a source table as an order-independent digest: their count and the sum of
 * each row's SHA256 in four 64-bit lanes, over the comparable values {@link SourceRow#values}
 * gives (column order, nulls marked, lengths prefixed). The row audit compares the rows the
 * capture says the table now holds with PostgreSQL's table this way, so the table is read in
 * physical order by block ranges in parallel: an ordered scan of a table whose key does not follow
 * its heap (YCSB's hashed keys) was an external sort of the whole table.
 */
final class FinalState {
    private FinalState() {}

    /** Rows and the lane sums of their digests; equal digests are equal multisets of rows. */
    static final class Digest {
        private long rows;
        private final long[] lanes = new long[4];
        private final MessageDigest sha;
        private byte[] buffer = new byte[1 << 12];

        Digest() {
            try {
                sha = MessageDigest.getInstance("SHA-256");
            } catch (java.security.NoSuchAlgorithmException error) {
                throw new IllegalStateException(error);
            }
        }

        long rows() {
            return rows;
        }

        void add(SourceSchema.Table table, Map<String, String> values) {
            int length = 4;
            var columns = table.columns();
            var texts = new byte[columns.size()][];
            for (int i = 0; i < columns.size(); i++) {
                String value = values.get(columns.get(i).name());
                texts[i] = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
                length += 1 + (texts[i] == null ? 0 : 4 + texts[i].length);
            }
            if (buffer.length < length) buffer = new byte[Math.max(length, buffer.length * 2)];
            var out = java.nio.ByteBuffer.wrap(buffer);
            out.putInt(values.size());
            for (byte[] text : texts) {
                if (text == null) out.put((byte) 0);
                else out.put((byte) 1).putInt(text.length).put(text);
            }
            sha.update(buffer, 0, length);
            var hash = java.nio.ByteBuffer.wrap(sha.digest());
            for (int i = 0; i < lanes.length; i++) lanes[i] += hash.getLong();
            rows++;
        }

        void addAll(Digest other) {
            rows += other.rows;
            for (int i = 0; i < lanes.length; i++) lanes[i] += other.lanes[i];
        }

        boolean same(Digest other) {
            return rows == other.rows && java.util.Arrays.equals(lanes, other.lanes);
        }

        @Override
        public String toString() {
            var text = new StringBuilder(rows + " rows, digest ");
            for (long lane : lanes) text.append(String.format("%016x", lane));
            return text.toString();
        }
    }

    /** Workers that scan block ranges: many tasks per worker, so ranges even out. */
    static int threads() {
        return Integer.getInteger(
                "sibench.audit.scan_threads", Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
    }

    /**
     * PostgreSQL's rows of the table under the snapshot of {@code main}, a read-only repeatable-read
     * transaction. With {@code workers}, block ranges are read by connections importing that
     * snapshot; otherwise, or for a table of few blocks, {@code main} reads the whole table.
     */
    static Digest scan(Connection main, PgBaselineVerify.Connections workers, SourceSchema.Table table)
            throws Exception {
        String name = "\"" + table.name().replace("\"", "\"\"") + "\"";
        long blocks;
        try (Statement statement = main.createStatement();
                ResultSet size =
                        statement.executeQuery(
                                "SELECT pg_relation_size('" + name.replace("'", "''")
                                        + "'::regclass) / current_setting('block_size')::bigint")) {
            size.next();
            blocks = size.getLong(1);
        }
        int threads = threads();
        int tasks = (int) Math.min(blocks / 64, threads * 8L);
        if (workers == null || tasks <= 1) return rows(main, table, "SELECT * FROM " + name);
        String snapshot;
        try (Statement statement = main.createStatement();
                ResultSet rows = statement.executeQuery("SELECT pg_export_snapshot()")) {
            rows.next();
            snapshot = rows.getString(1);
        }
        var queries = new ArrayList<String>();
        for (int i = 0; i < tasks; i++) {
            long low = blocks * i / tasks, high = blocks * (i + 1) / tasks;
            var where = new ArrayList<String>();
            // The first and last ranges are open, so rows past the size read above still count.
            if (i > 0) where.add("ctid >= '(" + low + ",0)'::tid");
            if (i < tasks - 1) where.add("ctid < '(" + high + ",0)'::tid");
            queries.add("SELECT * FROM " + name + " WHERE " + String.join(" AND ", where));
        }
        var pool = java.util.concurrent.Executors.newFixedThreadPool(Math.min(threads, tasks));
        var opened = new java.util.concurrent.ConcurrentLinkedQueue<Connection>();
        var own = ThreadLocal.<Connection>withInitial(() -> null);
        try {
            var futures = new ArrayList<java.util.concurrent.Future<Digest>>();
            for (String query : queries)
                futures.add(pool.submit(() -> {
                    Connection pg = own.get();
                    if (pg == null) {
                        pg = workers.open();
                        opened.add(pg);
                        pg.setAutoCommit(false);
                        pg.setReadOnly(true);
                        pg.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                        try (Statement statement = pg.createStatement()) {
                            statement.execute("SET TRANSACTION SNAPSHOT '" + snapshot + "'");
                        }
                        own.set(pg);
                    }
                    return rows(pg, table, query);
                }));
            var result = new Digest();
            for (var future : futures)
                try {
                    result.addAll(future.get());
                } catch (java.util.concurrent.ExecutionException failed) {
                    if (failed.getCause() instanceof Exception error) throw error;
                    throw failed;
                }
            return result;
        } finally {
            pool.shutdownNow();
            for (Connection pg : opened)
                try {
                    pg.rollback();
                    pg.close();
                } catch (SQLException ignored) {
                    // The scan's result or failure is what counts.
                }
        }
    }

    private static Digest rows(Connection pg, SourceSchema.Table table, String query) throws Exception {
        var digest = new Digest();
        try (Statement statement = pg.createStatement()) {
            statement.setFetchSize(8192);
            try (ResultSet rows = statement.executeQuery(query)) {
                while (rows.next()) digest.add(table, RowStateAudit.finalRow(table, rows));
            }
        }
        return digest;
    }
}
