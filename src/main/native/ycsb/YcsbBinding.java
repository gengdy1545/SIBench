package org.sibench.capture;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/** Thin bridge compiled against the pinned upstream client; workload semantics live in the core. */
public final class YcsbBinding extends DB {
    private RocksYcsb.Session api;
    private String table;

    @Override public void init() throws DBException {
        try {
            table = required("table");
            getProperties().put("sibench.stop", (java.util.function.BooleanSupplier) RocksYcsb::stopRequested);
            api = RocksYcsb.open(Boolean.parseBoolean(getProperties().getProperty("sibench.baseline", "false")),
                    Integer.parseInt(required("fieldcount")));
        } catch (Exception failure) { throw new DBException(failure); }
    }

    private String required(String name) {
        String value = getProperties().getProperty(name);
        if (value == null || value.isEmpty()) throw new IllegalArgumentException("Missing YCSB property: " + name);
        return value;
    }

    @Override public void cleanup() throws DBException {
        try { api.close(); } catch (Exception failure) { throw new DBException(failure); }
    }

    private void table(String requested) {
        if (!table.equals(requested)) throw new IllegalArgumentException("Unexpected YCSB table: " + requested);
    }

    private static Status status(RocksYcsb.Status value) {
        return switch (value) { case OK -> Status.OK; case NOT_FOUND -> Status.NOT_FOUND; case ERROR -> Status.ERROR; };
    }

    private static HashMap<String, ByteIterator> fields(RocksYcsb.Row row) {
        var result = new HashMap<String, ByteIterator>();
        row.fields().forEach((name, value) -> {
            if (value != null) result.put(name, new ByteArrayByteIterator(value));
        });
        return result;
    }

    private static Map<String, byte[]> bytes(Map<String, ByteIterator> values) {
        var result = new HashMap<String, byte[]>();
        values.forEach((name, value) -> result.put(name, value.toArray()));
        return result;
    }

    @Override public Status read(String table, String key, Set<String> selected, Map<String, ByteIterator> result) {
        table(table);
        try {
            var read = api.read(key, selected, 0);
            if (!read.rows().isEmpty()) result.putAll(fields(read.rows().get(0)));
            return status(read.status());
        } catch (Exception failure) { throw new IllegalStateException("YCSB read failed", failure); }
    }

    @Override public Status scan(String table, String start, int count, Set<String> selected,
            Vector<HashMap<String, ByteIterator>> result) {
        table(table);
        if (count <= 0) throw new IllegalArgumentException("Scan count must be positive");
        try {
            var scan = api.read(start, selected, count);
            for (var row : scan.rows()) result.add(fields(row));
            return status(scan.status());
        } catch (Exception failure) { throw new IllegalStateException("YCSB scan failed", failure); }
    }

    private Status change(String table, String key, Map<String, ByteIterator> values, String kind) {
        table(table);
        try { return status(api.change(kind, key, bytes(values))); }
        catch (Exception failure) { throw new IllegalStateException("YCSB " + kind + " failed", failure); }
    }
    @Override public Status update(String table, String key, Map<String, ByteIterator> values) {
        return change(table, key, values, "update");
    }
    @Override public Status insert(String table, String key, Map<String, ByteIterator> values) {
        return change(table, key, values, "insert");
    }
    @Override public Status delete(String table, String key) {
        return change(table, key, Map.of(), "delete");
    }
}
