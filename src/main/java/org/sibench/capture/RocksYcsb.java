package org.sibench.capture;

import java.io.IOException;
import java.util.*;
import org.sibench.generation.RocksDbRows;

/** In-process YCSB binding sessions; the upstream client still owns every workload decision. */
public final class RocksYcsb {
    public enum Status { OK, NOT_FOUND, ERROR }
    public record Row(String key, Map<String, byte[]> fields) {}
    public record Result(Status status, List<Row> rows) {}
    private static RocksYcsb active;
    private final RocksDbRows rows;
    private final RocksDbRows.Baseline baseline;
    private final int fieldCount;
    private long opened, completed, calls;
    private boolean loading;
    private RequestBudget budget;

    void budget(RequestBudget value) { budget=value; }
    public static synchronized boolean stopRequested() {
        return active != null && !active.loading && active.budget != null && active.budget.reached();
    }

    RocksYcsb(RocksDbRows rows, RocksDbRows.Baseline baseline, int fieldCount) {
        this.rows = rows; this.baseline = baseline; this.fieldCount = fieldCount;
    }
    static synchronized void install(RocksYcsb binding) {
        if (binding != null && active != null) throw new IllegalStateException("A YCSB source is already running");
        active = binding;
    }
    synchronized void stage(boolean load) { loading = load; opened = 0; completed = 0; calls = 0; }
    synchronized long completed(long expectedThreads) throws IOException {
        if (opened != expectedThreads || completed != opened) throw new IOException("Incomplete YCSB binding sessions");
        return calls;
    }
    public static synchronized Session open(boolean baseline, int fields) {
        if (active == null || baseline != active.loading || fields != active.fieldCount)
            throw new IllegalStateException("YCSB binding does not match its generation stage");
        synchronized (active) { active.opened++; }
        return active.new Session();
    }
    public final class Session implements AutoCloseable {
        private final ArrayList<RocksDbRows.Row> buffered = new ArrayList<>();
        private long query;
        private int bufferedBytes;
        private boolean closed;
        private Exception failure;

        public Result read(String key, Set<String> fields, int limit) throws Exception {
            requireWorkload(); var selected = selected(fields);
            var found = rows.read(key, limit); query++;
            var result = new ArrayList<Row>(found.size());
            for (var row : found) {
                var values = new LinkedHashMap<String, byte[]>();
                for (int field : selected) values.put("field" + field, row.fields()[field]);
                result.add(new Row(row.key(), values));
            }
            return new Result(limit == 0 && result.isEmpty() ? Status.NOT_FOUND : Status.OK, result);
        }
        public Status change(String kind, String key, Map<String, byte[]> values) throws Exception {
            if (closed || failure != null) throw new IllegalStateException("YCSB session is closed or failed", failure);
            try {
            var updates = new LinkedHashMap<Integer, byte[]>();
            for (var field : values.entrySet()) updates.put(field(field.getKey()), Objects.requireNonNull(field.getValue()));
            if (kind.equals("update") && updates.isEmpty()) throw new IllegalArgumentException("Empty YCSB update");
            if (loading) {
                if (!kind.equals("insert")) throw new IllegalStateException("Only inserts are allowed during baseline load");
                if (updates.size() != fieldCount) throw new IllegalArgumentException("Baseline insert requires every field");
                var fields = new byte[fieldCount][];
                for (var field : updates.entrySet()) { fields[field.getKey()] = field.getValue().clone(); bufferedBytes = Math.addExact(bufferedBytes, field.getValue().length); }
                buffered.add(new RocksDbRows.Row(0, key, fields)); query++;
                if (buffered.size() >= 512 || bufferedBytes >= 1 << 20) flush();
                return Status.OK;
            }
            var changed = rows.change(kind, key, updates); query++;
            return changed != null ? Status.OK : kind.equals("insert") ? Status.ERROR : Status.NOT_FOUND;
            } catch (Exception error) { failure = error; throw error; }
        }
        private void flush() throws Exception {
            if (buffered.isEmpty()) return;
            rows.load(buffered, baseline); buffered.clear(); bufferedBytes = 0;
        }
        private void requireWorkload() {
            if (closed || loading) throw new IllegalStateException("YCSB workload session is not active");
        }
        @Override public void close() throws Exception {
            if (closed) throw new IllegalStateException("YCSB session already closed");
            if (failure != null) throw new IOException("YCSB session failed", failure);
            flush(); closed = true;
            synchronized (RocksYcsb.this) { completed++; calls += query; }
        }
    }
    private int[] selected(Set<String> selected) {
        if (selected == null) return java.util.stream.IntStream.range(0, fieldCount).toArray();
        return selected.stream().mapToInt(this::field).sorted().toArray();
    }
    private int field(String name) {
        if (!name.startsWith("field")) throw new IllegalArgumentException("Unknown YCSB field: " + name);
        int index = Integer.parseInt(name.substring(5));
        if (index < 0 || index >= fieldCount || !name.equals("field" + index)) throw new IllegalArgumentException("Unknown YCSB field: " + name);
        return index;
    }
}
