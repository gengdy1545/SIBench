package org.sibench.analysis;

import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;
import org.sibench.adapters.IndexBackend;
import org.sibench.adapters.TraceRequests;
import org.sibench.adapters.ValueKind;
import org.sibench.adapters.VersionedIndexBackend;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexCatalog;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.config.Digests;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayInput;
import org.sibench.state.ExternalSort;
import org.sibench.state.ByteKeys;
import org.sibench.state.SpillingMap;
import org.sibench.trace.IndexEntry;
import org.sibench.trace.IndexKey;
import org.sibench.trace.TraceManifest;

/** Exact returned-row locality in Trace request order, not physical engine I/O. */
public final class TraceAccesses {
    private TraceAccesses() {}

    public static void run(Path trace, Path output, long windowRequests, long sortBytes)
            throws IOException {
        trace = trace.toRealPath();
        output = output.toAbsolutePath().normalize();
        Path ancestor = output;
        while (!Files.exists(ancestor)) ancestor = ancestor.getParent();
        output = ancestor.toRealPath().resolve(ancestor.relativize(output)).normalize();
        if (output.startsWith(trace) || trace.startsWith(output))
            throw new IOException("Analysis output must not overlap its Trace");
        if (windowRequests < 1 || sortBytes < 1)
            throw new IllegalArgumentException("Positive window and sort budgets required");
        var manifest = TraceManifest.read(trace);
        var tables = IndexCatalog.fromManifest(manifest).definitions().stream()
                .filter(d -> d.indexId() == 0).toList();
        String identity = Digests.sha256(trace.resolve("manifest.json"));
        TraceManifest.requireValue(manifest, ValueKind.ROW_ID);
        var input = ReplayInput.prepare(trace, manifest,
                new ReplayConfig(TraceManifest.secondaryIndexes(manifest), 0, 0, 0, 0, "operation", 1, 4096, 60, 1));
        Files.createDirectories(output);
        Files.deleteIfExists(output.resolve("summary.json"));
        var workingRoot = Path.of("build/sibench/analysis");
        Files.createDirectories(workingRoot);
        Path working = Files.createTempDirectory(workingRoot, "trace-accesses-");
        try {
            var result = analyze(input.baseline(), input.workload(), working, output,
                    tables, windowRequests, sortBytes);
            result.put("manifest_sha256", identity);
            result.put("baseline_trace_sha256", manifest.get("baseline_trace_sha256").getAsString());
            result.put("workload_trace_sha256", manifest.get("workload_trace_sha256").getAsString());
            result.put("baseline_rows", manifest.get("baseline_rows").getAsLong());
            result.put("secondary_index_coverage", manifest.get("secondary_index_coverage").getAsString());
            if (!identity.equals(Digests.sha256(trace.resolve("manifest.json"))))
                throw new IOException("Trace manifest changed during analysis");
            var counts = (Map<?, ?>) result.get("operation_counts");
            for (String kind : List.of("get", "scan", "put", "delete")) {
                long recorded = 0;
                var stats = manifest.getAsJsonObject("proto_files").getAsJsonObject("workload")
                        .getAsJsonObject("statistics").getAsJsonObject("index_operations");
                for (String scope : List.of("primary", "secondary")) {
                    var value = stats.getAsJsonObject(scope).get(kind);
                    if (value != null) recorded = Math.addExact(recorded, value.getAsLong());
                }
                if (!Long.valueOf(recorded).equals(counts.get(kind)))
                    throw new IOException("Manifest operation count mismatch: " + kind);
            }
            removeWorking(working);
            Files.writeString(output.resolve("summary.json"),
                    new GsonBuilder().setPrettyPrinting().create().toJson(result) + "\n");
        } catch (IOException | RuntimeException error) {
            throw new IOException("Access analysis failed; diagnostics: " + working, error);
        }
    }

    @FunctionalInterface
    private interface Handler {
        void accept(TraceOperation request, List<Operation> views) throws IOException;
    }

    private static IndexOperationTrace.Consumer records(Handler handler) {
        return new IndexOperationTrace.Consumer() {
            @Override public void accept(Operation operation) throws IOException {
                throw new IOException("Access analysis requires stored Trace payloads");
            }
            @Override public void accept(TraceRecord record, List<Operation> views) throws IOException {
                if (record.hasOperation()) handler.accept(record.getOperation(), views);
            }
        };
    }

    private static byte[] primary(Operation operation) {
        return new IndexEntry(operation.table(), 0, null, operation.key()).bytes().toByteArray();
    }

    private static byte[] row(int table, ByteString value) throws IOException {
        if (value.size() != 8) throw new IOException("Expected a RowID result");
        return ByteBuffer.allocate(12).putInt(table).put(value.asReadOnlyByteBuffer()).array();
    }

    private static void collect(Operation operation, ExternalSort rows) throws IOException {
        if ((operation.kind() == IndexOperationTrace.INSERT || IndexOperationTrace.isMutation(operation))
                && operation.kind() != IndexOperationTrace.LOGICAL_DELETE)
            rows.add(row(operation.table(), IndexEntry.rowId(operation.rowId())), primary(operation));
        for (Operation local : operation.localWrites()) collect(local, rows);
    }

    /** Analyzes baseline and workload sources that have already passed replay preflight. */
    static Map<String, Object> analyze(IndexOperationTrace.Source baseline,
            IndexOperationTrace.Source workload, Path working, Path output,
            List<IndexCatalog.Definition> tables, long windowRequests, long sortBytes) throws IOException {
        Files.createDirectories(working);
        Files.createDirectories(output);
        try (var backend = VersionedIndexBackend.create(working.resolve("index"), ValueKind.ROW_ID);
                var rowSort = new ExternalSort(working.resolve("rows"), sortBytes);
                var rowKeys = new SpillingMap(working.resolve("row-keys"), 65536, 8)) {
            var writes = new ArrayList<Mutation>();
            var loader = records((request, views) -> {
                for (var view : views) collect(view, rowSort);
                if (request.hasPut()) writes.add(Mutation.newBuilder().setPut(request.getPut()).build());
                if (request.hasDelete()) writes.add(Mutation.newBuilder().setDelete(request.getDelete()).build());
                if (writes.size() >= 1024) { backend.writeBatch(writes); writes.clear(); }
            });
            baseline.read(loader);
            workload.read(loader);
            backend.writeBatch(writes);
            try (var sorted = rowSort.sorted(); var bulk = rowKeys.bulk()) {
                byte[] previous = null, primary = null;
                while (sorted.hasNext()) {
                    byte[] key = sorted.peekKey(), value = sorted.peekValue();
                    if (!Arrays.equals(previous, key)) {
                        bulk.append(key, value); previous = key; primary = value;
                    } else if (!Arrays.equals(primary, value)) {
                        throw new IOException("One table/RowID maps to different primary keys");
                    }
                    sorted.advance();
                }
                bulk.finish();
            }
            var hotKeys = new LinkedHashMap<ByteString, byte[]>(65536, 0.75f, true) {
                @Override protected boolean removeEldestEntry(Map.Entry<ByteString, byte[]> eldest) {
                    return size() > 65536;
                }
            };
            try (var density = new Density(working, output, tables, windowRequests, sortBytes)) {
                workload.read(records((request, views) -> {
                    if (request.hasReadGroup()) {
                        var group = request.getReadGroup();
                        Path groupDirectory = working.resolve("group-" + density.sequence);
                        try (var members = new SpillingMap(groupDirectory, 4096, 8)) {
                            for (int i = 0; i < group.getRequestsCount(); i++)
                                read(backend, TraceRequests.read(group.getRequests(i)), views.get(i),
                                        rowKeys, hotKeys, density, members, i);
                            long combined = 0;
                            try (var entries = members.scan(null, null, false)) {
                                while (entries.next()) {
                                    long seen = ByteBuffer.wrap(entries.value()).getLong();
                                    if (group.getCombineMode() == ReadGroup.CombineMode.UNION
                                            || seen == group.getRequestsCount()) combined++;
                                }
                            }
                            requireCount(combined, request.getMetadata().getExpectedCount());
                        }
                        if (Files.exists(groupDirectory)) removeWorking(groupDirectory);
                    } else if (request.hasGet() || request.hasScan()) {
                        long hits = read(backend, request.hasGet() ? request.getGet() : request.getScan(),
                                views.get(0), rowKeys, hotKeys, density, null, 0);
                        requireCount(hits, request.getMetadata().getExpectedCount());
                    } else {
                        int kind = request.hasPut() ? 2 : 3;
                        density.begin(kind);
                        density.hit(kind, primary(views.get(0)));
                    }
                }));
                density.write();
                var result = density.describe();
                result.put("sort_memory_bytes", sortBytes);
                return result;
            }
        }
    }

    private static long read(IndexBackend backend, Message request, Operation operation,
            SpillingMap keys, Map<ByteString, byte[]> hotKeys, Density density,
            SpillingMap members, int member) throws IOException {
        int kind = request instanceof GetRequest ? 0 : 1;
        density.begin(kind);
        long[] hits = {0};
        IndexBackend.ValueSink sink = value -> {
            var identity = ByteString.copyFrom(row(operation.table(), value));
            byte[] key = hotKeys.get(identity);
            if (key == null) {
                key = keys.getBytes(identity.toByteArray());
                if (key == null) throw new IOException("Result has no primary key");
                hotKeys.put(identity, key);
            }
            density.hit(kind, key);
            hits[0]++;
            if (members != null) {
                byte[] rowId = value.toByteArray(), old = members.getBytes(rowId);
                long seen = old == null ? 0 : ByteBuffer.wrap(old).getLong();
                long last = old == null ? -1 : ByteBuffer.wrap(old).getLong(8);
                if (last != member)
                    members.putBytes(rowId, ByteBuffer.allocate(16).putLong(seen + 1).putLong(member).array());
            }
        };
        if (request instanceof GetRequest get) {
            var value = backend.get(get);
            if (value != null) sink.accept(value);
        } else backend.scan((ScanRequest) request, sink);
        return hits[0];
    }

    private static void requireCount(long actual, long expected) throws IOException {
        if (actual != expected)
            throw new IOException("Query result count mismatch: expected " + expected + ", found " + actual);
    }

    private static final class Density implements AutoCloseable {
        private final Path output;
        private final ExternalSort counts;
        private final Map<Integer, IndexCatalog.Definition> tables = new TreeMap<>();
        private final long windowRequests;
        private final long[] requests = new long[4], hits = new long[4];
        private long sequence, window;

        Density(Path working, Path output, List<IndexCatalog.Definition> tables,
                long windowRequests, long sortBytes) {
            this.output = output;
            this.windowRequests = windowRequests;
            for (var table : tables) this.tables.put(table.tableId(), table);
            counts = new ExternalSort(working.resolve("access-counts"), sortBytes);
        }

        void begin(int kind) {
            window = sequence / windowRequests;
            requests[kind]++; sequence++;
        }

        void hit(int kind, byte[] primary) throws IOException {
            int table = ByteBuffer.wrap(primary).getInt();
            if (!tables.containsKey(table)) throw new IOException("Access names an unknown table");
            // The window is part of the key: only repeated hits on the same PK are combined.
            byte[] key = ByteBuffer.allocate(primary.length + 8).putInt(table).putLong(window)
                    .put(primary, 4, primary.length - 4).array();
            counts.add(key, new byte[]{(byte) kind});
            hits[kind]++;
        }

        void write() throws IOException {
            try (var sorted = counts.sorted()) {
                for (var table : tables.values()) {
                    try (var csv = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(
                            Files.newOutputStream(output.resolve("table-" + table.tableId() + ".csv.gz"))),
                            StandardCharsets.UTF_8))) {
                        csv.write("window,pk,key,get_hits,scan_hits,put_hits,delete_hits\n");
                        while (sorted.hasNext() && ByteBuffer.wrap(sorted.peekKey()).getInt() == table.tableId()) {
                            byte[] key = sorted.peekKey();
                            long[] totals = new long[4];
                            do {
                                totals[sorted.peekValue()[0]]++;
                                sorted.advance();
                            } while (sorted.hasNext() && Arrays.equals(key, sorted.peekKey()));
                            var pk = IndexKey.decode(ByteKeys.decode(ByteBuffer.wrap(key, 16, key.length - 16)));
                            csv.write(ByteBuffer.wrap(key).getLong(4) + ",\""
                                    + label(pk, table.type()).replace("\"", "\"\"") + "\","
                                    + HexFormat.of().formatHex(pk.bytes()) + "," + totals[0] + ","
                                    + totals[1] + "," + totals[2] + "," + totals[3] + "\n");
                        }
                    }
                }
            }
        }

        Map<String, Object> describe() {
            var result = new LinkedHashMap<String, Object>();
            result.put("status", "complete");
            result.put("window_requests", windowRequests);
            result.put("tables", tables.values().stream().map(t ->
                    Map.of("id", t.tableId(), "name", t.table(), "key_type", t.type())).toList());
            result.put("workload_requests", sequence);
            result.put("windows", sequence / windowRequests + (sequence % windowRequests == 0 ? 0 : 1));
            result.put("operation_counts", Map.of("get", requests[0], "scan", requests[1], "put", requests[2], "delete", requests[3]));
            result.put("row_hits", Map.of("get", hits[0], "scan", hits[1], "put", hits[2], "delete", hits[3]));
            result.put("scope", "One count per window and exact table/PK, without PK bins or ranks. All exported primitive requests; baseline/markers excluded. Each ReadGroup member occupies one request position; hits count member results before union/intersection. Writes include secondary maintenance; updates are Puts/Deletes. Empty reads have no row hits. Counts describe returned rows/write entries, not physical I/O.");
            return result;
        }

        @Override public void close() throws IOException { counts.close(); }
    }

    private static String label(IndexKey key, String type) throws IOException {
        if (type.equals("int")) return Long.toString(key.intValue());
        if (type.equals("timestamp")) {
            long micros = key.intValue();
            if (micros == Long.MIN_VALUE) return "-infinity";
            if (micros == Long.MAX_VALUE) return "infinity";
            return LocalDateTime.ofEpochSecond(Math.floorDiv(micros, 1_000_000),
                    (int) Math.floorMod(micros, 1_000_000) * 1000, ZoneOffset.UTC).toString();
        }
        if (type.equals("varchar") || type.equals("text")) {
            byte[] bytes = key.bytes();
            String text = new String(bytes, 1, bytes.length - 2, StandardCharsets.UTF_8);
            if (!key.equals(IndexKey.ofText(text))) throw new IOException("Invalid textual PK");
            return text;
        }
        throw new IOException("Unsupported primary key type: " + type);
    }

    private static void removeWorking(Path directory) throws IOException {
        try (var paths = Files.walk(directory)) {
            for (var path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(path);
        }
    }
}
