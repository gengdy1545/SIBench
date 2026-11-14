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
import java.util.ArrayList;
import java.util.Arrays;
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
import org.sibench.state.SpillingMap;
import org.sibench.trace.IndexEntry;
import org.sibench.trace.TraceManifest;

/** Exact returned-row locality in Trace request order, not physical engine I/O. */
public final class TraceAccesses {
    private TraceAccesses() {}

    public static void run(Path trace, Path output, long windowRequests, int bins, long sortBytes)
            throws IOException {
        trace = trace.toRealPath();
        output = output.toAbsolutePath().normalize();
        Path ancestor = output;
        while (!Files.exists(ancestor)) ancestor = ancestor.getParent();
        output = ancestor.toRealPath().resolve(ancestor.relativize(output)).normalize();
        if (output.startsWith(trace) || trace.startsWith(output))
            throw new IOException("Analysis output must not overlap its Trace");
        if (windowRequests < 1 || bins < 1 || bins > 1_000_000 || sortBytes < 1)
            throw new IllegalArgumentException("Positive window/sort budgets and 1..1000000 PK bins required");
        var manifest = TraceManifest.read(trace);
        String identity = Digests.sha256(trace.resolve("manifest.json"));
        TraceManifest.requireValue(trace, ValueKind.ROW_ID);
        var input = ReplayInput.prepare(trace,
                new ReplayConfig(TraceManifest.secondaryIndexes(manifest), 0, 0, 0, 0, "operation", 1, 4096, 60, 1));
        Files.createDirectories(output);
        Files.deleteIfExists(output.resolve("summary.json"));
        var workingRoot = Path.of("build/sibench/analysis");
        Files.createDirectories(workingRoot);
        Path working = Files.createTempDirectory(workingRoot, "trace-accesses-");
        try {
            var result = analyze(input.baseline(), input.workload(), working, output,
                    windowRequests, bins, sortBytes);
            result.put("manifest_sha256", identity);
            result.put("baseline_trace_sha256", manifest.get("baseline_trace_sha256").getAsString());
            result.put("workload_trace_sha256", manifest.get("workload_trace_sha256").getAsString());
            result.put("baseline_rows", manifest.get("baseline_rows").getAsLong());
            result.put("secondary_index_coverage", manifest.get("secondary_index_coverage").getAsString());
            var tables = IndexCatalog.fromManifest(manifest).definitions().stream()
                    .filter(d -> d.indexId() == 0)
                    .map(d -> Map.of("id", d.tableId(), "name", d.table())).toList();
            result.put("table_names", tables);
            if (!identity.equals(Digests.sha256(trace.resolve("manifest.json"))))
                throw new IOException("Trace manifest changed during analysis");
            var counts = (Map<?, ?>) result.get("operation_counts");
            for (String kind : List.of("get", "scan", "put", "delete")) {
                long recorded = 0;
                for (var file : manifest.getAsJsonObject("proto_files").getAsJsonArray("workload")) {
                    var stats = file.getAsJsonObject().getAsJsonObject("statistics").getAsJsonObject("index_operations");
                    for (String scope : List.of("primary", "secondary")) {
                        var value = stats.getAsJsonObject(scope).get(kind);
                        if (value != null) recorded = Math.addExact(recorded, value.getAsLong());
                    }
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

    private static void collect(Operation operation, ExternalSort keys) throws IOException {
        if (operation.kind() == IndexOperationTrace.INSERT || IndexOperationTrace.isMutation(operation))
            keys.add(primary(operation), operation.kind() == IndexOperationTrace.LOGICAL_DELETE
                    ? new byte[0] : row(operation.table(), IndexEntry.rowId(operation.rowId())));
        for (Operation local : operation.localWrites()) collect(local, keys);
    }

    /** Analyzes baseline and workload sources that have already passed replay preflight. */
    static Map<String, Object> analyze(IndexOperationTrace.Source baseline,
            IndexOperationTrace.Source workload, Path working, Path output,
            long windowRequests, int bins, long sortBytes) throws IOException {
        Files.createDirectories(working);
        Files.createDirectories(output);
        try (var backend = VersionedIndexBackend.create(working.resolve("index"), ValueKind.ROW_ID);
                var keys = new ExternalSort(working.resolve("keys"), sortBytes);
                var rowSort = new ExternalSort(working.resolve("rows"), sortBytes);
                var primaryRanks = new SpillingMap(working.resolve("primary-ranks"), 65536, 8);
                var rowRanks = new SpillingMap(working.resolve("row-ranks"), 65536, 8)) {
            var writes = new ArrayList<Mutation>();
            var loader = records((request, views) -> {
                for (var view : views) collect(view, keys);
                if (request.hasPut()) writes.add(Mutation.newBuilder().setPut(request.getPut()).build());
                if (request.hasDelete()) writes.add(Mutation.newBuilder().setDelete(request.getDelete()).build());
                if (writes.size() >= 1024) { backend.writeBatch(writes); writes.clear(); }
            });
            baseline.read(loader);
            workload.read(loader);
            backend.writeBatch(writes);
            long count = 0;
            var tables = new TreeMap<Integer, long[]>();
            try (var sorted = keys.sorted(); var bulk = primaryRanks.bulk()) {
                byte[] previous = null;
                long rank = -1;
                while (sorted.hasNext()) {
                    byte[] key = sorted.peekKey(), value = sorted.peekValue();
                    if (!Arrays.equals(previous, key)) {
                        rank = count++;
                        bulk.append(key, rank);
                        int table = ByteBuffer.wrap(key).getInt();
                        if (!tables.containsKey(table)) tables.put(table, new long[]{rank, 0});
                        tables.get(table)[1]++;
                        previous = key;
                    }
                    if (value.length != 0)
                        rowSort.add(value, ByteBuffer.allocate(8).putLong(rank).array());
                    sorted.advance();
                }
                bulk.finish();
            }
            try (var sorted = rowSort.sorted(); var bulk = rowRanks.bulk()) {
                byte[] previous = null, rank = null;
                while (sorted.hasNext()) {
                    byte[] key = sorted.peekKey(), value = sorted.peekValue();
                    if (!Arrays.equals(previous, key)) {
                        bulk.append(key, value); previous = key; rank = value;
                    } else if (!Arrays.equals(rank, value)) {
                        throw new IOException("One table/RowID maps to different primary keys");
                    }
                    sorted.advance();
                }
                bulk.finish();
            }
            var hotRanks = new LinkedHashMap<ByteString, Long>(65536, 0.75f, true) {
                @Override protected boolean removeEldestEntry(Map.Entry<ByteString, Long> eldest) {
                    return size() > 65536;
                }
            };
            try (var density = new Density(output, windowRequests, bins, count)) {
                workload.read(records((request, views) -> {
                    if (request.hasReadGroup()) {
                        var group = request.getReadGroup();
                        Path groupDirectory = working.resolve("group-" + density.sequence);
                        try (var members = new SpillingMap(groupDirectory, 4096, 8)) {
                            for (int i = 0; i < group.getRequestsCount(); i++)
                                read(backend, TraceRequests.read(group.getRequests(i)), views.get(i),
                                        rowRanks, hotRanks, density, members, i);
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
                                views.get(0), rowRanks, hotRanks, density, null, 0);
                        requireCount(hits, request.getMetadata().getExpectedCount());
                    } else {
                        int kind = request.hasPut() ? 2 : 3;
                        density.begin(kind);
                        long rank = primaryRanks.get(primary(views.get(0)));
                        density.hit(kind, rank);
                    }
                }));
                var result = density.describe();
                var ranges = new ArrayList<Map<String, Object>>();
                for (var table : tables.entrySet())
                    ranges.add(Map.of("table", table.getKey(), "start", table.getValue()[0], "keys", table.getValue()[1]));
                result.put("tables", ranges);
                result.put("sort_memory_bytes", sortBytes);
                return result;
            }
        }
    }

    private static long read(IndexBackend backend, Message request, Operation operation,
            SpillingMap ranks, Map<ByteString, Long> hotRanks, Density density,
            SpillingMap members, int member) throws IOException {
        int kind = request instanceof GetRequest ? 0 : 1;
        density.begin(kind);
        long[] hits = {0};
        IndexBackend.ValueSink sink = value -> {
            var identity = ByteString.copyFrom(row(operation.table(), value));
            Long rank = hotRanks.get(identity);
            if (rank == null) {
                rank = ranks.get(identity.toByteArray());
                hotRanks.put(identity, rank);
            }
            density.hit(kind, rank);
            hits[0]++;
            if (members != null) {
                byte[] key = value.toByteArray(), old = members.getBytes(key);
                long seen = old == null ? 0 : ByteBuffer.wrap(old).getLong();
                long last = old == null ? -1 : ByteBuffer.wrap(old).getLong(8);
                if (last != member)
                    members.putBytes(key, ByteBuffer.allocate(16).putLong(seen + 1).putLong(member).array());
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
        private final BufferedWriter csv;
        private final long windowRequests, keys, ranksPerBin;
        private final int requestedBins, bins;
        private final long[][] cells;
        private final long[] requests = new long[4], hits = new long[4];
        private long sequence, window;

        Density(Path output, long windowRequests, int bins, long keys) throws IOException {
            this.windowRequests = windowRequests; this.keys = keys; this.requestedBins = bins;
            ranksPerBin = Math.max(1, keys / bins + (keys % bins == 0 ? 0 : 1));
            this.bins = (int) Math.max(1, keys / ranksPerBin + (keys % ranksPerBin == 0 ? 0 : 1));
            cells = new long[4][this.bins];
            csv = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(
                    Files.newOutputStream(output.resolve("access_density.csv.gz"))), StandardCharsets.UTF_8));
            csv.write("window,pk_bin,get_hits,scan_hits,put_hits,delete_hits\n");
        }

        void begin(int kind) throws IOException {
            long next = sequence / windowRequests;
            if (next != window) { flush(); window = next; }
            requests[kind]++; sequence++;
        }

        void hit(int kind, long rank) throws IOException {
            if (rank < 0 || rank >= keys) throw new IOException("Result/write has no primary-key rank");
            cells[kind][(int) (rank / ranksPerBin)]++;
            hits[kind]++;
        }

        private void flush() throws IOException {
            for (int bin = 0; bin < bins; bin++) {
                if (cells[0][bin] != 0 || cells[1][bin] != 0 || cells[2][bin] != 0 || cells[3][bin] != 0)
                    csv.write(window + "," + bin + "," + cells[0][bin] + "," + cells[1][bin]
                            + "," + cells[2][bin] + "," + cells[3][bin] + "\n");
                for (var series : cells) series[bin] = 0;
            }
        }

        Map<String, Object> describe() {
            var result = new LinkedHashMap<String, Object>();
            result.put("status", "complete");
            result.put("window_requests", windowRequests);
            result.put("requested_pk_bins", requestedBins);
            result.put("pk_bins", bins);
            result.put("ranks_per_bin", ranksPerBin);
            result.put("primary_key_count", keys);
            result.put("workload_requests", sequence);
            result.put("windows", sequence / windowRequests + (sequence % windowRequests == 0 ? 0 : 1));
            result.put("operation_counts", Map.of("get", requests[0], "scan", requests[1], "put", requests[2], "delete", requests[3]));
            result.put("row_hits", Map.of("get", hits[0], "scan", hits[1], "put", hits[2], "delete", hits[3]));
            result.put("scope", "All exported primitive requests; baseline/markers excluded. Each ReadGroup member occupies one request position; hits count member results before union/intersection. Writes include secondary maintenance; updates are Puts/Deletes. PK ranks sort distinct table/PK keys in baseline, writes and private contexts; empty reads have no row hits. Counts describe returned rows/write entries, not physical I/O.");
            return result;
        }

        @Override public void close() throws IOException { flush(); csv.close(); }
    }

    private static void removeWorking(Path directory) throws IOException {
        try (var paths = Files.walk(directory)) {
            for (var path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(path);
        }
    }
}
