package org.sibench.capture;

import static org.sibench.capture.IndexOperationTrace.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

/**
 * Projects Pixels primary-index facts into the Trace. LOAD facts form the version-0 baseline. A
 * workload transaction is the set of facts carrying one Pixels timestamp; it is exported once the
 * sink reported its commit and a tracer barrier sent afterwards came back, and only after every
 * transaction begun before it. Versions follow begin order, which is the source commit order.
 * Transactions writing only the marker table are window barriers and are not exported.
 */
final class PixelsTrace implements AutoCloseable {
    static final int LOOKUP = 1, PUT = 2, UPDATE = 3, DELETE = 4, UNSUPPORTED = 9;

    /** One raw fact from the Retina tracer. */
    record Fact(int kind, long table, long index, long timestamp, byte[] key, long rowId, long fileId, int rgId,
            int rgRowOffset) {}

    /** A Pixels table: its Trace table id (0 for the marker), primary index and key column type. */
    record Table(String name, int id, long primaryIndex, String keyType) {}

    private final Path directory;
    private final Map<String, Object> manifest;
    private final Map<Long, Table> tables;
    private final ProtoTrace.Writer baseline, workload;
    private final WorkUnits.Output units;
    private final Map<String, Long> baselineCounts = new LinkedHashMap<>();
    private final ArrayDeque<Long> begun = new ArrayDeque<>();
    private final Set<Long> committed = new HashSet<>(), complete = new HashSet<>();
    private final Map<Long, List<Fact>> pending = new HashMap<>();
    private final List<Long> markers = new ArrayList<>();
    private boolean baselineOpen = true, closed;
    private long version, sourceEvent, searches, changes, lastBegin, lastLoadTimestamp = -1;

    PixelsTrace(Path directory, SourceSchema source, Map<Long, Table> tables, RequestBudget budget) throws IOException {
        this.directory = directory;
        this.tables = new HashMap<>(tables);
        var catalog = IndexCatalog.fromSchema(source);
        manifest = new LinkedHashMap<>(IndexCatalog.provenance(source, catalog.definitions()));
        IndexCatalog.fromManifest(new Gson().toJsonTree(manifest).getAsJsonObject());
        Files.createDirectory(directory);
        Files.writeString(directory.resolve("INCOMPLETE"), "Native generation/publication pending\n", StandardOpenOption.CREATE_NEW);
        baseline = new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"), false);
        workload = new ProtoTrace.Writer(directory.resolve("workload.pb.zst"), false, false, budget);
        units = WorkUnits.create(directory);
        manifest.put("output", "proto");
        manifest.put("value_kinds", List.of("row_id", "locator"));
        manifest.put("mode", "cdc");
        manifest.put("generation", "pixels");
        manifest.put("secondary_index_coverage", "primary_only");
        manifest.put("locator_encoding", "file=Pixels fileId; position=(rgId << 32) | rgRowOffset");
    }

    /** Called on the projection thread before LOAD starts for each newly created table. */
    void register(Map<Long, Table> created) throws IOException {
        if (!baselineOpen) throw new IOException("Pixels tables registered after LOAD");
        for (var entry : created.entrySet()) {
            if (tables.containsKey(entry.getKey()) || tables.values().stream().anyMatch(t -> t.id() == entry.getValue().id()))
                throw new IOException("Duplicate Pixels table registration");
            tables.put(entry.getKey(), entry.getValue());
        }
    }

    /** A tracer fact; LOAD facts while the baseline is open, workload facts afterwards. */
    void fact(Fact fact) throws IOException {
        if (fact.kind() == UNSUPPORTED)
            throw new IOException("Pixels called an unsupported index method: " + new String(fact.key(), StandardCharsets.UTF_8));
        Table table = table(fact);
        if (baselineOpen) {
            if (fact.kind() != PUT || table.id() == 0) throw new IOException("Unexpected Pixels index call during LOAD");
            IndexMutation.emit(write(0, INSERT, table.id(), 0, key(fact, table), 0, fact.fileId(), position(fact), fact.rowId())
                    .withWorkUnitId(1 + baseline.count() / BLOCK_RECORDS), Map.of(), Map.of(), baseline);
            baselineCounts.merge(table.name(), 1L, Long::sum);
            lastLoadTimestamp = Math.max(lastLoadTimestamp, fact.timestamp());
            return;
        }
        if (fact.timestamp() <= lastLoadTimestamp) throw new IOException("Workload fact at a LOAD timestamp");
        pending.computeIfAbsent(fact.timestamp(), ignored -> new ArrayList<>()).add(fact);
    }

    /** Closes the baseline after a barrier covered every LOAD; counts must match the source tables. */
    void finishBaseline(Map<String, Long> expected) throws IOException {
        if (!baselineOpen) throw new IOException("Pixels baseline already finished");
        if (!baselineCounts.equals(expected))
            throw new IOException("Pixels baseline entries " + baselineCounts + " differ from source rows " + expected);
        baselineOpen = false;
        baseline.close();
        manifest.put("baseline_rows", expected.values().stream().mapToLong(Long::longValue).sum());
        stream("baseline", baseline);
    }

    void begin(long timestamp) throws IOException {
        if (baselineOpen || timestamp <= lastBegin) throw new IOException("Pixels transaction began out of order: " + timestamp);
        lastBegin = timestamp;
        begun.add(timestamp);
    }

    void commit(long timestamp) throws IOException {
        if (!begun.contains(timestamp) || complete.contains(timestamp) || !committed.add(timestamp))
            throw new IOException("Unexpected Pixels commit: " + timestamp);
    }

    /** Commits reported before the barrier was sent; their facts all precede its acknowledgement. */
    Set<Long> committedSnapshot() {
        var snapshot = Set.copyOf(committed);
        committed.clear();
        return snapshot;
    }

    /** Marks transactions complete and exports the prefix whose predecessors are all complete. */
    void covered(Set<Long> timestamps) throws IOException {
        complete.addAll(timestamps);
        while (!begun.isEmpty() && complete.remove(begun.peekFirst())) export(begun.removeFirst());
    }

    /** Marker identities exported so far, in source commit order. */
    List<Long> markers() { return List.copyOf(markers); }

    boolean hasUncoveredCommits() { return !committed.isEmpty(); }

    private void export(long timestamp) throws IOException {
        List<Fact> facts = pending.remove(timestamp);
        if (facts == null) facts = List.of();
        long markerFacts = facts.stream().filter(fact -> tables.get(fact.table()).id() == 0).count();
        if (markerFacts > 0) {
            if (markerFacts != facts.size() || facts.size() != 1 || facts.get(0).kind() != PUT)
                throw new IOException("Marker transaction changed source tables");
            markers.add(key(facts.get(0), tables.get(facts.get(0).table())).intValue());
            return;
        }
        version++;
        units.lines().write("{\"work_unit_id\":" + version + ",\"source_event\":" + (sourceEvent + 1) + "}\n");
        workload.accept(transaction(0, TRANSACTION_BEGIN, version, version, 0));
        var written = new HashSet<Map.Entry<Integer, IndexKey>>();
        // Retina resolves a key before changing it; a miss must precede an insert and a hit an update or delete.
        var resolved = new HashMap<Map.Entry<Integer, IndexKey>, Boolean>();
        long ordinal = 0;
        for (Fact fact : facts) {
            Table table = tables.get(fact.table());
            IndexKey key = key(fact, table);
            long event = ++sourceEvent;
            if (fact.kind() == LOOKUP) {
                boolean local = written.contains(Map.entry(table.id(), key));
                if (resolved.put(Map.entry(table.id(), key), fact.rowId() >= 0) != null)
                    throw new IOException("Repeated Pixels key resolution without a change");
                var search = search(event, table.id(), 0, Range.equal(key), version - 1, fact.rowId() >= 0 ? 1 : 0,
                        local ? TRANSACTION_LOCAL_SEARCH : SNAPSHOT_SEARCH).withTransaction(version, ordinal).withWorkUnitId(version);
                workload.accept(local ? search.withVersion(version) : search);
                searches++;
                continue;
            }
            if (fact.kind() != DELETE && fact.kind() != PUT && fact.kind() != UPDATE) throw new IOException("Unknown Pixels fact");
            Boolean found = resolved.remove(Map.entry(table.id(), key));
            if (found != null && found != (fact.kind() != PUT))
                throw new IOException("Pixels key resolution disagrees with the applied source change");
            ordinal++;
            changes++;
            written.add(Map.entry(table.id(), key));
            var operation = fact.kind() == DELETE
                    ? write(event, LOGICAL_DELETE, table.id(), 0, key, version, 0, 0, fact.rowId())
                    : write(event, UPSERT, table.id(), fact.kind() == PUT ? BUSINESS_INSERT : BUSINESS_UPDATE, key, version,
                            fact.fileId(), position(fact), fact.rowId());
            workload.accept(operation.withTransaction(version, ordinal).withWorkUnitId(version).withExpansion(0, 1));
        }
        if (!resolved.isEmpty()) throw new IOException("Pixels resolved keys the source transaction did not change");
        workload.accept(transaction(0, TRANSACTION_COMMIT, version, version, ordinal));
    }

    /** Every begun transaction was exported or recognised as a marker; no fact is left over. */
    void finish() throws IOException {
        if (baselineOpen || !begun.isEmpty() || !committed.isEmpty() || !complete.isEmpty() || !pending.isEmpty())
            throw new IOException("Unresolved Pixels transactions: begun=" + begun.size() + " committed=" + committed.size()
                    + " facts=" + pending.size());
        close();
        stream("workload", workload);
        manifest.put("source_work_units", version);
        manifest.put("source_statements", version);
        manifest.put("committed_transactions", version);
        manifest.put("aborted_transactions", 0L);
        manifest.put("searched_accesses", searches);
        manifest.put("logical_row_changes", changes);
        manifest.put("last_resolved_version", workload.lastCommittedVersion());
        manifest.put("window_markers", markers.size());
        manifest.put("work_units_sha256", HexFormat.of().formatHex(units.digest().digest()));
        manifest.put("work_unit_rule", "one source transaction applied by Pixels; numbered in Pixels timestamp order");
    }

    Map<String, Object> publish(Map<String, Object> evidence) throws IOException {
        if (!manifest.containsKey("workload_trace_sha256")) throw new IOException("Pixels workload did not finish");
        manifest.put("source_evidence", evidence);
        manifest.put("status", "complete_for_declared_projection");
        Path partial = directory.resolve("manifest.json.partial");
        Files.writeString(partial, new GsonBuilder().setPrettyPrinting().create().toJson(manifest), StandardOpenOption.CREATE_NEW);
        Files.move(partial, directory.resolve("manifest.json"), StandardCopyOption.ATOMIC_MOVE);
        Files.delete(directory.resolve("INCOMPLETE"));
        return Map.copyOf(manifest);
    }

    long version() { return version; }

    Map<String, Object> progress() {
        return Map.of("baseline_entries", baseline.count(), "version", version,
                "workload", workload.statistics(), "pending_transactions", pending.size());
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try (baseline; workload; var lines = units.lines()) { }
    }

    private Table table(Fact fact) throws IOException {
        Table table = tables.get(fact.table());
        if (table == null || fact.index() != table.primaryIndex()) throw new IOException("Fact outside the Pixels primary indexes");
        return table;
    }

    private void stream(String name, ProtoTrace.Writer writer) {
        manifest.put(name + "_operations", writer.count());
        var files = List.of(ProtoTrace.file(name + ".pb.zst", writer));
        @SuppressWarnings("unchecked") var streams = (Map<String, Object>) manifest.computeIfAbsent("proto_files", ignored -> new LinkedHashMap<>());
        streams.put(name, files);
        manifest.put(name + "_trace_sha256", ProtoTrace.identity(new Gson().toJsonTree(files).getAsJsonArray()));
    }

    /** Pixels keeps the row group in the high and the row offset in the low half of the position. */
    static long position(Fact fact) {
        return ((long) fact.rgId() << 32) | (fact.rgRowOffset() & 0xffffffffL);
    }

    /** Pixels primary-key bytes: big-endian int/bigint, raw UTF-8 text (CHAR keeps its padding). */
    static IndexKey key(Fact fact, Table table) throws IOException {
        byte[] bytes = fact.key();
        return switch (table.keyType()) {
            case "int" -> {
                if (bytes.length != 4) throw new IOException("Invalid Pixels int key");
                yield IndexKey.ofInt(ByteBuffer.wrap(bytes).getInt());
            }
            case "bigint" -> {
                if (bytes.length != 8) throw new IOException("Invalid Pixels bigint key");
                yield IndexKey.ofInt(ByteBuffer.wrap(bytes).getLong());
            }
            case "varchar", "char" -> {
                try {
                    yield IndexKey.ofText(StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString());
                } catch (CharacterCodingException error) {
                    throw new IOException("Pixels text key is not UTF-8", error);
                }
            }
            default -> throw new IOException("Unsupported Pixels key type: " + table.keyType());
        };
    }
}
