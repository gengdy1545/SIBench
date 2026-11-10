package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.sibench.generation.RocksDbRows;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;
import static org.sibench.capture.IndexOperationTrace.*;

/** Final API Trace output, ordered with native transaction publication. */
final class RocksTrace implements AutoCloseable {
    private final Path directory;
    private final Map<String, Object> manifest;
    private final ProtoTrace.Writer baseline, workload;
    private final WorkUnits.Output units;
    private long commits, aborts, searches, changes, version;
    private boolean baselineClosed, closed;

    RocksTrace(Path directory, SourceSchema source, boolean secondaryIndexes) throws IOException {
        this(directory, source, secondaryIndexes, null);
    }

    RocksTrace(Path directory, SourceSchema source, boolean secondaryIndexes, RequestBudget budget) throws IOException {
        this.directory = directory;
        var catalog = IndexCatalog.fromSchema(source);
        manifest = new LinkedHashMap<>(IndexCatalog.provenance(source, catalog.definitions()));
        IndexCatalog.fromManifest(new Gson().toJsonTree(manifest).getAsJsonObject());
        Files.createDirectory(directory);
        Files.writeString(directory.resolve("INCOMPLETE"), "Native generation/publication pending\n", StandardOpenOption.CREATE_NEW);
        baseline = new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"),secondaryIndexes);
        workload = new ProtoTrace.Writer(directory.resolve("workload.pb.zst"),secondaryIndexes,budget);
        units = WorkUnits.create(directory);
        manifest.put("output", "proto"); manifest.put("value_kinds", List.of("row_id"));
        manifest.put("mode", "api"); manifest.put("generation", "native_rocksdb");
        manifest.put("secondary_index_coverage", secondaryIndexes ? "all_source_definitions" : "primary_only");
    }
    void baseline(RocksDbRows.Row row) throws IOException {
        IndexMutation.emit(write(0, INSERT, 1, 0, IndexKey.ofText(row.key()), 0, 0, 0, row.id())
                .withWorkUnitId(1 + baseline.count() / BLOCK_RECORDS), Map.of(), Map.of(), baseline);
    }
    void finishBaseline(long expected) throws IOException {
        if (baseline.count() != expected) throw new IOException("RocksDB baseline count differs from source configuration");
        baseline.close(); baselineClosed = true;
        manifest.put("baseline_rows", expected); stream("baseline", baseline);
    }
    void event(RocksDbRows.Event event) throws IOException {
        if (!baselineClosed || event.version() != version + 1 || event.readVersion() > version)
            throw new IOException("Invalid RocksDB source publication order");
        version = event.version();
        units.lines().write("{\"work_unit_id\":" + version + ",\"source_event\":" + version + "}\n");
        workload.accept(transaction(0, TRANSACTION_BEGIN, version, version, 0));
        long writes = 0;
        if (event.kind().equals("read") || event.kind().equals("scan")) {
            var key = IndexKey.ofText(event.key());
            var range = event.kind().equals("read") ? Range.equal(key) : new Range(key, true, null, false, false, event.limit());
            workload.accept(search(version, 1, 0, range, event.readVersion(), event.count(), SNAPSHOT_SEARCH)
                    .withTransaction(version, 0).withWorkUnitId(version)); searches++;
        } else if (event.committed() && event.count() == 1) {
            boolean delete = event.kind().equals("delete");
            workload.accept(write(version, delete ? LOGICAL_DELETE : UPSERT, 1,
                    delete ? 0 : event.kind().equals("insert") ? BUSINESS_INSERT : BUSINESS_UPDATE,
                    IndexKey.ofText(event.key()), version, 0, 0, event.rowId())
                    .withTransaction(version, 1).withWorkUnitId(version).withExpansion(0, 1));
            writes = 1; changes++;
        }
        workload.accept(transaction(0, event.committed() ? TRANSACTION_COMMIT : TRANSACTION_ABORT, version, version, writes));
        if (event.committed()) commits++; else aborts++;
    }
    private void stream(String name, ProtoTrace.Writer writer) {
        manifest.put(name + "_operations", writer.count());
        var files = List.of(ProtoTrace.file(name + ".pb.zst", writer));
        @SuppressWarnings("unchecked") var streams = (Map<String, Object>) manifest.computeIfAbsent("proto_files", ignored -> new LinkedHashMap<>());
        streams.put(name, files);
        manifest.put(name + "_trace_sha256", ProtoTrace.identity(new Gson().toJsonTree(files).getAsJsonArray()));
    }
    @Override public void close() throws IOException {
        if (closed) return;
        try (baseline; workload; var lines = units.lines()) { }
        closed = true;
    }
    Map<String, Object> publish(Map<String, Object> evidence) throws Exception {
        if (!closed || !baselineClosed) throw new IOException("RocksDB streams have not finished");
        stream("workload", workload);
        manifest.put("source_work_units", version); manifest.put("source_statements", version);
        manifest.put("committed_transactions", commits); manifest.put("aborted_transactions", aborts);
        manifest.put("searched_accesses", searches); manifest.put("logical_row_changes", changes);
        manifest.put("last_resolved_version", version);
        manifest.put("work_units_sha256", HexFormat.of().formatHex(units.digest().digest()));
        manifest.put("work_unit_rule", "one YCSB API call; numbered in native publication order");
        manifest.put("source_evidence", evidence); manifest.put("status", "complete_for_declared_projection");
        Path partial = directory.resolve("manifest.json.partial");
        Files.writeString(partial, new GsonBuilder().setPrettyPrinting().create().toJson(manifest), StandardOpenOption.CREATE_NEW);
        Files.move(partial, directory.resolve("manifest.json"), StandardCopyOption.ATOMIC_MOVE);
        Files.delete(directory.resolve("INCOMPLETE")); return Map.copyOf(manifest);
    }
}
