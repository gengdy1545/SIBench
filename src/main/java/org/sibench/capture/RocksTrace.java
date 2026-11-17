package org.sibench.capture;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.sibench.generation.RocksDbRows;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;
import static org.sibench.capture.IndexOperationTrace.*;

/** Final API Trace output, ordered with native transaction publication. */
final class RocksTrace implements AutoCloseable {
    record Baseline(Path file,Map<String,Object> description,long rows,String identity) {}
    private final Path directory;
    private final Map<String, Object> manifest;
    private final ProtoTrace.Writer baseline, workload;
    private final WorkUnits.Output units;
    private long commits, aborts, searches, changes, version;
    private boolean baselineClosed, closed;

    RocksTrace(Path directory, SourceSchema source, boolean secondaryIndexes, RequestBudget budget) throws IOException {
        this(directory,source,secondaryIndexes,budget,null);
    }
    RocksTrace(Path directory,SourceSchema source,boolean secondaryIndexes,RequestBudget budget,Baseline initial) throws IOException {
        this.directory = directory;
        manifest = TraceDirectory.create(directory, source, IndexCatalog.fromSchema(source).definitions(),
                "api", "native_rocksdb", secondaryIndexes, List.of("row_id"));
        baseline = initial==null?new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"),secondaryIndexes):null;
        workload = new ProtoTrace.Writer(directory.resolve("workload.pb.zst"),secondaryIndexes,budget);
        units = WorkUnits.create(directory);
        if(initial!=null) {
            Files.copy(initial.file(),directory.resolve("baseline.pb.zst"));baselineClosed=true;
            manifest.put("baseline_rows",initial.rows());TraceDirectory.stream(manifest,"baseline",initial.description());
        }
    }
    void baseline(RocksDbRows.Row row) throws IOException {
        IndexMutation.emit(write(0, INSERT, 1, 0, IndexKey.ofText(row.key()), 0, 0, 0, row.id())
                .withWorkUnitId(1 + baseline.count() / BLOCK_RECORDS), Map.of(), Map.of(), baseline);
    }
    void finishBaseline(long expected) throws IOException {
        if (baseline.count() != expected) throw new IOException("RocksDB baseline count differs from source configuration");
        baseline.close(); baselineClosed = true;
        manifest.put("baseline_rows", expected); TraceDirectory.stream(manifest, "baseline", ProtoTrace.file(baseline));
    }
    Baseline baseline()throws IOException {
        if(!baselineClosed||baseline==null)throw new IOException("Initial baseline is not sealed");
        return new Baseline(directory.resolve("baseline.pb.zst"),ProtoTrace.file(baseline),
                ((Number)manifest.get("baseline_rows")).longValue(),(String)manifest.get("baseline_trace_sha256"));
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
    @Override public void close() throws IOException {
        if (closed) return;
        try (baseline; workload; var lines = units.lines()) { }
        closed = true;
    }
    Map<String, Object> publish(Map<String, Object> evidence) throws Exception {
        if (!closed || !baselineClosed) throw new IOException("RocksDB streams have not finished");
        TraceDirectory.stream(manifest, "workload", ProtoTrace.file(workload));
        manifest.put("source_work_units", version);
        manifest.put("committed_transactions", commits); manifest.put("aborted_transactions", aborts);
        manifest.put("searched_accesses", searches); manifest.put("logical_row_changes", changes);
        manifest.put("last_resolved_version", workload.lastCommittedVersion());
        manifest.put("work_units_sha256", HexFormat.of().formatHex(units.digest().digest()));
        manifest.put("work_unit_rule", "one YCSB API call; numbered in native publication order");
        return TraceDirectory.publish(directory, manifest, evidence);
    }
}
