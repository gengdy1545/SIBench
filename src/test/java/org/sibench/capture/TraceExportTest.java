package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.capture.IndexOperationTrace.*;

import com.google.gson.Gson;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayInput;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

public class TraceExportTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private Path trace(boolean secondary, long version, boolean baselineSearch) throws Exception {
        return trace(secondary,version,baselineSearch,0);
    }

    private Path trace(boolean secondary, long version, boolean baselineSearch, long baselineVersion) throws Exception {
            Path directory = temporary.newFolder().toPath();
            var baseline = new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"), secondary);
            try (baseline) {
                IndexMutation.emit(write(0, INSERT, 1, 0, IndexKey.ofInt(1), baselineVersion, 0, 0, 1).withWorkUnitId(1),
                        Map.of(), Map.of(1, IndexKey.ofInt(2)), baseline);
                if (baselineSearch) baseline.accept(search(0, 1, 0, Range.equal(IndexKey.ofInt(1)),
                        0, 1, SNAPSHOT_SEARCH).withWorkUnitId(1));
            }
            var workload = new ProtoTrace.Writer(directory.resolve("workload.pb.zst"), secondary);
            try (workload) {
                workload.accept(transaction(0, TRANSACTION_BEGIN, version, version, 0));
                workload.accept(search(1, 1, 1, Range.equal(IndexKey.ofInt(2)), 0, 1, SNAPSHOT_SEARCH)
                        .withTransaction(version, 0).withWorkUnitId(1));
                IndexMutation.emit(write(1, UPSERT, 1, BUSINESS_UPDATE, IndexKey.ofInt(1), version, 0, 0, 1)
                                .withTransaction(version, 1).withWorkUnitId(1),
                        Map.of(1, IndexKey.ofInt(2)), Map.of(1, IndexKey.ofInt(3)), workload);
                workload.accept(transaction(0, TRANSACTION_COMMIT, version, version, 1));
            }
            var units = WorkUnits.create(directory);
            try (var lines = units.lines()) {
                lines.write("{\"work_unit_id\":1,\"source_event\":1}\n");
            }
            var source = SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), Map.of("tables", 1));
            var manifest = IndexCatalog.fromSchema(source).provenance();
            manifest.put("secondary_index_coverage", secondary ? "all_source_definitions" : "primary_only");
            manifest.put("output", "proto");
            manifest.put("value_kinds", List.of("row_id"));
            manifest.put("baseline_rows", 1);
            manifest.put("source_work_units", 1);
            manifest.put("work_units_sha256", HexFormat.of().formatHex(units.digest().digest()));
            manifest.put("baseline_operations", baseline.count());
            manifest.put("workload_operations", workload.count());
            manifest.put("last_resolved_version", version);
            var baseFiles = List.of(ProtoTrace.file("baseline.pb.zst", baseline));
            var workFiles = List.of(ProtoTrace.file("workload.pb.zst", workload));
            var gson = new Gson();
            manifest.put("proto_files", Map.of("baseline", baseFiles, "workload", workFiles));
            manifest.put("baseline_trace_sha256", ProtoTrace.identity(gson.toJsonTree(baseFiles).getAsJsonArray()));
            manifest.put("workload_trace_sha256", ProtoTrace.identity(gson.toJsonTree(workFiles).getAsJsonArray()));
            Path path = directory.resolve("manifest.json");
            Files.writeString(path, gson.toJson(manifest));
            return directory;
    }

    @Test public void declaredCoverageControlsCompleteMaintenanceAndRejectsMissingIndexes() throws Exception {
        for (boolean secondary : List.of(false, true)) {
            Path directory = trace(secondary, 1, false);
            Path path = directory.resolve("manifest.json");
            var manifest = com.google.gson.JsonParser.parseString(Files.readString(path)).getAsJsonObject();
            var primary = new ReplayConfig(false, 0, 0, 0, 0, "operation");
            var all = new ReplayConfig(true, 0, 0, 0, 0, "operation");
            assertEquals("trace_integrity_and_supply_valid", ReplayInput.check(directory, primary).get("status"));
            if (secondary) assertEquals("trace_integrity_and_supply_valid", ReplayInput.check(directory, all).get("status"));
            else assertTrue(assertThrows(IOException.class, () -> ReplayInput.prepare(directory, all))
                    .getMessage().contains("does not provide secondary indexes"));
            // A false coverage claim must fail even when all file digests still match.
            manifest.addProperty("secondary_index_coverage", secondary ? "primary_only" : "all_source_definitions");
            Files.writeString(path, manifest.toString());
            assertThrows(IOException.class, () -> ReplayInput.check(directory, primary));
        }
    }

    @Test public void acceptsGapsLeftByExcludedAbortedTransactions() throws Exception {
        Path directory = trace(false, 2, false);
        assertEquals("trace_integrity_and_supply_valid",
                ReplayInput.check(directory, new ReplayConfig(false, 0, 0, 0, 0)).get("status"));
    }

    @Test public void baselineContainsOnlyInserts() throws Exception {
        Path directory = trace(false, 1, true);
        assertTrue(assertThrows(IOException.class,
                () -> ReplayInput.check(directory, new ReplayConfig(false, 0, 0, 0, 0)))
                .getMessage().contains("Baseline"));
    }

    @Test public void baselineCannotIntroduceAnUnresolvedVersion() throws Exception {
        Path file = temporary.getRoot().toPath().resolve("invalid-baseline.pb.zst");
        try (var writer = new ProtoTrace.Writer(file, true)) {
            assertTrue(assertThrows(IOException.class,
                    () -> writer.append(write(0, INSERT, 1, 0, IndexKey.ofInt(1), 1, 0, 0, 1)))
                    .getMessage().contains("Baseline"));
        }
    }

    @Test public void rejectsIncorrectResolutionWatermark() throws Exception {
        Path directory = trace(false, 1, false), path = directory.resolve("manifest.json");
        var manifest = com.google.gson.JsonParser.parseString(Files.readString(path)).getAsJsonObject();
        manifest.addProperty("last_resolved_version", 2);
        Files.writeString(path, manifest.toString());
        assertTrue(assertThrows(IOException.class,
                () -> ReplayInput.prepare(directory, new ReplayConfig(false, 0, 0, 0, 0)))
                .getMessage().contains("watermark"));
    }
}
