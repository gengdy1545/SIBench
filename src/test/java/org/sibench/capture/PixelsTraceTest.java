package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.capture.IndexOperationTrace.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

public class PixelsTraceTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private static final long TABLE = 7, INDEX = 9, MARKER = 8;

    private PixelsTrace trace(RequestBudget budget) throws IOException {
        var source = SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), Map.of("tables", 1L));
        return new PixelsTrace(temporary.newFolder().toPath().resolve("trace"), source, Map.of(
                TABLE, new PixelsTrace.Table("sbtest1", 1, INDEX, "int"),
                MARKER, new PixelsTrace.Table("sibench_marker", 0, 10, "bigint")), budget);
    }

    private static PixelsTrace.Fact fact(int kind, long timestamp, int key, long rowId) {
        return new PixelsTrace.Fact(kind, TABLE, INDEX, timestamp, ByteBuffer.allocate(4).putInt(key).array(), rowId,
                kind == PixelsTrace.PUT || kind == PixelsTrace.UPDATE ? 40 + key : 0, 2, 5);
    }

    private static List<Operation> workload(Map<String, Object> manifest) throws IOException {
        @SuppressWarnings("unchecked")
        var file = ((List<Map<String, Object>>) ((Map<String, Object>) manifest.get("proto_files")).get("workload")).get(0);
        var operations = new ArrayList<Operation>();
        ProtoTrace.read(Path.of((String) manifest.get("trace_directory")).resolve("workload.pb.zst"), (String) file.get("sha256"),
                (String) file.get("logical_sha256"), operations::add);
        return operations;
    }

    @Test public void exportsInBeginOrderWithSnapshotReadsLocatorsAndExcludedLocalReads() throws Exception {
        var trace = trace(null);
        Path directory = temporary.getRoot().toPath().resolve(temporary.getRoot().list()[0]).resolve("trace");
        // LOAD: two baseline rows at the LOAD timestamp; the count must match the source exactly.
        trace.fact(fact(PixelsTrace.PUT, 3, 1, 100));
        trace.fact(fact(PixelsTrace.PUT, 3, 2, 101));
        trace.finishBaseline(Map.of("sbtest1", 2L));
        trace.begin(10);
        trace.begin(11);
        trace.begin(12);
        trace.begin(13);
        // The later transaction's facts and commit arrive first, but it waits for its predecessor.
        trace.fact(fact(PixelsTrace.PUT, 11, 5, 200));
        trace.commit(11);
        trace.covered(trace.committedSnapshot());
        assertEquals(0, trace.version());
        trace.fact(fact(PixelsTrace.LOOKUP, 10, 1, 100));
        trace.fact(fact(PixelsTrace.UPDATE, 10, 1, 201));
        // Insert, read back and update of one key inside one transaction: the read saw its own write.
        trace.fact(fact(PixelsTrace.PUT, 12, 7, 300));
        trace.fact(fact(PixelsTrace.LOOKUP, 12, 7, 300));
        trace.fact(fact(PixelsTrace.UPDATE, 12, 7, 301));
        trace.fact(new PixelsTrace.Fact(PixelsTrace.PUT, MARKER, 10, 13, ByteBuffer.allocate(8).putLong(4).array(), 1, 1, 0, 0));
        for (long timestamp : List.of(10L, 12L, 13L)) trace.commit(timestamp);
        trace.covered(trace.committedSnapshot());
        assertEquals(3, trace.version());
        assertEquals(List.of(4L), trace.markers());
        trace.finish();
        var manifest = new java.util.LinkedHashMap<>(trace.publish(Map.of()));
        manifest.put("trace_directory", directory.toString());
        assertEquals(List.of("row_id", "locator"), manifest.get("value_kinds"));
        assertEquals(3L, manifest.get("last_resolved_version"));

        var operations = workload(manifest);
        var kinds = operations.stream().map(operation -> kindName(operation.kind()) + "@" + operation.version()).toList();
        // Version 1 is source timestamp 10: the snapshot read of key 1 sees the baseline (read point 0).
        assertEquals(List.of("transaction_begin@1", "search@0", "upsert@1", "transaction_commit@1",
                "transaction_begin@2", "upsert@2", "transaction_commit@2",
                "transaction_begin@3", "upsert@3", "transaction_commit@3"), kinds);
        Operation read = operations.get(1);
        assertEquals(0, read.readTimestamp());
        assertEquals(1, read.expectedCount());
        assertEquals(IndexKey.ofInt(1), read.lower());
        Operation update = operations.get(2);
        assertEquals(201, update.rowId());
        assertEquals(41, update.file());
        assertEquals((2L << 32) | 5, update.position());
        assertEquals(IndexKey.ofInt(5), operations.get(5).key());
        // The local read is excluded and the intermediate insert of key 7 is coalesced into its update.
        assertEquals(301, operations.get(8).rowId());
        @SuppressWarnings("unchecked")
        var projection = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) ((List<?>) ((Map<String, Object>)
                manifest.get("proto_files")).get("workload")).get(0)).get("statistics")).get("source_projection");
        assertEquals(1L, projection.get("excluded_local_search_members"));
        assertEquals(1L, projection.get("coalesced_writes"));
    }

    @Test public void rejectsIncompleteBaselineUnsupportedCallsAndUnresolvedTransactions() throws Exception {
        var trace = trace(null);
        trace.fact(fact(PixelsTrace.PUT, 3, 1, 100));
        assertThrows(IOException.class, () -> trace.finishBaseline(Map.of("sbtest1", 2L)));
        assertThrows(IOException.class, () -> trace.fact(fact(PixelsTrace.LOOKUP, 3, 1, 100)));
        assertThrows(IOException.class, () -> trace.fact(new PixelsTrace.Fact(PixelsTrace.UNSUPPORTED, 0, 0, 0,
                "purgeEntries".getBytes(java.nio.charset.StandardCharsets.UTF_8), -1, 0, 0, 0)));

        var workload = trace(null);
        workload.finishBaseline(Map.of());
        workload.begin(5);
        assertThrows(IOException.class, () -> workload.begin(5));
        assertThrows(IOException.class, () -> workload.commit(6));
        assertThrows(IOException.class, workload::finish);
        workload.commit(5);
        workload.covered(Set.of(5L));
        workload.close();
    }

    @Test public void mixedMarkerTransactionIsRejected() throws Exception {
        var trace = trace(null);
        trace.finishBaseline(Map.of());
        trace.begin(4);
        trace.fact(fact(PixelsTrace.PUT, 4, 1, 10));
        trace.fact(new PixelsTrace.Fact(PixelsTrace.PUT, MARKER, 10, 4, ByteBuffer.allocate(8).putLong(1).array(), 1, 1, 0, 0));
        trace.commit(4);
        assertThrows(IOException.class, () -> trace.covered(trace.committedSnapshot()));
        trace.close();
    }

    @Test public void deleteAndReinsertKeepTheTombstoneAndNewRowIdentityInBothValueModes() throws Exception {
        var trace = trace(null);
        trace.fact(fact(PixelsTrace.PUT, 3, 1, 0));
        trace.finishBaseline(Map.of("sbtest1", 1L));
        trace.begin(10);
        trace.fact(fact(PixelsTrace.LOOKUP, 10, 1, 0));
        trace.fact(fact(PixelsTrace.DELETE, 10, 1, 0));
        trace.commit(10); trace.covered(trace.committedSnapshot());
        trace.begin(11);
        trace.fact(fact(PixelsTrace.LOOKUP, 11, 1, -1));
        trace.fact(fact(PixelsTrace.PUT, 11, 1, 7));
        trace.commit(11); trace.covered(trace.committedSnapshot());
        trace.finish();
        Path directory = temporary.getRoot().toPath().resolve(temporary.getRoot().list()[0]).resolve("trace");
        var manifest = new java.util.LinkedHashMap<>(trace.publish(Map.of()));
        manifest.put("trace_directory", directory.toString());
        var operations = workload(manifest);
        assertEquals(LOGICAL_DELETE, operations.get(2).kind());
        assertEquals(0, operations.get(2).rowId());
        assertEquals(0, operations.get(5).expectedCount());
        assertEquals(1, operations.get(5).readTimestamp());
        assertEquals(7, operations.get(6).rowId());
        assertEquals(2, operations.get(6).version());
        var report = org.sibench.replay.ReferenceReplay.run(directory,
                new org.sibench.replay.ReplayConfig(false, 0, 0, 0, 0, "operation"), temporary.newFolder().toPath().resolve("report.json"));
        assertEquals("passed", report.get("status"));
        assertEquals(2, ((List<?>) report.get("results")).size());
    }
}
