package org.sibench.analysis;

import static org.junit.Assert.*;
import static org.sibench.capture.IndexOperationTrace.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.capture.IndexMutation;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.ProtoTrace;
import org.sibench.trace.IndexKey;

public class TraceAccessesTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static Operation put(long source, int key, long row, long version) {
        var op = write(source, version == 0 ? INSERT : UPSERT, 1, BUSINESS_UPDATE,
                IndexKey.ofInt(key), version, 0, 0, row).withExpansion(0, 1);
        return version == 0 ? op : op.withTransaction(version, 1).withWorkUnitId(version);
    }

    private static Operation get(long source, int key, long timestamp, long count) {
        return search(source, 1, 0, Range.equal(IndexKey.ofInt(key)), timestamp,
                count, SNAPSHOT_SEARCH).withWorkUnitId(source);
    }

    private static Operation scan(long source, long timestamp, boolean reverse, long limit, long expected) {
        return search(source, 1, 0, new Range(IndexKey.ofInt(10), true,
                IndexKey.ofInt(30), true, reverse, limit), timestamp, expected,
                SNAPSHOT_SEARCH).withWorkUnitId(source);
    }

    private IndexOperationTrace.Source export(List<Operation> operations) throws Exception {
        Path file = temporary.newFolder().toPath().resolve("trace.pb.zst");
        var writer = new ProtoTrace.Writer(file, true);
        try (writer) { for (var op : operations) writer.accept(op); }
        return consumer -> ProtoTrace.read(file, writer.sha256(), writer.logicalSha256(), consumer);
    }

    private static List<Operation> baseline() {
        return List.of(put(0, 10, 100, 0), put(0, 20, 200, 0), put(0, 30, 300, 0));
    }

    private static List<String> density(Path output) throws IOException {
        try (var input = new GZIPInputStream(Files.newInputStream(output.resolve("access_density.csv.gz")))) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8).lines().skip(1).toList();
        }
    }

    @Test public void snapshotsReverseLimitAndDeletesUseRequestWindowsRatherThanReturnedRowPositions() throws Exception {
        var operations = List.of(
                transaction(0, TRANSACTION_BEGIN, 1, 1, 0), put(1, 20, 201, 1),
                transaction(0, TRANSACTION_COMMIT, 1, 1, 1),
                get(2, 20, 0, 1), get(3, 20, 1, 1), scan(4, 0, true, 2, 2),
                transaction(0, TRANSACTION_BEGIN, 2, 2, 0),
                write(5, LOGICAL_DELETE, 1, 0, IndexKey.ofInt(20), 2, 0, 0, 201)
                        .withTransaction(2, 1).withWorkUnitId(2).withExpansion(0, 1),
                transaction(0, TRANSACTION_COMMIT, 2, 2, 1),
                scan(6, 2, false, 0, 2), get(7, 99, 2, 0));
        Path output = temporary.newFolder().toPath();
        var report = TraceAccesses.analyze(export(baseline()), export(operations),
                temporary.newFolder().toPath(), output, 2, 3, 128);
        assertEquals(Map.of("get", 3L, "scan", 2L, "put", 1L, "delete", 1L), report.get("operation_counts"));
        assertEquals(Map.of("get", 2L, "scan", 4L, "put", 1L, "delete", 1L), report.get("row_hits"));
        assertEquals(3L, report.get("primary_key_count"));
        assertEquals(4L, report.get("windows"));
        assertEquals(List.of("0,1,1,0,1,0", "1,1,1,1,0,0", "1,2,0,1,0,0",
                "2,0,0,1,0,0", "2,1,0,0,0,1", "2,2,0,1,0,0"), density(output));
    }

    @Test public void privateSecondaryGroupCountsMemberHitsWithoutPublishingAbortedWrites() throws Exception {
        var base = new ArrayList<Operation>();
        IndexMutation.emit(put(0, 1, 10, 0), Map.of(), Map.of(1, IndexKey.ofInt(5)), base::add);
        var ops = new ArrayList<Operation>();
        ops.add(transaction(0, TRANSACTION_BEGIN, 1, 1, 0));
        IndexMutation.emit(put(1, 2, 20, 1), Map.of(), Map.of(1, IndexKey.ofInt(5)), ops::add);
        for (int index = 0; index < 2; index++)
            ops.add(search(2, 1, index, Range.equal(IndexKey.ofInt(index == 0 ? 2 : 5)),
                    null, 0, 1, TRANSACTION_LOCAL_SEARCH | INTERSECT_GROUP, index, 2)
                    .withTransaction(1, 1).withVersion(1).withWorkUnitId(1));
        ops.add(search(3, 1, 0, new Range(null, false, null, false, true, 1),
                0, 1, TRANSACTION_LOCAL_SEARCH).withTransaction(1, 1).withVersion(1).withWorkUnitId(1));
        ops.add(transaction(0, TRANSACTION_ABORT, 1, 1, 1));
        ops.add(get(4, 2, 0, 0));
        Path output = temporary.newFolder().toPath();
        var report = TraceAccesses.analyze(export(base), export(ops), temporary.newFolder().toPath(),
                output, 2, 2, 128);
        assertEquals(Map.of("get", 2L, "scan", 2L, "put", 0L, "delete", 0L), report.get("operation_counts"));
        assertEquals(Map.of("get", 1L, "scan", 3L, "put", 0L, "delete", 0L), report.get("row_hits"));
        assertEquals(2L, report.get("primary_key_count"));
        assertEquals(List.of("0,0,0,1,0,0", "0,1,1,1,0,0", "1,1,0,1,0,0"), density(output));
    }

    @Test public void rejectsDeliberatelyWrongQueryCounts() throws Exception {
        var error = assertThrows(IOException.class, () -> TraceAccesses.analyze(export(baseline()),
                export(List.of(scan(1, 0, false, 0, 1))), temporary.newFolder().toPath(),
                temporary.newFolder().toPath(), 10, 3, 128));
        assertTrue(error.getMessage(), error.getMessage().contains("Query result count mismatch"));
    }

    @Test public void successiveLargeGroupsReleaseTheirSpilledMemberSets() throws Exception {
        var base = new ArrayList<Operation>();
        for (int i = 1; i <= 5000; i++) base.add(put(0, i, i, 0));
        var workload = new ArrayList<Operation>();
        for (int source = 1; source <= 2; source++)
            for (int member = 0; member < 2; member++)
                workload.add(search(source, 1, 0, new Range(null, false, null, false, false, 0),
                        null, 0, 5000, SNAPSHOT_SEARCH | UNION_GROUP, member, 2).withWorkUnitId(source));
        Path working = temporary.newFolder().toPath();
        var report = TraceAccesses.analyze(export(base), export(workload), working,
                temporary.newFolder().toPath(), 2, 10, 1 << 20);
        assertEquals(4L, ((Map<?, ?>) report.get("operation_counts")).get("scan"));
        assertEquals(20000L, ((Map<?, ?>) report.get("row_hits")).get("scan"));
        try (var paths = Files.list(working)) {
            assertFalse(paths.anyMatch(p -> p.getFileName().toString().startsWith("group-")));
        }
    }

    @Test public void refusesOutputInsideItsInputBeforeTouchingFiles() throws Exception {
        Path input = temporary.newFolder().toPath();
        Path sentinel = input.resolve("summary.json");
        Files.writeString(sentinel, "keep");
        assertThrows(IOException.class, () -> TraceAccesses.run(input, input, 10, 3, 128));
        Path alias = temporary.getRoot().toPath().resolve("input-alias");
        Files.createSymbolicLink(alias, input);
        assertThrows(IOException.class, () -> TraceAccesses.run(input, alias, 10, 3, 128));
        assertEquals("keep", Files.readString(sentinel));
    }
}
