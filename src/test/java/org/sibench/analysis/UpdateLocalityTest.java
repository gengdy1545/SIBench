package org.sibench.analysis;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.sibench.capture.IndexCatalog;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.ProtoTrace;
import org.sibench.trace.IndexKey;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class UpdateLocalityTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void exportedAbortedSourceKeepsSnapshotQueriesWithoutMaintenance() throws Exception {
        var operations = new ArrayList<Operation>();
        for (long version = 1; version <= 2; version++) {
            operations.add(IndexOperationTrace.transaction(version, IndexOperationTrace.TRANSACTION_BEGIN, version, version, 0));
            operations.add(IndexOperationTrace.search(version, 1, 0,
                    IndexOperationTrace.Range.equal(IndexKey.ofInt(1)), version - 1, 1,
                    IndexOperationTrace.SNAPSHOT_SEARCH).withTransaction(version, 0).withWorkUnitId(version));
            var write = IndexOperationTrace.write(version, IndexOperationTrace.UPSERT, 1,
                    IndexOperationTrace.BUSINESS_UPDATE, IndexKey.ofInt(1), version, 0, 0, 101)
                    .withTransaction(version, 1).withWorkUnitId(version);
            org.sibench.capture.IndexMutation.emit(write, Map.of(1, IndexKey.ofInt(10)), Map.of(1, IndexKey.ofInt(10)), operations::add);
            operations.add(IndexOperationTrace.transaction(version,
                    version == 1 ? IndexOperationTrace.TRANSACTION_ABORT : IndexOperationTrace.TRANSACTION_COMMIT,
                    version, version, 1));
        }
        var selected = new ArrayList<Operation>();
        IndexOperationTrace.Source source = export(operations);
        assertEquals(8, UpdateLocality.committed(source).read(selected::add));
        assertEquals(4, selected.stream().filter(IndexOperationTrace::isControl).count());
        assertEquals(2, selected.stream().filter(operation -> operation.kind() == IndexOperationTrace.SEARCH).count());
        var writes = selected.stream().filter(IndexOperationTrace::isMutation).toList();
        assertEquals(2, writes.size());
        assertTrue(writes.stream().allMatch(write -> write.version() == 2));
        assertThrows(IOException.class, () -> UpdateLocality.committed(consumer -> {
            for (var operation : selected.subList(0, selected.size() - 1)) consumer.accept(operation);
            return selected.size() - 1;
        }).read(ignored -> {}));
    }

    private IndexOperationTrace.Source export(List<Operation> operations) throws Exception {
        Path file = temporary.newFolder().toPath().resolve("workload.pb.zst");
        try (var writer = new ProtoTrace.Writer(file)) {
            for (var operation : operations) writer.append(operation);
        }
        return consumer -> ProtoTrace.read(file, null, null, consumer);
    }

    @Test public void finalSecondaryChangesStayWithTheirPrimaryAcrossWindows() throws Exception {
        var operations = new ArrayList<Operation>();
        operations.add(IndexOperationTrace.transaction(1, IndexOperationTrace.TRANSACTION_BEGIN, 1, 1, 0));
        // Row 1 moves A -> B -> C, with row 2's A -> D update between those source changes.
        for (int[] update : List.of(new int[] {1, 1, 10, 20}, new int[] {2, 2, 10, 40},
                new int[] {3, 1, 20, 30})) {
            var write = IndexOperationTrace.write(update[0], IndexOperationTrace.UPSERT, 1,
                    IndexOperationTrace.BUSINESS_UPDATE, IndexKey.ofInt(update[1]), 1, 0, 0, 100 + update[1])
                    .withTransaction(1, update[0]).withWorkUnitId(1);
            org.sibench.capture.IndexMutation.emit(write, Map.of(1, IndexKey.ofInt(update[2])),
                    Map.of(1, IndexKey.ofInt(update[3])), operations::add);
        }
        operations.add(IndexOperationTrace.transaction(4, IndexOperationTrace.TRANSACTION_COMMIT, 1, 1, 3));
        var source = export(operations);
        var encoded = new ArrayList<Operation>();
        source.read(encoded::add);
        assertEquals(IndexOperationTrace.LOGICAL_DELETE, encoded.get(1).kind());
        assertEquals(1, encoded.get(1).index()); // Final Trace legitimately starts with secondary A's delete.
        var grouped = new ArrayList<Operation>();
        assertEquals(9, UpdateLocality.committed(source).read(grouped::add));
        var writes = grouped.stream().filter(IndexOperationTrace::isMutation).toList();
        assertEquals(List.of(1, 1, 1, 1, 2, 2, 2), writes.stream()
                .map(write -> write.key().equals(IndexKey.ofInt(1)) ? 1 : 2).toList());
        assertEquals(List.of(0, 1, 1, 1, 0, 1, 1), writes.stream().map(Operation::index).toList());
        assertEquals(List.of(IndexKey.ofInt(10), IndexKey.ofInt(20), IndexKey.ofInt(30)),
                writes.subList(1, 4).stream().map(Operation::indexedValue).toList());
        assertEquals(List.of(IndexOperationTrace.LOGICAL_DELETE, IndexOperationTrace.LOGICAL_DELETE,
                        IndexOperationTrace.UPSERT), writes.subList(1, 4).stream().map(Operation::kind).toList());

        Path output = temporary.newFolder().toPath();
        var definitions = List.of(new IndexCatalog.Definition(1, "t", 0, "pk", "id", "int"),
                new IndexCatalog.Definition(1, "t", 1, "sec", "k", "int"));
        var secondaryKeys = new ArrayList<byte[]>();
        for (int value : new int[] {10, 20, 30, 40})
            for (int key : new int[] {1, 2}) secondaryKeys.add(composite(value, key));
        var report = UpdateLocality.analyzeSource(definitions,
                Map.of(1L << 32, layout(0, "pk", primaryKeys()),
                        (1L << 32) | 1L, layout(1, "sec", secondaryKeys)),
                8, new UpdateLocality.Config(2, 2, 1, 4, 1 << 20, 1),
                UpdateLocality.committed(source), output);
        assertEquals(2L, report.get("windows"));
        assertEquals(Map.of("insert", 0L, "update", 2L, "delete", 0L), report.get("business_changes"));
        var windows = csv(output.resolve("windows.csv"));
        assertEquals("1", row(windows, 1, 0, "business").get("modifications"));
        assertEquals("3", row(windows, 1, 1, "secondary_index").get("modifications"));
        assertEquals("1", row(windows, 2, 0, "business").get("modifications"));
        assertEquals("2", row(windows, 2, 1, "secondary_index").get("modifications"));
        var batches = csv(output.resolve("l0-batches.csv"));
        assertEquals(List.of("1", "1", "2", "2"), batches.stream().map(row -> row.get("batch")).toList());
        assertEquals(List.of("1", "3", "1", "2"), batches.stream().map(row -> row.get("entries")).toList());
    }

    private static byte[] composite(int value, int key) {
        byte[] prefix = IndexKey.ofInt(value).bytes(), suffix = IndexKey.ofInt(key).bytes();
        byte[] result = Arrays.copyOf(prefix, prefix.length + suffix.length);
        System.arraycopy(suffix, 0, result, prefix.length, suffix.length);
        return result;
    }

    private static Operation primary(int key, int flags, int kind, long transaction, long ordinal) {
        return IndexOperationTrace.write(
                        0, kind, 1, flags, IndexKey.ofInt(key), 1, 0, 0, 100 + key)
                .withTransaction(transaction, ordinal)
                .withWorkUnitId(1)
                .withExpansion(0, 1);
    }

    private static Operation secondary(int key, int flags, long transaction, long ordinal) {
        return IndexOperationTrace.write(
                        0,
                        IndexOperationTrace.UPSERT,
                        1,
                        flags,
                        IndexKey.ofInt(key),
                        1,
                        0,
                        0,
                        100 + key)
                .withIndex(1, IndexKey.ofInt(10), IndexOperationTrace.UPSERT)
                .withTransaction(transaction, ordinal)
                .withWorkUnitId(1)
                .withExpansion(1, 2);
    }

    @Test
    public void windowsSeparateBusinessAndIndexMaintenanceAndHandleOverflow() throws Exception {
        Path output = temporary.newFolder().toPath().resolve("out");
        Files.createDirectories(output);
        var primaryDefinition = new IndexCatalog.Definition(1, "t", 0, "pk", "id", "int");
        var secondaryDefinition = new IndexCatalog.Definition(1, "t", 1, "sec", "k", "int");
        var primaryLayout = layout(0, "pk", primaryKeys());
        var secondaryLayout = layout(1, "sec", secondaryKeys());
        Map<Long, UpdateLocality.IndexLayout> layouts = new LinkedHashMap<>();
        layouts.put((1L << 32), primaryLayout);
        layouts.put((1L << 32) | 1L, secondaryLayout);
        var operations = new ArrayList<Operation>();
        operations.add(primary(1, IndexOperationTrace.BUSINESS_INSERT, IndexOperationTrace.UPSERT, 1, 1));
        operations.add(secondary(1, IndexOperationTrace.BUSINESS_INSERT, 1, 1));
        operations.add(primary(2, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 2, 1));
        operations.add(primary(7, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 3, 1));
        operations.add(primary(8, 0, IndexOperationTrace.LOGICAL_DELETE, 4, 1));
        operations.add(primary(0, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 5, 1));
        operations.add(primary(9, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 6, 1));
        var result =
                UpdateLocality.analyzeSource(
                        List.of(primaryDefinition, secondaryDefinition),
                        layouts,
                        8,
                        new UpdateLocality.Config(2, 2, 2, 4, 1 << 20, 2),
                        consumer -> {
                            for (var operation : operations) consumer.accept(operation);
                            return operations.size();
                        },
                        output);
        assertEquals(3L, result.get("windows"));
        assertEquals(Map.of("insert", 1L, "update", 4L, "delete", 1L), result.get("business_changes"));
        var rows = csv(output.resolve("windows.csv"));
        var window1 = row(rows, 1, 0, "business");
        assertEquals("2", window1.get("modifications"));
        assertEquals("2", window1.get("unique_keys"));
        assertEquals("1", window1.get("hit_blocks"));
        assertEquals("1", window1.get("hit_files"));
        assertEquals("1", window1.get("blocks_for_50pct"));
        assertEquals("1.000000", window1.get("top_1pct_share"));
        assertEquals("-1.000000", window1.get("hot_overlap"));
        var window2 = row(rows, 2, 0, "business");
        assertEquals("2", window2.get("modifications"));
        assertEquals("1", window2.get("hit_blocks"));
        assertEquals("0.000000", window2.get("hot_overlap"));
        var window3 = row(rows, 3, 0, "business");
        assertEquals("0", window3.get("modifications"));
        assertEquals("1", window3.get("overflow_left"));
        assertEquals("1", window3.get("overflow_right"));
        var secondaryWindow = row(rows, 1, 1, "secondary_index");
        assertEquals("1", secondaryWindow.get("modifications"));
        assertEquals("1", secondaryWindow.get("hit_blocks"));
        var blocks = gzipCsv(output.resolve("blocks.csv.gz"));
        assertTrue(blocks.stream().anyMatch(line -> line.contains(",business_insert,0,1")));
        assertTrue(blocks.stream().anyMatch(line -> line.contains(",business_update,0,1")));
        assertTrue(blocks.stream().anyMatch(line -> line.contains(",business_update,3,1")));
        var l0 = Files.readAllLines(output.resolve("l0-batches.csv"));
        assertEquals(l0.toString(), 6, l0.size());
        assertTrue(l0.stream().anyMatch(line -> line.startsWith("1,1,1,sec,")));
    }

    @Test
    public void twoDistantHotspotsHaveFewHitBlocksButAWiderL1Range() throws Exception {
        Path output = temporary.newFolder().toPath().resolve("distant");
        Files.createDirectories(output);
        var definition = new IndexCatalog.Definition(1, "t", 0, "pk", "id", "int");
        var keys = new ArrayList<byte[]>();
        var boundaries = new ArrayList<byte[]>();
        for (int key = 1; key <= 16; key++) {
            keys.add(IndexKey.ofInt(key).bytes());
            boundaries.add(IndexKey.ofInt(key).bytes());
        }
        var layout =
                new UpdateLocality.IndexLayout(
                        1, 0, "pk", 16, 16, 4, keys.get(0), keys.get(15),
                        boundaries.toArray(new byte[0][]));
        Map<Long, UpdateLocality.IndexLayout> layouts = Map.of((1L << 32), layout);
        var operations =
                List.of(
                        primary(1, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 1, 1),
                        primary(16, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 2, 1));
        UpdateLocality.analyzeSource(
                List.of(definition),
                layouts,
                16,
                new UpdateLocality.Config(1, 4, 100, 8, 1 << 20, 100),
                consumer -> {
                    for (var operation : operations) consumer.accept(operation);
                    return operations.size();
                },
                output);
        var rows = Files.readAllLines(output.resolve("l0-batches.csv"));
        assertEquals(2, rows.size());
        var fields = rows.get(1).split(",", -1);
        assertEquals("2", fields[9]); // hit blocks
        assertEquals("16", fields[8]); // span blocks
        assertEquals("2", fields[11]); // files with actual hits
        assertEquals("4", fields[10]); // files in the min..max range
    }

    @Test
    public void controlledUniformFixedAndMovingPatternsMatchTheirHotBlockOverlap() throws Exception {
        var definition = new IndexCatalog.Definition(1, "t", 0, "pk", "id", "int");
        var keys = new ArrayList<byte[]>();
        for (int key = 1; key <= 16; key++) keys.add(IndexKey.ofInt(key).bytes());
        var layout =
                new UpdateLocality.IndexLayout(
                        1, 0, "pk", 16, 16, 4, keys.get(0), keys.get(15),
                        keys.toArray(new byte[0][]));
        Map<Long, UpdateLocality.IndexLayout> layouts = Map.of((1L << 32), layout);

        var uniform = new ArrayList<Operation>();
        for (int key = 1; key <= 16; key++)
            uniform.add(primary(key, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, key, 1));
        var uniformRows = patternRows(definition, layouts, uniform, 16, 2);
        assertEquals("16", uniformRows.get(0).get("hit_blocks"));
        assertEquals("8", uniformRows.get(0).get("blocks_for_50pct"));
        assertEquals("0.125000", uniformRows.get(0).get("top_10pct_share"));

        var fixed = new ArrayList<Operation>();
        for (int transaction = 1; transaction <= 4; transaction++)
            fixed.add(
                    primary(
                            1 + transaction % 2,
                            IndexOperationTrace.BUSINESS_UPDATE,
                            IndexOperationTrace.UPSERT,
                            transaction,
                            1));
        var fixedRows = patternRows(definition, layouts, fixed, 2, 2);
        assertEquals("1.000000", fixedRows.get(1).get("hot_overlap"));

        var moving = new ArrayList<Operation>();
        moving.add(primary(1, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 1, 1));
        moving.add(primary(2, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 2, 1));
        moving.add(primary(16, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 3, 1));
        moving.add(primary(15, IndexOperationTrace.BUSINESS_UPDATE, IndexOperationTrace.UPSERT, 4, 1));
        var movingRows = patternRows(definition, layouts, moving, 2, 2);
        assertEquals("0.000000", movingRows.get(1).get("hot_overlap"));
    }

    private List<Map<String, String>> patternRows(
            IndexCatalog.Definition definition,
            Map<Long, UpdateLocality.IndexLayout> layouts,
            List<Operation> operations,
            long window,
            long l0)
            throws Exception {
        Path output = temporary.newFolder().toPath().resolve("pattern");
        Files.createDirectories(output);
        UpdateLocality.analyzeSource(
                List.of(definition),
                layouts,
                16,
                new UpdateLocality.Config(1, 4, window, 8, 1 << 20, (int) l0),
                consumer -> {
                    for (var operation : operations) consumer.accept(operation);
                    return operations.size();
                },
                output);
        return csv(output.resolve("windows.csv")).stream()
                .filter(row -> row.get("index").equals("0") && row.get("category").equals("business"))
                .toList();
    }

    @Test
    public void emptyWorkloadProducesNoWindowsOrL0Batches() throws Exception {
        Path output = temporary.newFolder().toPath().resolve("empty");
        Files.createDirectories(output);
        var definition = new IndexCatalog.Definition(1, "t", 0, "pk", "id", "int");
        var layout =
                new UpdateLocality.IndexLayout(
                        1, 0, "pk", 1, 1, 1, IndexKey.ofInt(1).bytes(), IndexKey.ofInt(1).bytes(),
                        new byte[][] {IndexKey.ofInt(1).bytes()});
        var result =
                UpdateLocality.analyzeSource(
                        List.of(definition),
                        Map.of((1L << 32), layout),
                        1,
                        new UpdateLocality.Config(1, 1, 10, 1, 1 << 20, 10),
                        consumer -> 0,
                        output);
        assertEquals(0L, result.get("windows"));
        assertEquals(1, Files.readAllLines(output.resolve("windows.csv")).size());
        assertEquals(1, Files.readAllLines(output.resolve("l0-batches.csv")).size());
    }

    private static UpdateLocality.IndexLayout layout(int index, String name, List<byte[]> keys) {
        List<byte[]> boundaries = new ArrayList<>();
        for (int i = 0; i < keys.size(); i += 2) boundaries.add(keys.get(i));
        return new UpdateLocality.IndexLayout(
                1,
                index,
                name,
                keys.size(),
                (keys.size() + 1) / 2,
                (keys.size() + 3) / 4,
                keys.get(0),
                keys.get(keys.size() - 1),
                boundaries.toArray(new byte[0][]));
    }

    private static List<byte[]> primaryKeys() {
        var keys = new ArrayList<byte[]>();
        for (int key = 1; key <= 8; key++) keys.add(IndexKey.ofInt(key).bytes());
        return keys;
    }

    private static List<byte[]> secondaryKeys() {
        var keys = new ArrayList<byte[]>();
        for (int key = 1; key <= 8; key++) keys.add(composite(10, key));
        return keys;
    }

    private static List<Map<String, String>> csv(Path path) throws Exception {
        var lines = Files.readAllLines(path);
        var header = lines.get(0).split(",", -1);
        var rows = new ArrayList<Map<String, String>>();
        for (int i = 1; i < lines.size(); i++) {
            var fields = lines.get(i).split(",", -1);
            var row = new LinkedHashMap<String, String>();
            for (int f = 0; f < header.length; f++) row.put(header[f], fields[f]);
            rows.add(row);
        }
        return rows;
    }

    private static List<String> gzipCsv(Path path) throws Exception {
        var rows = new ArrayList<String>();
        try (var reader =
                new BufferedReader(
                        new InputStreamReader(
                                new GZIPInputStream(Files.newInputStream(path)),
                                StandardCharsets.UTF_8))) {
            for (String line; (line = reader.readLine()) != null; ) rows.add(line);
        }
        return rows;
    }

    private static Map<String, String> row(
            List<Map<String, String>> rows, int window, int index, String category) {
        return rows.stream()
                .filter(
                        row ->
                                row.get("window").equals(String.valueOf(window))
                                        && row.get("index").equals(String.valueOf(index))
                                        && row.get("category").equals(category))
                .findFirst()
                .orElseThrow();
    }
}
