package org.sibench.capture;

import static org.junit.Assert.*;

import java.nio.file.*;
import java.util.*;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.generation.ParquetDataFileTest;
import org.sibench.schema.SourceSchema;

public class PgBaselineExportTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void exportedSnapshotMustIncludeEveryNativeLoadBatch() throws Exception {
        var config = NativeCaptureConfig.read(Path.of(""), Path.of("src/test/resources/config/ycsb_1k.yaml"));
        PgBaselineExport.requireRows(config.layout(), Map.of("usertable", 1000L));
        // Client BATCHED_OK counters do not establish that PG contains the last batch.
        for (var actual : List.of(Map.of("usertable", 999L), Map.of("usertable", 1001L),
                Map.<String, Long>of(), Map.of("another_table", 1000L)))
            assertTrue(assertThrows(java.io.IOException.class, () -> PgBaselineExport.requireRows(config.layout(), actual))
                    .getMessage().contains("different row count"));
    }

    @Test
    public void nativeCountsRejectMissingRowsAndBalancedTableErrors() throws Exception {
        var sysbench = NativeCaptureConfig.read(Path.of(""), Path.of("src/test/resources/config/sysbench_2k.yaml"));
        PgBaselineExport.requireRows(sysbench.layout(), Map.of("sbtest1", 1000L, "sbtest2", 1000L));
        for (var rows : List.of(Map.of("sbtest1", 999L, "sbtest2", 1001L),
                Map.of("sbtest1", 1000L), Map.of("sbtest1", 0L, "sbtest2", 1000L)))
            assertThrows(java.io.IOException.class, () -> PgBaselineExport.requireRows(sysbench.layout(), rows));
        var hybench = CaptureConfig.read(Path.of(""), Path.of("config/benchmark/hybench.yaml"));
        // SF10 source parameters: 3m customers, 20k companies, two 3.02m accounts,
        // 60m transfers and 6m each checking, loan applications and loan transactions.
        var rows = new LinkedHashMap<>(Map.of("customer", 3000000L, "company", 20000L,
                "savingaccount", 3020000L, "checkingaccount", 3020000L,
                "transfer", 60000000L, "checking", 6000000L, "loanapps", 6000000L, "loantrans", 6000000L));
        PgBaselineExport.requireRows(hybench.layout(), rows);
        rows.put("loanapps", 5999999L);
        rows.put("loantrans", 6000001L);
        assertThrows(java.io.IOException.class, () -> PgBaselineExport.requireRows(hybench.layout(), rows));
    }

    @Test
    public void prefetchByteBudgetRejectsRowsOverTheCreditLimit() throws Exception {
        assertEquals(28, PgBaselineExport.prefetchCost(28, 28));
        assertThrows(java.io.IOException.class, () -> PgBaselineExport.prefetchCost(28, 27));
        assertThrows(java.io.IOException.class,
                () -> PgBaselineExport.prefetchCost(1L + Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    @Test(timeout = 10_000)
    public void failedPrefetchWakesAWaitingConsumerAndPreservesTheFirstError() throws Exception {
        var sessionClass = Class.forName("org.sibench.capture.PgBaselineExport$Session");
        var sessionConstructor = sessionClass.getDeclaredConstructors()[0];
        sessionConstructor.setAccessible(true);
        var rowsClass = Class.forName("org.sibench.capture.PgBaselineExport$PrefetchRows");
        var rowsConstructor = rowsClass.getDeclaredConstructors()[0];
        rowsConstructor.setAccessible(true);
        var rangeClass = Class.forName("org.sibench.capture.PgBaselineExport$PrefetchRows$Range");
        var rangeConstructor = rangeClass.getDeclaredConstructors()[0];
        rangeConstructor.setAccessible(true);
        var read = rangeClass.getDeclaredMethod("read");
        var take = rangeClass.getDeclaredMethod("take");
        read.setAccessible(true);
        take.setAccessible(true);
        for (boolean failFirst : List.of(false, true)) {
            var error = new java.sql.SQLException("deliberate source connection failure");
            PgBaselineVerify.Connections connections = () -> { throw error; };
            Object session = sessionConstructor.newInstance(connections, "unused", new java.util.concurrent.ConcurrentLinkedQueue<>());
            try (var rows = (AutoCloseable) rowsConstructor.newInstance(session, null, 1)) {
                Object range = rangeConstructor.newInstance(rows, null, "user2", 2, 128);
                var consumer = java.util.concurrent.Executors.newSingleThreadExecutor();
                try {
                    if (failFirst) read.invoke(range);
                    var started = new java.util.concurrent.CountDownLatch(1);
                    var thread = new java.util.concurrent.atomic.AtomicReference<Thread>();
                    var result = consumer.submit(() -> {
                        thread.set(Thread.currentThread());
                        started.countDown();
                        return take.invoke(range);
                    });
                    assertTrue(started.await(1, java.util.concurrent.TimeUnit.SECONDS));
                    if (!failFirst) {
                        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(1);
                        while (thread.get().getState() != Thread.State.WAITING && System.nanoTime() < deadline)
                            Thread.yield();
                        assertEquals(Thread.State.WAITING, thread.get().getState());
                        assertFalse(result.isDone());
                        read.invoke(range);
                    }
                    var failed = assertThrows(java.util.concurrent.ExecutionException.class,
                            () -> result.get(3, java.util.concurrent.TimeUnit.SECONDS));
                    assertSame(error, failed.getCause().getCause());
                } finally {
                    consumer.shutdownNow();
                }
            }
        }
    }

    @Test
    public void primaryReadRejectsSortsAndOtherIndexes() throws Exception {
        PgBaselineExport.requirePrimaryPlan(com.google.gson.JsonParser.parseString(
                "{\"Node Type\":\"Index Scan\",\"Index Name\":\"usertable_pkey\"}").getAsJsonObject(), "usertable_pkey");
        for (String plan : List.of(
                "{}",
                "{\"Node Type\":\"Seq Scan\"}",
                "{\"Node Type\":\"Index Only Scan\",\"Index Name\":\"usertable_pkey\"}",
                "{\"Node Type\":\"Bitmap Heap Scan\"}",
                "{\"Node Type\":\"Sort\",\"Plans\":[{\"Node Type\":\"Index Scan\",\"Index Name\":\"usertable_pkey\"}]}",
                "{\"Node Type\":\"Index Scan\",\"Index Name\":\"another_index\"}"))
            assertThrows(java.io.IOException.class, () -> PgBaselineExport.requirePrimaryPlan(
                    com.google.gson.JsonParser.parseString(plan).getAsJsonObject(), "usertable_pkey"));
    }

    @Test
    public void exportRetainsCompleteRowsAndDuplicateSecondaryValues() throws Exception {
        Path output = temporary.newFolder().toPath();
        var config = ParquetDataFileTest.layout(output, 192 * 1024, 5);
        var source = SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), Map.of("tables", 1L));
        var catalog = IndexCatalog.fromSchema(source);
        var schema = MessageTypeParser.parseMessageType("message sbtest1 { optional int64 id; optional int64 k;"
                + " optional binary c (UTF8); optional binary pad (UTF8);"
                + " required int64 _sib_row_id; required int64 _sib_timestamp; }");
        var table = new PgBaselineExport.Table(source.tables().get(0), "id", true, schema, 2, true, List.of());
        var rows = List.of(new String[] {"1", "10", "first", "pad1"},
                new String[] {"2", "10", "second", "pad2"}).iterator();
        var files = PgBaselineExport.writeTable(table, 0, 1, 1,
                () -> rows.hasNext() ? rows.next() : null, output, config);
        Path shard = output.resolve((String) files.get(0).file().get("path"));
        try (var reader = ParquetReader.<Group>builder(new GroupReadSupport(),
                new org.apache.hadoop.fs.Path(shard.toUri())).build()) {
            var first = reader.read(); var second = reader.read();
            assertEquals("first", first.getString("c", 0));
            assertEquals("pad2", second.getString("pad", 0));
            assertEquals(2L, second.getLong("_sib_row_id", 0));
            assertNull(reader.read());
        }
        Files.write(output.resolve("files.jsonl"), files.stream().map(f -> new com.google.gson.Gson().toJson(f.file())).toList());
        BaselineManifest.publish(source, output, Map.of("sbtest1", 2L));
        var manifest = BaselineManifest.verify(source, output);
        var operations = new ArrayList<IndexOperationTrace.Operation>();
        BaselineOperations.derive(output, catalog, operations::add, new BaselineOperations.Rows() {
            public void file(String table, int id) {}
            public void row(String table, org.apache.parquet.schema.GroupType type, IndexOperationTrace.Operation primary) {}
            public void fileFinished() {}
        });
        assertEquals(4, operations.size());
        assertEquals(List.of(1L, 1L, 1L, 1L), operations.stream().map(IndexOperationTrace.Operation::file).toList());
        assertEquals(List.of(0L, 0L, 1L, 1L), operations.stream().map(IndexOperationTrace.Operation::position).toList());
        assertEquals(List.of(1L, 1L, 2L, 2L), operations.stream().map(IndexOperationTrace.Operation::rowId).toList());
        assertEquals(List.of(0, 1, 0, 1), operations.stream().map(IndexOperationTrace.Operation::index).toList());
        assertEquals(org.sibench.trace.IndexKey.ofInt(10), operations.get(1).indexedValue());
        assertEquals(org.sibench.trace.IndexKey.ofInt(10), operations.get(3).indexedValue());
        assertEquals("parquet", manifest.get("data_format").getAsString());
        // A correct manifest must not make a damaged Parquet footer acceptable.
        byte[] corrupt = Files.readAllBytes(shard);
        corrupt[corrupt.length - 1] = 1;
        Files.write(shard, corrupt);
        assertTrue(assertThrows(RuntimeException.class, () -> BaselineOperations.derive(output, catalog,
                operation -> {}, null)).getMessage().contains("Expected magic number at tail"));

    }

    @Test
    public void groupsDoNotBecomeFilesAndFileIdsDoNotDependOnWorkerOrder() throws Exception {
        Path output = temporary.newFolder().toPath();
        var config = ParquetDataFileTest.layout(output, 192 * 1024, 5);
        var schema = MessageTypeParser.parseMessageType("message t { optional int64 id; optional binary payload (UTF8); required int64 _sib_row_id; required int64 _sib_timestamp; }");
        var source = new SourceSchema.Table("t", List.of(), List.of());
        var table = new PgBaselineExport.Table(source, "id", true, schema, 3000, true, List.of());
        var random = new Random(17);
        var rows = new ArrayList<String[]>();
        for (int i = 0; i < 3000; i++) {
            byte[] bytes = new byte[256];
            random.nextBytes(bytes);
            rows.add(new String[] {Integer.toString(i), Base64.getEncoder().encodeToString(bytes)});
        }
        var iterator = rows.iterator();
        var written = PgBaselineExport.writeTable(table, 1, 3, 41,
                () -> iterator.hasNext() ? iterator.next() : null, output, config);
        assertTrue(written.size() > 1);
        assertTrue(written.size() < (3000 + config.maxRows() - 1) / config.maxRows());
        assertTrue(written.stream().anyMatch(w -> (long) w.file().get("rows") > config.maxRows()));
        long count = 0;
        for (int i = 0; i < written.size(); i++) {
            var metadata = written.get(i).file();
            assertEquals(2L + 3L * i, metadata.get("file_id"));
            Path file = output.resolve((String) metadata.get("path"));
            long size = Files.size(file);
            assertTrue(size <= 192 * 1024 * 105 / 100);
            if (i + 1 < written.size()) assertTrue(size >= 192 * 1024);
            try (var reader = ParquetReader.<Group>builder(new GroupReadSupport(),
                    new org.apache.hadoop.fs.Path(file.toUri())).build()) {
                Group row;
                while ((row = reader.read()) != null) {
                    assertEquals(count, row.getLong("id", 0));
                    assertEquals(41 + count, row.getLong("_sib_row_id", 0));
                    assertEquals(rows.get((int) count)[1], row.getString("payload", 0));
                    count++;
                }
            }
        }
        assertEquals(3000, count);
    }
}
