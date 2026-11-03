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
                "{\"Node Type\":\"Bitmap Heap Scan\"}",
                "{\"Node Type\":\"Sort\",\"Plans\":[{\"Node Type\":\"Index Scan\",\"Index Name\":\"usertable_pkey\"}]}",
                "{\"Node Type\":\"Index Scan\",\"Index Name\":\"another_index\"}"))
            assertThrows(java.io.IOException.class, () -> PgBaselineExport.requirePrimaryPlan(
                    com.google.gson.JsonParser.parseString(plan).getAsJsonObject(), "usertable_pkey"));
    }

    @Test
    public void protoExportProjectsIndexColumnsAndPreservesDuplicateSecondaryValues() throws Exception {
        Path output = temporary.newFolder().toPath();
        var config = ParquetDataFileTest.layout(output, 192 * 1024, 5);
        var source = SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), Map.of("tables", 1L));
        var catalog = IndexCatalog.fromSchema(source);
        var schema = MessageTypeParser.parseMessageType("message sbtest1 { optional int64 id; optional int64 k;"
                + " optional binary c (UTF8); optional binary pad (UTF8);"
                + " required int64 _sib_row_id; required int64 _sib_timestamp; }");
        var full = new PgBaselineExport.Table(source.tables().get(0), "id", true, schema, 2, true, List.of());
        var table = PgBaselineExport.indexColumns(full, catalog);
        assertEquals(List.of("id", "k", "_sib_row_id", "_sib_timestamp"),
                table.schema().getFields().stream().map(org.apache.parquet.schema.Type::getName).toList());
        var rows = List.of(new String[] {"1", "10"}, new String[] {"2", "10"}).iterator();
        var files = PgBaselineExport.writeIndexTable(table, 0, 1,
                () -> rows.hasNext() ? rows.next() : null, output, config, catalog);
        var operations = new ArrayList<IndexOperationTrace.Operation>();
        assertEquals(4, ProtoTrace.read(output.resolve("sbtest1.pb.zst"), null,
                (String) files.get(0).file().get("logical_sha256"), operations::add));
        assertEquals(List.of(1L, 1L, 2L, 2L), operations.stream().map(IndexOperationTrace.Operation::rowId).toList());
        assertEquals(List.of(0, 1, 0, 1), operations.stream().map(IndexOperationTrace.Operation::index).toList());
        assertEquals(org.sibench.trace.IndexKey.ofInt(10), operations.get(1).indexedValue());
        assertEquals(org.sibench.trace.IndexKey.ofInt(10), operations.get(3).indexedValue());
        assertFalse(Files.exists(output.resolve("sbtest1.parquet")));
        Path shard = output.resolve("sbtest1.pb.zst");
        String expectedHash = java.util.HexFormat.of().formatHex(
                java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(shard)));
        Files.write(output.resolve("files.jsonl"), files.stream().map(f -> new com.google.gson.Gson().toJson(f.file())).toList());
        BaselineManifest.publish(source, output, Map.of("sbtest1", 2L), TraceOutput.PROTO);
        var manifest = BaselineManifest.verify(source, output);
        assertEquals(expectedHash, manifest.getAsJsonArray("files").get(0).getAsJsonObject().get("sha256").getAsString());
        try (var identities = new org.sibench.state.RowIdentities(output.resolve("state"), 4, 4)) {
            assertEquals(2, ProtoBaseline.derive(output, manifest, catalog, identities, operation -> {}).rows());
        }
        // Even an unchanged file-cache identity must not let corrupt bytes enter the projection.
        var modified = Files.getLastModifiedTime(shard);
        byte[] corrupt = Files.readAllBytes(shard);
        corrupt[corrupt.length - 1] = 1;
        Files.write(shard, corrupt);
        Files.setLastModifiedTime(shard, modified);
        try (var identities = new org.sibench.state.RowIdentities(output.resolve("corrupt-state"), 4, 4)) {
            assertThrows(java.io.IOException.class,
                    () -> ProtoBaseline.derive(output, manifest, catalog, identities, operation -> {}));
        }
        PgBaselineExport.requirePrimaryPlan(com.google.gson.JsonParser.parseString(
                "{\"Node Type\":\"Index Only Scan\",\"Index Name\":\"usertable_pkey\"}").getAsJsonObject(), "usertable_pkey", true);
        assertThrows(java.io.IOException.class, () -> PgBaselineExport.requirePrimaryPlan(
                com.google.gson.JsonParser.parseString("{\"Node Type\":\"Index Only Scan\",\"Index Name\":\"wrong\"}")
                        .getAsJsonObject(), "usertable_pkey", true));
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
