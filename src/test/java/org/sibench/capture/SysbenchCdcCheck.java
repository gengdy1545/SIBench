package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;

import io.pixelsdb.pixels.sink.SinkProto;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

/** Explicit PG integration: committed write transactions and their actual pre-workload baseline. */
public class SysbenchCdcCheck {
    @Test(timeout = 60_000) public void concurrentReplacementCanMakeAnUpdateAffectZeroRows() throws Exception {
        Path root = Path.of("").toRealPath();
        String build = System.getProperty("sibench.pixels.build");
        assertNotNull("Set sibench.pixels.build", build);
        Files.createDirectories(root.resolve("results/capture"));
        Path work = Files.createTempDirectory(root.resolve("results/capture"), "zero-row-update-check-");
        var pool = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var database = org.sibench.generation.PostgresDatabase.create(root.resolve(build).resolve("postgres/runtime"),
                work.resolve("database"), Map.of("fsync", "off", "shared_buffers", "64MB"));
                var replacing = database.connect("replace"); var updating = database.connect("update");
                var observer = database.connect("observe")) {
            try (var sql = replacing.createStatement()) {
                sql.execute("CREATE TABLE t(id integer PRIMARY KEY, k integer)");
                sql.execute("INSERT INTO t VALUES (1,10)");
            }
            replacing.setAutoCommit(false);
            try (var sql = replacing.createStatement()) {
                assertEquals(1, sql.executeUpdate("DELETE FROM t WHERE id=1"));
                assertEquals(1, sql.executeUpdate("INSERT INTO t VALUES (1,20)"));
            }
            int pid;
            try (var sql = updating.createStatement(); var rows = sql.executeQuery("SELECT pg_backend_pid()")) {
                assertTrue(rows.next()); pid = rows.getInt(1);
            }
            var update = pool.submit(() -> {
                try (var sql = updating.createStatement()) { return sql.executeUpdate("UPDATE t SET k=k+1 WHERE id=1"); }
            });
            boolean blocked = false;
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                try (var sql = observer.createStatement(); var rows = sql.executeQuery("SELECT wait_event_type FROM pg_stat_activity WHERE pid=" + pid)) {
                    assertTrue(rows.next());
                    if ("Lock".equals(rows.getString(1))) { blocked = true; break; }
                }
                Thread.sleep(10);
            }
            assertTrue("The UPDATE must overlap the uncommitted replacement", blocked);
            replacing.commit();
            assertEquals(Integer.valueOf(0), update.get(10, java.util.concurrent.TimeUnit.SECONDS));
            try (var sql = observer.createStatement(); var rows = sql.executeQuery("SELECT k FROM t WHERE id=1")) {
                assertTrue(rows.next()); assertEquals(20, rows.getInt(1)); assertFalse(rows.next());
            }
        } finally { pool.shutdownNow(); }
        Generation.deleteTree(work);
    }

    @Test(timeout = 240_000) public void exportsBaselineAndKeepsDeleteReinsertTransactions() throws Exception {
        Path root = Path.of("").toRealPath();
        String build = System.getProperty("sibench.pixels.build"), client = System.getProperty("sibench.sysbench.build");
        assertNotNull("Set sibench.pixels.build", build); assertNotNull("Set sibench.sysbench.build", client);
        Files.createDirectories(root.resolve("results/capture"));
        Path work = Files.createTempDirectory(root.resolve("results/capture"), "sysbench-cdc-check-");
        var profile = parse(Files.readString(root.resolve("config/benchmark/sysbench.yaml")));
        var n = object(profile, "native");
        n.putAll(Map.of("tables", 2, "table-size", 500, "threads", 2, "prepare_threads", 2,
                "build_directory", client, "timeout_minutes", 3));
        n.put("workloads", Map.of("writes", Map.of("script", "oltp_read_write", "events", 100,
                "rand-zipfian-exp", 0.99, "point-selects", 0, "range-selects", false)));
        profile.put("native", n); profile.put("artifacts", Map.of("directory", work.resolve("input").toString()));
        Path benchmark = work.resolve("sysbench.yaml"), engine = work.resolve("pixels.yaml"), config = work.resolve("runtime.yaml");
        var yaml = new org.yaml.snakeyaml.Yaml(); Files.writeString(benchmark, yaml.dump(profile));
        var settings = new LinkedHashMap<>(parse(Files.readString(root.resolve("config/engine/pixels/pixels.yaml"))));
        settings.put("build_directory", build);
        settings.put("properties", root.resolve("config/engine/pixels/pixels.properties").toString());
        settings.put("sink_properties", root.resolve("config/engine/pixels/pixels-sink.properties").toString());
        Files.writeString(engine, yaml.dump(settings));
        Files.writeString(config, yaml.dump(Map.of("schema_version", 1, "purpose", "sibench",
                "benchmarks", Map.of("sysbench", org.sibench.config.BenchmarkFixtures.entry(benchmark, "postgres", work.resolve("trace"), "writes")),
                "engines", Map.of("pixels", engine.toString()))));
        var out = new java.io.ByteArrayOutputStream(); var err = new java.io.ByteArrayOutputStream();
        int status = org.sibench.cli.Sibench.run(new String[]{"prepare-cdc", config.toString()},
                new java.io.PrintStream(out), new java.io.PrintStream(err));
        assertEquals(err.toString(), 0, status);
        Path input = work.resolve("input");
        var csv = CsvInput.open(input.resolve("baseline-input"));
        assertEquals("sysbench", CsvInput.benchmark(csv.manifest()));
        assertEquals(500, csv.manifest().getAsJsonObject("baseline_rows").get("sbtest1").getAsInt());
        var cdc = CdcInput.open(input);
        assertEquals(101, cdc.counts().transactions());
        assertEquals(1, cdc.counts().markers());
        assertFalse(Files.exists(input.resolve("database/data")));
        var operations = new java.util.ArrayList<SinkProto.RowRecord>();
        int[] transactions = {0};
        CdcInput.stream(input, cdc.manifest(), 0, (table, bytes) -> {
            if (table != -1) {
                var row = SinkProto.RowRecord.parseFrom(bytes);
                if (!row.getSource().getTable().equals("sibench_marker")) operations.add(row);
            } else if (SinkProto.TransactionMetadata.parseFrom(bytes).getStatus() == SinkProto.TransactionStatus.END && !operations.isEmpty()) {
                var delete = operations.stream().filter(row -> row.getOp() == SinkProto.OperationType.DELETE).toList();
                var insert = operations.stream().filter(row -> row.getOp() == SinkProto.OperationType.INSERT).toList();
                assertEquals(1, delete.size()); assertEquals(1, insert.size());
                assertEquals(delete.get(0).getSource().getTable(), insert.get(0).getSource().getTable());
                assertEquals(delete.get(0).getBefore().getValues(0), insert.get(0).getAfter().getValues(0));
                transactions[0]++; operations.clear();
            }
        });
        assertEquals(100, transactions[0]);
        var summary = parse(Files.readString(input.resolve("writes/summary.json")));
        assertEquals(0, integer(summary, "read"));
        assertEquals(100, integer(summary, "transactions"));
    }
}
