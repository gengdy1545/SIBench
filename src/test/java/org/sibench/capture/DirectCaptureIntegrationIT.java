package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.GsonBuilder;
import org.junit.Test;
import org.sibench.config.ConfigValues;
import org.sibench.replay.*;

import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Explicit real-PG source probe: three rows, two sessions, then local service replay. */
public class DirectCaptureIntegrationIT {
    @Test(timeout = 120000)
    public void capturesNativeFactsDirectlyWithSnapshotsRollbackAndFreshReinsertIdentity() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        Path runRoot = Path.of(Objects.requireNonNull(System.getenv("SIBENCH_DIRECT_RUN")));
        String admin = Objects.requireNonNull(System.getenv("SIBENCH_DIRECT_PG_JDBC"));
        for (String benchmark : List.of("sysbench", "ycsb")) {
            Path artifacts = Files.createDirectory(runRoot.resolve(benchmark));
            Files.createDirectory(artifacts.resolve("native"));
            var runtime = org.sibench.config.SibenchConfig.read(root.resolve("config/sibench.yaml"));
            var generation = ConfigValues.object(runtime, "generation");
            generation.put("operation_target", 0);
            generation.put("output_byte_target", 0);
            runtime.put("generation", generation);
            Files.writeString(artifacts.resolve("native/sibench.yaml"), new org.yaml.snakeyaml.Yaml().dump(runtime));
            var document = ConfigValues.parse(Files.readString(root.resolve("src/test/resources/config/"
                    + (benchmark.equals("sysbench") ? "sysbench_2k.yaml" : "ycsb_1k.yaml"))));
            var nativeSettings = ConfigValues.object(document, "native");
            nativeSettings.put("threads", 2);
            if (benchmark.equals("sysbench")) { nativeSettings.put("tables", 1); nativeSettings.put("table-size", 3); }
            else { nativeSettings.put("recordcount", 3); nativeSettings.put("insertcount", 3); }
            document.put("native", nativeSettings);
            var pg = ConfigValues.object(document, "postgres");
            String database = "direct_probe_" + benchmark;
            pg.put("database", database);
            pg.put("port", Integer.parseInt(admin.substring(admin.lastIndexOf(':') + 1, admin.lastIndexOf('/'))));
            document.put("postgres", pg);
            document.put("artifacts", Map.of("directory", root.relativize(artifacts).toString()));
            var capture = ConfigValues.object(document, "capture");
            capture.put("output", "proto");
            capture.put("raw_directory", root.relativize(artifacts.resolve("source")).toString());
            document.put("capture", capture);
            Path config = artifacts.resolve("native/benchmark.yaml");
            Files.writeString(config, new org.yaml.snakeyaml.Yaml().dump(document));
            var profile = CaptureProfile.read(root, config);
            NativeLoad.requireDirectCapture(admin, "postgres", NativeLoad.library(root));
            String jdbc = admin.substring(0, admin.lastIndexOf('/') + 1) + database;
            try (var connection = DriverManager.getConnection(admin, "postgres", ""); var sql = connection.createStatement()) {
                sql.execute("CREATE DATABASE " + database);
            }
            boolean sysbench = benchmark.equals("sysbench");
            String table = sysbench ? "sbtest1" : "usertable", pk = sysbench ? "id" : "ycsb_key", value = sysbench ? "k" : "field0";
            try (var connection = DriverManager.getConnection(jdbc, "postgres", ""); var sql = connection.createStatement()) {
                NativeLoad.unloggedTables(connection);
                if (sysbench) {
                    sql.execute("CREATE TABLE sbtest1(id SERIAL PRIMARY KEY,k integer NOT NULL DEFAULT 0,c char(120) NOT NULL DEFAULT '',pad char(60) NOT NULL DEFAULT '')");
                    sql.execute("CREATE INDEX k_1 ON sbtest1(k)");
                    sql.execute("INSERT INTO sbtest1(id,k) VALUES(1,10),(2,20),(3,30)");
                } else {
                    String columns = String.join(",", java.util.stream.IntStream.range(0, 10).mapToObj(i -> "field" + i + " text").toList());
                    sql.execute("CREATE TABLE usertable(ycsb_key varchar PRIMARY KEY," + columns + ")");
                    sql.execute("INSERT INTO usertable(ycsb_key,field0) VALUES('user1','10'),('user2','20'),('user3','30')");
                }
            }
            NativeLoad.captureReady(jdbc, "postgres", NativeLoad.library(root), profile.schema());
            PgBaselineExport.export(jdbc, profile.baseline(), profile.layout());
            CaptureCompleteness.snapshot(jdbc, "postgres", artifacts.resolve("index-counters-before.json"), config.toString());
            try (var direct = DirectCapture.begin(profile, jdbc, "postgres");
                    var run = CaptureRun.beginExternal(profile.raw(), "controlled_pg_source_probe")) {
                run.schemaPreflight(org.sibench.schema.PgSchemaCheck.check(root, document, jdbc, "postgres"));
                run.build(CaptureBuild.identity(root, Map.of("kind", "controlled_sql_probe")));
                try (var writer = connection(jdbc, profile.raw(), !sysbench, false);
                        var reader = connection(jdbc, profile.raw(), !sysbench, true)) {
                    modify(writer, "UPDATE " + table + " SET " + value + "=? WHERE " + pk + "=?", sysbench, "40", 1);
                    assertEquals("10", select(reader, table, pk, value, sysbench, 1));
                    writer.commit();
                    assertEquals("10", select(reader, table, pk, value, sysbench, 1));
                    reader.commit();
                    modify(writer, "DELETE FROM " + table + " WHERE " + pk + "=?", sysbench, null, 1);
                    writer.commit();
                    modify(writer, "INSERT INTO " + table + "(" + value + "," + pk + ") VALUES(?,?)", sysbench, "50", 1);
                    writer.commit();
                    modify(writer, "UPDATE " + table + " SET " + value + "=? WHERE " + pk + "=?", sysbench, "999", 2);
                    assertEquals("999", select(writer, table, pk, value, sysbench, 2));
                    writer.rollback();
                    assertEquals("50", select(writer, table, pk, value, sysbench, 1));
                    writer.commit();
                }
                NativeProcess.drain(jdbc, "postgres");
                var sessions = direct.paths(true).stream().map(path -> Map.of("pg_file", path.getFileName().toString())).toList();
                run.completeExternal(!sysbench, Map.of("workloads", List.of(Map.of("script", "controlled", "sessions", sessions))));
                direct.endSources();
                CaptureCompleteness.snapshot(jdbc, "postgres", artifacts.resolve("index-counters-after.json"), config.toString());
                assertEquals("consistent", direct.finish().get("capture_completeness"));
            }
            assertFalse(Files.exists(artifacts.resolve("transactions")));
            try (var files = Files.list(profile.raw())) { assertFalse(files.anyMatch(p -> p.toString().endsWith(".jsonl.zst"))); }
            var manifest = org.sibench.trace.TraceManifest.read(artifacts.resolve("trace"));
            var operations = new ArrayList<IndexOperationTrace.Operation>();
            ProtoTrace.source(artifacts.resolve("trace"), manifest, "workload").read(operations::add);
            assertTrue(operations.stream().anyMatch(op -> op.kind() == IndexOperationTrace.SEARCH && op.readTimestamp() == 0));
            assertTrue(operations.stream().anyMatch(op -> op.kind() == IndexOperationTrace.UPSERT && op.rowId() == 4));
            Path reference = artifacts.resolve("reference.json");
            assertEquals("passed", ReferenceReplay.run(artifacts.resolve("trace"), ReplayConfig.defaults(), List.of(true), reference).get("status"));
            var service = new org.sibench.adapters.ServiceConfig("rocksdb", null, artifacts.resolve("index"), 0,
                    org.sibench.adapters.proto.ValueKind.ROW_ID, null, false);
            assertEquals("passed", org.sibench.adapters.ServiceReplay.run(artifacts.resolve("trace"), ReplayConfig.defaults(), service,
                    reference, artifacts.resolve("service.json")).get("status"));
            Files.writeString(artifacts.resolve("probe.json"), new GsonBuilder().setPrettyPrinting().create().toJson(
                    Map.of("status", "passed", "scope", "controlled native-PG source facts, not a benchmark performance run", "baseline_rows", 3)));
        }
        verifyNativeWatermark(admin.substring(0, admin.lastIndexOf('/') + 1) + "direct_probe_sysbench", runRoot);
        verifyImplicitRollbackAtClose(admin.substring(0, admin.lastIndexOf('/') + 1) + "direct_probe_sysbench", runRoot);
        verifyDisconnectedReceiver(admin.substring(0, admin.lastIndexOf('/') + 1) + "direct_probe_sysbench", runRoot);
    }

    /** A delayed receiver must hold back the source position without gating each commit. */
    private static void verifyNativeWatermark(String jdbc, Path runRoot) throws Exception {
        Path endpoint = Path.of(System.getenv("SIBENCH_CAPTURE_SOCKET"));
        var measurements = new LinkedHashMap<String, Object>();
        try (var listener = java.nio.channels.ServerSocketChannel.open(java.net.StandardProtocolFamily.UNIX);
                var control = DriverManager.getConnection(jdbc, "postgres", "");
                var producer = DriverManager.getConnection(jdbc, "postgres", "")) {
            listener.bind(java.net.UnixDomainSocketAddress.of(endpoint));
            int pid;
            try (var sql = producer.createStatement(); var rows = sql.executeQuery("SELECT pg_backend_pid()")) {
                assertTrue(rows.next()); pid = rows.getInt(1);
            }
            // Both source commits return before the receiver accepts or acknowledges them.
            try (var sql = producer.createStatement()) {
                sql.setQueryTimeout(5);
                sql.execute("SET application_name='sibench:watermark_probe'");
                assertEquals(1, sql.executeUpdate("UPDATE sbtest1 SET k=k+1 WHERE id=3"));
            }
            try (var socket = listener.accept(); var input = new java.io.BufferedReader(new java.io.InputStreamReader(
                    java.nio.channels.Channels.newInputStream(socket), java.nio.charset.StandardCharsets.UTF_8))) {
                assertEquals("pg-" + pid + ".jsonl.zst", input.readLine());
                var first = sourceCommit(input);
                var writer = sourceCommit(input);
                long before = watermark(control, 0, 0, 0);
                assertEquals(1, first.get("capture_sequence").getAsLong());
                assertEquals(2, writer.get("capture_sequence").getAsLong());
                long initial = first.get("csn").getAsLong(), committed = writer.get("csn").getAsLong();
                assertTrue(committed > initial);
                assertTrue(before <= initial);
                assertTrue(watermark(control, pid, 1, initial) <= initial);
                assertTrue(watermark(control, pid, 2, committed) >= committed);
                try (var sql = producer.createStatement()) {
                    sql.execute("SELECT k FROM sbtest1 WHERE id=3");
                    assertEquals(1, sql.executeUpdate("UPDATE sbtest1 SET k=k+1 WHERE id=3"));
                }
                var reader = sourceCommit(input);
                var nextWriter = sourceCommit(input);
                assertEquals(3, reader.get("capture_sequence").getAsLong());
                assertEquals(4, nextWriter.get("capture_sequence").getAsLong());
                long next = nextWriter.get("csn").getAsLong();
                assertTrue(next > committed);
                assertTrue(watermark(control, 0, 0, 0) <= committed);
                assertTrue(watermark(control, pid, 4, next) >= next);
                try (var sql = producer.createStatement()) {
                    assertEquals(1, sql.executeUpdate("UPDATE sbtest1 SET k=k+1 WHERE id=3"));
                }
                var tail = sourceCommit(input);
                assertEquals(5, tail.get("capture_sequence").getAsLong());
                long finalCounter = tail.get("csn").getAsLong();
                assertTrue(finalCounter > next);
                assertTrue(watermark(control, 0, 0, 0) <= next);
                producer.close();
                for (String line; ; ) {
                    line = input.readLine();
                    assertNotNull("Source must have a final handoff", line);
                    if (com.google.gson.JsonParser.parseString(line).getAsJsonObject().get("event").getAsString().equals("capture_end")) break;
                }
                try (var sql = control.createStatement(); var rows = sql.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE pid=" + pid)) {
                    assertTrue(rows.next());
                    assertEquals("Backend stays visible until its final facts reach the receiver", 1, rows.getInt(1));
                }
                var acknowledgement = java.nio.ByteBuffer.wrap(new byte[] {'E'});
                while (acknowledgement.hasRemaining()) socket.write(acknowledgement);
                CaptureRun.awaitBackendExit(pid);
                assertTrue("Final handoff must release the source watermark even without a per-transaction tail acknowledgement",
                        watermark(control, 0, 0, 0) >= finalCounter);
                measurements.put("initial_counter", initial);
                measurements.put("first_writer_counter", committed);
                measurements.put("second_writer_counter", next);
                measurements.put("handoff_tail_counter", finalCounter);
                measurements.put("watermark_before_acknowledgement", before);
                measurements.put("status", "passed");
            }
        } finally { Files.deleteIfExists(endpoint); }
        Files.writeString(runRoot.resolve("watermark.json"), new GsonBuilder().setPrettyPrinting().create().toJson(measurements));
    }

    private static void verifyImplicitRollbackAtClose(String jdbc, Path runRoot) throws Exception {
        Path endpoint = Path.of(System.getenv("SIBENCH_CAPTURE_SOCKET"));
        for (boolean writer : List.of(false, true)) {
            try (var listener = java.nio.channels.ServerSocketChannel.open(java.net.StandardProtocolFamily.UNIX);
                    var control = DriverManager.getConnection(jdbc, "postgres", "");
                    var producer = DriverManager.getConnection(jdbc, "postgres", "")) {
                listener.bind(java.net.UnixDomainSocketAddress.of(endpoint));
                int original;
                try (var sql = control.createStatement(); var rows = sql.executeQuery("SELECT k FROM sbtest1 WHERE id=3")) {
                    assertTrue(rows.next()); original = rows.getInt(1);
                }
                producer.setAutoCommit(false);
                try (var sql = producer.createStatement()) {
                    sql.execute("SET application_name='sibench:close_probe'");
                    if (writer) assertEquals(1, sql.executeUpdate("UPDATE sbtest1 SET k=k+1 WHERE id=3"));
                    else try (var rows = sql.executeQuery("SELECT k FROM sbtest1 WHERE id=3")) { assertTrue(rows.next()); }
                }
                producer.close();
                try (var socket = listener.accept(); var input = new java.io.BufferedReader(new java.io.InputStreamReader(
                        java.nio.channels.Channels.newInputStream(socket), java.nio.charset.StandardCharsets.UTF_8))) {
                    assertTrue(input.readLine().startsWith("pg-"));
                    boolean aborted = false;
                    for (String line; ; ) {
                        line = input.readLine();
                        assertNotNull("Open transaction must finish before source handoff", line);
                        String event = com.google.gson.JsonParser.parseString(line).getAsJsonObject().get("event").getAsString();
                        assertNotEquals("Closing an open transaction must not commit it", "transaction_commit", event);
                        if (event.equals("transaction_abort")) { assertFalse(aborted); aborted = true; }
                        if (event.equals("capture_end")) {
                            assertTrue("Source handoff must follow PostgreSQL's implicit rollback", aborted);
                            socket.write(java.nio.ByteBuffer.wrap(new byte[] {'E'}));
                            break;
                        }
                    }
                }
                try (var sql = control.createStatement(); var rows = sql.executeQuery("SELECT k FROM sbtest1 WHERE id=3")) {
                    assertTrue(rows.next()); assertEquals("Closed transaction must leave no writes", original, rows.getInt(1));
                }
            } finally { Files.deleteIfExists(endpoint); }
        }
        Files.writeString(runRoot.resolve("implicit-close-rollback.json"), "{\"status\":\"passed\",\"scope\":\"read-only and writing transactions implicitly abort before source completion; no uncommitted writes survive\"}");
    }

    private static void verifyDisconnectedReceiver(String jdbc, Path runRoot) throws Exception {
        Path endpoint = Path.of(System.getenv("SIBENCH_CAPTURE_SOCKET"));
        try (var listener = java.nio.channels.ServerSocketChannel.open(java.net.StandardProtocolFamily.UNIX);
                var producer = DriverManager.getConnection(jdbc, "postgres", "")) {
            listener.bind(java.net.UnixDomainSocketAddress.of(endpoint));
            int pid;
            try (var sql = producer.createStatement(); var rows = sql.executeQuery("SELECT pg_backend_pid()")) {
                assertTrue(rows.next()); pid = rows.getInt(1);
            }
            try (var sql = producer.createStatement()) {
                sql.setQueryTimeout(5);
                sql.execute("SET application_name='sibench:disconnect_probe'");
                try (var socket = listener.accept(); var input = new java.io.BufferedReader(new java.io.InputStreamReader(
                        java.nio.channels.Channels.newInputStream(socket), java.nio.charset.StandardCharsets.UTF_8))) {
                    assertEquals("pg-" + pid + ".jsonl.zst", input.readLine());
                    sourceCommit(input);
                }
                SQLException failure = null;
                for (int i = 0; i < 4 && failure == null; i++) {
                    try { sql.execute("UPDATE sbtest1 SET k=k+1 WHERE id=3"); }
                    catch (SQLException error) { failure = error; }
                }
                assertNotNull("Disconnected capture must fail the backend", failure);
                assertTrue(failure.toString(), failure.getMessage().contains("FATAL: SIBench"));
                CaptureRun.awaitBackendExit(pid);
            }
            try (var control = DriverManager.getConnection(jdbc, "postgres", "");
                    var sql = control.createStatement(); var row = sql.executeQuery("SELECT 1")) {
                assertTrue("Postmaster must survive the capture transport failure", row.next());
                assertEquals(1, row.getInt(1));
            }
        } finally { Files.deleteIfExists(endpoint); }
        Files.writeString(runRoot.resolve("disconnected-receiver.json"), "{\"status\":\"passed\",\"scope\":\"backend FATAL without recursive abort/PANIC; postmaster remains usable\"}");
    }

    private static com.google.gson.JsonObject sourceCommit(java.io.BufferedReader input) throws Exception {
        for (String line; (line = input.readLine()) != null; ) {
            var event = com.google.gson.JsonParser.parseString(line).getAsJsonObject();
            if (event.get("event").getAsString().equals("transaction_commit")) return event;
        }
        throw new java.io.EOFException("Missing source commit");
    }

    private static long watermark(Connection connection, int pid, long sequence, long csn) throws Exception {
        try (var sql = connection.prepareStatement("SELECT public.sibench_capture_watermark(?,?,?)")) {
            sql.setInt(1, pid); sql.setLong(2, sequence); sql.setLong(3, csn);
            try (var result = sql.executeQuery()) { assertTrue(result.next()); return result.getLong(1); }
        }
    }

    private static Connection connection(String jdbc, Path source, boolean paired, boolean repeatable) throws Exception {
        Connection connection = DriverManager.getConnection(jdbc, "postgres", "");
        try (var sql = connection.createStatement()) {
            sql.execute("SET enable_seqscan=off");
            sql.execute("SET enable_bitmapscan=off");
        }
        if (repeatable) connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.setAutoCommit(false);
        if (paired) return CaptureDriver.wrap(connection, source);
        try (var sql = connection.createStatement()) { sql.execute("SET application_name='sibench:controlled'"); }
        return connection;
    }

    private static void key(PreparedStatement sql, int position, boolean integer, int value) throws SQLException {
        if (integer) sql.setInt(position, value); else sql.setString(position, "user" + value);
    }

    private static void modify(Connection connection, String query, boolean integer, String value, int key) throws Exception {
        try (var sql = connection.prepareStatement(query)) {
            if (value != null) {
                if (integer) sql.setInt(1, Integer.parseInt(value)); else sql.setString(1, value);
            }
            key(sql, value == null ? 1 : 2, integer, key);
            assertEquals(1, sql.executeUpdate());
        }
    }

    private static String select(Connection connection, String table, String pk, String value, boolean integer, int key) throws Exception {
        try (var sql = connection.prepareStatement("SELECT " + value + " FROM " + table + " WHERE " + pk + "=?")) {
            key(sql, 1, integer, key);
            try (var rows = sql.executeQuery()) {
                assertTrue(rows.next());
                String result = rows.getString(1);
                assertFalse(rows.next());
                return result;
            }
        }
    }
}
