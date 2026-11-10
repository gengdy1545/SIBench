package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import static org.sibench.capture.IndexOperationTrace.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;
import org.sibench.config.Engine;
import org.sibench.config.SibenchConfig;
import org.sibench.generation.PostgresDatabase;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

/** Explicit native deletion/reinsert counterexample on eight hand-written baseline rows. */
public class PixelsCdcCheck {
    @Test public void nativeDeleteReinsertAndRollbackPreserveBothValueHistories() throws Exception {
        run(false);
    }

    @Test public void rejectedVisibilityPublicationFailsNativeLoad() throws Exception {
        run(true);
    }

    private void run(boolean loadFailure) throws Exception {
        String configured = System.getProperty("sibench.pixels.build");
        assertNotNull("Set sibench.pixels.build", configured);
        Path root = Path.of("").toRealPath(), build = root.resolve(configured).toRealPath();
        var identity = new com.google.gson.Gson().toJsonTree(PixelsBuild.verify(root, build)).getAsJsonObject();
        var settings = SibenchConfig.engine(root, SibenchConfig.read(root.resolve("config/sibench.yaml")), Engine.PIXELS);
        var source = SourceSchema.hybench(root.resolve("benchmark/HyBench-2024"));
        Path output = Files.createTempDirectory(root.resolve("results/capture"), "pixels-cdc-check-");
        Path data = Files.createDirectory(output.resolve("csv"));
        HybenchTest.fixture(data);
        var sourceSettings = new LinkedHashMap<>(SibenchConfig.postgresSettings(object(object(settings, "source"), "settings")));
        sourceSettings.putAll(Map.of("wal_level", "logical", "max_wal_senders", "4", "max_replication_slots", "4"));
        var expected = new LinkedHashMap<String, Long>();
        source.tables().forEach(table -> expected.put(table.name(), 1L));
        Duration timeout = Duration.ofMinutes(3);
        var loopback = InetAddress.getLoopbackAddress();
        try (var host = PixelsHost.local(build, output.resolve("pixels"), identity);
             var database = PostgresDatabase.create(build.resolve("postgres/runtime"), output.resolve("database"), sourceSettings);
             var traceSocket = new ServerSocket(0, 1, loopback);
             var reportSocket = new ServerSocket(0, 1, loopback);
             var trace = new PixelsTrace(output.resolve("trace"), source, Map.of(), null);
             var capture = new PixelsCapture(trace, traceSocket, "trace-test", reportSocket, "sink-test")) {
            var pixels = host.call(Map.of("op", "start", "run", "pixels", "overlay", object(settings, "properties"),
                    "memory", Map.of("coordinator_heap", "1g", "retina_heap", "4g"),
                    "trace_endpoint", "127.0.0.1:" + traceSocket.getLocalPort(), "trace_token", "trace-test"));
            capture.acceptTracer(timeout);
            PixelsGeneration.loadBaseline(database, host, capture, trace, source, data, output, expected, object(settings, "load"), timeout);
            if (loadFailure) {
                Path input = Files.writeString(output.resolve("rejected.tbl"), "99|1|2300|0|2023-01-01 00:00:00|\\N\n");
                host.upload("savingaccount", "rejected.tbl", input, timeout);
                Path classes = Files.createDirectory(output.resolve("probe-classes"));
                String classpath = build.resolve("jars/pixels-cli.jar") + ":" + build.resolve("jars/pixels-daemon.jar");
                var compile = NativeProcess.run("compile-probe", java.util.List.of(Path.of(System.getProperty("java.home"), "bin/javac").toString(),
                        "--release", "17", "-cp", classpath, "-d", classes.toString(), root.resolve("src/test/native/pixels/LoadFailureProbe.java").toString()),
                        Map.of(), output, output.resolve("probe-build"), Duration.ofSeconds(30));
                assertEquals(compile.stdout(), 0, compile.exit());
                Path properties = Files.writeString(output.resolve("probe.properties"), pixels.get("properties").getAsString());
                var failure = NativeProcess.run("load-rejection", java.util.List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                        "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-cp", classes + ":" + classpath,
                        "org.sibench.pixels.LoadFailureProbe", "file://" + output.resolve("pixels/load/savingaccount") + "/"),
                        Map.of("PIXELS_CONFIG", properties.toString(), "PIXELS_HOME", output.resolve("pixels/home").toString()),
                        output, output.resolve("probe-run"), Duration.ofSeconds(60));
                assertEquals(failure.stdout(), 0, failure.exit());
                return;
            }
            Path sinkRoot = Files.createDirectory(output.resolve("sink"));
            Path properties = Files.writeString(sinkRoot.resolve("pixels.properties"), pixels.get("properties").getAsString());
            Process sink = PixelsGeneration.replicate(database, capture, source, build, settings, sinkRoot,
                    properties, reportSocket.getLocalPort(), "sink-test", timeout);
            try (var connection = database.connect("counterexample"); var statement = connection.createStatement()) {
                // The source observations are the oracle: present, updated, absent, reinserted, rollback unchanged.
                assertThrows(java.sql.SQLException.class,
                        () -> statement.executeUpdate("UPDATE savingaccount SET accountid=99 WHERE accountid=1"));
                assertEquals(1, statement.executeUpdate("UPDATE savingaccount SET balance=2100 WHERE accountid=1"));
                marker(statement, capture, 1, timeout);
                assertEquals(1, statement.executeUpdate("DELETE FROM savingaccount WHERE accountid=1"));
                marker(statement, capture, 2, timeout);
                assertEquals(1, statement.executeUpdate("INSERT INTO savingaccount VALUES(1,1,2200,0,'2023-01-01',NULL)"));
                marker(statement, capture, 3, timeout);
                connection.setAutoCommit(false);
                assertEquals(1, statement.executeUpdate("DELETE FROM savingaccount WHERE accountid=1"));
                connection.rollback(); connection.setAutoCommit(true);
                marker(statement, capture, 4, timeout);
                try (var rows = statement.executeQuery("SELECT balance FROM savingaccount WHERE accountid=1")) {
                    assertTrue(rows.next()); assertEquals(2200, rows.getInt(1)); assertFalse(rows.next());
                }
                assertEquals(3, trace.version());
                capture.sinkStopping(); PixelsGeneration.stopSink(sink);
                capture.call(trace::finish, false, timeout);
                capture.close(); host.call(Map.of("op", "stop")); database.close();
                var manifest = new com.google.gson.Gson().toJsonTree(trace.publish(Map.of("build", identity))).getAsJsonObject();
                int table = 1 + source.tables().stream().map(SourceSchema.Table::name).toList().indexOf("savingaccount");
                for (boolean rowIds : new boolean[] {true, false}) try (var index = new IndexReferenceReplay(rowIds, false)) {
                    for (String stream : new String[] {"baseline", "workload"})
                        ProtoTrace.source(output.resolve("trace"), manifest, stream).read(op -> {
                            if (!isControl(op)) index.acceptCommitted(op);
                        });
                    var baseline = index.lookupPrimary(table, IndexKey.ofInt(1), 0);
                    var update = index.lookupPrimary(table, IndexKey.ofInt(1), 1);
                    assertNotNull(baseline); assertNotNull(update);
                    assertNull(index.lookupPrimary(table, IndexKey.ofInt(1), 2));
                    var reinsert = index.lookupPrimary(table, IndexKey.ofInt(1), 3);
                    assertNotNull(reinsert);
                    assertEquals(1, update.commitVersion()); assertEquals(3, reinsert.commitVersion());
                    if (rowIds) {
                        assertNotEquals(baseline.value(), update.value());
                        assertNotEquals(update.value(), reinsert.value());
                    } else {
                        assertFalse(update.value() == reinsert.value() && update.position() == reinsert.position());
                    }
                    assertEquals(baseline, index.lookupPrimary(table, IndexKey.ofInt(1), 0));
                }
            } finally {
                if (sink.isAlive()) { sink.destroyForcibly(); sink.waitFor(10, java.util.concurrent.TimeUnit.SECONDS); }
            }
        }
    }

    private static void marker(java.sql.Statement statement, PixelsCapture capture, int id, Duration timeout) throws Exception {
        statement.executeUpdate("INSERT INTO sibench_marker VALUES(" + id + ")");
        capture.awaitMarker(id, timeout);
    }
}
