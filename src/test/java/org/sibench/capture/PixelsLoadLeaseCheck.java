package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

/**
 * Explicit bounded native LOAD checks. One LOAD outlives its write-transaction lease (300 s in the
 * pinned coordinator) and still commits: a FIFO input keeps the LOAD consumer reading for longer
 * than the lease without large data, and a build without lease renewal fails at commit. A LOAD whose
 * visibility publication is rejected must fail. Needs sibench.pixels.build (a two-phase build) and
 * about seven minutes.
 */
public class PixelsLoadLeaseCheck {
    private static final Duration LEASE = Duration.ofMillis(300_000), MARGIN = Duration.ofSeconds(40);

    @Test public void loadBeyondOneLeaseCommits() throws Exception {
        String configured = System.getProperty("sibench.pixels.build");
        assertNotNull("Set sibench.pixels.build", configured);
        Path build = Path.of(configured).toRealPath(), root = Path.of("").toRealPath();
        Path output = Files.createTempDirectory(root.resolve("results/capture"), "pixels-load-lease-");
        try (var host = PixelsHost.local(build, output.resolve("pixels"), PixelsBuild.identity(build))) {
            var started = host.call(Map.of("op", "start", "run", "pixels", "overlay", Map.of(),
                    "memory", Map.of("coordinator_heap", "1g", "retina_heap", "4g"), "trace", false), Duration.ofMinutes(10));
            host.call(Map.of("op", "create", "schema", "postgres", "table", "leased",
                    "spec", "leased\tid\tid:bigint;name:varchar(8)\n"), Duration.ofMinutes(5));
            Path input = Files.createDirectories(Path.of(started.get("directory").getAsString(), "load/leased")).resolve("part-000000.tbl");
            var mkfifo = new ProcessBuilder("mkfifo", input.toString()).inheritIO().start();
            assertEquals(0, mkfifo.waitFor());
            // The consumer opens the FIFO and blocks on its rest until the lease has expired once.
            var writer = CompletableFuture.runAsync(() -> {
                try (BufferedWriter rows = Files.newBufferedWriter(input, StandardCharsets.UTF_8)) {
                    rows.write("1|a\n2|b\n");
                    rows.flush();
                    Thread.sleep(LEASE.plus(MARGIN).toMillis());
                    rows.write("3|c\n0|\n");
                } catch (Exception failure) { throw new java.util.concurrent.CompletionException(failure); }
            });
            long begin = System.nanoTime();
            var loaded = host.call(Map.of("op", "load", "schema", "postgres", "tables", List.of("leased"),
                    "rows_per_file", 1024, "threads", 1), Duration.ofMinutes(15));
            writer.join();
            assertTrue("LOAD must outlive one lease", Duration.ofNanos(System.nanoTime() - begin).compareTo(LEASE) > 0);
            assertEquals(4L, loaded.getAsJsonObject("rows").get("leased").getAsLong());
            host.call(Map.of("op", "stop"));
            host.call(Map.of("op", "delete"));
        }
        Generation.deleteTree(output);
    }
    @Test public void rejectedVisibilityPublicationFailsNativeLoad() throws Exception {
        String configured = System.getProperty("sibench.pixels.build");
        assertNotNull("Set sibench.pixels.build", configured);
        Path build = Path.of(configured).toRealPath(), root = Path.of("").toRealPath();
        Path output = Files.createTempDirectory(root.resolve("results/capture"), "pixels-load-rejection-");
        var source = org.sibench.schema.SourceSchema.hybench(root.resolve("benchmark/HyBench-2024"));
        Path data = Files.createDirectory(output.resolve("csv"));
        HybenchTest.fixture(data);
        var timeout = Duration.ofMinutes(3);
        // The public overrides bound Retina's per-table buffers (one virtual node) for all HyBench tables.
        var overlay = org.sibench.config.ConfigValues.object(org.sibench.config.SibenchConfig.engine(root,
                org.sibench.config.SibenchConfig.read(root.resolve("config/sibench.yaml")), org.sibench.config.Engine.PIXELS), "properties");
        try (var host = PixelsHost.local(build, output.resolve("pixels"), PixelsBuild.identity(build))) {
            var pixels = host.call(Map.of("op", "start", "run", "pixels", "overlay", overlay,
                    "memory", Map.of("coordinator_heap", "1g", "retina_heap", "4g"), "trace", false), Duration.ofMinutes(10));
            host.call(Map.of("op", "create", "schema", "postgres", "table", "baseline",
                    "spec", PixelsBaseline.targetSchema(PixelsInputs.inspect(source, data).schema().text())), timeout);
            Path input = Files.writeString(output.resolve("rejected.tbl"), "99|1|2300|0|2023-01-01 00:00:00|\\N\n");
            host.upload("savingaccount", "rejected.tbl", input, timeout);
            Path classes = Files.createDirectory(output.resolve("probe-classes"));
            String classpath = build.resolve("jars/pixels-cli.jar") + ":" + build.resolve("jars/pixels-daemon.jar");
            var compile = NativeProcess.run("compile-probe", List.of(Path.of(System.getProperty("java.home"), "bin/javac").toString(),
                    "--release", "17", "-cp", classpath, "-d", classes.toString(), root.resolve("src/test/native/pixels/LoadFailureProbe.java").toString()),
                    Map.of(), output, output.resolve("probe-build"), Duration.ofSeconds(30));
            assertEquals(compile.stdout(), 0, compile.exit());
            Path properties = Files.writeString(output.resolve("probe.properties"), pixels.get("properties").getAsString());
            var failure = NativeProcess.run("load-rejection", List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                    "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-cp", classes + ":" + classpath,
                    "org.sibench.pixels.LoadFailureProbe", "file://" + output.resolve("pixels/load/savingaccount") + "/"),
                    Map.of("PIXELS_CONFIG", properties.toString(), "PIXELS_HOME", output.resolve("pixels/home").toString()),
                    output, output.resolve("probe-run"), Duration.ofSeconds(60));
            assertEquals(failure.stdout(), 0, failure.exit());
        }
    }
}
