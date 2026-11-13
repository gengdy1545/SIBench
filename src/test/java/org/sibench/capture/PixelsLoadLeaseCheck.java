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
 * Explicit bounded check that one Pixels LOAD outlives its write-transaction lease (300 s in the
 * pinned coordinator) and still commits. A FIFO input keeps the LOAD consumer reading for longer
 * than the lease without large data. Needs sibench.pixels.build (a two-phase build) and about
 * seven minutes; a build without lease renewal fails at commit.
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
}
