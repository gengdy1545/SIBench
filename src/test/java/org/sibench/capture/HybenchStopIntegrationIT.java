package org.sibench.capture;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.DriverManager;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Explicit native TP/XP lifecycle check on a retained diagnostic database, without new Trace. */
public class HybenchStopIntegrationIT {
    @Test(timeout = 45000)
    public void finishesActiveNativeOperationsAndClosesConnectionsBeforeTheNativeTimer() throws Exception {
        Path root = Path.of(Objects.requireNonNull(System.getenv("SIBENCH_PROJECT_ROOT")));
        Path profile = Path.of(Objects.requireNonNull(System.getenv("SIBENCH_HYBENCH_PROFILE")));
        String phase = Objects.requireNonNull(System.getenv("SIBENCH_HYBENCH_PHASE"));
        assertTrue(List.of("tp", "xp").contains(phase));
        Path stop = Path.of(Objects.requireNonNull(System.getenv("SIBENCH_BUDGET_STOP")));
        assertFalse(Files.exists(stop));
        var config = CaptureConfig.read(root, profile);
        NativeBuildCheck.verify(root, profile);
        var properties = config.nativeProperties();
        String url = config.jdbc() + "?ApplicationName=hybench_stop_" + phase;
        for (String suffix : List.of("", "_ap")) {
            properties.setProperty("url" + suffix, url);
            properties.setProperty("classname" + suffix, "org.postgresql.Driver");
        }
        Path resolved = stop.resolveSibling("resolved-" + phase + ".props");
        try (var output = Files.newOutputStream(resolved, StandardOpenOption.CREATE_NEW)) {
            properties.store(output, "Controlled native admission-stop lifecycle check");
        }
        Object benchmark = HybenchNativeRun.benchmark(resolved, config.source().resolve("conf/stmt_postgres.toml"));
        Object result = benchmark.getClass().getMethod("getRes").invoke(benchmark);
        Object hist = result.getClass().getMethod("getHist").invoke(result);
        var stopped = CompletableFuture.runAsync(() -> {
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
                while (count(hist, phase) == 0) {
                    if (System.nanoTime() > deadline) throw new AssertionError("Native workers made no progress");
                    Thread.sleep(5);
                }
                Files.writeString(stop, "native-lifecycle-check\n", StandardOpenOption.CREATE_NEW);
            } catch (Exception error) { throw new RuntimeException(error); }
        });
        long started = System.nanoTime();
        if (phase.equals("tp")) benchmark.getClass().getMethod("runTP").invoke(benchmark);
        else benchmark.getClass().getMethod("runXP", int.class).invoke(benchmark, 0);
        stopped.get(1, TimeUnit.SECONDS);
        assertTrue(Files.exists(stop));
        assertTrue(count(hist, phase) > 0);
        assertTrue("Native timer is 60 seconds; admission stop must finish earlier",
                System.nanoTime() - started < TimeUnit.SECONDS.toNanos(30));
        try (var connection = DriverManager.getConnection(config.jdbc(), "postgres", "");
                var sql = connection.createStatement()) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (true) {
                try (var rows = sql.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'hybench_stop_%'")) {
                    assertTrue(rows.next());
                    if (rows.getLong(1) == 0) break;
                }
                assertTrue("Native connections did not close", System.nanoTime() < deadline);
                Thread.sleep(20);
            }
        }
    }

    private static long count(Object hist, String phase) throws Exception {
        long total = 0;
        for (String method : phase.equals("tp") ? List.of("getTPItem") : List.of("getXPIQItem", "getXPATItem"))
            for (int i = 0; i < (phase.equals("tp") ? 18 : 6); i++) {
                Object item = hist.getClass().getMethod(method, int.class).invoke(hist, i);
                total += ((Number) item.getClass().getMethod("getN").invoke(item)).longValue();
            }
        return total;
    }
}
