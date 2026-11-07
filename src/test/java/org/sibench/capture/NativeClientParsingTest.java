package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class NativeClientParsingTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void timeoutStopsTheNativeClientAndKeepsItsDiagnostics() throws Exception {
        Path root = folder.newFolder().toPath(), pid = root.resolve("client.pid");
        long started = System.nanoTime();
        var error = assertThrows(IOException.class, () -> NativeProcess.run("blocked",
                List.of("/bin/sh", "-c", "echo $$ > \"$1\"; echo started; exec sleep 30", "timeout-probe", pid.toString()),
                Map.of(), root, root.resolve("logs"), java.time.Duration.ofMillis(500)));
        assertTrue(error.getMessage().contains("timed out: blocked"));
        assertTrue(System.nanoTime() - started < java.util.concurrent.TimeUnit.SECONDS.toNanos(3));
        assertEquals("started\n", Files.readString(root.resolve("logs/blocked.out")));
        var child = ProcessHandle.of(Long.parseLong(Files.readString(pid).strip()));
        if (child.isPresent()) child.get().onExit().get(3, java.util.concurrent.TimeUnit.SECONDS);
        assertFalse(child.isPresent() && child.get().isAlive());
    }

    @Test
    public void captureFailureStopsTheClientBeforeItsExecutionTimeout()throws Exception {
        Path root=folder.newFolder().toPath();var failure=new IOException("capture failed");
        var calls=new java.util.concurrent.atomic.AtomicInteger();long started=System.nanoTime();
        var actual=assertThrows(IOException.class,()->NativeProcess.run("capture-failure",List.of("sleep","30"),
                Map.of(),root,root.resolve("logs"),java.time.Duration.ofSeconds(30),()->{
                    if(calls.incrementAndGet()>1)throw failure;
                }));
        assertSame(failure,actual);
        assertTrue(System.nanoTime()-started<java.util.concurrent.TimeUnit.SECONDS.toNanos(3));
    }

    @Test
    public void readsYcsbOperationCountsAndIgnoresLatencies() {
        var parsed =
                YcsbSummary.parse(
                        """
                        [OVERALL], RunTime(ms), 1500
                        [OVERALL], Throughput(ops/sec), 666.6666666666666
                        [READ], Operations, 146
                        [READ], AverageLatency(us), 693.156
                        [READ], Return=OK, 146
                        [UPDATE], Operations, 154
                        [UPDATE], Return=ERROR, 2
                        [READ-MODIFY-WRITE], Operations, 93
                        [TOTAL_GC_TIME_%], Time(%), 0.35
                        some log line
                        """);
        assertEquals(146L, (long) parsed.get("READ").get("Operations"));
        assertEquals(146L, (long) parsed.get("READ").get("Return=OK"));
        assertEquals(2L, (long) parsed.get("UPDATE").get("Return=ERROR"));
        assertEquals(93L, (long) parsed.get("READ-MODIFY-WRITE").get("Operations"));
        assertFalse(parsed.get("READ").containsKey("AverageLatency(us)"));
        assertEquals(1500L, (long) parsed.get("OVERALL").get("RunTime(ms)"));
    }

    @Test
    public void readsSysbenchCountersFromTheSqlStatisticsBlockOnly() {
        var parsed =
                SysbenchSummary.parse(
                        """
                        sysbench 1.1.0 (using bundled LuaJIT 2.1.0-beta3)

                        SQL statistics:
                            queries performed:
                                read:                            1400
                                write:                           400
                                other:                           200
                                total:                           2000
                            transactions:                        100    (30.43 per sec.)
                            queries:                             2000   (608.58 per sec.)
                            ignored errors:                      0      (0.00 per sec.)
                            reconnects:                          0      (0.00 per sec.)

                        General statistics:
                            total time:                          3.2862s
                            total number of events:              100
                        """);
        assertEquals(1400L, (long) parsed.get("read"));
        assertEquals(400L, (long) parsed.get("write"));
        assertEquals(200L, (long) parsed.get("other"));
        assertEquals(100L, (long) parsed.get("transactions"));
        assertEquals(0L, (long) parsed.get("ignored errors"));
        assertEquals(0L, (long) parsed.get("reconnects"));
        assertFalse(parsed.containsKey("total number of events"));
    }

    @Test
    public void observedOperationsSeparateTheReadAndUpdateOfAReadModifyWrite() {
        // DBWrapper measures every db.read / db.update, so workload F counts each RMW twice.
        var observed =
                YcsbSummary.observed(
                        YcsbSummary.parse(
                                """
                                [READ], Operations, 150
                                [UPDATE], Operations, 50
                                [READ-MODIFY-WRITE], Operations, 50
                                """));
        assertEquals(100L, (long) observed.get("read"));
        assertEquals(0L, (long) observed.get("update"));
        assertEquals(50L, (long) observed.get("read_modify_write"));
        assertEquals(150L, (long) observed.get("total"));
        assertEquals(
                200L,
                (long)
                        YcsbSummary.observed(
                                        YcsbSummary.parse(
                                                """
                                                [SCAN], Operations, 190
                                                [INSERT], Operations, 10
                                                """))
                                .get("total"));
    }

    @Test
    public void failedYcsbCallsStillCountTowardSourceCompleteness() {
        var measured=YcsbSummary.parse("""
                [READ], Operations, 491
                [UPDATE], Operations, 491
                [UPDATE-FAILED], Operations, 18
                [UPDATE], Return=OK, 491
                [UPDATE], Return=ERROR, 18
                [CLEANUP], Operations, 32
                """);
        assertEquals(509,YcsbSummary.operations(measured,"UPDATE"));
        assertEquals(1000,(long)YcsbSummary.observed(measured).get("total"));
        var rmw=YcsbSummary.parse("""
                [READ], Operations, 100
                [UPDATE], Operations, 95
                [UPDATE-FAILED], Operations, 5
                [READ-MODIFY-WRITE], Operations, 100
                """);
        assertEquals(100,(long)YcsbSummary.observed(rmw).get("total"));
        assertEquals(0,(long)YcsbSummary.observed(rmw).get("update"));
    }

    @Test
    public void buildCopyMustMatchThePinnedSourceByteForByte() throws Exception {
        Path pinned = folder.newFolder("pinned").toPath(), build = folder.newFolder("build").toPath();
        Files.createDirectories(pinned.resolve("src/lua"));
        Files.createDirectories(build.resolve("src/lua"));
        Files.writeString(pinned.resolve("src/lua/a.lua"), "print(1)\n");
        Files.writeString(pinned.resolve("src/lua/notes.txt"), "ignored\n");
        Files.writeString(build.resolve("src/lua/a.lua"), "print(1)\n");
        index(pinned);
        NativeBuildCheck.require(pinned, build, List.of("src/lua"));
        Files.createDirectories(pinned.resolve("target"));
        Files.writeString(pinned.resolve("target/checkstyle-checker.xml"), "generated, not a pinned source");
        NativeBuildCheck.require(pinned, build, List.of("."));
        Files.writeString(build.resolve("src/lua/a.lua"), "print(2)\n");
        assertTrue(
                assertThrows(
                                IOException.class,
                                () -> NativeBuildCheck.require(pinned, build, List.of("src/lua")))
                        .getMessage()
                        .contains("differs from the pinned source"));
        Files.delete(build.resolve("src/lua/a.lua"));
        assertThrows(IOException.class, () -> NativeBuildCheck.require(pinned, build, List.of("src/lua")));
        assertThrows(IOException.class, () -> NativeBuildCheck.require(pinned, build, List.of("src/missing")));
        // A single pinned file is compared too.
        Files.writeString(pinned.resolve("src/one.c"), "int x;\n");
        Files.writeString(build.resolve("src/one.c"), "int x;\n");
        Files.delete(pinned.resolve("target/checkstyle-checker.xml"));
        index(pinned);
        NativeBuildCheck.require(pinned, build, List.of("src/one.c"));
    }
    @Test
    public void nativeExecutableMustMatchItsRecordedBuild() throws Exception {
        Path pinned = folder.newFolder("pinned-binary").toPath();
        Path build = folder.getRoot().toPath().resolve("build/benchmarks/sysbench");
        Files.createDirectories(build);
        Files.createDirectories(pinned.resolve("src"));
        Files.createDirectories(build.resolve("src"));
        Files.writeString(pinned.resolve("src/a.c"), "int main() { return 0; }\n");
        Files.copy(pinned.resolve("src/a.c"), build.resolve("src/a.c"));
        index(pinned);
        Path patch = folder.getRoot().toPath().resolve("scripts/patches/sysbench-1.1.0.patch");
        Files.createDirectories(patch.getParent());
        Files.writeString(patch, "--- a/src/a.c\n+++ b/src/a.c\n@@ -1 +1 @@\n"
                + "-int main() { return 0; }\n+int main() { return 1; }\n");
        Files.writeString(build.resolve("src/a.c"), "int main() { return 1; }\n");
        Path executable = build.resolve("src/sysbench");
        Files.writeString(executable, "compiled client");
        Path driver = folder.getRoot().toPath().resolve("src/main/native/sysbench/drv_duckdb.cc");
        Files.createDirectories(driver.getParent()); Files.writeString(driver, "driver source");
        Files.createDirectories(build.resolve("src/drivers/duckdb"));
        Files.copy(driver, build.resolve("src/drivers/duckdb/drv_duckdb.cc"));
        Path retry = driver.resolveSibling("retryable_error.hpp"); Files.writeString(retry, "retry policy");
        Files.copy(retry, build.resolve("src/drivers/duckdb/retryable_error.hpp"));
        Path errorTypes = folder.getRoot().toPath().resolve("third_party/duckdb/src/include/duckdb/common/exception.hpp");
        Files.createDirectories(errorTypes.getParent()); Files.writeString(errorTypes, "error types");
        assertTrue(executable.toFile().setExecutable(true));
        assertThrows(IOException.class, () -> NativeBuildCheck.verify(
                folder.getRoot().toPath(), "sysbench", pinned, build, "source-identity"));
        var manifest = Map.of("benchmark", "sysbench", "source_commit", "source-identity",
                "native_stop_patch_sha256", FileDigests.sha256(patch),
                "duckdb_driver_sha256", FileDigests.sha256(driver),
                "duckdb_retry_policy_sha256", FileDigests.sha256(retry),
                "duckdb_error_types_sha256", FileDigests.sha256(errorTypes),
                "artifacts", Map.of("src/sysbench", FileDigests.sha256(executable)));
        Files.writeString(build.resolve("native-build.json"), new com.google.gson.Gson().toJson(manifest));
        NativeBuildCheck.verify(folder.getRoot().toPath(), "sysbench", pinned, build, "source-identity");
        Files.writeString(build.resolve("src/a.c"), "int main() { return 1; }\nint unrelated;\n");
        assertThrows(IOException.class, () -> NativeBuildCheck.verify(
                folder.getRoot().toPath(), "sysbench", pinned, build, "source-identity"));
        Files.writeString(build.resolve("src/a.c"), "int main() { return 1; }\n");
        Files.writeString(executable, "#!/bin/sh\nexit 0\n");
        assertTrue(assertThrows(IOException.class, () -> NativeBuildCheck.verify(
                folder.getRoot().toPath(), "sysbench", pinned, build, "source-identity"))
                .getMessage().contains("differs from its recorded build"));
    }

    private static void index(Path directory)throws Exception {
        for(var arguments:List.of(List.of("git","-C",directory.toString(),"init","-q"),
                List.of("git","-C",directory.toString(),"add","."))) {
            var process=new ProcessBuilder(arguments).redirectErrorStream(true).start();
            assertTrue(process.waitFor(5,java.util.concurrent.TimeUnit.SECONDS));assertEquals(0,process.exitValue());
        }
    }

    @Test
    public void persistedCommandsRedactPasswords() throws Exception {
        Path logs = folder.newFolder("commands").toPath();
        NativeProcess.run("redaction", List.of("/bin/true", "db.passwd=example-private-password"),
                Map.of(), logs, logs, java.time.Duration.ofSeconds(2));
        assertFalse(Files.readString(logs.resolve("redaction.command")).contains("example-private-password"));
    }

    @Test
    public void shellAssignmentsPreserveQuotedPathsWithoutExecutingTheirContents() throws Exception {
        String value = "path with spaces'$(printf injected)";
        var buffer = new java.io.ByteArrayOutputStream();
        var previous = System.out;
        try (var output = new java.io.PrintStream(buffer, true, java.nio.charset.StandardCharsets.UTF_8)) {
            System.setOut(output);
            NativeProcess.assignment("CAPTURE_PATH", value);
        } finally { System.setOut(previous); }
        var process = new ProcessBuilder("/bin/sh", "-c", buffer.toString(java.nio.charset.StandardCharsets.UTF_8)
                + "printf '%s' \"$CAPTURE_PATH\"").start();
        assertEquals(value, new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(0, process.waitFor());
    }

}
