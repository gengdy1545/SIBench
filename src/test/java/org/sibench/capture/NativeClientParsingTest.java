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
    public void readsYcsbOperationCountsAndIgnoresLatencies() {
        var parsed =
                YcsbNativeRun.parse(
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
                SysbenchNativeRun.parse(
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
    public void buildCopyMustMatchThePinnedSourceByteForByte() throws Exception {
        Path pinned = folder.newFolder("pinned").toPath(), build = folder.newFolder("build").toPath();
        Files.createDirectories(pinned.resolve("src/lua"));
        Files.createDirectories(build.resolve("src/lua"));
        Files.writeString(pinned.resolve("src/lua/a.lua"), "print(1)\n");
        Files.writeString(pinned.resolve("src/lua/notes.txt"), "ignored\n");
        Files.writeString(build.resolve("src/lua/a.lua"), "print(1)\n");
        NativeBuildCheck.require(pinned, build, List.of("src/lua"));
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
        NativeBuildCheck.require(pinned, build, List.of("src/one.c"));
        assertEquals(Map.of(), Map.of());
    }
}
