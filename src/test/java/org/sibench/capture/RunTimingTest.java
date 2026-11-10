package org.sibench.capture;

import static org.junit.Assert.*;
import com.google.gson.JsonParser;
import java.nio.file.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RunTimingTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void missingProcessMetricsAreUnknownButRealZeroIsRetained() throws Exception {
        var root = temporary.getRoot().toPath();
        var missing = RunTiming.process(root.resolve("missing-status"), java.util.Optional.empty());
        for (String field : java.util.List.of("cpu_seconds", "peak_rss_bytes", "rss_bytes", "threads"))
            assertNull(field, missing.get(field));
        var status = Files.writeString(root.resolve("status"), "VmHWM: 3 kB\nVmRSS: 0 kB\nThreads: 2\n");
        var observed = RunTiming.process(status, java.util.Optional.of(java.time.Duration.ZERO));
        assertEquals(3072L, observed.get("peak_rss_bytes"));
        assertEquals(0L, observed.get("rss_bytes"));
        assertEquals(2L, observed.get("threads"));
        assertEquals(0.0, observed.get("cpu_seconds"));
    }

    @Test public void anUnmeasurableDirectoryIsUnknownRatherThanZero() throws Exception {
        var root = temporary.getRoot().toPath();
        var file = Files.writeString(root.resolve("not-a-directory"), "payload");
        var report = root.resolve("timing.json");
        try (var timing = RunTiming.open(report)) { timing.measure(file); }
        var disk = JsonParser.parseString(Files.readString(report)).getAsJsonObject()
                .getAsJsonObject("directories").getAsJsonObject(file.toString());
        for (String field : java.util.List.of("bytes", "peak_bytes", "files"))
            assertTrue(field + "=" + disk.get(field), disk.get(field).isJsonNull());
    }
}
