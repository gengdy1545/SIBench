package org.sibench.adapters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.Test;

public class CloudRocksStoreTest {
    @Test
    public void requestLatencyBoundsIncludeFailedRequestsAndMatchHandComputedRanks() throws Exception {
        // Nine observed durations: 0, 1, 2, 5, 6, 7, 8, 17, 32 us. Nearest-rank p50=6, p95=p99=32.
        long[] counts = new long[36];
        counts[0] = 9;
        counts[1] = 1;
        counts[2] = 17;
        counts[3] = 78;
        counts[4] = 2;
        counts[5] = 1;
        counts[7] = 4;
        counts[9] = 2;
        String text = "write " + Arrays.stream(counts).mapToObj(Long::toString).collect(Collectors.joining(" "));
        assertArrayEquals(counts, CloudRocksStore.parseRequests(text + "\n").get("write"));
        var stats = RocksDbBackend.objectRequestStats(counts);
        assertEquals(9L, stats.get("requests"));
        assertEquals(1L, stats.get("failed"));
        var histogram = (java.util.Map<?, ?>) stats.get("latency_micros");
        assertEquals(9L, histogram.get("samples"));
        assertEquals(8L, histogram.get("p50_upper_bound"));
        assertEquals(32L, histogram.get("p95_upper_bound"));
        assertEquals(32L, histogram.get("p99_upper_bound"));
        counts = new long[36];
        histogram = (java.util.Map<?, ?>) RocksDbBackend.objectRequestStats(counts).get("latency_micros");
        assertEquals(0L, histogram.get("samples"));
        assertEquals(0L, histogram.get("p99_upper_bound"));
        counts[35] = 1;
        histogram = (java.util.Map<?, ?>) RocksDbBackend.objectRequestStats(counts).get("latency_micros");
        assertEquals(Long.MAX_VALUE, histogram.get("p99_upper_bound"));
    }

    @Test
    public void rejectsMissingHistogramAndMalformedNativeStatistics() {
        assertThrows(IOException.class, () -> CloudRocksStore.parseRequests("write 1 0 10 20"));
        String tail = " 0".repeat(35);
        assertThrows(IOException.class, () -> CloudRocksStore.parseRequests("write -1" + tail));
        assertThrows(IOException.class, () -> CloudRocksStore.parseRequests("write nope" + tail));
    }
}
