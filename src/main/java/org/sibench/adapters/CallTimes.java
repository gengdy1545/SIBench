package org.sibench.adapters;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Cumulative call durations in the object-request statistics layout (count, failed, bytes, microseconds,
 * then 32 power-of-two microsecond buckets), so {@link #describe} renders it.
 */
final class CallTimes {
    private final AtomicLongArray counts = new AtomicLongArray(36);

    void record(long nanos, boolean ok) {
        long micros = Math.max(0, nanos) / 1000;
        counts.incrementAndGet(0);
        if (!ok) counts.incrementAndGet(1);
        counts.addAndGet(3, micros);
        counts.incrementAndGet(4 + Math.min(31, micros == 0 ? 0 : 64 - Long.numberOfLeadingZeros(micros - 1)));
    }

    long[] snapshot() {
        long[] copy = new long[36];
        for (int i = 0; i < copy.length; i++) copy[i] = counts.get(i);
        return copy;
    }

    static java.util.Map<String, Object> describe(long[] counts) {
        long[] buckets = java.util.Arrays.copyOfRange(counts, 4, 36);
        long[] upperBounds = new long[32];
        for (int i = 0; i < 31; i++) upperBounds[i] = 1L << i;
        upperBounds[31] = Long.MAX_VALUE;
        long samples = java.util.Arrays.stream(buckets).sum();
        return java.util.Map.of(
                "requests", counts[0], "failed", counts[1], "bytes", counts[2], "micros", counts[3],
                "latency_micros", java.util.Map.of(
                        "samples", samples, "bucket_counts", buckets,
                        "bucket_upper_bounds", upperBounds, "overflow_after", 1L << 30,
                        "p50_upper_bound", latencyBound(buckets, samples, 50),
                        "p95_upper_bound", latencyBound(buckets, samples, 95),
                        "p99_upper_bound", latencyBound(buckets, samples, 99)));
    }

    private static long latencyBound(long[] buckets, long samples, int percentile) {
        if (samples == 0) return 0;
        long rank = (samples / 100) * percentile + ((samples % 100) * percentile + 99) / 100;
        for (int i = 0; i < buckets.length; i++) {
            rank -= buckets[i];
            if (rank <= 0) return i == 31 ? Long.MAX_VALUE : 1L << i;
        }
        throw new IllegalArgumentException("Incomplete request latency histogram");
    }
}
