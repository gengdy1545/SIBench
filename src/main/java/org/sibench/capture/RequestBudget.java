package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Map;

/** Counts exported primitive index operations of one workload run and stops admission at complete business events. */
final class RequestBudget {
    private final long target;
    private final long timeoutNanos;
    private long started, total;
    private Path stop;
    private boolean begun, ended, signalled;

    RequestBudget(long target, Duration timeout) {
        if (target < 0 || timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("Invalid request budget");
        this.target = target;
        this.timeoutNanos = timeout.toNanos();
    }

    /** Starts the run's clock; the optional file is created when the target is reached so clients stop admitting events. */
    synchronized void begin(Path stopFile) throws IOException {
        if (begun) throw new IOException("Workload request budget already started");
        started = System.nanoTime();
        stop = stopFile;
        if (stop != null && Files.exists(stop)) throw new IOException("Budget stop path already exists: " + stop);
        begun = true;
    }

    /** The common writer counts a complete request's primitive operations after export filtering. */
    synchronized void record(long operations) throws IOException {
        if (!begun || ended) throw new IOException("Workload output outside the request budget");
        if (operations < 1) throw new IllegalArgumentException("Expected positive primitive operation count");
        total = Math.addExact(total, operations);
        if (!signalled && reached()) {
            if (stop != null) Files.writeString(stop, "Request budget reached; drain admitted events\n", StandardOpenOption.CREATE_NEW);
            signalled = true;
        }
    }

    synchronized boolean reached() { return target > 0 && total >= target; }
    synchronized long count() { return total; }
    boolean enabled() { return target > 0; }

    synchronized void checkTime() throws IOException {
        if (started != 0 && System.nanoTime() - started >= timeoutNanos) throw new IOException("Workload request budget timed out");
    }

    synchronized void requireProgress(long previous) throws IOException {
        checkTime();
        if (total <= previous && target > 0) throw new IOException("No exported request progress in the workload");
    }

    synchronized void end() throws IOException {
        if (!begun || ended || target > 0 && !reached()) throw new IOException("Workload ended before its request budget");
        ended = true;
    }

    synchronized Map<String, Object> finish() throws IOException {
        if (begun && !ended || total < target) throw new IOException("Incomplete workload request budget");
        return Map.of("target_requests", target, "effective_requests", total,
                "overshoot_requests", target == 0 ? 0 : total - target);
    }
}
