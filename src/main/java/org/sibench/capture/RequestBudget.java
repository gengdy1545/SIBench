package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/** Counts exported primitive index operations and stops admission at complete business events. */
final class RequestBudget {
    private final long target;
    private final long timeoutNanos;
    private final Map<String, Object> stages = new LinkedHashMap<>();
    private long started, total, stageStart, stageTarget;
    private String stage;
    private Path stop;
    private boolean signalled;

    RequestBudget(long target, Duration timeout) {
        if (target < 0 || timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("Invalid request budget");
        this.target = target;
        this.timeoutNanos = timeout.toNanos();
    }

    synchronized void begin(String name, int remainingStages, Path stopFile) throws IOException {
        if (stage != null || stages.containsKey(name) || remainingStages < 0) throw new IOException("Invalid workload stage transition");
        if (started == 0) started = System.nanoTime();
        checkTime();
        stage = name;
        stageStart = total;
        long remaining = Math.max(1, target - total);
        stageTarget = target == 0 || remainingStages == 0 ? 0 : 1 + (remaining - 1) / remainingStages;
        stop = stopFile;
        if (stop != null && Files.exists(stop)) throw new IOException("Budget stop path already exists: " + stop);
        signalled = false;
    }

    /** The common writer counts a complete request's primitive operations after export filtering. */
    synchronized void record(long operations) throws IOException {
        if (stage == null) throw new IOException("Workload output outside a budget stage");
        if (operations < 1) throw new IllegalArgumentException("Expected positive primitive operation count");
        total = Math.addExact(total, operations);
        if (!signalled && reached()) {
            if (stop != null) Files.writeString(stop, "Request budget reached; drain admitted events\n", StandardOpenOption.CREATE_NEW);
            signalled = true;
        }
    }

    synchronized boolean reached() { return stageTarget > 0 && total - stageStart >= stageTarget; }
    synchronized long count() { return total; }
    boolean enabled() { return target > 0; }

    synchronized void checkTime() throws IOException {
        if (started != 0 && System.nanoTime() - started >= timeoutNanos) throw new IOException("Workload request budget timed out");
    }

    synchronized void requireProgress(long previous) throws IOException {
        checkTime();
        if (total <= previous && stageTarget > 0) throw new IOException("No exported request progress in workload " + stage);
    }

    synchronized void end() throws IOException {
        if (stage == null || stageTarget > 0 && !reached()) throw new IOException("Workload ended before its request budget");
        long actual = total - stageStart;
        stages.put(stage, Map.of("target_requests", stageTarget, "effective_requests", actual,
                "overshoot_requests", stageTarget == 0 ? 0 : actual - stageTarget));
        stage = null;
        stop = null;
    }

    synchronized Map<String, Object> finish() throws IOException {
        if (stage != null || total < target) throw new IOException("Incomplete workload request budget");
        return Map.of("target_requests", target, "effective_requests", total,
                "overshoot_requests", target == 0 ? 0 : total - target, "stages", Map.copyOf(stages));
    }
}
