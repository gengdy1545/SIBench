package org.sibench.replay;

import static org.sibench.config.ConfigValues.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Dispatch policy is independent of capture and never changes the source trace. */
public record ReplayConfig(
        boolean secondaryIndexes,
        String gcOwner,
        long durationSeconds,
        long plannedOperationsPerSecond,
        long warmupOperations,
        int reservePercent,
        String dispatchMode) {
    public ReplayConfig(
            boolean secondaryIndexes,
            String gcOwner,
            long durationSeconds,
            long plannedOperationsPerSecond,
            long warmupOperations,
            int reservePercent) {
        this(
                secondaryIndexes,
                gcOwner,
                durationSeconds,
                plannedOperationsPerSecond,
                warmupOperations,
                reservePercent,
                "operation");
    }

    public ReplayConfig {
        require(Set.of("operation", "transaction").contains(dispatchMode), "Invalid dispatch mode");
        require(Set.of("benchmark", "backend", "none").contains(gcOwner), "Invalid GC owner");
        require(durationSeconds >= 0 && plannedOperationsPerSecond >= 0, "Invalid trace budget");
        require(
                (durationSeconds == 0) == (plannedOperationsPerSecond == 0),
                "Duration and planned rate must both be specified");
        require(warmupOperations >= 0, "Warmup operations must not be negative");
        require(reservePercent >= 0 && reservePercent <= 100, "Reserve must be between 0 and 100");
        requiredOperations(
                durationSeconds, plannedOperationsPerSecond, warmupOperations, reservePercent);
    }

    public static ReplayConfig defaults() {
        return new ReplayConfig(true, "none", 0, 0, 0, 0);
    }

    public static ReplayConfig read(Path root, Path path) throws java.io.IOException {
        return from(parse(Files.readString(root.resolve(path))));
    }

    public static ReplayConfig from(Map<String, Object> document) {
        keys(document, Set.of("schema_version", "purpose", "target", "replay", "versions"));
        require(integer(document, "schema_version") == 1, "Expected replay config v1");
        require(
                string(document, "purpose").equals("index_replay"),
                "Expected index_replay purpose");
        var target = object(document, "target");
        keys(target, Set.of("secondary_indexes"));
        var secondary = object(target, "secondary_indexes");
        keys(secondary, Set.of("enabled"));
        var replay = object(document, "replay");
        keys(replay, Set.of("strategy", "dispatch_mode", "trace_budget"));
        String dispatchMode =
                replay.containsKey("dispatch_mode") ? string(replay, "dispatch_mode") : "operation";
        require(
                string(replay, "strategy").equals("single_pass"),
                "Only single_pass replay is supported");
        var versions = object(document, "versions");
        keys(versions, Set.of("gc"));
        var gc = object(versions, "gc");
        keys(gc, Set.of("owner"));
        long seconds = 0, rate = 0, warmup = 0;
        int reserve = 0;
        if (replay.containsKey("trace_budget")) {
            var budget = object(replay, "trace_budget");
            keys(
                    budget,
                    Set.of(
                            "duration_seconds",
                            "planned_operations_per_second",
                            "warmup_operations",
                            "reserve_percent"));
            seconds = positive(budget, "duration_seconds");
            rate = positive(budget, "planned_operations_per_second");
            warmup = integer(budget, "warmup_operations");
            reserve = Math.toIntExact(integer(budget, "reserve_percent"));
        }
        return new ReplayConfig(
                bool(secondary, "enabled"),
                string(gc, "owner"),
                seconds,
                rate,
                warmup,
                reserve,
                dispatchMode);
    }

    public long requiredOperations() {
        return requiredOperations(
                durationSeconds, plannedOperationsPerSecond, warmupOperations, reservePercent);
    }

    private static long requiredOperations(long seconds, long rate, long warmup, int reserve) {
        long measurement = Math.multiplyExact(seconds, rate);
        long extra = Math.multiplyExact(measurement / 100, reserve);
        extra = Math.addExact(extra, (Math.multiplyExact(measurement % 100, reserve) + 99) / 100);
        return Math.addExact(warmup, Math.addExact(measurement, extra));
    }

    public void requireReferenceCapabilities() {
        require(
                gcOwner.equals("none"),
                "Reference replay supports GC owner none only; physical GC/backend retention is not"
                    + " implemented");
    }

    public Map<String, Object> describe() {
        return Map.of(
                "strategy", "single_pass",
                "secondary_indexes_enabled", secondaryIndexes,
                "dispatch_mode", dispatchMode,
                "gc_owner", gcOwner,
                "duration_seconds", durationSeconds,
                "planned_operations_per_second", plannedOperationsPerSecond,
                "warmup_operations", warmupOperations,
                "reserve_percent", reservePercent,
                "required_effective_workload_operations", requiredOperations());
    }

    private static void keys(Map<String, Object> map, Set<String> allowed) {
        for (String key : map.keySet())
            require(allowed.contains(key), "Unsupported replay option: " + key);
    }
}
