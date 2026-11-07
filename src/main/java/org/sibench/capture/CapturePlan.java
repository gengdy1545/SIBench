package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayProjection;
import org.sibench.trace.TraceManifest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The native parameters a capture needs to reach a target, from a calibration capture of the same
 * source and scale. Only the benchmark's own knobs are scaled, all by one factor so the workload
 * mix is kept: sysbench events and YCSB operation counts per workload, HyBench's TP and XP phase
 * durations (AP runs its rounds as before). The output is not linear in these knobs everywhere
 * (HyBench depends on time and concurrency), so the capture made with the plan reports its own
 * result, which is the fact.
 *
 * <p>The target of a formal capture is the replay budget ({@link #operations}): enough effective
 * workload operations for a replay configuration's measured duration at its planned rate, counted
 * exactly as {@code check-replay} counts them.
 */
public final class CapturePlan {
    private CapturePlan() {}

    /**
     * Parameters for the effective workload operations a replay configuration requires: its
     * warmup plus its measured duration at its planned rate, with its reserve.
     */
    public static Map<String, Object> operations(Path root, Path profile, Path calibration, Path replay) throws IOException {
        Map<String, Object> document = parse(Files.readString(root.resolve(profile)));
        ReplayConfig config = ReplayConfig.read(root, replay);
        long target = config.requiredOperations();
        if (target <= 0) throw new IllegalArgumentException("The replay configuration sets no operation budget: " + replay);
        long measured;
        try (var runtime = new ReplayProjection(config, true, operation -> {})) {
            var manifest = TraceManifest.read(calibration);
            boolean secondary = TraceManifest.secondaryIndexes(manifest);
            if (config.secondaryIndexes() && !secondary)
                throw new IOException("Calibration Trace does not provide secondary indexes");
            long records = ProtoTrace.source(calibration, manifest, "workload").read(runtime);
            if (records != manifest.get("workload_operations").getAsLong())
                throw new IOException("Calibration workload record count mismatch");
            runtime.finish();
            measured = runtime.effectiveRequests();
        }
        if (measured <= 0) throw new IOException("The calibration capture has no effective workload operations");
        double factor = (double) target / measured;
        var result = new LinkedHashMap<String, Object>();
        result.put("benchmark", string(document, "benchmark"));
        result.put("calibration", calibration.toString());
        result.put("replay", replay.toString());
        result.put("calibration_effective_operations", measured);
        result.put("target_effective_operations", target);
        result.put("factor", factor);
        result.put("parameters", scaled(document, factor));
        result.put("note", "estimate from one calibration capture; check-replay with the replay configuration is the fact");
        return result;
    }

    private static Map<String, Object> scaled(Map<String, Object> document, double factor) {
        var nativeConfig = object(document, "native");
        var parameters = new LinkedHashMap<String, Object>();
        String benchmark = string(document, "benchmark");
        switch (benchmark) {
            case "sysbench", "ycsb" -> {
                String knob = benchmark.equals("sysbench") ? "events" : "operationcount";
                var workloads = object(nativeConfig, "workloads");
                for (String workload : workloads.keySet()) {
                    long current = positive(object(workloads, workload), knob);
                    parameters.put("native.workloads." + workload + "." + knob, (long) Math.ceil(current * factor));
                }
            }
            case "hybench" -> {
                var durations = nativeConfig.containsKey("phase_duration_minutes")
                        ? object(nativeConfig, "phase_duration_minutes") : Map.<String, Object>of();
                long minutes = positive(nativeConfig, "duration_minutes");
                for (String phase : new String[] {"tp", "xp"}) {
                    long current = durations.containsKey(phase) ? positive(durations, phase) : minutes;
                    parameters.put("native.phase_duration_minutes." + phase, (long) Math.ceil(current * factor));
                }
            }
            default -> throw new IllegalArgumentException("Unknown benchmark: " + benchmark);
        }
        return parameters;
    }
}
