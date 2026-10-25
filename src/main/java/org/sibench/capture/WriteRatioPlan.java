package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The native parameters a capture needs for its committed row writes to reach a share of the
 * baseline, from a calibration capture of the same source and scale. Only the benchmark's own
 * knobs are scaled, all by one factor so the workload mix is kept: sysbench events and YCSB
 * operation counts per workload, HyBench's TP and XP phase durations (AP runs its rounds as
 * before). Writes are not linear in these knobs everywhere (HyBench writes depend on time and
 * concurrency), so the capture made with the plan reports its own ratio, which is the fact.
 */
public final class WriteRatioPlan {
    private WriteRatioPlan() {}

    public static Map<String, Object> plan(Path root, Path profile, Path calibration, double ratio) throws IOException {
        if (!(ratio > 0 && ratio <= 10)) throw new IllegalArgumentException("Write ratio must be in (0, 10]");
        Map<String, Object> document = parse(Files.readString(root.resolve(profile)));
        JsonObject manifest = JsonParser.parseString(Files.readString(calibration.resolve("manifest.json"))).getAsJsonObject();
        long baseline = manifest.get("baseline_rows").getAsLong();
        long writes = manifest.get("logical_row_version_writes").getAsLong();
        if (writes <= 0) throw new IOException("The calibration capture committed no row writes");
        double measured = (double) writes / baseline, factor = ratio / measured;
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
        var result = new LinkedHashMap<String, Object>();
        result.put("benchmark", benchmark);
        result.put("calibration", calibration.toString());
        result.put("baseline_rows", baseline);
        result.put("calibration_row_writes", writes);
        result.put("calibration_ratio", measured);
        result.put("target_ratio", ratio);
        result.put("target_row_writes", (long) Math.ceil(ratio * baseline));
        result.put("factor", factor);
        result.put("parameters", parameters);
        result.put("note", "estimate from one calibration capture; the planned capture reports its actual ratio");
        return result;
    }
}
