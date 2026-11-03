package org.sibench.capture;

import org.sibench.config.ConfigValues;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/** Soft output targets: request native admission stop, then retain every produced transaction. */
final class GenerationBudget {
    private final long operationTarget, outputTarget;
    private final Path stopFile;
    private volatile String reason;
    private long requestedOperations, requestedBytes;

    GenerationBudget(Map<String, Object> settings, Path stopFile) {
        operationTarget = target(settings, "operation_target");
        outputTarget = target(settings, "output_byte_target");
        this.stopFile = stopFile;
    }

    static long target(Map<String, Object> settings, String name) {
        long value = settings.containsKey(name) ? ConfigValues.integer(settings, name) : 0;
        ConfigValues.require(value >= 0, "generation." + name + " must be nonnegative");
        return value;
    }

    boolean enabled() { return operationTarget != 0 || outputTarget != 0; }
    boolean requested() { return reason != null; }

    void baseline(long bytes) throws IOException {
        if (outputTarget != 0 && bytes >= outputTarget)
            throw new IOException("generation.output_byte_target must exceed the complete baseline proto size: " + bytes);
    }

    void observe(long operations, long bytes) throws IOException {
        if (requested()) return;
        String reached = operationTarget != 0 && operations >= operationTarget ? "operation_target"
                : outputTarget != 0 && bytes >= outputTarget ? "output_byte_target" : null;
        if (reached == null) return;
        requestedOperations = operations;
        requestedBytes = bytes;
        reason = reached;
        try {
            Files.writeString(stopFile, reached + "\n", StandardOpenOption.CREATE_NEW);
        } catch (IOException failure) {
            reason = null;
            throw failure;
        }
    }

    Map<String, Object> report(long operations, long bytes) {
        var result = new LinkedHashMap<String, Object>();
        result.put("operation_target", operationTarget);
        result.put("output_byte_target", outputTarget);
        result.put("stop_reason", requested() ? reason : "native_workload_completed");
        result.put("stop_requested", requested());
        result.put("operations_at_request", requestedOperations);
        result.put("proto_bytes_at_request", requestedBytes);
        result.put("actual_operations", operations);
        result.put("actual_proto_bytes", bytes);
        result.put("operation_overshoot", operationTarget == 0 ? 0 : Math.max(0, operations - operationTarget));
        result.put("output_byte_overshoot", outputTarget == 0 ? 0 : Math.max(0, bytes - outputTarget));
        return result;
    }
}
