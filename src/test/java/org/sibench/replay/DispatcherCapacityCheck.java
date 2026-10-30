package org.sibench.replay;

import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.trace.IndexKey;

/** Controlled controller-cost comparison; generated unique baseline operations, no backend/RPC. */
public final class DispatcherCapacityCheck {
    public static void main(String[] args) throws Exception {
        long count = Long.parseLong(args[0]);
        Path report = Path.of(args[1]);
        var config = new ReplayConfig(true, "none", 0, 0, 0, 0, "operation", 64, 4096, 60);
        long[] received = {0};
        long start = System.nanoTime();
        Map<String, Object> result;
        try (var dispatcher = new ReplayDispatcher(config, client -> (sequence, operation) -> {
            if (sequence != ++received[0] || operation.sourceEvent() != sequence
                    || operation.rowId() != sequence || operation.key().intValue() != sequence)
                throw new IOException("Generated operation was lost, reordered or changed");
            return ReplayTarget.SendHandle.completed();
        })) {
            for (long i = 1; i <= count; i++) {
                var operation = new IndexOperationTrace.Operation(i, IndexOperationTrace.INSERT,
                        1, 0, 0, IndexKey.ofInt(i), null, 0, 1, i - 1, i, 0, 0, 0, -1,
                        1 + (i - 1) / IndexOperationTrace.BLOCK_RECORDS, 0, 1, null, null, 0);
                dispatcher.send(operation);
            }
            dispatcher.drain();
            result = dispatcher.describe();
        }
        if (received[0] != count || ((Number) result.get("completed_responses")).longValue() != count
                || ((Number) result.get("pending_responses")).longValue() != 0)
            throw new IOException("Controller comparison did not process every operation");
        Files.writeString(report, new GsonBuilder().setPrettyPrinting().create().toJson(Map.of(
                "status", "passed", "operations", count, "seconds", (System.nanoTime() - start) / 1e9,
                "dispatch", result, "scope", "generated unique operations; isolated controller cost, no RPC/backend capacity claim")),
                StandardOpenOption.CREATE_NEW);
    }
}
