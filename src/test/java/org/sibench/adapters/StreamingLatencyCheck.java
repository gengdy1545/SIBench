package org.sibench.adapters;

import com.google.gson.GsonBuilder;
import org.sibench.adapters.proto.Response;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayDispatcher;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/** Explicit bounded integration check against a fresh IndexServerAcceptance service. */
public final class StreamingLatencyCheck {
    private static final String BINDING = "streaming-latency-check";
    private static final int BASELINE_ROWS = 64, CYCLES = 100, WARMUP = 16;

    private StreamingLatencyCheck() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("ENDPOINT REPORT");
        Path output = Path.of(args[1]);
        if (Files.exists(output)) throw new IOException("Report already exists: " + output);
        var report = new LinkedHashMap<String, Object>();
        report.put("scope", "committed-update completion, timestamp dependencies and ordinary query counts; bounded integration only");
        report.put("endpoint", args[0]);
        report.put("workload", "64 baseline inserts in four batches; 100 cycles of upsert/read-one, "
                + "delete/read-zero, reinsert/read-one; first 16 basic operations warmup");
        report.put("expected", Map.of("baseline_operations", 64, "baseline_execute_requests", 4,
                "workload_operations", 600, "workload_execute_requests", 600,
                "measured_operations", 584, "measured_writes", 292, "measured_searches", 292,
                "checked_results", 200));
        var config = new ReplayConfig(false, 0, 0, WARMUP, 0, "operation", 2, 64, 5);
        var requests = new TraceRequests(ValueKind.ROW_ID);
        requests.baselineBatch(16);
        try (var grpc = new GrpcService(args[0], requests, BINDING, 5)) {
            try (var baseline = new ReplayDispatcher(config, grpc.factory())) {
                for (int key = 0; key < BASELINE_ROWS; key++) {
                    baseline.send(IndexOperationTrace.write(key + 1L, IndexOperationTrace.INSERT,
                            1, 0, IndexKey.ofInt(key), 0, 0, 0, key + 1L)
                            .withWorkUnitId(key / 32 + 1L));
                }
                baseline.drain();
                baseline.requireClientCoverage();
                var counts = baseline.describe();
                verify(counts, 64, 64, 0, 64);
                equal(requests.rpcRequests(), 4, "baseline RPC requests");
                report.put("baseline", counts);
            }
            report.put("service_description", grpc.description());
            try (var replay = new ReplayDispatcher(config, grpc.factory(), WARMUP)) {
                long event = BASELINE_ROWS, version = 0;
                for (int cycle = 0; cycle < CYCLES; cycle++) {
                    var key = IndexKey.ofInt(cycle % BASELINE_ROWS);
                    for (int step = 0; step < 3; step++) {
                        version++;
                        long workUnit = version + 2;
                        int kind = step == 1 ? IndexOperationTrace.LOGICAL_DELETE : IndexOperationTrace.UPSERT;
                        replay.send(IndexOperationTrace.write(++event, kind, 1, 0, key,
                                version, 0, 0, 1000 + version)
                                .withWorkUnitId(workUnit));
                        replay.send(IndexOperationTrace.search(++event, 1, 0,
                                IndexOperationTrace.Range.equal(key), version, step == 1 ? 0 : 1, 0)
                                .withWorkUnitId(workUnit));
                    }
                }
                replay.drain();
                replay.requireClientCoverage();
                var counts = replay.describe();
                verify(counts, 600, 584, 292, 292);
                report.put("workload_dispatch", counts);
            }
            equal(requests.rpcRequests(), 604, "total RPC requests");
            equal(requests.queries(), 300, "ordinary queries");
            equal(requests.checkedResults(), 200, "returned rows");
            report.put("rpc_requests", requests.rpcRequests());
            report.put("checked_queries", requests.queries());
            report.put("checked_results", requests.checkedResults());
            rejectWrongCount();
            report.put("wrong_expected_count_rejected", true);
            report.put("status", "passed");
        } catch (Throwable error) {
            report.put("status", "failed");
            report.put("error", error.toString());
            throw error;
        } finally {
            Files.writeString(output, new GsonBuilder().setPrettyPrinting().create().toJson(report) + "\n",
                    StandardOpenOption.CREATE_NEW);
        }
    }

    private static void verify(Map<String, Object> report, long operations,
                               long measured, long searches, long writes) {
        equal(number(report, "basic_operations"), operations, "basic operations");
        equal(number(report, "target_calls"), operations, "target calls");
        equal(number(report, "completed_responses"), operations, "logical final responses");
        equal(number(report, "completed_operations"), operations, "completed operations");
        for (String key : new String[] {"failed_response_operations", "timed_out_operations", "pending_responses", "pending_completion_groups"})
            equal(number(report, key), 0, key);
        if (report.containsKey("failure")) throw new AssertionError(report.get("failure"));
        equal(number(section(report, "measurement"), "measured_operations"), measured, "measured operations");
        equal(number(section(report, "measurement"), "completed_operations"), measured, "measured completed operations");
        equal(number(section(report, "search_latency"), "count"), searches, "search latency samples");
        equal(number(section(report, "write_latency"), "count"), writes, "write latency samples");
    }

    private static void rejectWrongCount() throws IOException {
        var check = new TraceRequests(ValueKind.ROW_ID);
        var lookup = IndexOperationTrace.search(1, 1, 0,
                IndexOperationTrace.Range.equal(IndexKey.ofInt(1)), 0, 1, 0);
        try {
            check.check(lookup, Response.newBuilder().setId(1).setStatus(Status.OK).build());
        } catch (IOException expected) {
            if (expected.getMessage().contains("Search count mismatch")) return;
            throw expected;
        }
        throw new AssertionError("Empty response incorrectly accepted for an expected one-row lookup");
    }

    private static Map<?, ?> section(Map<String, Object> report, String key) {
        return (Map<?, ?>) report.get(key);
    }

    private static long number(Map<?, ?> report, String key) {
        return ((Number) report.get(key)).longValue();
    }

    private static void equal(long actual, long expected, String label) {
        if (actual != expected) throw new AssertionError(label + ": expected " + expected + ", found " + actual);
    }
}
