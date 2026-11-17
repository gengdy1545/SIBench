package org.sibench.adapters;

import org.sibench.replay.ReplayInput;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import org.sibench.capture.RunTiming;

import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayProjection;
import org.sibench.trace.TraceManifest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Replays a canonical trace against an index service: batched baseline load and individual
 * workload calls, each through the multi-client dispatcher with routing, in-flight bounds and
 * timeouts. Every search checks its native result count.
 */
public final class ServiceReplay {
    private ServiceReplay() {}

    /** The validated Trace manifest and inputs a replay shares with its target. */
    private record Session(JsonObject manifest, ReplayInput.Input input, String binding, TraceRequests requests) {
        GrpcService connect(ServiceTarget service, ReplayConfig config) {
            return new GrpcService(service.endpoint(), requests, binding, config.requestTimeoutSeconds());
        }
    }

    /** Reads the manifest once and checks supply before the service is contacted. */
    private static Session prepare(Path directory, ReplayConfig config, ServiceTarget service) throws IOException {
        var manifest = TraceManifest.read(directory);
        // The declared value kinds come from the manifest; check them before decoding the workload.
        TraceManifest.requireValue(manifest, service.value());
        return new Session(manifest, ReplayInput.prepare(directory, manifest, config),
                ReplayInput.binding(manifest, config), new TraceRequests(service.value()));
    }

    public static Map<String, Object> run(
            Path directory, ReplayConfig config, ServiceTarget service, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Replay report already exists: " + report);
        Path baselineReport = report.resolveSibling(report.getFileName() + ".baseline.json");
        if (Files.exists(baselineReport)) throw new IOException("Baseline report already exists: " + baselineReport);
        var session = prepare(directory, config, service);
        var input = session.input();
        var preflight = input.preflight();
        String binding = session.binding();
        var requests = session.requests();
        try (var remote = session.connect(service, config)) {
            var factory = remote.factory();
            requests.baselineBatch(config.baselineBatchOperations());
            var result = new LinkedHashMap<String, Object>();
            IndexDescription description = null;
            for (boolean runtime : new boolean[] {false, true}) {
                String phase = runtime ? "workload" : "baseline";
                RunTiming.current().phase(phase);
                long start = System.nanoTime();
                long requestBytesBefore = requests.requestBytes();
                long rpcRequestsBefore = requests.rpcRequests();
                long resultBytesBefore = requests.resultBytes();
                var kindsBefore = requests.rpcKinds();
                try (var projection =
                        ReplayProjection.withClients(
                                config, runtime, factory)) {
                    (runtime
                                    ? input.workload()
                                    : input.baseline())
                            .read(projection);
                    projection.finish();
                    if (runtime) projection.requireClientCoverage();
                    double seconds = (System.nanoTime() - start) / 1e9;
                    description = remote.description();
                    var phaseReport = new LinkedHashMap<String, Object>();
                    phaseReport.put("operations", projection.dispatched());
                    phaseReport.put("rpc_requests", requests.rpcRequests() - rpcRequestsBefore);
                    if (!runtime) phaseReport.put("batch_operations", config.baselineBatchOperations());
                    var kinds = new java.util.TreeMap<String,Long>();
                    requests.rpcKinds().forEach((kind,count) -> {
                        long calls = count - kindsBefore.getOrDefault(kind,0L);
                        if (calls > 0) kinds.put(kind,calls);
                    });
                    phaseReport.put("rpc_by_kind", kinds);
                    phaseReport.put("seconds", seconds);
                    phaseReport.put("operations_per_second", projection.dispatched() / seconds);
                    phaseReport.put(
                            "request_bytes", requests.requestBytes() - requestBytesBefore);
                    phaseReport.put("result_bytes", requests.resultBytes() - resultBytesBefore);
                    phaseReport.put("dispatch", projection.describe());
                    result.put(phase, phaseReport);
                }
                if (!runtime) {
                    var completed = new LinkedHashMap<String, Object>();
                    completed.put("status", "baseline_passed");
                    completed.put("finished_at_utc", java.time.Instant.now().toString());
                    completed.put("scope", "complete baseline integrity checks and all write ACKs passed;"
                            + " workload, maintenance drain and independent stored-value verification are not certified");
                    completed.put("index_binding", binding);
                    completed.put("build_sha256", buildIdentity());
                    completed.put("service", description);
                    completed.put("preflight", preflight);
                    completed.put("result", result.get("baseline"));
                    Files.writeString(baselineReport, new GsonBuilder().setPrettyPrinting().create().toJson(completed),
                            StandardOpenOption.CREATE_NEW);
                }
            }
            result.put("service", description);
            result.put("rpc_by_kind", requests.rpcKinds());
            result.put("queries", requests.queries());
            result.put("search_requests", requests.rpcKinds().getOrDefault("Get",0L) + requests.rpcKinds().getOrDefault("Scan",0L));
            var workload = (Map<?, ?>) result.get("workload");
            var dispatch = (Map<?, ?>) workload.get("dispatch");
            var byKind = (Map<?, ?>) dispatch.get("dispatched_by_kind");
            result.put(
                    "trace_search_operations",
                    byKind.containsKey("search")
                            ? ((Number) byKind.get("search")).longValue()
                            : 0L);
            result.put("checked_result_items", requests.checkedResults());
            result.put("returned_result_items", requests.returnedResults());
            result.put("request_bytes", requests.requestBytes());
            result.put("result_bytes", requests.resultBytes());
            result.put("max_results_per_query", requests.maxResults());
            result.put("max_group_members", requests.maxGroupMembers());
            var output = new LinkedHashMap<String, Object>();
            output.put("status", "passed");
            output.put("preflight", preflight);
            output.put("result", result);
            output.put("scope", "every search returned its native expected count; result identities, versions"
                    + " and order were not compared; single sending thread,"
                    + " concurrent individual RPCs; not a sustained-rate, recovery or cross-client"
                    + " visibility test");
            writeIndexManifest(report, session.manifest(), binding, description, lastVersion(preflight), config);
            Files.writeString(report, new GsonBuilder().setPrettyPrinting().create().toJson(output), StandardOpenOption.CREATE_NEW);
            return output;
        }
    }

    /**
     * Checks an index that already holds the whole trace, e.g. after a restart or a cold start:
     * every snapshot search of the workload, at its read version, must return its native
     * result count. Writes are not sent; the Trace contains only committed-snapshot searches.
     */
    public static Map<String, Object> verify(
            Path directory, ReplayConfig config, ServiceTarget service, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Verification report already exists: " + report);
        var session = prepare(directory, config, service);
        var requests = session.requests();
        try (var remote = session.connect(service, config)) {
            RunTiming.current().phase("verify");
            long start = System.nanoTime();
            Map<String, Object> dispatch;
            try (var projection = ReplayProjection.snapshotSearches(config, remote.factory())) {
                session.input().workload().read(projection);
                projection.finish();
                dispatch = projection.describe();
            }
            double seconds = (System.nanoTime() - start) / 1e9;
            var result = new LinkedHashMap<String, Object>();
            if (requests.queries() == 0) throw new IOException("Trace has no snapshot queries to verify");
            result.put("service", remote.description());
            result.put("seconds", seconds);
            result.put("queries", requests.queries());
            result.put("checked_result_items", requests.checkedResults());
            result.put("result_bytes", requests.resultBytes());
            result.put("dispatch", dispatch);
            var output = new LinkedHashMap<String, Object>();
            output.put("status", "passed");
            output.put("preflight", session.input().preflight());
            output.put("result", result);
            output.put("scope", "every snapshot search of the trace at its read version matched its"
                    + " native result count on an index that already held the whole trace; writes were"
                    + " not sent");
            Files.writeString(report, new GsonBuilder().setPrettyPrinting().create().toJson(output),
                    StandardOpenOption.CREATE_NEW);
            return output;
        }
    }

    private static long lastVersion(Map<String, Object> preflight) {
        return ((Number) ((Map<?, ?>) preflight.get("workload")).get("last_resolved_version")).longValue();
    }

    /**
     * The minimal index manifest, next to the report: what the index holds (trace identity and
     * index definitions), up to which source version, where it lives, how it was checked, and with which
     * build and configuration.
     */
    private static void writeIndexManifest(
            Path report,
            JsonObject trace,
            String binding,
            IndexDescription description,
            long lastVersion,
            ReplayConfig config)
            throws IOException {
        var manifest = new LinkedHashMap<String, Object>();
        manifest.put("index_binding", binding);
        manifest.put("workload_trace_sha256", trace.get("workload_trace_sha256").getAsString());
        manifest.put("baseline_trace_sha256", trace.get("baseline_trace_sha256").getAsString());
        manifest.put("last_source_version", lastVersion);
        manifest.put("index_definitions", trace.get("index_definitions"));
        manifest.put("value_kind", description.valueKind().name().toLowerCase(java.util.Locale.ROOT));
        manifest.put("backend", description.name());
        manifest.put("index_location", description.location());
        manifest.put("write_ack", description.writeAck().name());
        manifest.put("wal", description.wal());
        manifest.put("durability", description.durability());
        manifest.put(
                "coverage",
                Map.of(
                        "versions", "0.." + lastVersion,
                        "updates", "committed updates only",
                        "secondary_indexes", config.secondaryIndexes()));
        manifest.put("build_sha256", buildIdentity());
        manifest.put("replay_configuration", config.describe());
        Files.writeString(
                report.resolveSibling(report.getFileName() + ".index-manifest.json"),
                new GsonBuilder().setPrettyPrinting().create().toJson(manifest),
                StandardOpenOption.CREATE_NEW);
    }

    private static String buildIdentity() throws IOException {
        Path jar = Path.of(java.net.URI.create(
                ServiceReplay.class.getProtectionDomain().getCodeSource().getLocation().toString()));
        return Files.isRegularFile(jar) ? org.sibench.config.Digests.sha256(jar) : "N/A (not a jar)";
    }

}
