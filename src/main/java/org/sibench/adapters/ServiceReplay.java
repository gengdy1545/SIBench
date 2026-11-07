package org.sibench.adapters;

import org.sibench.replay.ReplayInput;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import org.sibench.adapters.proto.DescribeResponse;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.RunTiming;

import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayProjection;
import org.sibench.replay.ReplayTarget;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Replays a canonical trace against an index service: the baseline load, a flush, the workload
 * and a flush, each through the multi-client dispatcher with routing, in-flight bounds and
 * timeouts. Every search checks its native result count.
 */
public final class ServiceReplay {
    private ServiceReplay() {}

    public static Map<String, Object> run(
            Path directory, ReplayConfig config, ServiceConfig service, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Replay report already exists: " + report);
        var input = ReplayInput.prepare(directory, config);
        org.sibench.trace.TraceManifest.requireValue(directory,service.value());
        var preflight = input.preflight();
        String mode = service.value() == ValueKind.ROW_ID ? "rowid" : "locator";
        String binding = ReplayInput.binding(directory, config) + " mode=" + mode;
        var requests = new TraceRequests(service.value());
        DescribeResponse description;
        ReplayTarget.Factory factory;
        GrpcService remote = null;
        VersionedIndexBackend local = null;
        IndexExecutor executor = null;
        Throwable failure = null;
        try {
            if (service.adapter().equals("grpc")) {
                remote = new GrpcService(service.endpoint(), requests, binding, config.requestTimeoutSeconds());
                description = remote.describe();
                factory = remote.factory();
            } else {
                local = VersionedIndexBackend.create(service.path(), service.value());
                RunTiming.current().measure(service.path());
                description = local.describe();
                executor = new IndexExecutor(local);
                local.bind(binding);
                factory = InProcessService.factory(executor, requests);
            }
            require(description, service.value(), (Map<?, ?>) preflight.get("workload"));
            // Held Baseline INSERTs stay below the in-flight bound, so some request is always sent.
            if (description.getBaselineWriteBatches())
                requests.baselineBatch(Math.max(1, config.maxInFlight() / config.clientCount()));
            var result = new LinkedHashMap<String, Object>();
            result.put("service", Map.of(
                    "adapter", service.adapter(),
                    "name", description.getName(),
                    "value_kind", description.getValueKind().name().toLowerCase(java.util.Locale.ROOT),
                    "durability", description.getDurability(),
                    "write_ack", description.getWriteAck().name(),
                    "baseline_durable_only_at_flush", description.getBaselineDurableOnlyAtFlush(),
                    "flush_makes_durable", description.getFlushMakesDurable(),
                    "mutation_ack_visible", true));
            String location = null;
            for (boolean runtime : new boolean[] {false, true}) {
                String phase = runtime ? "workload" : "baseline";
                RunTiming.current().phase(phase);
                long start = System.nanoTime();
                long requestBytesBefore = requests.requestBytes();
                long executeRequestsBefore = requests.executeRequests();
                long resultBytesBefore = requests.resultBytes();
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
                    long flushStart = System.nanoTime();
                    if (remote != null) {
                        var flushed = remote.flush(binding);
                        requireFlush(flushed, binding);
                        location = flushed.getIndexLocation();
                    } else {
                        local.bind(binding);
                        executor.flush();
                        location = local.location();
                    }
                    var phaseReport = new LinkedHashMap<String, Object>();
                    phaseReport.put("operations", projection.dispatched());
                    phaseReport.put("execute_requests", requests.executeRequests() - executeRequestsBefore);
                    phaseReport.put("seconds", seconds);
                    phaseReport.put("operations_per_second", projection.dispatched() / seconds);
                    phaseReport.put(
                            "request_bytes", requests.requestBytes() - requestBytesBefore);
                    phaseReport.put("result_bytes", requests.resultBytes() - resultBytesBefore);
                    phaseReport.put("flush_seconds", (System.nanoTime() - flushStart) / 1e9);
                    phaseReport.put("dispatch", projection.describe());
                    result.put(phase, phaseReport);
                }
            }
            result.put("queries", requests.queries());
            result.put("search_requests", requests.queries());
            var workload = (Map<?, ?>) result.get("workload");
            var dispatch = (Map<?, ?>) workload.get("dispatch");
            var byKind = (Map<?, ?>) dispatch.get("dispatched_by_kind");
            result.put(
                    "trace_search_operations",
                    byKind.containsKey("search")
                            ? ((Number) byKind.get("search")).longValue()
                            : 0L);
            result.put("checked_result_items", requests.checkedResults());
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
                    + " per-client ordered streams; not a sustained-rate, recovery or cross-client"
                    + " visibility test");
            Files.writeString(report, new GsonBuilder().setPrettyPrinting().create().toJson(output),
                    StandardOpenOption.CREATE_NEW);
            writeIndexManifest(report, directory, binding, description, location, lastVersion(preflight), config);
            return output;
        } catch (IOException | RuntimeException | Error error) {
            failure = error;
            throw error;
        } finally {
            close(failure, remote, executor, local);
        }
    }

    /**
     * Checks an index that already holds the whole trace, e.g. after a restart or a cold start:
     * every snapshot search of the workload, at its read version, must return its native
     * result count. Writes are not sent; the Trace contains only committed-snapshot searches.
     */
    public static Map<String, Object> verify(
            Path directory, ReplayConfig config, ServiceConfig service, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Verification report already exists: " + report);
        var input = ReplayInput.prepare(directory, config);
        org.sibench.trace.TraceManifest.requireValue(directory,service.value());
        var preflight = input.preflight();
        String mode = service.value() == ValueKind.ROW_ID ? "rowid" : "locator";
        String binding = ReplayInput.binding(directory, config) + " mode=" + mode;
        var requests = new TraceRequests(service.value());
        DescribeResponse description;
        ReplayTarget.Factory factory;
        GrpcService remote = null;
        VersionedIndexBackend local = null;
        IndexExecutor executor = null;
        Throwable failure = null;
        try {
            if (service.adapter().equals("grpc")) {
                remote = new GrpcService(service.endpoint(), requests, binding, config.requestTimeoutSeconds());
                description = remote.describe();
                factory = remote.factory();
            } else {
                local = VersionedIndexBackend.open(service.path(), service.value());
                executor = new IndexExecutor(local);
                description = local.describe();
                factory = InProcessService.factory(executor, requests);
            }
            require(description, service.value(), (Map<?, ?>) preflight.get("workload"));
            if (remote != null) requireFlush(remote.flush(binding), binding);
            else {
                local.bind(binding);
                executor.flush();
            }
            RunTiming.current().phase("verify");
            long start = System.nanoTime();
            Map<String, Object> dispatch;
            try (var projection = ReplayProjection.snapshotSearches(config, factory)) {
                input.workload().read(projection);
                projection.finish();
                dispatch = projection.describe();
            }
            double seconds = (System.nanoTime() - start) / 1e9;
            var result = new LinkedHashMap<String, Object>();
            result.put("service", description.getName());
            result.put("seconds", seconds);
            result.put("queries", requests.queries());
            result.put("checked_result_items", requests.checkedResults());
            result.put("result_bytes", requests.resultBytes());
            result.put("dispatch", dispatch);
            var output = new LinkedHashMap<String, Object>();
            output.put("status", "passed");
            output.put("preflight", preflight);
            output.put("result", result);
            output.put("scope", "every snapshot search of the trace at its read version matched its"
                    + " native result count on an index that already held the whole trace; writes were"
                    + " not sent");
            Files.writeString(report, new GsonBuilder().setPrettyPrinting().create().toJson(output),
                    StandardOpenOption.CREATE_NEW);
            return output;
        } catch (IOException | RuntimeException | Error error) {
            failure = error;
            throw error;
        } finally {
            close(failure, remote, executor, local);
        }
    }

    private static void close(Throwable failure, GrpcService remote, IndexExecutor executor, VersionedIndexBackend local)
            throws IOException {
        try {
            if (remote != null) remote.close();
            if (executor != null) executor.close();
            if (local != null) local.close();
        } catch (IOException | RuntimeException close) {
            if (failure == null) throw close;
            failure.addSuppressed(close);
        }
    }

    private static long lastVersion(Map<String, Object> preflight) {
        return ((Number) ((Map<?, ?>) preflight.get("workload")).get("last_resolved_version")).longValue();
    }

    static void requireFlush(org.sibench.adapters.proto.FlushResponse response, String binding)
            throws IOException {
        if (!binding.equals(response.getIndexBinding())
                || response.getIndexLocation().isBlank())
            throw new IOException("Flush response differs from the trace binding or index location");
    }

    /**
     * The minimal index manifest, next to the report: what the index holds (trace identity and
     * index definitions), up to which source version, where it lives, how it was checked, and with which
     * build and configuration.
     */
    private static void writeIndexManifest(
            Path report,
            Path directory,
            String binding,
            DescribeResponse description,
            String location,
            long lastVersion,
            ReplayConfig config)
            throws IOException {
        var trace = JsonParser.parseString(Files.readString(directory.resolve("manifest.json"))).getAsJsonObject();
        var manifest = new LinkedHashMap<String, Object>();
        manifest.put("index_binding", binding);
        manifest.put("workload_trace_sha256", trace.get("workload_trace_sha256").getAsString());
        manifest.put("baseline_trace_sha256", trace.get("baseline_trace_sha256").getAsString());
        manifest.put("last_source_version", lastVersion);
        manifest.put("index_definitions", trace.get("index_definitions"));
        manifest.put("key_encoding", "IndexKey order-preserving components; table(u32) index(u32) [value] PK ~version(u64)");
        manifest.put("value_kind", description.getValueKind().name().toLowerCase(java.util.Locale.ROOT));
        manifest.put("versioning_mode", "encoded_key");
        manifest.put("backend", description.getName());
        manifest.put("index_location", location);
        manifest.put("durability", description.getDurability());
        manifest.put(
                "coverage",
                Map.of(
                        "versions", "0.." + lastVersion,
                        "updates", "committed updates only",
                        "secondary_indexes", config.secondaryIndexes()));
        Path jar = Path.of(java.net.URI.create(
                ServiceReplay.class.getProtectionDomain().getCodeSource().getLocation().toString()));
        manifest.put(
                "build_sha256",
                Files.isRegularFile(jar) ? org.sibench.config.Digests.sha256(jar) : "N/A (not a jar)");
        manifest.put("replay_configuration", config.describe());
        Files.writeString(
                report.resolveSibling(report.getFileName() + ".index-manifest.json"),
                new GsonBuilder().setPrettyPrinting().create().toJson(manifest),
                StandardOpenOption.CREATE_NEW);
    }

    /** The service must implement every semantic the trace relies on. */
    static void require(DescribeResponse description, ValueKind value, Map<?, ?> workload)
            throws IOException {
        if (description.getValueKind() != value)
            throw new IOException("Service stores " + description.getValueKind() + ", replay sends " + value);
        if (!description.getVersionedReads())
            throw new IOException("Service lacks versioned reads");
        if (description.getWriteAck()
                        == org.sibench.adapters.proto.Acknowledgement.ACK_UNSPECIFIED
                || !description.getFlushMakesDurable())
            throw new IOException("Service must declare its write acknowledgement and Flush durability");
        if (!description.getSearchGroups() && ((Number) workload.get("search_groups")).longValue() > 0)
            throw new IOException("Trace has search groups; the service does not execute them");
    }

}
