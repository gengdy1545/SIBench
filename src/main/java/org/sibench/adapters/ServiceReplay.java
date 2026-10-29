package org.sibench.adapters;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.adapters.proto.DescribeResponse;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.RunTiming;
import org.sibench.replay.ReferenceReplay;
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
 * timeouts. Every search checks its native count and complete ordered per-query digest.
 */
public final class ServiceReplay {
    private ServiceReplay() {}

    public static Map<String, Object> run(
            Path directory, ReplayConfig config, ServiceConfig service, Path reference, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Replay report already exists: " + report);
        var preflight = ReferenceReplay.check(directory, config);
        String mode = service.value() == ValueKind.ROW_ID ? "rowid" : "locator";
        String binding = ReferenceReplay.binding(directory, config) + " mode=" + mode;
        Path expectedResults =
                reference == null ? null : ReferenceReplay.queryDigestFile(reference, mode);
        Path generated = null;
        if (expectedResults == null) {
            Path directoryForDigests =
                    Files.createTempDirectory(
                            report.toAbsolutePath().getParent(), ".sibench-expected-");
            expectedResults = directoryForDigests.resolve("queries");
            generated = directoryForDigests;
            ReferenceReplay.writeQueryDigests(
                    directory,
                    config,
                    service.value() == ValueKind.ROW_ID,
                    expectedResults,
                    binding);
        }
        var requests = new TraceRequests(service.value(), expectedResults, binding);
        DescribeResponse description;
        ReplayTarget.Factory factory;
        GrpcService remote = null;
        RocksDbBackend local = null;
        PublishedIndex published = null;
        if (service.adapter().equals("grpc")) {
            remote = new GrpcService(service.endpoint(), requests);
            description = remote.describe();
            factory = remote.factory();
        } else {
            local = RocksDbBackend.create(service.path(), service.value());
            RunTiming.current().measure(service.path());
            description = local.describe().toBuilder().setCompleteCommitPublication(true).build();
            published = new PublishedIndex(local);
            factory = InProcessService.factory(published, requests);
        }
        try {
            require(description, service.value(), directory);
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
                    "complete_commit_publication", true));
            String location = null;
            for (boolean runtime : new boolean[] {false, true}) {
                String phase = runtime ? "workload" : "baseline";
                RunTiming.current().phase(phase);
                long start = System.nanoTime();
                long requestBytesBefore = requests.requestBytes();
                long resultBytesBefore = requests.resultBytes();
                try (var projection =
                        ReplayProjection.withClients(
                                config, runtime, factory, true)) {
                    (runtime
                                    ? IndexOperationTrace.Source.file(directory.resolve("workload.ops"))
                                    : org.sibench.capture.BaselineOperations.of(directory))
                            .read(projection);
                    projection.finish();
                    if (runtime) projection.requireClientCoverage();
                    double seconds = (System.nanoTime() - start) / 1e9;
                    long flushStart = System.nanoTime();
                    long publishedVersion;
                    if (remote != null) {
                        var flushed = remote.flush(binding);
                        publishedVersion = flushed.getPublishedVersion();
                        location = flushed.getIndexLocation();
                    } else {
                        local.bind(binding);
                        publishedVersion = published.flush();
                        location = local.location();
                    }
                    var phaseReport = new LinkedHashMap<String, Object>();
                    phaseReport.put("operations", projection.dispatched());
                    phaseReport.put("seconds", seconds);
                    phaseReport.put("operations_per_second", projection.dispatched() / seconds);
                    phaseReport.put(
                            "request_bytes", requests.requestBytes() - requestBytesBefore);
                    phaseReport.put("result_bytes", requests.resultBytes() - resultBytesBefore);
                    phaseReport.put("flush_seconds", (System.nanoTime() - flushStart) / 1e9);
                    phaseReport.put("published_version", publishedVersion);
                    phaseReport.put("dispatch", projection.describe());
                    result.put(phase, phaseReport);
                }
            }
            requests.finish();
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
            result.put("max_expected_in_flight", requests.maxExpected());
            result.put("max_group_members", requests.maxGroupMembers());
            result.put("per_query_digest_checks", requests.digestChecks());
            result.put("per_query_digest_mismatches", requests.digestMismatches());
            result.put(
                    "expected_results_source",
                    generated == null ? "reference_report" : "generated_from_trace");
            result.put(
                    "expected_results_sha256",
                    org.sibench.generation.BaselineGenerator.sha256(expectedResults));
            if (reference != null)
                result.put("reference", Map.of("report", reference.toString(), "mode", mode,
                        "status", "per_query_digests_matched"));
            var output = new LinkedHashMap<String, Object>();
            output.put("status", "passed");
            output.put("preflight", preflight);
            output.put("result", result);
            output.put("scope", "every search returned its native expected count and its per-query"
                    + " (PK, version, value) digest matched the reference; single sending thread,"
                    + " per-client ordered streams; not a sustained-rate, recovery or cross-client"
                    + " visibility test");
            Files.writeString(report, new GsonBuilder().setPrettyPrinting().create().toJson(output),
                    StandardOpenOption.CREATE_NEW);
            writeIndexManifest(report, directory, binding, description, location, result, config);
            return output;
        } finally {
            if (remote != null) remote.close();
            if (published != null) published.close();
            if (local != null) local.close();
            if (generated != null) {
                try (var paths = Files.walk(generated)) {
                    for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList())
                        Files.deleteIfExists(path);
                }
            }
        }
    }

    /**
     * Checks an index that already holds the whole trace, e.g. after a restart or a cold start:
     * every snapshot search of the workload, at its read version, must return its reference
     * digest. Writes are not sent. Transaction-local searches are not checked: their answer is
     * their transaction's state at that point, which later writes of the transaction overwrite.
     */
    public static Map<String, Object> verify(
            Path directory, ReplayConfig config, ServiceConfig service, Path reference, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Verification report already exists: " + report);
        if (reference == null)
            throw new IOException("Snapshot verification needs a reference report's digests");
        var preflight = ReferenceReplay.check(directory, config);
        String mode = service.value() == ValueKind.ROW_ID ? "rowid" : "locator";
        String binding = ReferenceReplay.binding(directory, config) + " mode=" + mode;
        var requests =
                new TraceRequests(
                        service.value(), ReferenceReplay.queryDigestFile(reference, mode), binding);
        requests.snapshotReads();
        long[] lastCommit = {0};
        IndexOperationTrace.read(
                directory.resolve("workload.ops"),
                operation -> {
                    if (operation.kind() == IndexOperationTrace.TRANSACTION_COMMIT)
                        lastCommit[0] = operation.version();
                });
        DescribeResponse description;
        ReplayTarget.Factory factory;
        GrpcService remote = null;
        RocksDbBackend local = null;
        PublishedIndex published = null;
        try {
            if (service.adapter().equals("grpc")) {
                remote = new GrpcService(service.endpoint(), requests);
                description = remote.describe();
                factory = remote.factory();
            } else {
                local = RocksDbBackend.open(service.path(), service.value());
                published = new PublishedIndex(local);
                description = local.describe().toBuilder().setCompleteCommitPublication(true).build();
                factory = InProcessService.factory(published, requests);
            }
            require(description, service.value(), directory);
            long publishedVersion;
            if (remote != null) publishedVersion = remote.flush(binding).getPublishedVersion();
            else {
                local.bind(binding);
                publishedVersion = published.flush();
            }
            if (publishedVersion != lastCommit[0])
                throw new IOException(
                        "Index publishes version "
                                + publishedVersion
                                + ", the trace ends at "
                                + lastCommit[0]);
            RunTiming.current().phase("verify");
            long start = System.nanoTime();
            Map<String, Object> dispatch;
            try (var projection = ReplayProjection.snapshotSearches(config, factory, requests::skipExpected)) {
                IndexOperationTrace.Source.file(directory.resolve("workload.ops")).read(projection);
                projection.finish();
                dispatch = projection.describe();
            }
            double seconds = (System.nanoTime() - start) / 1e9;
            requests.finish();
            var result = new LinkedHashMap<String, Object>();
            result.put("service", description.getName());
            result.put("published_version", publishedVersion);
            result.put("seconds", seconds);
            result.put("queries", requests.queries());
            result.put("per_query_digest_checks", requests.digestChecks());
            result.put("per_query_digest_mismatches", requests.digestMismatches());
            result.put("unchecked_transaction_local_queries", requests.skippedQueries());
            result.put("checked_result_items", requests.checkedResults());
            result.put("result_bytes", requests.resultBytes());
            result.put("dispatch", dispatch);
            var output = new LinkedHashMap<String, Object>();
            output.put("status", "passed");
            output.put("preflight", preflight);
            output.put("result", result);
            output.put("scope", "every snapshot search of the trace at its read version matched its"
                    + " reference digest on an index that already held the whole trace; writes were"
                    + " not sent and transaction-local searches were not checked");
            Files.writeString(report, new GsonBuilder().setPrettyPrinting().create().toJson(output),
                    StandardOpenOption.CREATE_NEW);
            return output;
        } finally {
            if (remote != null) remote.close();
            if (published != null) published.close();
            if (local != null) local.close();
        }
    }

    /**
     * The minimal index manifest, next to the report: what the index holds (trace identity and
     * index definitions), up to which commit, where it lives, how it was checked, and with which
     * build and configuration. There is no source snapshot yet (no lakehouse source): N/A.
     */
    private static void writeIndexManifest(
            Path report,
            Path directory,
            String binding,
            DescribeResponse description,
            String location,
            Map<String, Object> result,
            ReplayConfig config)
            throws IOException {
        var trace = JsonParser.parseString(Files.readString(directory.resolve("manifest.json"))).getAsJsonObject();
        var manifest = new LinkedHashMap<String, Object>();
        manifest.put("index_binding", binding);
        manifest.put("workload_trace_sha256", trace.get("workload_trace_sha256").getAsString());
        manifest.put("baseline_trace_sha256", trace.get("baseline_trace_sha256").getAsString());
        manifest.put("operation_trace_version", trace.get("operation_trace_version").getAsInt());
        manifest.put("commit_version", ((Map<?, ?>) result.get("workload")).get("published_version"));
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
                        "versions", "0.." + ((Map<?, ?>) result.get("workload")).get("published_version"),
                        "commits", "complete commits only",
                        "secondary_indexes", config.secondaryIndexes()));
        manifest.put(
                "checksum",
                Map.of(
                        "per_query_digest_checks", result.get("per_query_digest_checks"),
                        "per_query_digest_mismatches", result.get("per_query_digest_mismatches"),
                        "expected_results_sha256", result.get("expected_results_sha256")));
        Path jar = Path.of(java.net.URI.create(
                ServiceReplay.class.getProtectionDomain().getCodeSource().getLocation().toString()));
        manifest.put(
                "build_sha256",
                Files.isRegularFile(jar) ? org.sibench.generation.BaselineGenerator.sha256(jar) : "N/A (not a jar)");
        manifest.put("replay_configuration", config.describe());
        manifest.put("source_snapshot_id", "N/A");
        Files.writeString(
                report.resolveSibling(report.getFileName() + ".index-manifest.json"),
                new GsonBuilder().setPrettyPrinting().create().toJson(manifest),
                StandardOpenOption.CREATE_NEW);
    }

    /** The service must implement every semantic the trace relies on. */
    static void require(DescribeResponse description, ValueKind value, Path directory)
            throws IOException {
        if (description.getProtocolVersion() != 2) throw new IOException("Unsupported protocol version");
        if (!description.getCompleteCommitPublication())
            throw new IOException("Service must publish complete commits across client streams");
        if (description.getValueKind() != value)
            throw new IOException("Service stores " + description.getValueKind() + ", replay sends " + value);
        if (!description.getVersionedReads() || !description.getOrderedSameVersionOverwrite())
            throw new IOException("Service lacks versioned reads or ordered same-version overwrite");
        if (description.getWriteAck()
                        == org.sibench.adapters.proto.Acknowledgement.ACK_UNSPECIFIED
                || !description.getFlushMakesDurable())
            throw new IOException("Service must declare its write acknowledgement and Flush durability");
        JsonObject manifest = JsonParser.parseString(Files.readString(directory.resolve("manifest.json"))).getAsJsonObject();
        if (!description.getSearchGroups() && manifest.has("searched_accesses_by_shape")) {
            var shapes = manifest.getAsJsonObject("searched_accesses_by_shape");
            if (shapes.has("intersection") || shapes.has("union"))
                throw new IOException("Trace has search groups; the service does not execute them");
        }
        if (!description.getTransactionLocalReads()
                && manifest.has("transaction_local_searches")
                && manifest.get("transaction_local_searches").getAsLong() != 0)
            throw new IOException(
                    "Trace has transaction-local searches; the service does not implement"
                            + " snapshot-plus-own-writes reads");
    }

}
