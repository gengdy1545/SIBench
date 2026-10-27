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
 * and a flush, each through the multi-client dispatcher with its routing, in-flight bound and
 * timeouts. Every search response must return the native expected count; the results' checksum
 * equals the reference replay's when the service returns the same entries.
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
        java.util.concurrent.ExecutorService background = null;
        if (service.adapter().equals("grpc")) {
            remote = new GrpcService(service.endpoint(), requests);
            description = remote.describe();
            factory = remote.factory();
        } else {
            local = RocksDbBackend.create(service.path(), service.value());
            description =
                    local.describe().toBuilder()
                            .setCrossClientOrdering(!service.publication())
                            .setCompleteCommitPublication(service.publication())
                            .build();
            published = description.getCompleteCommitPublication() ? new PublishedIndex(local) : null;
            if (published == null) factory = InProcessService.factory(local, requests);
            else {
                background =
                        java.util.concurrent.Executors.newCachedThreadPool(
                                runnable -> {
                                    Thread thread = new Thread(runnable, "sibench-inprocess-published");
                                    thread.setDaemon(true);
                                    return thread;
                                });
                factory = InProcessService.factory(published::execute, requests, background);
            }
        }
        boolean publication = description.getCompleteCommitPublication();
        try {
            require(description, service.value(), config.clientCount(), directory);
            var result = new LinkedHashMap<String, Object>();
            result.put("service", Map.of(
                    "adapter", service.adapter(),
                    "name", description.getName(),
                    "value_kind", description.getValueKind().name().toLowerCase(java.util.Locale.ROOT),
                    "durability", description.getDurability(),
                    "write_ack", description.getWriteAck().name(),
                    "baseline_durable_only_at_flush", description.getBaselineDurableOnlyAtFlush(),
                    "flush_makes_durable", description.getFlushMakesDurable(),
                    "complete_commit_publication", publication));
            for (boolean runtime : new boolean[] {false, true}) {
                String phase = runtime ? "workload" : "baseline";
                RunTiming.current().phase(phase);
                long start = System.nanoTime();
                try (var projection =
                        ReplayProjection.withClients(
                                config, runtime, factory, publication)) {
                    (runtime
                                    ? IndexOperationTrace.Source.file(directory.resolve("workload.ops"))
                                    : org.sibench.capture.BaselineOperations.of(directory))
                            .read(projection);
                    projection.finish();
                    if (runtime) projection.requireClientCoverage();
                    double seconds = (System.nanoTime() - start) / 1e9;
                    long flushStart = System.nanoTime();
                    long publishedVersion;
                    if (remote != null) publishedVersion = remote.flush();
                    else if (published != null) publishedVersion = published.flush();
                    else {
                        local.flush();
                        publishedVersion = 0;
                    }
                    var phaseReport = new LinkedHashMap<String, Object>();
                    phaseReport.put("operations", projection.dispatched());
                    phaseReport.put("seconds", seconds);
                    phaseReport.put("operations_per_second", projection.dispatched() / seconds);
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
            result.put("result_checksum", requests.checksum());
            result.put("per_query_digest_checks", requests.digestChecks());
            result.put("per_query_digest_mismatches", requests.digestMismatches());
            result.put(
                    "expected_results_source",
                    generated == null ? "reference_report" : "generated_from_trace");
            result.put(
                    "expected_results_sha256",
                    org.sibench.generation.BaselineGenerator.sha256(expectedResults));
            if (reference != null) result.put("reference", compare(reference, service.value(), requests));
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
            return output;
        } finally {
            if (remote != null) remote.close();
            if (published != null) published.close();
            if (background != null) background.shutdownNow();
            if (local != null) local.close();
            if (generated != null) {
                try (var paths = Files.walk(generated)) {
                    for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList())
                        Files.deleteIfExists(path);
                }
            }
        }
    }

    /** The service must implement every semantic the trace relies on. */
    private static void require(DescribeResponse description, ValueKind value, int clients, Path directory)
            throws IOException {
        if (description.getProtocolVersion() != 1) throw new IOException("Unsupported protocol version");
        // A search's result depends on every earlier write in source order; across client streams
        // that is the service's to guarantee, never something the driver repairs by waiting.
        if (clients > 1
                && !description.getCrossClientOrdering()
                && !description.getCompleteCommitPublication())
            throw new IOException(
                    "Replay with "
                            + clients
                            + " clients needs a service that keeps the driver's send order or"
                            + " publishes complete commits across client streams; "
                            + description.getName()
                            + " declares neither cross_client_ordering nor"
                            + " complete_commit_publication");
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

    /** The reference replay of the same trace and value kind found the same entries. */
    private static Map<String, Object> compare(Path reference, ValueKind value, TraceRequests requests)
            throws IOException {
        String mode = value == ValueKind.ROW_ID ? "rowid" : "locator";
        var document = JsonParser.parseString(Files.readString(reference)).getAsJsonObject();
        for (var item : document.getAsJsonArray("results")) {
            var result = item.getAsJsonObject();
            if (!result.get("mode").getAsString().equals(mode)) continue;
            long queries = result.get("queries").getAsLong(),
                    items = result.get("checked_result_items").getAsLong(),
                    checksum = result.get("result_checksum").getAsLong();
            if (queries != requests.queries() || items != requests.checkedResults() || checksum != requests.checksum())
                throw new IOException("Service results differ from the reference replay: queries "
                        + requests.queries() + "/" + queries + ", items " + requests.checkedResults() + "/" + items
                        + ", checksum " + requests.checksum() + "/" + checksum);
            return Map.of("report", reference.toString(), "mode", mode, "status", "same_results");
        }
        throw new IOException("Reference report has no " + mode + " result");
    }
}
