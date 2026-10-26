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
        var requests = new TraceRequests(service.value());
        DescribeResponse description;
        ReplayTarget.Factory factory;
        GrpcService remote = null;
        RocksDbBackend local = null;
        if (service.adapter().equals("grpc")) {
            remote = new GrpcService(service.endpoint(), requests);
            description = remote.describe();
            factory = remote.factory();
        } else {
            local = RocksDbBackend.create(service.path(), service.value());
            // Requests run on the sending thread, one at a time in send order.
            description = local.describe().toBuilder().setCrossClientOrdering(true).build();
            factory = InProcessService.factory(local, requests);
        }
        try {
            require(description, service.value(), config.clientCount(), directory);
            var result = new LinkedHashMap<String, Object>();
            result.put("service", Map.of(
                    "adapter", service.adapter(),
                    "name", description.getName(),
                    "value_kind", description.getValueKind().name().toLowerCase(java.util.Locale.ROOT),
                    "durability", description.getDurability()));
            for (boolean runtime : new boolean[] {false, true}) {
                String phase = runtime ? "workload" : "baseline";
                RunTiming.current().phase(phase);
                long start = System.nanoTime();
                try (var projection = ReplayProjection.withClients(config, runtime, factory)) {
                    (runtime
                                    ? IndexOperationTrace.Source.file(directory.resolve("workload.ops"))
                                    : org.sibench.capture.BaselineOperations.of(directory))
                            .read(projection);
                    projection.finish();
                    if (runtime) projection.requireClientCoverage();
                    double seconds = (System.nanoTime() - start) / 1e9;
                    long flushStart = System.nanoTime();
                    if (remote != null) remote.flush();
                    else local.flush();
                    var phaseReport = new LinkedHashMap<String, Object>();
                    phaseReport.put("operations", projection.dispatched());
                    phaseReport.put("seconds", seconds);
                    phaseReport.put("operations_per_second", projection.dispatched() / seconds);
                    phaseReport.put("flush_seconds", (System.nanoTime() - flushStart) / 1e9);
                    phaseReport.put("dispatch", projection.describe());
                    result.put(phase, phaseReport);
                }
            }
            result.put("queries", requests.queries());
            result.put("checked_result_items", requests.checkedResults());
            result.put("result_checksum", requests.checksum());
            if (reference != null) result.put("reference", compare(reference, service.value(), requests));
            var output = new LinkedHashMap<String, Object>();
            output.put("status", "passed");
            output.put("preflight", preflight);
            output.put("result", result);
            output.put("scope", "every search returned its native expected count on the service;"
                    + " single sending thread, per-client ordered streams; not a sustained-rate,"
                    + " recovery or cross-client visibility test");
            Files.writeString(report, new GsonBuilder().setPrettyPrinting().create().toJson(output),
                    StandardOpenOption.CREATE_NEW);
            return output;
        } finally {
            if (remote != null) remote.close();
            if (local != null) local.close();
        }
    }

    /** The service must implement every semantic the trace relies on. */
    private static void require(DescribeResponse description, ValueKind value, int clients, Path directory)
            throws IOException {
        if (description.getProtocolVersion() != 1) throw new IOException("Unsupported protocol version");
        // A search's result depends on every earlier write in source order; across client streams
        // that is the service's to guarantee, never something the driver repairs by waiting.
        if (clients > 1 && !description.getCrossClientOrdering())
            throw new IOException(
                    "Replay with "
                            + clients
                            + " clients needs a service that keeps the driver's send order across"
                            + " client streams; "
                            + description.getName()
                            + " does not declare cross_client_ordering");
        if (description.getValueKind() != value)
            throw new IOException("Service stores " + description.getValueKind() + ", replay sends " + value);
        if (!description.getVersionedReads() || !description.getOrderedSameVersionOverwrite())
            throw new IOException("Service lacks versioned reads or ordered same-version overwrite");
        JsonObject manifest = JsonParser.parseString(Files.readString(directory.resolve("manifest.json"))).getAsJsonObject();
        if (!description.getSearchGroups() && manifest.has("searched_accesses_by_shape")) {
            var shapes = manifest.getAsJsonObject("searched_accesses_by_shape");
            if (shapes.has("intersection") || shapes.has("union"))
                throw new IOException("Trace has search groups; the service does not execute them");
        }
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
