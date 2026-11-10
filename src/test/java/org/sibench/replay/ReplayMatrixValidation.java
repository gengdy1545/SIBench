package org.sibench.replay;

import com.google.gson.*;

import org.sibench.config.Digests;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Offline variant equivalence, not concurrent transport or timed performance. */
public final class ReplayMatrixValidation {
    private ReplayMatrixValidation() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IllegalArgumentException("ReplayMatrixValidation TRACE OUTPUT");
        validate(Path.of(args[0]), Path.of(args[1]));
    }

    public static void validate(Path trace, Path output) throws IOException {
        Files.createDirectory(output);
        Files.writeString(output.resolve("INCOMPLETE"), "Replay matrix not verified\n");
        var manifest = org.sibench.trace.TraceManifest.read(trace);
        boolean[] scopes = org.sibench.trace.TraceManifest.secondaryIndexes(manifest)
                ? new boolean[] {false, true} : new boolean[] {false};
        var reports = new ArrayList<Map<String, Object>>();
        var expected = new HashMap<String, String>();
        for (boolean secondary : scopes)
            for (String dispatch : List.of("operation", "transaction"))
                for (int clients : new int[] {1, 2}) {
                    String name = "secondary-" + secondary + "-" + dispatch + "-" + clients;
                    var config =
                            new ReplayConfig(
                                    secondary, 0, 0, 0, 0, dispatch, clients, 4096, 60);
                    var report =
                            ReferenceReplay.audit(trace, config, output.resolve(name + ".json"));
                    for (var item : (List<?>) report.get("results")) {
                        Map<?, ?> result = (Map<?, ?>) item;
                        Map<?, ?> workload = (Map<?, ?>) result.get("workload_dispatch");
                        String identity = secondary + "/" + result.get("mode");
                        String signature =
                                List.of(
                                                result.get("workload_operations"),
                                                result.get("queries"),
                                                result.get("primary_state_checksum"),
                                                result.get("selected_version_checksum"),
                                                workload.get("operation_order_checksum"))
                                        .toString();
                        String previous = expected.putIfAbsent(identity, signature);
                        if (previous != null && !previous.equals(signature))
                            throw new IOException(
                                    "Replay variants changed operation order or state/results");
                    }
                    reports.add(
                            Map.of(
                                    "report",
                                    name + ".json",
                                    "sha256",
                                    Digests.sha256(output.resolve(name + ".json")),
                                    "configuration",
                                    config.describe()));
                    System.out.println("Verified " + name);
                }
        Files.writeString(
                output.resolve("matrix.json"),
                new GsonBuilder()
                        .setPrettyPrinting()
                        .create()
                        .toJson(
                                Map.of(
                                        "status",
                                        "passed",
                                        "variants",
                                        reports,
                                        "scope",
                                        "1/2 clients x both dispatch modes x supported secondary settings;"
                                            + " declared value modes,"
                                            + " operation-order/state/count equivalence;"
                                            + " no real network or performance claim.")),
                StandardOpenOption.CREATE_NEW);
        Files.delete(output.resolve("INCOMPLETE"));
    }
}
