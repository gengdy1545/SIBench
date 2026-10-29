package org.sibench.replay;

import com.google.gson.*;

import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Offline variant equivalence, not concurrent transport or timed performance. */
public final class ReplayMatrixValidation {
    private ReplayMatrixValidation() {}

    public static void validate(Path normalized, Path output) throws IOException {
        Files.createDirectory(output);
        Files.writeString(output.resolve("INCOMPLETE"), "Replay matrix not verified\n");
        var reports = new ArrayList<Map<String, Object>>();
        var expected = new HashMap<String, String>();
        for (boolean secondary : new boolean[] {false, true})
            for (String dispatch : List.of("operation", "transaction"))
                for (int clients : new int[] {1, 64, 128}) {
                    String name = "secondary-" + secondary + "-" + dispatch + "-" + clients;
                    var config =
                            new ReplayConfig(
                                    secondary, "none", 0, 0, 0, 0, dispatch, clients, 4096, 60);
                    var report =
                            ReferenceReplay.run(normalized, config, output.resolve(name + ".json"));
                    for (var item : (List<?>) report.get("results")) {
                        Map<?, ?> result = (Map<?, ?>) item;
                        Map<?, ?> workload = (Map<?, ?>) result.get("workload_dispatch");
                        String identity = secondary + "/" + result.get("mode");
                        String signature =
                                List.of(
                                                result.get("workload_operations"),
                                                result.get("queries"),
                                                result.get("query_digest_sha256"),
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
                                    BaselineGenerator.sha256(output.resolve(name + ".json")),
                                    "configuration",
                                    config.describe()));
                    System.out.println("Verified " + name);
                }
        for (boolean secondary : new boolean[] {false, true})
            SnapshotLookupValidation.run(
                    normalized,
                    new ReplayConfig(secondary, "none", 0, 0, 0, 0),
                    output.resolve("parquet-oracle-secondary-" + secondary + ".json"));
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
                                        "1/64/128 clients x both dispatch modes x secondary on/off;"
                                            + " both value modes, exact"
                                            + " operation-order/state/result equivalence."
                                            + " Independent Parquet oracle at 1 client for both"
                                            + " secondary settings; no real network or performance"
                                            + " claim.")),
                StandardOpenOption.CREATE_NEW);
        Files.delete(output.resolve("INCOMPLETE"));
    }
}
