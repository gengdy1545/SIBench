package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.schema.SourceSchema;

/** The pending marker, stream inventory and atomic manifest publication shared by every Trace producer. */
final class TraceDirectory {
    private TraceDirectory() {}

    /** Creates a fresh pending directory and the manifest fields every producer declares. */
    static LinkedHashMap<String, Object> create(Path directory, SourceSchema source,
            List<IndexCatalog.Definition> definitions, String mode, String generation,
            boolean secondaryIndexes, List<String> valueKinds) throws IOException {
        var manifest = new LinkedHashMap<String, Object>(IndexCatalog.provenance(source, definitions));
        IndexCatalog.fromManifest(new Gson().toJsonTree(manifest).getAsJsonObject());
        Files.createDirectory(directory);
        Files.writeString(directory.resolve("INCOMPLETE"), "Trace generation/publication pending\n", StandardOpenOption.CREATE_NEW);
        manifest.put("output", "proto");
        manifest.put("value_kinds", valueKinds);
        manifest.put("mode", mode);
        manifest.put("generation", generation);
        manifest.put("secondary_index_coverage", secondaryIndexes ? "all_source_definitions" : "primary_only");
        return manifest;
    }

    /** Records a closed stream's descriptor, count and identity. */
    static void stream(Map<String, Object> manifest, String name, Map<String, Object> file) {
        manifest.put(name + "_operations", file.get("operations"));
        @SuppressWarnings("unchecked") var streams = (Map<String, Object>) manifest.computeIfAbsent("proto_files", ignored -> new LinkedHashMap<>());
        streams.put(name, file);
        manifest.put(name + "_trace_sha256",
                ProtoTrace.identity((String) file.get("logical_sha256"), ((Number) file.get("operations")).longValue()));
    }

    static Map<String, Object> publish(Path directory, Map<String, Object> manifest, Map<String, Object> evidence) throws IOException {
        if (!manifest.containsKey("baseline_trace_sha256") || !manifest.containsKey("workload_trace_sha256"))
            throw new IOException("Trace streams did not finish");
        if (!Files.isRegularFile(directory.resolve("INCOMPLETE")) || Files.exists(directory.resolve("manifest.json")))
            throw new IOException("Trace publication is not pending");
        var result = new LinkedHashMap<>(manifest);
        result.put("source_evidence", evidence);
        result.put("status", "complete_for_declared_projection");
        Path partial = directory.resolve("manifest.json.partial");
        Files.writeString(partial, new GsonBuilder().setPrettyPrinting().create().toJson(result), StandardOpenOption.CREATE_NEW);
        Files.move(partial, directory.resolve("manifest.json"), StandardCopyOption.ATOMIC_MOVE);
        Files.delete(directory.resolve("INCOMPLETE"));
        return Map.copyOf(result);
    }
}
