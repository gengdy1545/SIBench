package org.sibench.capture;

import com.google.gson.*;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Immutable export identity; independent comparisons with PostgreSQL are separate evidence. */
public final class BaselineManifest {
    public static final String FILE = "baseline-manifest.json";

    private BaselineManifest() {}

    static void publish(SourceSchema schema, Path directory, Map<String, Long> expected) throws Exception {
        var files = new JsonArray();
        var entries = Files.readAllLines(directory.resolve("files.jsonl")).stream()
                .map(line -> JsonParser.parseString(line).getAsJsonObject()).toList();
        var counts = coverage(directory, schema.tables().stream().map(SourceSchema.Table::name).toList(), entries);
        if (!counts.equals(expected)) throw new IOException("Baseline row coverage mismatch");
        var paths = new ArrayList<Path>();
        for (var entry : entries) paths.add(dataPath(directory, entry));
        var digests = FileDigests.hashFiles(paths);
        for (var entry : entries) {
            var file = entry.deepCopy();
            Path path = dataPath(directory, file);
            String hash = digests.get(path);
            file.addProperty("sha256", hash);
            files.add(file);
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("status", "complete");
        result.put("data_format", "parquet");
        result.put("rows", total(counts));
        result.put("tables", counts);
        result.put("files", files);
        result.put("source_schema", schema.tables());
        result.put("source_schema_sha256", schema.fingerprints());
        result.put("files_manifest_sha256", FileDigests.sha256(directory.resolve("files.jsonl")));
        Path partial = directory.resolve(FILE + ".partial");
        Files.writeString(partial, new GsonBuilder().setPrettyPrinting().create().toJson(result), StandardOpenOption.CREATE_NEW);
        Files.move(partial, directory.resolve(FILE), StandardCopyOption.ATOMIC_MOVE);
    }

    private static Map<String, Long> coverage(Path directory, List<String> tables, List<JsonObject> files)
            throws IOException {
        var counts = new LinkedHashMap<String, Long>();
        for (String table : tables)
            if (counts.put(table, 0L) != null) throw new IOException("Duplicate baseline table");
        var paths = new HashSet<Path>();
        var ids = new HashSet<Long>();
        for (var file : files) {
            String table = file.get("table").getAsString();
            long rows = file.get("rows").getAsLong(), id = file.get("file_id").getAsLong();
            if (!counts.containsKey(table) || rows < 0 || id < 1
                    || !paths.add(dataPath(directory, file)) || !ids.add(id))
                throw new IOException("Invalid baseline file coverage");
            counts.put(table, Math.addExact(counts.get(table), rows));
        }
        return counts;
    }

    private static long total(Map<String, Long> counts) {
        long total = 0;
        for (long rows : counts.values()) total = Math.addExact(total, rows);
        return total;
    }

    static Path dataPath(Path directory, JsonObject file) throws IOException {
        Path root = directory.toAbsolutePath().normalize();
        Path path = root.resolve(file.get("path").getAsString()).normalize();
        if (!path.startsWith(root) || !root.equals(path.getParent()))
            throw new IOException("Invalid baseline data path");
        return path;
    }

    public static JsonObject verifyFiles(Path baseline) throws Exception {
        if (Files.exists(baseline.resolve("INCOMPLETE"))) throw new IOException("Baseline is incomplete");
        JsonObject manifest = JsonParser.parseString(Files.readString(baseline.resolve(FILE))).getAsJsonObject();
        if (!manifest.get("status").getAsString().equals("complete")) throw new IOException("Baseline is incomplete");
        if (!new JsonPrimitive("parquet").equals(manifest.get("data_format")))
            throw new IOException("Missing or unsupported Baseline data format; regenerate it");
        if (!FileDigests.sha256(baseline.resolve("files.jsonl"))
                .equals(manifest.get("files_manifest_sha256").getAsString()))
            throw new IOException("Baseline file manifest changed");
        var entries = Files.readAllLines(baseline.resolve("files.jsonl")).stream()
                .map(line -> JsonParser.parseString(line).getAsJsonObject()).toList();
        var files = manifest.getAsJsonArray("files");
        if (entries.size() != files.size()) throw new IOException("Baseline file coverage changed");
        for (int i = 0; i < entries.size(); i++) {
            var identity = files.get(i).getAsJsonObject().deepCopy();
            identity.remove("sha256");
            if (!entries.get(i).equals(identity)) throw new IOException("Baseline file coverage changed");
        }
        var tables = new ArrayList<String>();
        for (var table : manifest.getAsJsonArray("source_schema"))
            tables.add(table.getAsJsonObject().get("name").getAsString());
        var counts = coverage(baseline, tables, entries);
        if (!new Gson().toJsonTree(counts).equals(manifest.get("tables"))
                || total(counts) != manifest.get("rows").getAsLong())
            throw new IOException("Baseline row coverage changed");
        var paths = new ArrayList<Path>();
        for (var entry : files) paths.add(dataPath(baseline, entry.getAsJsonObject()));
        var digests = FileDigests.hashFiles(paths);
        for (var entry : files) {
            var file = entry.getAsJsonObject();
            if (!digests.get(dataPath(baseline, file)).equals(file.get("sha256").getAsString()))
                throw new IOException("Baseline data changed");
        }
        return manifest;
    }

    static JsonObject verify(SourceSchema schema, Path baseline) throws Exception {
        var manifest = verifyFiles(baseline);
        var json = new Gson();
        if (!json.toJsonTree(schema.tables()).equals(manifest.get("source_schema"))
                || !json.toJsonTree(schema.fingerprints()).equals(manifest.get("source_schema_sha256")))
            throw new IOException("Baseline source schema changed");
        return manifest;
    }
}
