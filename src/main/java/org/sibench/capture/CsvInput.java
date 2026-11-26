package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;
import static org.sibench.capture.InputFiles.*;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import org.sibench.config.Digests;
import org.sibench.schema.SourceSchema;

/** Published baseline data shared by source capture and Pixels preparation tasks. */
public final class CsvInput {
    private CsvInput() {}

    record Table(int id, int columns) {}

    record Verified(Path directory, JsonObject manifest, String sha256) {}

    public static Map<String, Object> check(Path directory) throws IOException {
        var input = open(directory);
        return Map.of("status", "passed", "manifest_sha256", input.sha256(),
                "baseline_rows", input.manifest().getAsJsonObject("baseline_rows"));
    }

    static Verified open(Path directory) throws IOException { return open(directory, true); }

    /** Manifest, schema, DDL and file sizes only; a consumer reading every baseline CSV compares their digests itself. */
    static Verified bind(Path directory) throws IOException { return open(directory, false); }

    private static Verified open(Path directory, boolean contents) throws IOException {
        directory = directory.toRealPath();
        var manifest = manifest(directory);
        try {
            validate(directory, manifest.document(), contents);
            return new Verified(directory, manifest.document(), manifest.sha256());
        } catch (RuntimeException malformed) {
            throw new IOException("Invalid CSV input: " + directory, malformed);
        }
    }

    static void validate(Path root, JsonObject document, boolean hashes) throws IOException {
        require(java.util.Set.of("hybench_csv_input", "sysbench_csv_input").contains(document.get("kind").getAsString())
                && document.get("status").getAsString().equals("complete"), "Unpublished or unsupported CSV input");
        validateBaseline(root, document, false);
        var files = document.getAsJsonObject("baseline_files");
        if (document.get("kind").getAsString().equals("sysbench_csv_input")) {
            SysbenchInputs.validate(root, document);
        } else {
            HybenchInputs.validate(root, document);
        }
        if (hashes) {
            var source = source(root, document);
            var tables = new LinkedHashMap<String, Path>();
            for (var item : files.entrySet()) {
                var file = item.getValue().getAsJsonObject();
                if (file.has("table")) tables.put(file.get("table").getAsString(), resolve(root, file.get("path").getAsString()));
                else verified(root, file);
            }
            var inspected = PixelsInputs.prepare(source, tables, null, Long.MAX_VALUE);
            require(inspected.schema().text().equals(Files.readString(resolve(root,
                    document.getAsJsonObject("schema").get("path").getAsString()))), "CSV schema does not describe its data");
            for (var item : files.entrySet()) {
                var file = item.getValue().getAsJsonObject();
                if (!file.has("table")) continue;
                String table = file.get("table").getAsString();
                require(file.get("sha256").getAsString().equals(inspected.sha256().get(table))
                        && file.get("rows").getAsLong() == inspected.rows().get(table), "CSV content or row count changed: " + table);
            }
        }
    }

    /** Retained HyBench CSV/baseline artifacts omit benchmark; their source DDL fingerprint identifies them. */
    static String benchmark(JsonObject document) throws IOException {
        var evidence = document.getAsJsonObject("source_evidence");
        require(evidence != null, "Missing input source evidence");
        if (evidence.has("benchmark")) {
            String benchmark = evidence.get("benchmark").getAsString();
            require(java.util.Set.of("hybench", "sysbench").contains(benchmark), "Unsupported input benchmark");
            return benchmark;
        }
        var source = evidence.getAsJsonObject("benchmark_source");
        var files = source == null ? null : source.getAsJsonObject("files");
        require(files != null && document.getAsJsonObject("source_ddl").get("sha256").equals(files.get("conf/ddl_pg.sql")),
                "Missing input benchmark identity");
        return "hybench";
    }

    static SourceSchema source(Path directory, JsonObject document) throws IOException {
        var ddl = document.getAsJsonObject("source_ddl");
        return SourceSchema.captured(resolve(directory, ddl.get("path").getAsString()),
                document.getAsJsonObject("source_evidence").getAsJsonObject("benchmark_source").get("commit").getAsString(),
                ddl.get("sha256").getAsString(), benchmark(document));
    }

    static Map<String, Table> validateBaseline(Path directory, JsonObject document, boolean baselineHashes) throws IOException {
        Path schema = verified(directory, document.getAsJsonObject("schema"));
        verified(directory, document.getAsJsonObject("source_ddl"));
        var tables = new LinkedHashMap<String, Table>();
        int id = 0;
        for (String line : Files.readAllLines(schema)) {
            String[] fields = line.split("\t");
            require(fields.length == 3 && fields[0].matches("[a-z][a-z0-9_]*"), "Invalid input table schema");
            require(tables.put(fields[0], new Table(++id, fields[2].split(";").length)) == null, "Duplicate input table schema");
        }
        require(tables.containsKey("sibench_marker"), "Missing source window marker schema");
        var baselines = document.getAsJsonObject("baseline_files");
        require(baselines != null && !baselines.isEmpty(), "Missing bound baseline files");
        var baselineRows = document.getAsJsonObject("baseline_rows");
        require(baselineRows != null, "Missing baseline row counts");
        var expectedTables = new java.util.HashSet<>(tables.keySet());
        expectedTables.remove("sibench_marker");
        require(expectedTables.equals(baselineRows.keySet()), "Baseline table set differs from schema");
        var baselineTables = new java.util.HashSet<String>();
        var paths = new java.util.HashSet<String>();
        paths.add(document.getAsJsonObject("schema").get("path").getAsString());
        require(paths.add(document.getAsJsonObject("source_ddl").get("path").getAsString()), "Aliased input schema files");
        for (var item : baselines.entrySet()) {
            var file = item.getValue().getAsJsonObject();
            require(paths.add(file.get("path").getAsString()), "Duplicate baseline file");
            if (file.has("table")) {
                String table = file.get("table").getAsString();
                require(expectedTables.contains(table) && baselineTables.add(table)
                        && number(file, "rows") == number(baselineRows, table), "Baseline table/count binding differs");
            }
            if (baselineHashes) verifyBaseline(directory, file);
            else require(Files.size(resolve(directory, file.get("path").getAsString())) == number(file, "bytes"), "Baseline size changed");
        }
        require(baselineTables.equals(expectedTables), "Missing baseline table CSV");
        return tables;
    }

    private static void verifyBaseline(Path directory, JsonObject file) throws IOException {
        Path path = resolve(directory, file.get("path").getAsString());
        var digest = Digests.digest();
        long bytes = 0, rows = 0;
        int last = -1;
        try (var stream = new DigestInputStream(Files.newInputStream(path), digest)) {
            byte[] buffer = new byte[1 << 16];
            for (int n; (n = stream.read(buffer)) != -1;) {
                bytes += n;
                if (file.has("table")) for (int i = 0; i < n; i++) if (buffer[i] == '\n') rows++;
                if (n > 0) last = buffer[n - 1];
            }
        }
        if (last != -1 && last != '\n') rows++;
        require(bytes == number(file, "bytes")
                && java.util.HexFormat.of().formatHex(digest.digest()).equals(file.get("sha256").getAsString()), "Baseline input changed: " + path);
        if (file.has("table")) require(rows == number(file, "rows"), "Baseline CSV row count differs: " + path);
    }

    /** Every local payload descriptor a CSV-derived manifest binds; consumers without a field simply lack those files. */
    static java.util.List<JsonObject> files(JsonObject manifest) {
        var files = new java.util.ArrayList<JsonObject>();
        for (String key : java.util.List.of("schema", "source_ddl", "parameter_file", "load_inventory"))
            if (manifest.has(key)) files.add(manifest.getAsJsonObject(key));
        if (manifest.has("baseline_files"))
            manifest.getAsJsonObject("baseline_files").entrySet().forEach(e -> files.add(e.getValue().getAsJsonObject()));
        return files;
    }

    /** Materializes an independently publishable input; same-filesystem immutable payloads share disk blocks. */
    static void materialize(Verified csv, Path output) throws IOException {
        for (var file : files(csv.manifest())) {
            String name = file.get("path").getAsString();
            Path from = resolve(csv.directory(), name), to = output.resolve(name);
            Files.createDirectories(to.getParent());
            if (Files.getFileStore(from).equals(Files.getFileStore(to.getParent()))) Files.createLink(to, from);
            else Files.copy(from, to);
        }
    }

    private static void require(boolean valid, String message) throws IOException {
        if (!valid) throw new IOException(message);
    }
}
