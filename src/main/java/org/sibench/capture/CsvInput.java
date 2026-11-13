package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;
import static org.sibench.capture.InputFiles.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.sibench.config.Digests;
import org.sibench.config.Engine;

/** Published HyBench data shared by independent source-capture and Pixels preparation tasks. */
public final class CsvInput {
    private CsvInput() {}

    record Table(int id, int columns) {}

    record Verified(Path directory, JsonObject manifest, String sha256) {}

    public static Map<String, Object> prepare(Path root, Path configuration, Path outputDirectory) throws Exception {
        var plan = Generation.csvPlan(root, configuration, outputDirectory);
        var input = plan.inputs().get(0);
        Path output = input.output();
        Files.createDirectories(output.getParent());
        Files.createDirectory(output);
        try {
            Path load = Files.createDirectory(output.resolve("load"));
            Generation.hybench(plan, input, load, true, null, Engine.POSTGRES, "", null, () -> {});
            Path data = output.resolve("native/Data_" + string(input.nativeConfig(), "sf"));
            HybenchInputs.record(data, input.source().source().resolve("conf/load_data_pg.sql"), load.resolve("inputs.json"));
            Files.copy(input.source().source().resolve("conf/ddl_pg.sql"), output.resolve("schema.sql"));
            var inspected = PixelsInputs.inspect(input.source(), data);
            Generation.requireRows(input.baselineRows(), inspected.rows());
            Files.writeString(output.resolve("pixels-schema.tsv"), inspected.schema().text(), StandardOpenOption.CREATE_NEW);
            var evidence = new LinkedHashMap<String, Object>();
            evidence.put("benchmark_source", Map.of("commit", input.source().sourceCommit(), "files", input.source().fingerprints()));
            evidence.put("client", input.buildIdentity());
            Path code = NativeProcess.self();
            evidence.put("core_sha256", Files.isDirectory(code) ? PixelsBuild.treeSha256(code) : Digests.sha256(code));
            Files.writeString(output.resolve("effective-config.json"), new GsonBuilder().setPrettyPrinting().create()
                    .toJson(Map.of("benchmark", input.document())), StandardOpenOption.CREATE_NEW);
            return publish(output, input, evidence);
        } catch (Exception | Error failure) {
            var stack = new java.io.StringWriter();
            failure.printStackTrace(new java.io.PrintWriter(stack));
            try { Files.writeString(output.resolve("failure.txt"), stack.toString(), StandardOpenOption.CREATE_NEW); }
            catch (IOException diagnostic) { failure.addSuppressed(diagnostic); }
            throw failure;
        }
    }

    static Map<String, Object> publish(Path output, Generation.Input input, Map<String, Object> evidence) throws IOException {
        var document = baseline(output, input);
        document.addProperty("kind", "hybench_csv_input");
        document.addProperty("status", "complete");
        document.addProperty("sf", string(input.nativeConfig(), "sf"));
        document.add("parameters", new Gson().toJsonTree(input.parameters()));
        document.add("parameter_file", file(output, output.resolve("native/parameters/parameters.toml")));
        document.add("load_inventory", file(output, output.resolve("load/inputs.json")));
        document.add("source_evidence", new Gson().toJsonTree(evidence));
        validate(output, document, false);
        Files.writeString(output.resolve("manifest.json"), new GsonBuilder().setPrettyPrinting().create().toJson(document),
                StandardOpenOption.CREATE_NEW);
        return new Gson().fromJson(document, Map.class);
    }

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

    private static void validate(Path root, JsonObject document, boolean hashes) throws IOException {
        require(document.get("kind").getAsString().equals("hybench_csv_input")
                && document.get("status").getAsString().equals("complete"), "Unpublished or unsupported CSV input");
        validateBaseline(root, document, false);
        String sf = document.get("sf").getAsString();
        require(sf.matches("[1-9][0-9]*x"), "Invalid CSV scale");
        Path parameters = verified(root, document.getAsJsonObject("parameter_file"));
        var values = new Gson().fromJson(document.getAsJsonObject("parameters"), Map.class);
        var actual = new com.moandjiezana.toml.Toml().read(parameters.toFile()).getTable(sf);
        require(actual != null && new Gson().toJsonTree(actual.toMap()).equals(new Gson().toJsonTree(values)),
                "CSV parameters differ from the generator input");
        Path inventory = verified(root, document.getAsJsonObject("load_inventory"));
        var hashesByName = JsonParser.parseString(Files.readString(inventory)).getAsJsonObject().getAsJsonObject("data_sha256");
        var files = document.getAsJsonObject("baseline_files");
        require(files.keySet().equals(hashesByName.keySet()), "CSV inventory differs from baseline files");
        for (var entry : files.entrySet()) {
            var file = entry.getValue().getAsJsonObject();
            require(entry.getKey().matches("[A-Za-z0-9_.]+")
                    && file.get("path").getAsString().equals("native/Data_" + sf + "/" + entry.getKey())
                    && file.get("sha256").equals(hashesByName.get(entry.getKey())), "CSV inventory identity differs");
        }
        require(files.keySet().containsAll(HybenchInputs.RUNTIME_INPUTS), "Missing HyBench workload inputs");
        if (hashes) {
            var ddl = document.getAsJsonObject("source_ddl");
            var source = org.sibench.schema.SourceSchema.captured(resolve(root, ddl.get("path").getAsString()),
                    document.getAsJsonObject("source_evidence").getAsJsonObject("benchmark_source").get("commit").getAsString(),
                    ddl.get("sha256").getAsString());
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

    static JsonObject baseline(Path output, Generation.Input input) throws IOException {
        var document = new JsonObject();
        document.add("baseline_rows", new Gson().toJsonTree(input.baselineRows()));
        document.add("schema", file(output, output.resolve("pixels-schema.tsv")));
        document.add("source_ddl", file(output, output.resolve("schema.sql")));
        var baseline = new JsonObject();
        var original = JsonParser.parseString(Files.readString(output.resolve("load/inputs.json")))
                .getAsJsonObject().getAsJsonObject("data_sha256");
        Path data = output.resolve("native/Data_" + org.sibench.config.ConfigValues.string(input.nativeConfig(), "sf"));
        var csvTables = new HashMap<String, String>();
        for (String line : Files.readAllLines(input.source().source().resolve("conf/load_data_pg.sql"))) {
            if (line.isBlank()) continue;
            String[] copy = HybenchInputs.copyStatement(line);
            require(copy != null, "Invalid baseline load mapping");
            csvTables.put(copy[1], copy[0].toLowerCase(java.util.Locale.ROOT));
        }
        for (var entry : original.entrySet()) {
            var value = file(output, data.resolve(entry.getKey()));
            require(value.get("sha256").equals(entry.getValue()), "Baseline input changed after source loading: " + entry.getKey());
            if (csvTables.containsKey(entry.getKey())) {
                String table = csvTables.get(entry.getKey());
                value.addProperty("table", table);
                value.addProperty("rows", input.baselineRows().get(table));
            }
            baseline.add(entry.getKey(), value);
        }
        document.add("baseline_files", baseline);
        return document;
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

    static void requireCompatible(Verified csv, Generation.Input input) throws IOException {
        var manifest = csv.manifest();
        require(manifest.get("sf").getAsString().equals(string(input.nativeConfig(), "sf"))
                && manifest.get("parameters").equals(new Gson().toJsonTree(input.parameters()))
                && manifest.get("baseline_rows").equals(new Gson().toJsonTree(input.baselineRows())),
                "CSV scale, parameters or row counts differ from the capture configuration");
        require(manifest.getAsJsonObject("source_ddl").get("sha256").getAsString()
                .equals(Digests.sha256(input.source().source().resolve("conf/ddl_pg.sql"))), "CSV source DDL differs");
        var source = manifest.getAsJsonObject("source_evidence").getAsJsonObject("benchmark_source");
        require(source.get("commit").getAsString().equals(input.source().sourceCommit())
                && source.get("files").equals(new Gson().toJsonTree(input.source().fingerprints())), "CSV benchmark source differs");
    }

    /** Materializes an independently publishable input; same-filesystem immutable payloads share disk blocks. */
    static void materialize(Verified csv, Path output) throws IOException {
        var files = new java.util.ArrayList<JsonObject>();
        for (String key : java.util.List.of("schema", "source_ddl", "parameter_file", "load_inventory"))
            files.add(csv.manifest().getAsJsonObject(key));
        csv.manifest().getAsJsonObject("baseline_files").entrySet().forEach(e -> files.add(e.getValue().getAsJsonObject()));
        for (var file : files) {
            String name = file.get("path").getAsString();
            Path from = resolve(csv.directory(), name), to = output.resolve(name);
            Files.createDirectories(to.getParent());
            if (Files.getFileStore(from).equals(Files.getFileStore(to.getParent()))) Files.createLink(to, from);
            else Files.copy(from, to);
        }
        Files.writeString(output.resolve("csv-input.json"), new GsonBuilder().setPrettyPrinting().create().toJson(
                Map.of("manifest_sha256", csv.sha256(), "manifest", csv.manifest())), StandardOpenOption.CREATE_NEW);
    }

    private static void require(boolean valid, String message) throws IOException {
        if (!valid) throw new IOException(message);
    }
}
