package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;
import static org.sibench.capture.InputFiles.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.sibench.config.Engine;
import org.sibench.config.Digests;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

/** Generated upstream inputs: COPY files and the lists used by workload clients. */
public final class HybenchInputs {
    /** One upstream load statement: {@code \copy table from 'Data_1x/file.csv' CSV DELIMITER ','}. */
    private static final Pattern COPY =
            Pattern.compile("\\\\copy\\s+(\\w+)\\s+from\\s+'Data_1x/([\\w.]+)'\\s+CSV\\s+DELIMITER\\s+','\\s*;?", Pattern.CASE_INSENSITIVE);

    /**
     * The generated files the workload clients read when they start ({@code Client.init}). Upstream
     * only logs a missing one and leaves its id list null, so every AP client then fails on its
     * first query; the load stage checks them before workload execution.
     */
    static final java.util.List<String> RUNTIME_INPUTS = java.util.List.of("Related_transfer_bids", "Related_checking_bids");

    private HybenchInputs() {}

    static void record(Path data, Path loadScript, Path manifest) throws IOException {
        var names = new java.util.LinkedHashSet<String>();
        for (String line : Files.readAllLines(loadScript)) {
            if (line.isBlank()) continue;
            String[] input = copyStatement(line);
            if (input == null || !names.add(input[1]))
                throw new IOException("Invalid or repeated upstream load input: " + line);
        }
        names.addAll(RUNTIME_INPUTS);
        var hashes = new java.util.LinkedHashMap<String, String>();
        for (String name : names) {
            Path file = data.resolve(name);
            if (!Files.isRegularFile(file, java.nio.file.LinkOption.NOFOLLOW_LINKS))
                throw new IOException("Missing generated HyBench input: " + file);
            hashes.put(name, Digests.sha256(file));
        }
        Files.writeString(manifest, new com.google.gson.GsonBuilder().setPrettyPrinting().create()
                .toJson(java.util.Map.of("data_sha256", hashes)), java.nio.file.StandardOpenOption.CREATE_NEW);
    }

    /**
     * The client's runtime inputs are the files bound by this load's manifest.
     */
    static void requireRuntimeInputs(Path data, Path manifest) throws Exception {
        var recorded = JsonParser.parseString(Files.readString(manifest))
                .getAsJsonObject().getAsJsonObject("data_sha256");
        for (String name : RUNTIME_INPUTS) {
            Path file = data.resolve(name);
            if (!Files.isRegularFile(file))
                throw new IllegalStateException(
                        "HyBench runtime input missing: " + file + "; the work directory must hold the Data_ directory"
                                + " its database was generated with");
            if (!recorded.has(name) || !Digests.sha256(file).equals(recorded.get(name).getAsString()))
                throw new IllegalStateException("HyBench runtime input differs from the load manifest: " + file);
        }
    }

    /** Deletes the loaded CSV files of {@code Data_<sf>}, keeping the client's runtime inputs. */
    static void dropLoadInputs(Path data, Path loadScript) throws IOException {
        for (String line : Files.readAllLines(loadScript)) {
            if (line.isBlank()) continue;
            String[] loaded = copyStatement(line);
            if (loaded == null) throw new IOException("Unexpected upstream load statement: " + line);
            Path file = data.resolve(loaded[1]);
            if (!Files.isRegularFile(file)) continue;
            Files.delete(file);
        }
    }

    /** The table and file of one upstream load statement, or null when it is not one. */
    static String[] copyStatement(String line) {
        var match = COPY.matcher(line.strip());
        return match.matches() ? new String[] {match.group(1), match.group(2)} : null;
    }

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
            return publish(output, input, inspected.sha256(), evidence);
        } catch (Exception | Error failure) {
            var stack = new java.io.StringWriter();
            failure.printStackTrace(new java.io.PrintWriter(stack));
            try { Files.writeString(output.resolve("failure.txt"), stack.toString(), StandardOpenOption.CREATE_NEW); }
            catch (IOException diagnostic) { failure.addSuppressed(diagnostic); }
            throw failure;
        }
    }

    /** {@code tableHashes} are the digests of exactly the table bytes inspected for the schema and row counts. */
    static Map<String, Object> publish(Path output, Generation.Input input, Map<String, String> tableHashes,
            Map<String, Object> evidence) throws IOException {
        var document = baseline(output, input, tableHashes);
        document.addProperty("kind", "hybench_csv_input");
        document.addProperty("status", "complete");
        document.addProperty("sf", string(input.nativeConfig(), "sf"));
        document.add("parameters", new Gson().toJsonTree(input.parameters()));
        document.add("parameter_file", file(output, output.resolve("native/parameters/parameters.toml")));
        document.add("load_inventory", file(output, output.resolve("load/inputs.json")));
        var provenance = new Gson().toJsonTree(evidence).getAsJsonObject();
        provenance.addProperty("benchmark", "hybench");
        document.add("source_evidence", provenance);
        CsvInput.validate(output, document, false);
        Files.writeString(output.resolve("manifest.json"), new GsonBuilder().setPrettyPrinting().create().toJson(document),
                StandardOpenOption.CREATE_NEW);
        return new Gson().fromJson(document, Map.class);
    }

    /** Descriptors take the load inventory's digests; inspected table digests must match them. */
    private static JsonObject baseline(Path output, Generation.Input input, Map<String, String> tableHashes) throws IOException {
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
        require(tableHashes.keySet().equals(new java.util.HashSet<>(csvTables.values())), "Inspected tables differ from the load mapping");
        for (var entry : original.entrySet()) {
            var value = new JsonObject();
            Path path = data.resolve(entry.getKey());
            value.addProperty("path", output.relativize(path).toString());
            value.addProperty("bytes", Files.size(path));
            value.add("sha256", entry.getValue());
            if (csvTables.containsKey(entry.getKey())) {
                String table = csvTables.get(entry.getKey());
                require(entry.getValue().getAsString().equals(tableHashes.get(table)), "Baseline input changed after source loading: " + entry.getKey());
                value.addProperty("table", table);
                value.addProperty("rows", input.baselineRows().get(table));
            }
            baseline.add(entry.getKey(), value);
        }
        document.add("baseline_files", baseline);
        return document;
    }

    static void validate(Path root, JsonObject document) throws IOException {
        require(CsvInput.benchmark(document).equals("hybench"), "HyBench baseline source identity differs");
        var files = document.getAsJsonObject("baseline_files");
        String sf = document.get("sf").getAsString();
        require(sf.matches("[1-9][0-9]*x"), "Invalid CSV scale");
        Path parameters = verified(root, document.getAsJsonObject("parameter_file"));
        var values = new Gson().fromJson(document.getAsJsonObject("parameters"), Map.class);
        var actual = new com.moandjiezana.toml.Toml().read(parameters.toFile()).getTable(sf);
        require(actual != null && new Gson().toJsonTree(actual.toMap()).equals(new Gson().toJsonTree(values)),
                "CSV parameters differ from the generator input");
        Path inventory = verified(root, document.getAsJsonObject("load_inventory"));
        var hashesByName = JsonParser.parseString(Files.readString(inventory)).getAsJsonObject().getAsJsonObject("data_sha256");
        require(files.keySet().equals(hashesByName.keySet()), "CSV inventory differs from baseline files");
        for (var entry : files.entrySet()) {
            var file = entry.getValue().getAsJsonObject();
            require(entry.getKey().matches("[A-Za-z0-9_.]+")
                    && file.get("path").getAsString().equals("native/Data_" + sf + "/" + entry.getKey())
                    && file.get("sha256").equals(hashesByName.get(entry.getKey())), "CSV inventory identity differs");
        }
        require(files.keySet().containsAll(HybenchInputs.RUNTIME_INPUTS), "Missing HyBench workload inputs");
    }

    static void requireCompatible(CsvInput.Verified csv, Generation.Input input) throws IOException {
        var manifest = csv.manifest();
        require(CsvInput.benchmark(manifest).equals("hybench"), "HyBench preparation requires HyBench CSV input");
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

    private static void require(boolean valid, String message) throws IOException {
        if (!valid) throw new IOException(message);
    }
}
