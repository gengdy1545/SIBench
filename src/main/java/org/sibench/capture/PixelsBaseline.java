package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;
import static org.sibench.capture.InputFiles.number;
import com.google.gson.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.sibench.config.*;
import org.sibench.schema.SourceSchema;

/** A sealed COS columnar baseline, independent of PostgreSQL capture and mutable service state. */
public final class PixelsBaseline {
    private PixelsBaseline() {}
    record Verified(Path directory, JsonObject manifest, String sha256) {}

    /** Both consumers finish independently; one failure does not discard the other's published input. */
    public static Map<String, Object> prepareInputs(Path root, Path configuration, Path csvDirectory, Path baselineOutput) throws Exception {
        root = root.toRealPath();
        var plan = PixelsGeneration.plan(root, configuration, false);
        var csv = CsvInput.open(root.resolve(csvDirectory));
        CsvInput.requireCompatible(csv, plan.inputs().get(0));
        Path output = root.resolve(baselineOutput).toAbsolutePath().normalize(), cdcOutput = plan.inputs().get(0).output();
        require(!Files.exists(output), "Preparation outputs must be fresh");
        InputFiles.requireSeparate(output, cdcOutput, csv.directory());
        InputFiles.requireSeparate(cdcOutput, csv.directory());
        requireStorage(plan.engines().get(Engine.PIXELS).settings());
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        Path project = root;
        try {
            var input = plan.inputs().get(0);
            var cdc = pool.submit(() -> CdcPreparation.generate(plan, csv));
            var pixels = pool.submit(() -> prepare(project, configuration, csv, output, input.timeout()));
            var result = new LinkedHashMap<String, Object>();
            Exception failure = null;
            for (var entry : Map.of("cdc", cdc, "pixels_baseline", pixels).entrySet()) {
                try { result.put(entry.getKey(), entry.getValue().get()); }
                catch (java.util.concurrent.ExecutionException error) {
                    var cause = new IOException(entry.getKey() + " preparation failed; independently completed artifacts remain usable", error.getCause());
                    if (failure == null) failure = cause; else failure.addSuppressed(cause);
                }
            }
            if (failure != null) throw failure;
            result.put("csv_manifest_sha256", csv.sha256());
            return result;
        } finally { pool.shutdownNow(); }
    }

    public static Map<String, Object> prepare(Path root, Path configuration, Path csvDirectory, Path output) throws Exception {
        root = root.toRealPath(); output = root.resolve(output).toAbsolutePath().normalize();
        // Conversion reads and hashes every table CSV before any use; the descriptors suffice here.
        var csv = CsvInput.bind(root.resolve(csvDirectory));
        var profile = object(parse(Files.readString(Generation.hybenchSelection(root, SibenchConfig.read(root.resolve(configuration))).config())), "native");
        Generation.validateNative("hybench", profile);
        return prepare(root, configuration, csv, output, Duration.ofMinutes(positive(profile, "timeout_minutes")));
    }

    /** Preparation is bounded by the HyBench profile's native timeout, like the CDC branch, not by experiment settings. */
    private static Map<String, Object> prepare(Path root, Path configuration, CsvInput.Verified csv, Path output, Duration timeout) throws Exception {
        InputFiles.requireSeparate(output, csv.directory());
        var runtime = SibenchConfig.read(root.resolve(configuration));
        var settings = SibenchConfig.engine(root, runtime, Engine.PIXELS);
        requireStorage(settings);
        Path build = root.resolve(string(settings, "build_directory")).toRealPath();
        var identity = PixelsBuild.verify(root, build);
        var document = csv.manifest();
        var ddl = document.getAsJsonObject("source_ddl");
        var source = SourceSchema.captured(csv.directory().resolve(ddl.get("path").getAsString()),
                document.getAsJsonObject("source_evidence").getAsJsonObject("benchmark_source").get("commit").getAsString(), ddl.get("sha256").getAsString());
        Files.createDirectories(output.getParent()); Files.createDirectory(output);
        json(output.resolve("effective-config.json"), runtime);
        json(output.resolve("engine-build.json"), identity);
        long started = System.nanoTime();
        try {
            var expected = rows(document);
            Path prepared = output.resolve("load");
            var paths = tablePaths(csv);
            String spec = convert(csv, source, prepared, positive(object(settings, "load"), "rows_per_file"));
            String deployment = string(settings, "pixels_host");
            try (var host = deployment.equals("local") ? PixelsHost.local(build, output.resolve("pixels"), new Gson().toJsonTree(identity).getAsJsonObject())
                    : PixelsHost.Client.connect(deployment, PixelsExperiment.hostToken(), new Gson().toJsonTree(identity).getAsJsonObject())) {
                var pixels = host.call(Map.of("op", "start", "run", deployment.equals("local") ? "pixels" : "baseline-" + UUID.randomUUID(),
                        "overlay", object(settings, "properties"), "memory", object(settings, "memory"), "trace", false,
                        "data_storage", object(settings, "data_storage"), "retain_data", true), timeout);
                json(output.resolve("pixels-host.json"), Map.of("deployment", deployment, "directory", pixels.get("directory"),
                        "data_uri", pixels.get("data_uri"), "build", host.identity()));
                host.call(Map.of("op", "create", "schema", "postgres", "table", "baseline", "spec", targetSchema(spec)), timeout);
                for (var table : paths.keySet()) {
                    try (var files = Files.list(prepared.resolve(table))) {
                        for (Path file : files.sorted().toList()) { host.upload(table, file.getFileName().toString(), file, timeout); Files.delete(file); }
                    }
                    Files.delete(prepared.resolve(table));
                }
                Files.delete(prepared);
                var loaded = host.call(Map.of("op", "load", "schema", "postgres", "tables", List.copyOf(expected.keySet()),
                        "rows_per_file", positive(object(settings, "load"), "rows_per_file"), "threads", positive(object(settings, "load"), "threads")), timeout);
                var actual = new LinkedHashMap<String, Long>();
                loaded.getAsJsonObject("rows").entrySet().forEach(e -> actual.put(e.getKey(), e.getValue().getAsLong()));
                Generation.requireRows(expected, actual);
                var inventory = host.call(Map.of("op", "export-baseline", "tables", List.copyOf(expected.keySet())), timeout).getAsJsonObject("baseline");
                host.call(Map.of("op", "stop"), timeout);
                for (String field : List.of("schema", "source_ddl")) {
                    var descriptor = document.getAsJsonObject(field);
                    Files.copy(InputFiles.verified(csv.directory(), descriptor), output.resolve(descriptor.get("path").getAsString()));
                }
                var manifest = new JsonObject();
                manifest.addProperty("kind", "pixels_baseline_input"); manifest.addProperty("status", "complete");
                manifest.addProperty("csv_manifest_sha256", csv.sha256());
                manifest.add("schema", document.get("schema")); manifest.add("source_ddl", document.get("source_ddl"));
                manifest.add("baseline_rows", document.get("baseline_rows")); manifest.add("inventory", inventory);
                manifest.addProperty("data_uri", pixels.get("data_uri").getAsString());
                manifest.add("engine", new Gson().toJsonTree(identity));
                var profile = object(object(settings, "data_storage"), "profile");
                manifest.add("storage", new Gson().toJsonTree(Map.of("endpoint", string(profile, "endpoint"),
                        "region", string(profile, "region"), "bucket", string(profile, "bucket"))));
                manifest.add("source_evidence", document.get("source_evidence"));
                validate(output, manifest);
                json(output.resolve("manifest.json"), manifest);
                host.call(Map.of("op", "delete"), timeout);
                var result = Map.<String, Object>of("status", "complete", "seconds", (System.nanoTime() - started) / 1e9,
                        "manifest_sha256", Digests.sha256(output.resolve("manifest.json")), "data_uri", manifest.get("data_uri").getAsString());
                json(output.resolve("result.json"), result);
                return result;
            }
        } catch (Exception | Error failure) {
            var stack = new java.io.StringWriter(); failure.printStackTrace(new java.io.PrintWriter(stack));
            try { Files.writeString(output.resolve("failure.txt"), stack.toString(), StandardOpenOption.CREATE_NEW); }
            catch (Exception diagnostic) { failure.addSuppressed(diagnostic); }
            throw failure;
        }
    }

    private static Map<String, Path> tablePaths(CsvInput.Verified csv) {
        var paths = new LinkedHashMap<String, Path>();
        for (var item : csv.manifest().getAsJsonObject("baseline_files").entrySet()) {
            var file = item.getValue().getAsJsonObject();
            if (file.has("table")) paths.put(file.get("table").getAsString(), csv.directory().resolve(file.get("path").getAsString()));
        }
        return paths;
    }

    /**
     * Converts each table CSV once and compares the digests of exactly the bytes read with the sealed manifest.
     * A changed CSV fails here, before any Pixels service starts or any shard is uploaded. Returns the bound schema.
     */
    static String convert(CsvInput.Verified csv, SourceSchema source, Path prepared, long rowsPerFile) throws IOException {
        var document = csv.manifest();
        var inputs = PixelsInputs.prepare(source, tablePaths(csv), prepared, rowsPerFile);
        Generation.requireRows(rows(document), inputs.rows());
        for (var item : document.getAsJsonObject("baseline_files").entrySet()) {
            var file = item.getValue().getAsJsonObject();
            if (file.has("table")) require(file.get("sha256").getAsString().equals(inputs.sha256().get(file.get("table").getAsString())), "CSV changed during Pixels preparation");
        }
        String spec = Files.readString(csv.directory().resolve(document.getAsJsonObject("schema").get("path").getAsString()));
        require(spec.equals(inputs.schema().text()), "Pixels schema differs from sealed CSV");
        return spec;
    }

    static Verified open(Path directory) throws IOException {
        directory = directory.toRealPath();
        var manifest = InputFiles.manifest(directory);
        try {
            validate(directory, manifest.document());
            return new Verified(directory, manifest.document(), manifest.sha256());
        } catch (RuntimeException invalid) { throw new IOException("Invalid Pixels baseline manifest", invalid); }
    }

    public static Map<String, Object> check(Path directory) throws IOException {
        var baseline = open(directory);
        return Map.of("status", "passed", "manifest_sha256", baseline.sha256(), "baseline_rows", baseline.manifest().get("baseline_rows"),
                "scope", "manifest and local descriptors; remote files verified during import");
    }

    static void match(Verified baseline, JsonObject cdc) throws IOException {
        var document = baseline.manifest();
        var evidence = cdc.getAsJsonObject("source_evidence");
        require(evidence != null && document.get("csv_manifest_sha256").equals(evidence.get("csv_manifest_sha256")),
                "CDC and Pixels baseline must bind the same sealed CSV manifest");
        for (String field : List.of("schema", "source_ddl", "baseline_rows"))
            require(document.get(field).equals(cdc.get(field)), "CDC and Pixels baseline differ: " + field);
    }

    static void deployment(Verified baseline, Map<String, Object> settings, Map<String, Object> identity) throws IOException {
        var profile = object(object(settings, "data_storage"), "profile");
        var storage = baseline.manifest().getAsJsonObject("storage");
        for (String field : List.of("endpoint", "region", "bucket"))
            require(storage.get(field).getAsString().equals(string(profile, field)), "Baseline COS deployment differs: " + field);
        var current = new Gson().toJsonTree(identity).getAsJsonObject().getAsJsonObject("java");
        var original = baseline.manifest().getAsJsonObject("engine").getAsJsonObject("java");
        for (String field : List.of("pixels_commit", "pixels_patch_sha256"))
            require(current.get(field).equals(original.get(field)), "Baseline Pixels reader implementation differs: " + field);
    }

    private static void validate(Path directory, JsonObject document) throws IOException {
        require(document.get("kind").getAsString().equals("pixels_baseline_input") && document.get("status").getAsString().equals("complete"), "Unpublished Pixels baseline");
        require(document.get("csv_manifest_sha256").getAsString().matches("[a-f0-9]{64}"), "Invalid CSV identity");
        for (String field : List.of("schema", "source_ddl")) InputFiles.verified(directory, document.getAsJsonObject(field));
        var keyTypes = keyTypes(Files.readString(InputFiles.resolve(directory, document.getAsJsonObject("schema").get("path").getAsString())));
        var expectedTables = new HashSet<>(rows(document).keySet()); expectedTables.add("sibench_marker");
        require(keyTypes.keySet().equals(expectedTables), "Baseline schema/table inventory differs");
        var engine = document.getAsJsonObject("engine").getAsJsonObject("java");
        require(engine.get("pixels_commit").getAsString().matches("[a-f0-9]{40}")
                && engine.get("pixels_patch_sha256").getAsString().matches("[a-f0-9]{64}"), "Invalid baseline implementation identity");
        var storage = document.getAsJsonObject("storage");
        String base = document.get("data_uri").getAsString();
        require(base.matches("cos://[a-z0-9.-]+/[A-Za-z0-9_/-]+/data"), "Invalid baseline COS path");
        require(base.startsWith("cos://" + storage.get("bucket").getAsString() + "/"), "Baseline storage bucket differs");
        var endpoint = java.net.URI.create(storage.get("endpoint").getAsString());
        require(Set.of("http", "https").contains(endpoint.getScheme()) && endpoint.getHost() != null
                && endpoint.getUserInfo() == null && !storage.get("region").getAsString().isBlank(), "Invalid baseline storage endpoint");
        var inventory = document.getAsJsonObject("inventory");
        long maximum = number(inventory, "max_timestamp");
        require(maximum < Long.MAX_VALUE / 2 && number(inventory, "index_buckets") > 0, "Invalid baseline timestamp/buckets");
        var actual = new LinkedHashMap<String, Long>(); var uris = new HashSet<String>();
        long observedMax = 0;
        for (var item : inventory.getAsJsonArray("files")) {
            var file = item.getAsJsonObject(); String table = file.get("table").getAsString(), uri = file.get("uri").getAsString();
            require(table.matches("[a-z][a-z0-9_]*") && uri.startsWith(base + "/postgres/" + table + "/v-0-ordered/")
                    && uri.matches("cos://[a-z0-9.-]+/[A-Za-z0-9_./-]+") && !uri.contains("..") && uris.add(uri), "Invalid or duplicate baseline file path");
            require(file.get("sha256").getAsString().matches("[a-f0-9]{64}") && number(file, "bytes") > 0
                    && number(file, "row_groups") > 0 && number(file, "rows") > 0, "Invalid baseline file");
            actual.merge(table, number(file, "rows"), Math::addExact);
            observedMax = Math.max(observedMax, number(file, "max_timestamp"));
        }
        require(maximum == observedMax, "Baseline timestamp bound differs");
        Generation.requireRows(rows(document), actual);
    }

    static Map<String, Long> rows(JsonObject document) throws IOException {
        var rows = new LinkedHashMap<String, Long>();
        for (var item : document.getAsJsonObject("baseline_rows").entrySet()) rows.put(item.getKey(), number(document.getAsJsonObject("baseline_rows"), item.getKey()));
        require(!rows.isEmpty(), "Missing baseline tables"); return rows;
    }

    static Map<String, String> keyTypes(String spec) throws IOException {
        var types = new LinkedHashMap<String, String>();
        for (String line : spec.lines().toList()) {
            String[] fields = line.split("\t"); require(fields.length == 3, "Invalid Pixels schema");
            String type = Arrays.stream(fields[2].split(";")).filter(c -> c.startsWith(fields[1] + ":"))
                    .map(c -> c.substring(fields[1].length() + 1)).findFirst().orElseThrow(() -> new IOException("Missing primary key type"));
            require(types.put(fields[0], type) == null, "Duplicate Pixels table");
        }
        return types;
    }
    static String targetSchema(String spec) {
        return spec.lines().sorted(Comparator.comparing(line -> !line.startsWith("sibench_marker\t")))
                .collect(java.util.stream.Collectors.joining("\n", "", "\n"));
    }
    static void requireStorage(Map<String, Object> settings) throws IOException {
        require(settings.containsKey("data_storage"), "Configure Pixels data_storage for COS baseline preparation/import");
    }
    private static void require(boolean value, String message) throws IOException { if (!value) throw new IOException(message); }
    private static void json(Path path, Object value) throws IOException {
        Files.writeString(path, new GsonBuilder().setPrettyPrinting().create().toJson(value), StandardOpenOption.CREATE_NEW);
    }
}
