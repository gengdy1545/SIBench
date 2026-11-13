package org.sibench.capture;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

/**
 * HyBench IQ/AP statements rendered with literal parameters, so the same SQL text runs on Trino and
 * on an independent PostgreSQL oracle. Parameters follow HyBench's own ID domains; the blocked-account
 * sampling of IQ-1/IQ-2 and the random-row helpers are replaced by seeded draws from the same domains.
 */
public final class HybenchQueries {
    private HybenchQueries() {}

    /** Customers are 1..customer_number; companies follow them, as in HyBench's ID allocation. */
    record Domains(long customers, long companies) {}

    /** {@code orderColumn} is set for ORDER BY ... LIMIT statements, whose tied rows may legitimately differ. */
    record Template(String id, List<Parameter> parameters, Integer orderColumn) {}

    enum Parameter { CUSTOMER, COMPANY, LOAN_APPLICANT }

    /**
     * Statements whose results are fully determined by the data and the parameters. Excluded: IQ-6 and
     * AP-8/AP-10 (LIMIT inside a subquery selects among ties), AP-9/AP-10 (PostgreSQL casts and date
     * arithmetic need translation), AP-11 (its answer can list every investing customer and company,
     * too large to keep as an expected result), AP-12 (its 100.0 literal makes the rate a PostgreSQL
     * numeric but a one-decimal Trino DECIMAL, so the same text has no common answer) and the
     * random-row helpers AP-2.1/IQ-5.1.
     */
    static final List<Template> TEMPLATES = List.of(
            new Template("IQ-1", List.of(Parameter.CUSTOMER, Parameter.CUSTOMER), 0),
            new Template("IQ-2", List.of(Parameter.COMPANY, Parameter.COMPANY), 0),
            new Template("IQ-3", List.of(Parameter.COMPANY), null),
            new Template("IQ-4", List.of(Parameter.COMPANY, Parameter.COMPANY), null),
            new Template("IQ-5", List.of(Parameter.LOAN_APPLICANT), null),
            new Template("AP-1", List.of(Parameter.CUSTOMER, Parameter.CUSTOMER, Parameter.CUSTOMER, Parameter.CUSTOMER), null),
            new Template("AP-2", List.of(Parameter.CUSTOMER), null),
            new Template("AP-3", List.of(Parameter.CUSTOMER, Parameter.CUSTOMER), null),
            new Template("AP-4", List.of(Parameter.CUSTOMER, Parameter.CUSTOMER), null),
            new Template("AP-5", List.of(Parameter.COMPANY), null),
            new Template("AP-6", List.of(), null),
            new Template("AP-7", List.of(Parameter.COMPANY, Parameter.COMPANY, Parameter.COMPANY, Parameter.COMPANY), 3),
            new Template("AP-13", List.of(Parameter.CUSTOMER), null));

    /** Replaces each {@code ?} with a literal; strings are quoted for templates that take them. */
    static String render(String template, List<Object> values) throws IOException {
        var sql = new StringBuilder();
        int next = 0;
        for (char c : template.strip().replaceFirst(";\\s*$", "").toCharArray()) {
            if (c != '?') { sql.append(c); continue; }
            if (next == values.size()) throw new IOException("Statement has more parameters than declared");
            Object value = values.get(next++);
            if (value instanceof Long number) sql.append(number);
            else sql.append('\'').append(value.toString().replace("'", "''")).append('\'');
        }
        if (next != values.size()) throw new IOException("Statement has fewer parameters than declared");
        return sql.toString();
    }

    /** One draw per occurrence group: every placeholder of one kind in a statement receives the same value. */
    static List<Object> draw(Template template, Domains domains, Random random, Function<Parameter, Object> fromData) {
        var chosen = new java.util.EnumMap<Parameter, Object>(Parameter.class);
        var values = new ArrayList<Object>();
        for (Parameter parameter : template.parameters()) values.add(chosen.computeIfAbsent(parameter, kind -> switch (kind) {
            case CUSTOMER -> 1 + (long) random.nextInt(Math.toIntExact(domains.customers()));
            case COMPANY -> domains.customers() + 1 + random.nextInt(Math.toIntExact(domains.companies()));
            case LOAN_APPLICANT -> fromData.apply(kind);
        }));
        return values;
    }

    /**
     * Comparable form: numbers to 5 significant digits (PostgreSQL and Trino sum and average REAL
     * columns in different orders and precisions), timestamps without trailing fractional zeros, CHAR
     * without its padding. Results with fewer than 5 matching digits are reported as different.
     */
    static JsonElement canonical(Object value) {
        if (value == null || value instanceof JsonNull) return JsonNull.INSTANCE;
        if (value instanceof JsonPrimitive primitive) value = primitive.isNumber() ? primitive.getAsBigDecimal()
                : primitive.isBoolean() ? primitive.getAsBoolean() : primitive.getAsString();
        if (value instanceof Number number) {
            var decimal = new BigDecimal(number.toString()).round(new MathContext(5)).stripTrailingZeros();
            return new JsonPrimitive(decimal.signum() == 0 ? "0" : decimal.toPlainString());
        }
        if (value instanceof Boolean flag) return new JsonPrimitive(flag ? "1" : "0");
        String text = value.toString().replaceFirst(" +$", "");
        var timestamp = java.util.regex.Pattern.compile("(\\d{4}-\\d\\d-\\d\\d[ T]\\d\\d:\\d\\d:\\d\\d)(\\.\\d*?)0*").matcher(text);
        if (timestamp.matches()) text = timestamp.group(1).replace('T', ' ') + (timestamp.group(2).equals(".") ? "" : timestamp.group(2));
        return new JsonPrimitive(text);
    }

    static JsonArray canonicalRow(Iterable<?> row) {
        var result = new JsonArray();
        for (Object value : row) result.add(canonical(value));
        return result;
    }

    /** The expected answer: every row as a multiset, or, under ORDER BY ... LIMIT, the ordered key values. */
    static JsonArray expected(List<JsonArray> rows, Integer orderColumn) {
        var result = new JsonArray();
        if (orderColumn == null) rows.stream().sorted(Comparator.comparing(JsonArray::toString)).forEach(result::add);
        else rows.forEach(row -> result.add(row.get(orderColumn)));
        return result;
    }

    /** Null when the actual rows match the expected answer, otherwise the first difference. */
    static String difference(JsonArray expected, List<JsonArray> actual, Integer orderColumn) {
        var answer = expected(actual.stream().map(HybenchQueries::canonicalRow).toList(), orderColumn);
        if (answer.size() != expected.size()) return "expected " + expected.size() + " rows, got " + answer.size();
        for (int i = 0; i < answer.size(); i++)
            if (!answer.get(i).equals(expected.get(i))) return "row " + i + ": expected " + expected.get(i) + ", got " + answer.get(i);
        return null;
    }

    static List<JsonArray> query(Connection database, String sql) throws java.sql.SQLException {
        var rows = new ArrayList<JsonArray>();
        try (var statement = database.createStatement(); var result = statement.executeQuery(sql)) {
            int columns = result.getMetaData().getColumnCount();
            while (result.next()) {
                var row = new ArrayList<Object>();
                for (int i = 1; i <= columns; i++) row.add(result.getObject(i));
                rows.add(canonicalRow(row));
            }
        }
        return rows;
    }

    record Rendered(java.nio.file.Path directory, JsonObject manifest) {}

    /**
     * Renders the statements for one sealed CSV and records their answers on a fresh, uninstrumented
     * PostgreSQL loaded from that CSV. manifest.json is published last; the database is then discarded.
     */
    public static Map<String, Object> prepare(java.nio.file.Path root, java.nio.file.Path configuration,
            java.nio.file.Path csvDirectory, java.nio.file.Path output, long seed, int instances) throws Exception {
        if (instances < 1 || instances > 1000) throw new IllegalArgumentException("Query instances must be between 1 and 1000");
        root = root.toRealPath();
        var runtime = org.sibench.config.SibenchConfig.read(root.resolve(configuration));
        var input = Generation.inputs(root, runtime, List.of(Generation.hybenchSelection(root, runtime)), Map.of(),
                root.resolve(output)).inputs().get(0);
        var settings = org.sibench.config.SibenchConfig.cdcSource(root, runtime);
        var build = NativeBuildCheck.engineDirectory(root, settings, "pixels").toRealPath();
        var engine = PixelsBuild.verifySource(root, build);
        // PostgreSQL COPY reads the table files itself, so their contents are verified before loading.
        var csv = CsvInput.open(root.resolve(csvDirectory));
        CsvInput.requireCompatible(csv, input);
        var directory = input.output();
        InputFiles.requireSeparate(directory, csv.directory());
        var templateFile = input.build().resolve("conf/stmt_postgres.toml");
        var templates = new com.moandjiezana.toml.Toml().read(templateFile.toFile());
        var parameters = input.parameters();
        var domains = new Domains(((Number) parameters.get("customer_number")).longValue(),
                ((Number) parameters.get("company_number")).longValue());
        java.nio.file.Files.createDirectories(directory.getParent());
        java.nio.file.Files.createDirectory(directory);
        var sourceSettings = org.sibench.config.SibenchConfig.postgresSettings(
                org.sibench.config.ConfigValues.object(org.sibench.config.ConfigValues.object(settings, "source"), "settings"));
        try (var database = org.sibench.generation.PostgresDatabase.create(build.resolve("postgres/runtime"),
                directory.resolve("database"), sourceSettings)) {
            var queries = new JsonArray();
            try (var connection = database.connect("oracle")) {
                Generation.requireRows(input.baselineRows(), Hybench.load(connection, input.source().source(),
                        csv.directory().resolve("native/Data_" + csv.manifest().get("sf").getAsString())));
                var applicants = values(connection, "SELECT DISTINCT applicantid FROM loantrans ORDER BY 1");
                var random = new Random(seed);
                int number = 0;
                for (var template : TEMPLATES) for (int k = 0; k < instances; k++) {
                    var values = draw(template, domains, random, kind -> {
                        if (applicants.isEmpty()) throw new IllegalStateException("No loan applicants in the sealed CSV");
                        return applicants.get(random.nextInt(applicants.size()));
                    });
                    String sql = render(templates.getString(template.id() + ".sql"), values);
                    String file = String.format("%03d-%s-%d.sql", ++number, template.id(), k);
                    java.nio.file.Files.writeString(directory.resolve(file), sql + "\n", java.nio.file.StandardOpenOption.CREATE_NEW);
                    var rows = query(connection, sql);
                    var entry = InputFiles.file(directory, directory.resolve(file));
                    entry.addProperty("template", template.id());
                    entry.add("parameters", new com.google.gson.Gson().toJsonTree(values));
                    if (template.orderColumn() != null) entry.addProperty("order_column", template.orderColumn());
                    entry.addProperty("rows", rows.size());
                    entry.add("expected", expected(rows, template.orderColumn()));
                    queries.add(entry);
                }
            }
            database.close();
            var manifest = new JsonObject();
            manifest.addProperty("kind", "hybench_query_set");
            manifest.addProperty("status", "complete");
            manifest.addProperty("csv_manifest_sha256", csv.sha256());
            manifest.addProperty("templates_sha256", org.sibench.config.Digests.sha256(templateFile));
            manifest.addProperty("seed", seed);
            manifest.addProperty("instances", instances);
            manifest.add("client", new com.google.gson.Gson().toJsonTree(input.buildIdentity()));
            manifest.add("oracle_engine", new com.google.gson.Gson().toJsonTree(engine));
            manifest.addProperty("scope", "Answers describe the sealed CSV baseline only; experiments compare them only without updates");
            manifest.add("queries", queries);
            java.nio.file.Files.writeString(directory.resolve("manifest.json"), text(new com.google.gson.Gson().fromJson(manifest, Map.class)),
                    java.nio.file.StandardOpenOption.CREATE_NEW);
            // Published answers no longer need their working database.
            Generation.deleteTree(directory.resolve("database"));
            return Map.of("status", "complete", "directory", directory.toString(), "queries", queries.size(),
                    "manifest_sha256", InputFiles.manifest(directory).sha256());
        }
    }

    /** A rendered set binds every SQL file of its directory by digest; other directories are plain SQL lists. */
    static Rendered open(java.nio.file.Path directory) throws IOException {
        if (!java.nio.file.Files.exists(directory.resolve("manifest.json"))) return null;
        var manifest = InputFiles.manifest(directory).document();
        try {
            if (!manifest.get("kind").getAsString().equals("hybench_query_set") || !manifest.get("status").getAsString().equals("complete"))
                throw new IOException("Unpublished or unsupported query set");
            var listed = new java.util.TreeSet<String>();
            for (var item : manifest.getAsJsonArray("queries")) {
                var query = item.getAsJsonObject();
                if (!listed.add(InputFiles.verified(directory, query).getFileName().toString()) || !query.has("expected"))
                    throw new IOException("Invalid query set entry");
            }
            try (var files = java.nio.file.Files.list(directory)) {
                var present = new java.util.TreeSet<>(files.map(path -> path.getFileName().toString()).filter(name -> name.endsWith(".sql")).toList());
                if (!present.equals(listed)) throw new IOException("Query directory has unlisted or missing SQL files");
            }
        } catch (RuntimeException malformed) { throw new IOException("Invalid query set: " + directory, malformed); }
        return new Rendered(directory, manifest);
    }

    /** The answer recorded for one file of the set, with its comparison column under ORDER BY ... LIMIT. */
    static JsonObject entry(Rendered set, String file) throws IOException {
        for (var item : set.manifest().getAsJsonArray("queries"))
            if (item.getAsJsonObject().get("path").getAsString().equals(file)) return item.getAsJsonObject();
        throw new IOException("Query outside the rendered set: " + file);
    }

    private static List<Object> values(Connection database, String sql) throws java.sql.SQLException {
        var values = new ArrayList<Object>();
        try (var statement = database.createStatement(); var result = statement.executeQuery(sql)) {
            while (result.next()) values.add(result.getLong(1));
        }
        return values;
    }

    static String text(Map<?, ?> value) {
        return new GsonBuilder().setPrettyPrinting().create().toJson(value);
    }
}
