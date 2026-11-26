package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;
import static org.sibench.capture.InputFiles.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.sibench.config.Digests;
import org.postgresql.PGConnection;

/** Baseline exported from the same fresh PostgreSQL database that subsequently produces CDC. */
final class SysbenchInputs {
    private SysbenchInputs() {}

    static void requireWorkload(Generation.Input input) {
        var n = input.nativeConfig();
        var workload = object(object(n, "workloads"), input.selection().workload());
        require(string(workload, "script").equals("oltp_read_write") && !bool(n, "skip-trx"),
                "Sysbench CDC requires transactional oltp_read_write");
        require(workload.containsKey("point-selects") && integer(workload, "point-selects") == 0
                && workload.containsKey("range-selects") && !bool(workload, "range-selects"),
                "Sysbench CDC requires explicit point-selects: 0 and range-selects: false");
    }

    static CsvInput.Verified export(Path output, Generation.Input input, Connection database) throws Exception {
        Files.createDirectory(output);
        Path data = Files.createDirectory(output.resolve("baseline"));
        var paths = new LinkedHashMap<String, Path>();
        var counts = new LinkedHashMap<String, Long>();
        var copy = database.unwrap(PGConnection.class).getCopyAPI();
        // No workload runs until this export is sealed; one snapshot binds every table.
        database.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        database.setAutoCommit(false);
        for (var table : input.source().tables()) {
            Path file = data.resolve(table.name() + ".csv");
            try (var stream = Files.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
                counts.put(table.name(), copy.copyOut("COPY " + table.name() + " (id,k,c,pad) TO STDOUT WITH (FORMAT CSV)", stream));
            }
            paths.put(table.name(), file);
        }
        database.commit();
        database.setAutoCommit(true);
        Generation.requireRows(input.baselineRows(), counts);
        Files.writeString(output.resolve("schema.sql"), ddl(counts.size()), StandardOpenOption.CREATE_NEW);
        var inspected = PixelsInputs.prepare(input.source(), paths, null, Long.MAX_VALUE);
        Generation.requireRows(counts, inspected.rows());
        Files.writeString(output.resolve("pixels-schema.tsv"), inspected.schema().text(), StandardOpenOption.CREATE_NEW);
        var document = manifest(output, input, inspected.sha256());
        CsvInput.validate(output, document, false);
        Files.writeString(output.resolve("manifest.json"), new GsonBuilder().setPrettyPrinting().create().toJson(document),
                StandardOpenOption.CREATE_NEW);
        return CsvInput.bind(output);
    }

    static JsonObject manifest(Path output, Generation.Input input, Map<String, String> hashes) throws IOException {
        var document = new JsonObject();
        document.addProperty("kind", "sysbench_csv_input");
        document.addProperty("status", "complete");
        document.add("baseline_rows", new Gson().toJsonTree(input.baselineRows()));
        document.add("schema", file(output, output.resolve("pixels-schema.tsv")));
        document.add("source_ddl", file(output, output.resolve("schema.sql")));
        var settings = new LinkedHashMap<String, Object>();
        for (String key : java.util.List.of("tables", "table-size", "auto-inc", "rand-seed")) settings.put(key, input.nativeConfig().get(key));
        document.add("data_parameters", new Gson().toJsonTree(settings));
        var files = new JsonObject();
        for (var table : input.source().tables()) {
            String name = table.name() + ".csv";
            Path path = output.resolve("baseline").resolve(name);
            var descriptor = new JsonObject();
            descriptor.addProperty("path", "baseline/" + name);
            descriptor.addProperty("bytes", Files.size(path));
            descriptor.addProperty("sha256", hashes.get(table.name()));
            descriptor.addProperty("table", table.name());
            descriptor.addProperty("rows", input.baselineRows().get(table.name()));
            files.add(name, descriptor);
        }
        document.add("baseline_files", files);
        Path core = NativeProcess.self();
        document.add("source_evidence", new Gson().toJsonTree(Map.of("benchmark", "sysbench",
                "benchmark_source", Map.of("commit", input.source().sourceCommit(), "files", input.source().fingerprints()),
                "client", input.buildIdentity(), "core_sha256", Files.isDirectory(core) ? PixelsBuild.treeSha256(core) : Digests.sha256(core))));
        Files.writeString(output.resolve("effective-config.json"), new GsonBuilder().setPrettyPrinting().create()
                .toJson(Map.of("benchmark", input.document())), StandardOpenOption.CREATE_NEW);
        return document;
    }

    static void validate(Path root, JsonObject document) throws IOException {
        var n = parse(document.get("data_parameters").toString());
        require(n.keySet().equals(java.util.Set.of("tables", "table-size", "auto-inc", "rand-seed")), "Invalid Sysbench baseline parameter keys");
        long tables = positive(n, "tables"), rows = positive(n, "table-size");
        require(tables <= 10000 && integer(n, "rand-seed") >= 0, "Invalid Sysbench baseline parameters");
        bool(n, "auto-inc");
        var files = document.getAsJsonObject("baseline_files");
        require(files.size() == tables, "Sysbench baseline table count differs");
        for (int i = 1; i <= tables; i++) {
            String table = "sbtest" + i;
            var file = files.getAsJsonObject(table + ".csv");
            require(file != null && file.get("table").getAsString().equals(table)
                    && file.get("path").getAsString().equals("baseline/" + table + ".csv")
                    && number(file, "rows") == rows, "Sysbench baseline table descriptor differs");
        }
        require(CsvInput.benchmark(document).equals("sysbench"), "Sysbench baseline source identity differs");
        require(Files.readString(verified(root, document.getAsJsonObject("source_ddl"))).equals(ddl(Math.toIntExact(tables))),
                "Sysbench baseline DDL differs");
    }

    /** Portable row layout for Pixels; source defaults and secondary indexes remain owned by upstream prepare. */
    static String ddl(int tables) {
        var ddl = new StringBuilder();
        for (int i = 1; i <= tables; i++) ddl.append("CREATE TABLE sbtest").append(i)
                .append(" (id integer PRIMARY KEY, k integer NOT NULL, c char(120) NOT NULL, pad char(60) NOT NULL);\n");
        return ddl.toString();
    }
}
