package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import org.junit.Test;
import org.sibench.config.ConfigValues;
import org.sibench.generation.GenerationConfig;
import org.sibench.schema.SourceSchema;

/** Opt-in against an existing PG service; creates and drops only its own disposable database. */
public class PgBaselineExportIntegrationIT {
    @Test
    public void actualPgExportUsesBytesAndIndependentVerificationRejectsChangedData() throws Exception {
        String admin = Objects.requireNonNull(System.getenv("SIBENCH_SCHEMA_TEST_JDBC"));
        Path evidence = Files.createDirectory(Path.of(System.getProperty("sibench.layout.evidence")));
        String database = "sibench_layout_" + UUID.randomUUID().toString().replace("-", "");
        String jdbc = admin.substring(0, admin.lastIndexOf('/') + 1) + database;
        Path root = Path.of("").toAbsolutePath();
        var document = ConfigValues.parse(Files.readString(root.resolve("src/test/resources/config/ycsb_1k.yaml")));
        var nativeConfig = ConfigValues.object(document, "native");
        nativeConfig.put("recordcount", 20000L);
        nativeConfig.put("insertcount", 20000L);
        document.put("native", nativeConfig);
        var pgConfig = ConfigValues.object(document, "postgres");
        pgConfig.put("database", database);
        int port = java.net.URI.create(admin.substring("jdbc:".length())).getPort();
        pgConfig.put("port", port < 0 ? 5432 : port);
        document.put("postgres", pgConfig);
        var original = NativeCaptureConfig.read(root, Path.of("src/test/resources/config/ycsb_1k.yaml")).layout();
        long target = 8L << 20;
        var config = new GenerationConfig(root, document, "ycsb", 71, target, 5,
                128, 65536, 1024, 1024, true, original.codec(), 3, 1, 0, evidence.resolve("baseline"), 1);
        String rangeProperty = "sibench.baseline.sort_range_bytes";
        String previousRange = System.getProperty(rangeProperty);
        try (var pg = DriverManager.getConnection(admin, "postgres", ""); var sql = pg.createStatement()) {
            sql.execute("CREATE DATABASE " + database + " TEMPLATE template0");
        }
        try {
            try (var pg = DriverManager.getConnection(jdbc, "postgres", ""); var sql = pg.createStatement()) {
                sql.execute("CREATE TABLE usertable(ycsb_key VARCHAR PRIMARY KEY," + String.join(",",
                        java.util.stream.IntStream.range(0, 10).mapToObj(i -> "field" + i + " TEXT").toList()) + ")");
                var random = new Random(71);
                try (var insert = pg.prepareStatement("INSERT INTO usertable VALUES (" + String.join(",", Collections.nCopies(11, "?")) + ")")) {
                    for (int i = 0; i < 20000; i++) {
                        insert.setString(1, "user" + String.format(Locale.ROOT, "%08d", (i * 7919) % 20000));
                        if (i == 1) insert.setString(1, "用户🙂");
                        if (i == 2) insert.setString(1, "user'\\range");
                        for (int c = 0; c < 10; c++) {
                            byte[] bytes = new byte[75]; random.nextBytes(bytes);
                            insert.setString(c + 2, Base64.getEncoder().encodeToString(bytes));
                        }
                        if (i == 0) {
                            insert.setString(2, null);
                            insert.setString(3, "");
                            insert.setString(4, "中文🙂\n\t\\");
                        }
                        insert.addBatch();
                        if (i % 1000 == 999) insert.executeBatch();
                    }
                }
                sql.execute("ANALYZE usertable");
            }
            var schema = SourceSchema.read(root, document);
            System.setProperty(rangeProperty, Long.toString(8L << 20));
            try (var pg = DriverManager.getConnection(jdbc, "postgres", "")) {
                assertFalse(PgBaselineExport.plan(pg, schema.tables().get(0), 8L << 20).boundaries().isEmpty());
            }
            PgBaselineExport.export(jdbc, config.output(), config);
            // Independent JDBC text decoding, same existing Parquet writer: distinguish COPY
            // parsing errors from layout changes, including NULL, empty and non-ASCII values.
            Path reference = Files.createDirectory(evidence.resolve("jdbc-reference"));
            List<PgBaselineExport.Written> written;
            try (var pg = DriverManager.getConnection(jdbc, "postgres", "")) {
                pg.setAutoCommit(false);
                var table = PgBaselineExport.plan(pg, schema.tables().get(0), Long.MAX_VALUE);
                try (var sql = pg.createStatement()) {
                    sql.setFetchSize(8192);
                    try (var rows = sql.executeQuery("SELECT * FROM usertable ORDER BY ycsb_key COLLATE \"C\"")) {
                        written = PgBaselineExport.writeTable(table, 0, 1, 1, () -> {
                            if (!rows.next()) return null;
                            var row = new String[11];
                            for (int i = 0; i < row.length; i++) row[i] = rows.getString(i + 1);
                            return row;
                        }, reference, config);
                    }
                }
            }
            Files.write(reference.resolve("files.jsonl"), written.stream().map(w -> new Gson().toJson(w.file())).toList());
            try (var paths = Files.list(config.output())) {
                for (Path path : paths.filter(p -> p.toString().endsWith(".parquet") || p.endsWith("files.jsonl")).toList())
                    assertEquals(path.toString(), -1L, Files.mismatch(path, reference.resolve(path.getFileName())));
            }
            PgBaselineVerify.verify(jdbc, "postgres", schema, config.output());
            var files = Files.readAllLines(config.output().resolve("files.jsonl"));
            assertTrue(files.size() > 1);
            long rows = 0;
            for (int i = 0; i < files.size(); i++) {
                var file = JsonParser.parseString(files.get(i)).getAsJsonObject();
                long size = Files.size(config.output().resolve(file.get("path").getAsString()));
                assertTrue(size <= target * 105 / 100);
                if (i + 1 < files.size()) assertTrue(size >= target);
                rows += file.get("rows").getAsLong();
            }
            assertEquals(20000, rows);
            Files.move(config.output().resolve("verified-manifest.json"), evidence.resolve("verified-original.json"));
            try (var pg = DriverManager.getConnection(jdbc, "postgres", ""); var sql = pg.createStatement()) {
                sql.execute("UPDATE usertable SET field0='deliberately wrong' WHERE ycsb_key='user00000000'");
            }
            var rejected = assertThrows(Exception.class, () -> PgBaselineVerify.verify(jdbc, "postgres", schema, config.output()));
            String causes = "";
            for (Throwable error = rejected; error != null; error = error.getCause()) causes += error.getMessage() + "\n";
            assertTrue(causes, causes.contains("Baseline rows differ from the source") || causes.contains("Baseline row mismatch"));
            Files.writeString(evidence.resolve("result.json"), "{\"status\":\"passed\",\"rows\":20000,\"copy_and_jdbc_byte_identical\":true,\"wrong_pg_value_rejected\":true}\n");
        } finally {
            if (previousRange == null) System.clearProperty(rangeProperty);
            else System.setProperty(rangeProperty, previousRange);
            try (var pg = DriverManager.getConnection(admin, "postgres", ""); var sql = pg.createStatement()) {
                sql.execute("DROP DATABASE " + database + " WITH (FORCE)");
            }
        }
    }
}
