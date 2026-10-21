package org.sibench.schema;

import static org.junit.Assert.*;

import com.google.gson.JsonParser;

import org.junit.Test;
import org.sibench.capture.CaptureConfig;
import org.sibench.capture.HybenchNativeRun;
import org.sibench.capture.PgBaselineExport;
import org.sibench.cli.Sibench;
import org.sibench.config.ConfigValues;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Explicit opt-in: -Dtest=PgSchemaIntegrationIT with SIBENCH_SCHEMA_TEST_JDBC pointing to a
 * disposable-capable PG service. Never changes the configured SF1 database.
 */
public class PgSchemaIntegrationIT {
    private final Path root = Path.of("").toAbsolutePath();
    private final String user =
            System.getenv().getOrDefault("SIBENCH_SCHEMA_TEST_USER", "postgres");
    private final String password = System.getenv().getOrDefault("PGPASSWORD", "");
    private final Path evidence =
            Path.of(
                    System.getProperty(
                            "sibench.schema.evidence", "build/sibench/schema-preflight"));
    private final List<Map<String, Object>> checks = new ArrayList<>();

    private record Mutation(String name, String sql, String diagnostic) {}

    @Test
    public void realCatalogAndCaptureBoundaries() throws Exception {
        String adminJdbc = System.getenv("SIBENCH_SCHEMA_TEST_JDBC");
        assertNotNull("Explicit integration run requires SIBENCH_SCHEMA_TEST_JDBC", adminJdbc);
        assertTrue(adminJdbc.startsWith("jdbc:postgresql://"));
        Files.createDirectories(evidence);
        Path summary = evidence.resolve("integration-summary.json");
        assertFalse("Use fresh evidence directory", Files.exists(summary));
        String sf1 = System.getenv("SIBENCH_SF1_JDBC");
        if (sf1 != null) {
            var report =
                    PgSchemaCheck.check(
                            root, SourceSchemaTest.config("config/hybench_sf1.yaml"), sf1, user);
            save("hybench-sf1", report);
            assertEquals(8, ((List<?>) report.get("actual_tables")).size());
            assertEquals(
                    21,
                    SourceSchemaTest.read("config/hybench_sf1.yaml").tables().stream()
                            .mapToInt(t -> t.indexes().size())
                            .sum());
        }
        try (Database hybench = new Database(adminJdbc, "hybench");
                Database ycsb = new Database(adminJdbc, "ycsb");
                Database sysbench = new Database(adminJdbc, "sysbench")) {
            var hyConfig = SourceSchemaTest.config("config/hybench_sf1.yaml");
            SourceSchema hySchema = SourceSchema.read(root, hyConfig);
            hybenchFixture(hybench);
            verified("hybench-source-ddl", hybench, hySchema);
            hybench.execute("ALTER SCHEMA public RENAME TO bench");
            verified("hybench-nonpublic-schema", hybench, hySchema, "bench");
            hybench.execute("ALTER SCHEMA bench RENAME TO public");
            for (Mutation mutation : mutations()) {
                hybench.execute(mutation.sql());
                rejected(mutation.name(), hybench, hySchema, mutation.diagnostic(), null);
                hybenchFixture(hybench);
            }
            hybench.execute("CREATE SCHEMA shadow; CREATE TABLE shadow.customer(id integer)");
            verified("unrelated-schema", hybench, hySchema);
            rejected(
                    "search-path-shadow",
                    hybench,
                    hySchema,
                    "unqualified SQL resolves outside public",
                    "shadow, public");
            hybench.execute("DROP SCHEMA shadow CASCADE");
            configuredEnvironment(hybench, hyConfig);
            captureAndCliBoundaries(hybench, hyConfig);

            var ycConfig = SourceSchemaTest.config("config/ycsb_large.yaml");
            ycsbFixture(ycsb, ycConfig);
            verified("ycsb-ten-text-fields", ycsb, SourceSchema.read(root, ycConfig));
            rejected(
                    "ycsb-unexpected-secondary",
                    ycsb,
                    SourceSchema.read(root, ycConfig),
                    "unexpected indexes",
                    null,
                    "CREATE INDEX field_index ON usertable(field0)");
            var ycNative = ConfigValues.object(ycConfig, "native");
            ycNative.put("fieldcount", 3);
            ycConfig.put("native", ycNative);
            ycsbFixture(ycsb, ycConfig);
            verified("ycsb-configured-three-fields", ycsb, SourceSchema.read(root, ycConfig));

            var sbConfig = SourceSchemaTest.config("config/sysbench_large.yaml");
            sysbenchFixture(sysbench, sbConfig);
            verified("sysbench-hundred-tables-serial", sysbench, SourceSchema.read(root, sbConfig));
            rejected(
                    "sysbench-missing-last-secondary",
                    sysbench,
                    SourceSchema.read(root, sbConfig),
                    "missing index k_100",
                    null,
                    "DROP INDEX k_100");
            sysbench.execute("CREATE INDEX k_100 ON sbtest100(k)");
            rejected(
                    "sysbench-wrong-default",
                    sysbench,
                    SourceSchema.read(root, sbConfig),
                    "default/sequence",
                    null,
                    "ALTER TABLE sbtest100 ALTER COLUMN k SET DEFAULT 1");
            var sbNative = ConfigValues.object(sbConfig, "native");
            sbNative.put("tables", 2);
            sbNative.put("auto-inc", false);
            sbConfig.put("native", sbNative);
            sysbenchFixture(sysbench, sbConfig);
            verified(
                    "sysbench-configured-two-tables-no-serial",
                    sysbench,
                    SourceSchema.read(root, sbConfig));
        }
        Files.writeString(
                summary,
                PgSchemaCheck.json(
                        Map.of(
                                "status",
                                "passed",
                                "sf1_checked",
                                sf1 != null,
                                "fixtures",
                                "HyBench pinned DDL; independently specified YCSB/sysbench empty"
                                    + " layouts",
                                "native_ycsb_sysbench_execution",
                                false,
                                "checks",
                                checks,
                                "temporary_databases",
                                "created by this test and dropped after verification")),
                StandardOpenOption.CREATE_NEW);
        System.out.println("Real PG preflight checks passed: " + checks.size());
    }

    private List<Mutation> mutations() {
        return List.of(
                new Mutation(
                        "missing-table", "DROP TABLE checkingaccount CASCADE", "Missing table"),
                new Mutation(
                        "extra-table", "CREATE TABLE extra_table(id integer)", "Unexpected tables"),
                new Mutation(
                        "quoted-extra-table",
                        "CREATE TABLE \"Extra \"\"Table\"(id SERIAL)",
                        "Unexpected tables"),
                new Mutation(
                        "extra-view",
                        "CREATE VIEW extra_view AS SELECT id FROM transfer",
                        "Unexpected tables/views"),
                new Mutation(
                        "missing-column",
                        "ALTER TABLE customer DROP COLUMN age",
                        "column order/name"),
                new Mutation(
                        "extra-column",
                        "ALTER TABLE customer ADD COLUMN extra integer",
                        "column count"),
                new Mutation(
                        "wrong-column-type",
                        "ALTER TABLE customer ALTER COLUMN age TYPE bigint",
                        "type expected"),
                new Mutation(
                        "wrong-nullability",
                        "ALTER TABLE customer ALTER COLUMN age SET NOT NULL",
                        "nullability"),
                new Mutation(
                        "wrong-default",
                        "ALTER TABLE customer ALTER COLUMN age SET DEFAULT 1",
                        "default/sequence"),
                new Mutation(
                        "sequence-unowned",
                        "ALTER SEQUENCE transfer_id_seq OWNED BY NONE",
                        "default/sequence"),
                new Mutation(
                        "wrong-sequence-reference",
                        "ALTER TABLE transfer ALTER COLUMN id SET DEFAULT"
                            + " nextval('checking_id_seq')",
                        "default/sequence"),
                new Mutation(
                        "generated-column",
                        "ALTER TABLE customer ADD COLUMN extra integer GENERATED ALWAYS AS (age+1)"
                            + " STORED",
                        "column count"),
                new Mutation(
                        "identity-column",
                        "ALTER TABLE customer ALTER COLUMN age SET NOT NULL; ALTER TABLE customer"
                            + " ALTER COLUMN age ADD GENERATED ALWAYS AS IDENTITY",
                        "identity/generated"),
                new Mutation(
                        "dropped-column-slot",
                        "ALTER TABLE customer ADD COLUMN extra integer;"
                                + " ALTER TABLE customer DROP COLUMN extra",
                        "column count"),
                new Mutation(
                        "extra-constraint",
                        "ALTER TABLE customer ADD CONSTRAINT age_check CHECK(age>0)",
                        "unexpected constraints"),
                new Mutation(
                        "unlogged-table",
                        "ALTER TABLE customer SET UNLOGGED",
                        "ordinary persistent"),
                new Mutation(
                        "row-security",
                        "ALTER TABLE customer ENABLE ROW LEVEL SECURITY",
                        "inheritance/RLS"),
                new Mutation("missing-index", "DROP INDEX idx_transfer_1", "missing index"),
                new Mutation("wrong-index-column", replaceIndex("(targetid)"), "index definition"),
                new Mutation(
                        "unique-secondary",
                        "DROP INDEX idx_transfer_1;"
                                + " CREATE UNIQUE INDEX idx_transfer_1 ON transfer(sourceid)",
                        "index definition"),
                new Mutation(
                        "compound-index", replaceIndex("(sourceid,targetid)"), "index definition"),
                new Mutation(
                        "partial-index",
                        replaceIndex("(sourceid) WHERE sourceid>0"),
                        "index definition"),
                new Mutation(
                        "expression-index", replaceIndex("((sourceid+1))"), "index definition"),
                new Mutation(
                        "include-index",
                        replaceIndex("(sourceid) INCLUDE(targetid)"),
                        "index definition"),
                new Mutation(
                        "hash-index", replaceIndex("USING hash(sourceid)"), "index definition"),
                new Mutation(
                        "descending-index", replaceIndex("(sourceid DESC)"), "index definition"),
                new Mutation(
                        "extra-index",
                        "CREATE INDEX extra_index ON customer(age)",
                        "unexpected indexes"),
                new Mutation(
                        "custom-column-collation",
                        "ALTER TABLE company ALTER COLUMN category TYPE varchar COLLATE \"C\"",
                        "collation expected"),
                new Mutation(
                        "custom-index-collation",
                        "DROP INDEX idx_company_1;"
                                + " CREATE INDEX idx_company_1 ON company(category COLLATE \"C\")",
                        "index definition"),
                new Mutation(
                        "nondefault-operator-class",
                        "DROP INDEX idx_company_1; CREATE INDEX idx_company_1 ON company(category"
                            + " varchar_pattern_ops)",
                        "index definition"));
    }

    private static String replaceIndex(String definition) {
        return "DROP INDEX idx_transfer_1; CREATE INDEX idx_transfer_1 ON transfer " + definition;
    }

    private void configuredEnvironment(Database db, Map<String, Object> config) throws Exception {
        var pg = ConfigValues.object(config, "postgres");
        pg.put("database", db.name);
        pg.put("user", user);
        try (Connection connection = db.connect();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT current_setting('port')")) {
            assertTrue(rows.next());
            pg.put("port", Integer.parseInt(rows.getString(1)));
            PgSchemaCheck.checkConfiguredEnvironment(connection, pg);
        }
        config.put("postgres", pg);
        for (String field :
                List.of(
                        "database",
                        "user",
                        "version",
                        "port",
                        "isolation",
                        "max_parallel_workers_per_gather")) {
            var invalid = new LinkedHashMap<>(pg);
            invalid.put(field, "wrong");
            try (Connection connection = db.connect()) {
                var error =
                        assertThrows(
                                java.io.IOException.class,
                                () ->
                                        PgSchemaCheck.checkConfiguredEnvironment(
                                                connection, invalid));
                assertTrue(error.getMessage(), error.getMessage().contains(field));
                checks.add(
                        Map.of(
                                "name",
                                "environment-" + field,
                                "status",
                                "rejected",
                                "diagnostic",
                                error.getMessage()));
            }
        }
    }

    private void captureAndCliBoundaries(Database db, Map<String, Object> config) throws Exception {
        String id = UUID.randomUUID().toString().replace("-", "");
        Path artifacts = Path.of("results/poc/schema-preflight-integration-" + id);
        var capture = ConfigValues.object(config, "capture");
        capture.put("raw_directory", artifacts.resolve("raw").toString());
        config.put("capture", capture);
        config.put("artifacts", Map.of("directory", artifacts.toString()));
        Path profile = evidence.resolve("hybench-test-profile.yaml").toAbsolutePath();
        Files.writeString(profile, new Yaml().dump(config), StandardOpenOption.CREATE_NEW);
        cli(db, profile, 0, "schema_verified", "cli-valid");
        db.execute("DROP INDEX idx_transfer_1");
        cli(db, profile, 1, "schema_mismatch", "cli-invalid");
        CaptureConfig resolved = CaptureConfig.read(root, profile);
        Path output = evidence.resolve("rejected-baseline");
        assertThrows(
                PgSchemaCheck.Mismatch.class,
                () -> PgBaselineExport.export(db.jdbc, output, resolved.layout()));
        assertFalse("Preflight must precede baseline directory creation", Files.exists(output));
        assertThrows(
                PgSchemaCheck.Mismatch.class,
                () -> HybenchNativeRun.main(new String[] {root.toString(), profile.toString()}));
        var manifest =
                JsonParser.parseString(Files.readString(resolved.raw().resolve("capture-run.json")))
                        .getAsJsonObject();
        assertEquals("failed", manifest.get("status").getAsString());
        assertEquals(
                "schema_mismatch",
                manifest.getAsJsonObject("schema_preflight").get("status").getAsString());
        assertFalse(manifest.has("sessions"));
        try (var files = Files.list(resolved.raw())) {
            assertEquals(1, files.count());
        }
        assertFalse(Files.exists(resolved.artifacts().resolve("native/capture-resolved.props")));
        Files.writeString(
                evidence.resolve("rejected-capture-run.json"),
                manifest.toString(),
                StandardOpenOption.CREATE_NEW);
        checks.add(
                Map.of(
                        "name",
                        "baseline-and-native-run-boundaries",
                        "status",
                        "rejected",
                        "baseline_output_created",
                        false,
                        "recording_sessions",
                        0,
                        "capture_manifest",
                        root.resolve(artifacts).resolve("raw/capture-run.json").toString()));
        db.execute("CREATE INDEX idx_transfer_1 ON transfer(sourceid)");
    }

    private void cli(Database db, Path profile, int exit, String status, String name)
            throws Exception {
        var output = new ByteArrayOutputStream();
        var errors = new ByteArrayOutputStream();
        int actual;
        try (var out = new PrintStream(output);
                var err = new PrintStream(errors)) {
            actual =
                    Sibench.run(
                            new String[] {
                                "check-schema",
                                profile.toString(),
                                "--jdbc",
                                db.jdbc,
                                "--user",
                                user
                            },
                            out,
                            err);
        }
        assertEquals(errors.toString(), exit, actual);
        assertEquals(
                status,
                JsonParser.parseString(output.toString())
                        .getAsJsonObject()
                        .get("status")
                        .getAsString());
        Files.writeString(
                evidence.resolve(name + ".json"), output.toString(), StandardOpenOption.CREATE_NEW);
        checks.add(Map.of("name", name, "exit_code", actual, "status", status));
    }

    private void verified(String name, Database db, SourceSchema schema) throws Exception {
        verified(name, db, schema, "public");
    }

    private void verified(String name, Database db, SourceSchema schema, String namespace)
            throws Exception {
        try (Connection connection = db.connect()) {
            try (var statement = connection.createStatement()) {
                statement.execute("SET search_path TO " + namespace);
            }
            var report = PgSchemaCheck.check(connection, schema, namespace);
            save(name, report);
            assertTrue(connection.getAutoCommit());
            assertFalse(connection.isReadOnly());
        }
    }

    private void rejected(
            String name,
            Database db,
            SourceSchema schema,
            String diagnostic,
            String searchPath,
            String... mutation)
            throws Exception {
        for (String sql : mutation) db.execute(sql);
        try (Connection connection = db.connect()) {
            if (searchPath != null)
                try (var statement = connection.createStatement()) {
                    statement.execute("SET search_path TO " + searchPath);
                }
            var mismatch =
                    assertThrows(
                            PgSchemaCheck.Mismatch.class,
                            () -> PgSchemaCheck.check(connection, schema, "public"));
            assertTrue(mismatch.getMessage(), mismatch.getMessage().contains(diagnostic));
            save(name, mismatch.report());
            assertTrue(connection.getAutoCommit());
            assertFalse(connection.isReadOnly());
        }
    }

    private void save(String name, Map<String, Object> report) throws Exception {
        Files.writeString(
                evidence.resolve(name + ".json"),
                PgSchemaCheck.json(report),
                StandardOpenOption.CREATE_NEW);
        checks.add(Map.of("name", name, "status", report.get("status"), "report", name + ".json"));
    }

    private void hybenchFixture(Database db) throws Exception {
        db.reset();
        Path source = root.resolve("benchmark/HyBench-2024/conf");
        db.execute(Files.readString(source.resolve("ddl_pg.sql")));
        db.execute(Files.readString(source.resolve("create_index_pg.sql")));
    }

    // Independent source-layout fixtures, not generated from the checker or a native load run.
    private void ycsbFixture(Database db, Map<String, Object> config) throws Exception {
        db.reset();
        int fields =
                Math.toIntExact(
                        ConfigValues.positive(ConfigValues.object(config, "native"), "fieldcount"));
        StringBuilder sql =
                new StringBuilder("CREATE TABLE usertable(YCSB_KEY VARCHAR PRIMARY KEY");
        for (int i = 0; i < fields; i++) sql.append(", FIELD").append(i).append(" TEXT");
        db.execute(sql.append(")").toString());
    }

    private void sysbenchFixture(Database db, Map<String, Object> config) throws Exception {
        db.reset();
        var nativeConfig = ConfigValues.object(config, "native");
        int tables = Math.toIntExact(ConfigValues.positive(nativeConfig, "tables"));
        boolean auto = ConfigValues.bool(nativeConfig, "auto-inc");
        for (int i = 1; i <= tables; i++)
            db.execute(
                    "CREATE TABLE sbtest"
                            + i
                            + "(id "
                            + (auto ? "SERIAL" : "INTEGER NOT NULL")
                            + ", k INTEGER DEFAULT '0' NOT NULL, c CHAR(120) DEFAULT '' NOT NULL,"
                            + " pad CHAR(60) DEFAULT '' NOT NULL, PRIMARY KEY(id));"
                            + " CREATE INDEX k_"
                            + i
                            + " ON sbtest"
                            + i
                            + "(k)");
    }

    private final class Database implements AutoCloseable {
        private final String name;
        private final String admin;
        private final String jdbc;

        Database(String admin, String benchmark) throws Exception {
            this.admin = admin;
            this.name =
                    "sibench_schema_test_"
                            + benchmark
                            + "_"
                            + UUID.randomUUID().toString().replace("-", "");
            int slash = admin.indexOf('/', "jdbc:postgresql://".length());
            assertTrue("Admin URL must contain database", slash > 0);
            String options = admin.contains("?") ? admin.substring(admin.indexOf('?')) : "";
            this.jdbc = admin.substring(0, slash + 1) + name + options;
            try (Connection connection = DriverManager.getConnection(admin, user, password);
                    var statement = connection.createStatement()) {
                statement.execute("CREATE DATABASE " + name + " TEMPLATE template0");
            }
        }

        Connection connect() throws Exception {
            return DriverManager.getConnection(jdbc, user, password);
        }

        void execute(String sql) throws Exception {
            try (Connection connection = connect();
                    var statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }

        void reset() throws Exception {
            execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public");
        }

        @Override
        public void close() throws Exception {
            try (Connection connection = DriverManager.getConnection(admin, user, password);
                    var statement = connection.createStatement()) {
                statement.execute("DROP DATABASE " + name);
            }
        }
    }
}
