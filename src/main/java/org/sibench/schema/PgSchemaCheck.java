package org.sibench.schema;

import com.google.gson.GsonBuilder;

import org.sibench.schema.SourceSchema.*;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Read-only, point-in-time comparison of the live PG catalog and reviewed source layout. */
public final class PgSchemaCheck {
    public record ActualColumn(
            String name,
            String type,
            boolean nullable,
            String defaultExpression,
            String identity,
            String generated,
            boolean dropped,
            String collation,
            String sequence) {}

    public record ActualIndex(
            String name,
            String column,
            boolean primary,
            boolean unique,
            String method,
            int attributes,
            int keys,
            boolean valid,
            boolean ready,
            boolean live,
            boolean immediate,
            boolean exclusion,
            boolean expression,
            boolean partial,
            String operatorClass,
            boolean defaultOperatorClass,
            boolean columnCollation,
            int options,
            String definition) {}

    public record ActualTable(
            String name,
            String kind,
            String persistence,
            boolean inherited,
            boolean rowSecurity,
            List<ActualColumn> columns,
            List<ActualIndex> indexes,
            List<String> otherConstraints) {
        public ActualTable {
            columns = List.copyOf(columns);
            indexes = List.copyOf(indexes);
            otherConstraints = List.copyOf(otherConstraints);
        }
    }

    public static final class Mismatch extends IOException {
        private final Map<String, Object> report;

        private Mismatch(Map<String, Object> report, List<String> differences) {
            super("PG schema differs from pinned source: " + String.join("; ", differences));
            this.report = Collections.unmodifiableMap(report);
        }

        public Map<String, Object> report() {
            return report;
        }
    }

    private PgSchemaCheck() {}

    public static Map<String, Object> check(
            Path root, Map<String, Object> config, String jdbc, String user)
            throws IOException, SQLException, InterruptedException {
        SourceSchema expected = SourceSchema.read(root, config);
        String schema = "public";
        if (config.containsKey("postgres")) {
            var pg = org.sibench.config.ConfigValues.object(config, "postgres");
            if (pg.containsKey("schema"))
                schema = org.sibench.config.ConfigValues.string(pg, "schema");
        }
        try (Connection connection =
                DriverManager.getConnection(
                        jdbc, user, System.getenv().getOrDefault("PGPASSWORD", ""))) {
            if (config.containsKey("postgres"))
                checkConfiguredEnvironment(
                        connection, org.sibench.config.ConfigValues.object(config, "postgres"));
            return check(connection, expected, schema);
        }
    }

    static void checkConfiguredEnvironment(Connection connection, Map<String, Object> config)
            throws SQLException, IOException {
        try (Statement statement = connection.createStatement();
                ResultSet rows =
                        statement.executeQuery(
                                """
SELECT current_database(),current_user,current_setting('server_version'),
       current_setting('port'),current_setting('transaction_isolation'),
       current_setting('max_parallel_workers_per_gather')
""")) {
            if (!rows.next()) throw new IOException("Missing configured PG environment");
            for (var field :
                    Map.of(
                                    "database",
                                    1,
                                    "user",
                                    2,
                                    "version",
                                    3,
                                    "port",
                                    4,
                                    "isolation",
                                    5,
                                    "max_parallel_workers_per_gather",
                                    6)
                            .entrySet()) {
                if (!config.containsKey(field.getKey())) continue;
                String expected = config.get(field.getKey()).toString();
                if (field.getKey().equals("isolation")) expected = expected.replace('_', ' ');
                if (!expected.equals(rows.getString(field.getValue())))
                    throw new IOException(
                            "PG environment mismatch for "
                                    + field.getKey()
                                    + ": expected "
                                    + expected
                                    + ", actual "
                                    + rows.getString(field.getValue()));
            }
        }
    }

    public static Map<String, Object> check(
            Connection connection, SourceSchema expected, String schema)
            throws IOException, SQLException {
        if (!schema.matches("[a-z][a-z0-9_]*")
                || schema.startsWith("pg_")
                || schema.equals("information_schema"))
            throw new IOException("Unsupported benchmark schema: " + schema);
        if (!connection.getAutoCommit())
            throw new IOException("Schema preflight requires a fresh autocommit connection");
        connection.setReadOnly(true);
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.setAutoCommit(false);
        Throwable failure = null;
        try {
            var environment = new LinkedHashMap<String, Object>();
            try (Statement statement = connection.createStatement();
                    ResultSet rows =
                            statement.executeQuery(
                                    """
SELECT current_database(), current_user,
       current_setting('server_version'),
       current_setting('search_path'),
       current_setting('lc_collate'), current_setting('lc_ctype')
""")) {
                if (!rows.next()) throw new IOException("Missing PG environment");
                environment.put("database", rows.getString(1));
                environment.put("user", rows.getString(2));
                environment.put("version", rows.getString(3));
                environment.put("search_path", rows.getString(4));
                environment.put("lc_collate", rows.getString(5));
                environment.put("lc_ctype", rows.getString(6));
            }
            List<ActualTable> actual = read(connection, schema);
            var differences = new ArrayList<>(compare(expected, actual, schema));
            // Native benchmark SQL uses unqualified names; detect search_path shadowing too.
            try (PreparedStatement query =
                    connection.prepareStatement(
                            """
                            SELECT n.nspname, c.relname
                            FROM pg_catalog.pg_class c
                            JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                            WHERE c.oid=pg_catalog.to_regclass(?)
                            """)) {
                for (Table table : expected.tables()) {
                    query.setString(1, table.name());
                    try (ResultSet rows = query.executeQuery()) {
                        if (!rows.next()
                                || !schema.equals(rows.getString(1))
                                || !table.name().equals(rows.getString(2)))
                            differences.add(
                                    table.name() + ": unqualified SQL resolves outside " + schema);
                    }
                }
            }
            var report = new LinkedHashMap<String, Object>();
            report.put("status", differences.isEmpty() ? "schema_verified" : "schema_mismatch");
            report.put("checked_at", Instant.now().toString());
            report.put("benchmark", expected.benchmark());
            report.put("source_commit", expected.sourceCommit());
            report.put("source_directory", expected.source().toString());
            report.put("source_schema_sha256", expected.fingerprints());
            report.put("schema", schema);
            report.put("environment", environment);
            report.put("expected_tables", expected.tables());
            report.put("actual_tables", actual);
            report.put("differences", differences);
            connection.commit();
            if (!differences.isEmpty()) throw new Mismatch(report, differences);
            return report;
        } catch (SQLException | IOException | RuntimeException error) {
            failure = error;
            try {
                connection.rollback();
            } catch (SQLException rollback) {
                error.addSuppressed(rollback);
            }
            throw error;
        } finally {
            SQLException cleanup = null;
            try {
                connection.setAutoCommit(true);
            } catch (SQLException error) {
                cleanup = error;
            }
            try {
                connection.setReadOnly(false);
            } catch (SQLException error) {
                if (cleanup == null) cleanup = error;
                else cleanup.addSuppressed(error);
            }
            if (cleanup != null) {
                if (failure != null) failure.addSuppressed(cleanup);
                else throw cleanup;
            }
        }
    }

    static List<ActualTable> read(Connection connection, String schema) throws SQLException {
        var tables = new ArrayList<ActualTable>();
        try (PreparedStatement query =
                connection.prepareStatement(
                        """
SELECT c.oid,c.relname,c.relkind,c.relpersistence,
       EXISTS(SELECT 1 FROM pg_catalog.pg_inherits h
              WHERE h.inhrelid=c.oid OR h.inhparent=c.oid),
       c.relrowsecurity OR c.relforcerowsecurity
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname=? AND c.relkind IN ('r','p','v','m','f')
ORDER BY c.relname
""")) {
            query.setString(1, schema);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    long oid = rows.getLong(1);
                    String name = rows.getString(2);
                    tables.add(
                            new ActualTable(
                                    name,
                                    rows.getString(3),
                                    rows.getString(4),
                                    rows.getBoolean(5),
                                    rows.getBoolean(6),
                                    columns(connection, oid, schema, name),
                                    indexes(connection, oid),
                                    constraints(connection, oid)));
                }
            }
        }
        return tables;
    }

    private static List<ActualColumn> columns(
            Connection connection, long oid, String schema, String table) throws SQLException {
        var columns = new ArrayList<ActualColumn>();
        try (PreparedStatement query =
                connection.prepareStatement(
                        """
SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),NOT a.attnotnull,
       pg_catalog.pg_get_expr(d.adbin,d.adrelid),a.attidentity,a.attgenerated,a.attisdropped,
       CASE WHEN a.attcollation=0 THEN ''
            ELSE cn.nspname||'.'||co.collname END,
       CASE WHEN a.attisdropped THEN NULL
            ELSE pg_catalog.pg_get_serial_sequence(?,a.attname) END
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
LEFT JOIN pg_catalog.pg_collation co ON co.oid=a.attcollation
LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid=co.collnamespace
WHERE a.attrelid=CAST(? AS oid) AND a.attnum>0 ORDER BY a.attnum
""")) {
            query.setString(1, quoted(schema) + "." + quoted(table));
            query.setLong(2, oid);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next())
                    columns.add(
                            new ActualColumn(
                                    rows.getString(1),
                                    rows.getString(2),
                                    rows.getBoolean(3),
                                    rows.getString(4),
                                    rows.getString(5),
                                    rows.getString(6),
                                    rows.getBoolean(7),
                                    rows.getString(8),
                                    rows.getString(9)));
            }
        }
        return columns;
    }

    private static List<ActualIndex> indexes(Connection connection, long oid) throws SQLException {
        var indexes = new ArrayList<ActualIndex>();
        try (PreparedStatement query =
                connection.prepareStatement(
                        """
SELECT ic.relname,a.attname,i.indisprimary,i.indisunique,am.amname,
       i.indnatts,i.indnkeyatts,i.indisvalid,i.indisready,i.indislive,
       i.indimmediate,i.indisexclusion,i.indexprs IS NOT NULL,i.indpred IS NOT NULL,
       onsp.nspname||'.'||op.opcname,COALESCE(op.opcdefault,false),
       COALESCE(i.indcollation[0]=a.attcollation,false),i.indoption[0],
       pg_catalog.pg_get_indexdef(i.indexrelid)
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class ic ON ic.oid=i.indexrelid
JOIN pg_catalog.pg_am am ON am.oid=ic.relam
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
LEFT JOIN pg_catalog.pg_opclass op ON op.oid=i.indclass[0]
LEFT JOIN pg_catalog.pg_namespace onsp ON onsp.oid=op.opcnamespace
WHERE i.indrelid=CAST(? AS oid) ORDER BY ic.relname
""")) {
            query.setLong(1, oid);
            try (ResultSet r = query.executeQuery()) {
                while (r.next())
                    indexes.add(
                            new ActualIndex(
                                    r.getString(1),
                                    r.getString(2),
                                    r.getBoolean(3),
                                    r.getBoolean(4),
                                    r.getString(5),
                                    r.getInt(6),
                                    r.getInt(7),
                                    r.getBoolean(8),
                                    r.getBoolean(9),
                                    r.getBoolean(10),
                                    r.getBoolean(11),
                                    r.getBoolean(12),
                                    r.getBoolean(13),
                                    r.getBoolean(14),
                                    r.getString(15),
                                    r.getBoolean(16),
                                    r.getBoolean(17),
                                    r.getInt(18),
                                    r.getString(19)));
            }
        }
        return indexes;
    }

    private static List<String> constraints(Connection connection, long oid) throws SQLException {
        var constraints = new ArrayList<String>();
        try (PreparedStatement query =
                connection.prepareStatement(
                        """
                        SELECT conname||': '||pg_catalog.pg_get_constraintdef(oid)
                        FROM pg_catalog.pg_constraint
                        WHERE conrelid=CAST(? AS oid) AND contype<>'p' ORDER BY conname
                        """)) {
            query.setLong(1, oid);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) constraints.add(rows.getString(1));
            }
        }
        return constraints;
    }

    public static List<String> compare(SourceSchema expected, List<ActualTable> actual) {
        return compare(expected, actual, "public");
    }

    public static List<String> compare(
            SourceSchema expected, List<ActualTable> actual, String schema) {
        var differences = new ArrayList<String>();
        var byName = new LinkedHashMap<String, ActualTable>();
        for (ActualTable table : actual)
            if (byName.put(table.name(), table) != null)
                differences.add("Duplicate actual table: " + table.name());
        for (Table table : expected.tables()) {
            ActualTable found = byName.remove(table.name());
            if (found == null) {
                differences.add("Missing table: " + table.name());
                continue;
            }
            String prefix = table.name() + ": ";
            if (!found.kind().equals("r")
                    || !found.persistence().equals("p")
                    || found.inherited()
                    || found.rowSecurity())
                differences.add(
                        prefix + "expected ordinary persistent table without inheritance/RLS");
            if (!found.otherConstraints().isEmpty())
                differences.add(prefix + "unexpected constraints " + found.otherConstraints());
            if (table.columns().size() != found.columns().size())
                differences.add(
                        prefix
                                + "column count expected "
                                + table.columns().size()
                                + ", actual "
                                + found.columns().size());
            for (int i = 0; i < Math.min(table.columns().size(), found.columns().size()); i++) {
                Column column = table.columns().get(i);
                ActualColumn col = found.columns().get(i);
                String path = table.name() + "." + column.name() + ": ";
                if (!column.name().equals(col.name()) || col.dropped())
                    differences.add(path + "column order/name mismatch or dropped column");
                if (!column.type().equals(col.type()))
                    differences.add(
                            path + "type expected " + column.type() + ", actual " + col.type());
                if (column.nullable() != col.nullable())
                    differences.add(path + "nullability mismatch");
                if (!col.identity().isEmpty() || !col.generated().isEmpty())
                    differences.add(path + "unexpected identity/generated column");
                String collation =
                        column.type().startsWith("character") || column.type().equals("text")
                                ? "pg_catalog.default"
                                : "";
                if (!collation.equals(col.collation()))
                    differences.add(
                            path
                                    + "collation expected "
                                    + collation
                                    + ", actual "
                                    + col.collation());
                if (!defaultMatches(column.defaultValue(), col, schema))
                    differences.add(
                            path
                                    + "default/sequence expected "
                                    + column.defaultValue()
                                    + ", actual "
                                    + col.defaultExpression()
                                    + " / "
                                    + col.sequence());
            }
            var indexes = new LinkedHashMap<String, ActualIndex>();
            for (ActualIndex index : found.indexes())
                if (indexes.put(index.name(), index) != null)
                    differences.add(prefix + "duplicate index " + index.name());
            for (Index index : table.indexes()) {
                ActualIndex observed = indexes.remove(index.name());
                if (observed == null) {
                    differences.add(prefix + "missing index " + index.name());
                    continue;
                }
                String type =
                        table.columns().stream()
                                .filter(c -> c.name().equals(index.column()))
                                .findFirst()
                                .orElseThrow()
                                .type();
                String operatorClass =
                        switch (type) {
                            case "integer" -> "pg_catalog.int4_ops";
                            case "timestamp without time zone" -> "pg_catalog.timestamp_ops";
                            case "character varying", "text" -> "pg_catalog.text_ops";
                            default -> type.startsWith("character(") ? "pg_catalog.bpchar_ops" : "";
                        };
                if (!index.column().equals(observed.column())
                        || index.primary() != observed.primary()
                        || index.primary() != observed.unique()
                        || !observed.method().equals("btree")
                        || observed.attributes() != 1
                        || observed.keys() != 1
                        || !observed.valid()
                        || !observed.ready()
                        || !observed.live()
                        || !observed.immediate()
                        || observed.exclusion()
                        || observed.expression()
                        || observed.partial()
                        || !operatorClass.equals(observed.operatorClass())
                        || !observed.defaultOperatorClass()
                        || !observed.columnCollation()
                        || observed.options() != 0)
                    differences.add(
                            prefix
                                    + "index definition/state mismatch "
                                    + index.name()
                                    + ": "
                                    + observed.definition());
            }
            if (!indexes.isEmpty())
                differences.add(prefix + "unexpected indexes " + indexes.keySet());
        }
        if (!byName.isEmpty()) differences.add("Unexpected tables/views: " + byName.keySet());
        return List.copyOf(differences);
    }

    private static boolean defaultMatches(
            DefaultValue expected, ActualColumn actual, String schema) {
        if (expected.kind().equals("none"))
            return actual.defaultExpression() == null && actual.sequence() == null;
        if (actual.defaultExpression() == null) return false;
        try {
            List<String> tokens = SimpleDdl.tokens(actual.defaultExpression());
            return switch (expected.kind()) {
                case "current_timestamp" ->
                        tokens.equals(List.of("current_timestamp")) && actual.sequence() == null;
                case "integer" ->
                        tokens.equals(List.of(expected.value())) && actual.sequence() == null;
                case "string" ->
                        tokens.equals(
                                        List.of(
                                                "'" + expected.value().replace("'", "''") + "'",
                                                ":",
                                                ":",
                                                "bpchar"))
                                && actual.sequence() == null;
                case "sequence" -> {
                    if (!Objects.equals(actual.sequence(), schema + "." + expected.value()))
                        yield false;
                    String shortName = "'" + expected.value() + "'";
                    String qualified = "'" + schema + "." + expected.value() + "'";
                    yield tokens.equals(
                                    List.of("nextval", "(", shortName, ":", ":", "regclass", ")"))
                            || tokens.equals(
                                    List.of("nextval", "(", qualified, ":", ":", "regclass", ")"));
                }
                default -> false;
            };
        } catch (IOException error) {
            return false;
        }
    }

    public static String json(Map<String, Object> report) {
        return new GsonBuilder().serializeNulls().setPrettyPrinting().create().toJson(report);
    }

    private static String quoted(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
