package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageTypeParser;
import org.sibench.generation.GenerationConfig;
import org.sibench.generation.ParquetDataFile;
import org.sibench.generation.TableData;
import org.sibench.schema.SourceSchema;

import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Independent JDBC fixture: SQL results are supplied rows, not audit-derived expectations. */
final class RowAuditTestSupport {
    private static final Gson JSON = new GsonBuilder().serializeNulls().create();
    private static final java.util.concurrent.atomic.AtomicLong WRITES =
            new java.util.concurrent.atomic.AtomicLong();

    private RowAuditTestSupport() {}

    static Path baseline(Path fixture, SourceSchema schema, Map<String, List<JsonObject>> rows)
            throws Exception {
        return baseline(fixture, schema, rows, 0);
    }

    /** {@code groupRows}, when positive, caps the rows of every Parquet row group. */
    static Path baseline(
            Path fixture, SourceSchema schema, Map<String, List<JsonObject>> rows, long groupRows)
            throws Exception {
        Path directory = Files.createDirectory(fixture.resolve("baseline"));
        var original =
                CaptureConfig.read(Path.of(""), Path.of("config/hybench_sf1.yaml"))
                        .layout();
        var config =
                new GenerationConfig(
                        original.root(),
                        original.document(),
                        original.benchmark(),
                        original.seed(),
                        original.targetBytes(),
                        groupRows > 0 ? groupRows : original.maxRows(),
                        original.groupBytes(),
                        original.pageBytes(),
                        original.dictionaryBytes(),
                        original.dictionary(),
                        original.codec(),
                        original.codecLevel(),
                        1,
                        0,
                        directory,
                        1);
        var files = new ArrayList<String>();
        long rowId = 1, fileId = 1;
        for (var table : schema.tables()) {
            StringBuilder text = new StringBuilder("message " + table.name() + " { ");
            for (var column : table.columns()) {
                String type =
                        column.type().equals("integer")
                                ? "int64"
                                : column.type().equals("real") ? "double" : "binary";
                text.append("optional ")
                        .append(type)
                        .append(" ")
                        .append(column.name())
                        .append(type.equals("binary") ? " (UTF8); " : "; ");
            }
            text.append("required int64 _sib_row_id; required int64 _sib_timestamp; }");
            var parquetSchema = MessageTypeParser.parseMessageType(text.toString());
            var contents = rows.getOrDefault(table.name(), List.of());
            Path file = directory.resolve(table.name() + ".parquet");
            try (var output =
                    new ParquetDataFile(
                            file,
                            new TableData(
                                    table.name(),
                                    contents.size(),
                                    rowId,
                                    SourceRow.primary(table).name(),
                                    parquetSchema,
                                    null),
                            config,
                            Map.of())) {
                for (var value : contents) {
                    var group = new SimpleGroupFactory(parquetSchema).newGroup();
                    for (var column : table.columns()) {
                        var element = value.get(column.name());
                        if (element.isJsonNull()) continue;
                        if (column.type().equals("integer"))
                            group.add(column.name(), element.getAsLong());
                        else if (column.type().equals("real"))
                            group.add(column.name(), (double) element.getAsFloat());
                        else group.add(column.name(), element.getAsString());
                    }
                    group.add("_sib_row_id", rowId++);
                    group.add("_sib_timestamp", 0L);
                    output.write(group);
                }
                output.finish();
            }
            files.add(
                    new Gson()
                            .toJson(
                                    Map.of(
                                            "table",
                                            table.name(),
                                            "path",
                                            file.getFileName().toString(),
                                            "rows",
                                            contents.size(),
                                            "file_id",
                                            fileId++)));
        }
        Files.write(directory.resolve("files.jsonl"), files);
        return directory;
    }

    static Map<String, List<JsonObject>> baseline(Path baseline) throws Exception {
        var result = new LinkedHashMap<String, List<JsonObject>>();
        for (String line : Files.readAllLines(baseline.resolve("files.jsonl"))) {
            var file = JsonParser.parseString(line).getAsJsonObject();
            try (var input =
                    ParquetReader.builder(
                                    new GroupReadSupport(),
                                    new org.apache.hadoop.fs.Path(
                                            baseline.resolve(file.get("path").getAsString())
                                                    .toUri()))
                            .build()) {
                Group group;
                while ((group = input.read()) != null)
                    result.computeIfAbsent(
                                    file.get("table").getAsString(), ignored -> new ArrayList<>())
                            .add(SourceRow.json(group));
            }
        }
        return result;
    }

    /** Fresh connections over the same contents, as PgBaselineVerify opens one per worker. */
    static PgBaselineVerify.Connections connections(
            SourceSchema schema, Map<String, List<JsonObject>> contents, int[] queries) {
        return connections(schema, contents, queries, 1.0);
    }

    /** {@code correlation}: what pg_stats reports for every key, which decides how tables are read. */
    static PgBaselineVerify.Connections connections(
            SourceSchema schema, Map<String, List<JsonObject>> contents, int[] queries, double correlation) {
        return () -> connection(schema, contents, queries, correlation);
    }

    static Connection connection(
            SourceSchema schema, Map<String, List<JsonObject>> contents, int[] queries) {
        return connection(schema, contents, queries, 1.0);
    }

    static Connection connection(
            SourceSchema schema, Map<String, List<JsonObject>> contents, int[] queries, double correlation) {
        boolean[] transaction = {false}, readOnly = {false};
        return proxy(
                Connection.class,
                (p, method, args) ->
                        switch (method.getName()) {
                            case "getAutoCommit" -> !transaction[0];
                            case "setAutoCommit" -> {
                                transaction[0] = !(boolean) args[0];
                                yield null;
                            }
                            case "setReadOnly" -> {
                                readOnly[0] = (boolean) args[0];
                                yield null;
                            }
                            case "setTransactionIsolation", "commit", "rollback", "close" -> null;
                            case "createStatement" ->
                                    statement(schema, contents, queries, transaction, readOnly);
                            case "prepareStatement" -> {
                                if (!args[0].toString().contains("pg_stats")) throw new AssertionError(args[0]);
                                yield proxy(
                                        PreparedStatement.class,
                                        (s, m, a) ->
                                                switch (m.getName()) {
                                                    case "setString", "close" -> null;
                                                    case "executeQuery" -> rows(List.of(JsonParser.parseString(
                                                            "{\"correlation\":" + correlation + "}").getAsJsonObject()), true);
                                                    default -> throw new AssertionError(m);
                                                });
                            }
                            default -> throw new AssertionError(method);
                        });
    }

    private static final java.util.regex.Pattern BOUND =
            java.util.regex.Pattern.compile("(>|<=) (-?\\d+|'(?:[^']|'')*')");

    /**
     * A column query of PgBaselineVerify: the table's columns in schema order, in C key order, for
     * keys after a {@code >} bound and through a {@code <=} bound when the SQL states them.
     */
    private static ResultSet columns(SourceSchema schema, Map<String, List<JsonObject>> contents, int[] queries, String sql) {
        var table = table(schema, sql);
        var pk = SourceRow.primary(table);
        Object low = null, high = null;
        var matcher = BOUND.matcher(sql);
        while (matcher.find()) {
            String text = matcher.group(2);
            Object bound = text.startsWith("'") ? text.substring(1, text.length() - 1).replace("''", "'") : (Object) Long.parseLong(text);
            if (matcher.group(1).equals(">")) low = bound;
            else high = bound;
        }
        queries[0]++;
        var data = new ArrayList<JsonObject>();
        for (JsonObject row : sorted(table, contents)) {
            if (low != null && compare(pk, row, low) <= 0) continue;
            if (high != null && compare(pk, row, high) > 0) continue;
            data.add(row);
        }
        int[] cursor = {-1};
        return proxy(
                ResultSet.class,
                (r, m, a) ->
                        switch (m.getName()) {
                            case "next" -> ++cursor[0] < data.size();
                            case "close" -> null;
                            case "getString" -> {
                                var name = table.columns().get((Integer) a[0] - 1).name();
                                var value = data.get(cursor[0]).get(name);
                                yield value == null || value.isJsonNull() ? null : value.getAsString();
                            }
                            default -> throw new AssertionError(m);
                        });
    }

    private static SourceSchema.Table table(SourceSchema schema, String sql) {
        return schema.tables().stream()
                .filter(t -> sql.contains("FROM \"" + t.name() + "\""))
                .findFirst()
                .orElseThrow(() -> new AssertionError(sql));
    }

    private static int compare(SourceSchema.Column pk, JsonObject row, Object bound) {
        if (pk.type().equals("integer")) return Long.compare(row.get(pk.name()).getAsLong(), (Long) bound);
        return Arrays.compareUnsigned(
                row.get(pk.name()).getAsString().getBytes(StandardCharsets.UTF_8),
                ((String) bound).getBytes(StandardCharsets.UTF_8));
    }

    private static List<JsonObject> sorted(SourceSchema.Table table, Map<String, List<JsonObject>> contents) {
        var data = new ArrayList<>(contents.getOrDefault(table.name(), List.of()));
        var pk = SourceRow.primary(table);
        data.sort((a, b) -> pk.type().equals("integer")
                ? Long.compare(a.get(pk.name()).getAsLong(), b.get(pk.name()).getAsLong())
                : Arrays.compareUnsigned(
                        a.get(pk.name()).getAsString().getBytes(StandardCharsets.UTF_8),
                        b.get(pk.name()).getAsString().getBytes(StandardCharsets.UTF_8)));
        return data;
    }

    private static Statement statement(
            SourceSchema schema,
            Map<String, List<JsonObject>> contents,
            int[] queries,
            boolean[] transaction,
            boolean[] readOnly) {
        return proxy(
                Statement.class,
                (s, method, args) ->
                        switch (method.getName()) {
                            case "close", "setFetchSize" -> null;
                            case "execute" -> {
                                if (!transaction[0]
                                        || !(args[0].toString().startsWith("SET TRANSACTION SNAPSHOT ")
                                                || args[0].toString().startsWith("SET LOCAL work_mem = ")
                                                || args[0].toString().startsWith("SET LOCAL max_parallel_workers_per_gather = ")))
                                    throw new AssertionError(args[0]);
                                yield false;
                            }
                            case "executeQuery" -> {
                                if (!transaction[0] || !readOnly[0])
                                    throw new AssertionError("Not a read-only transaction");
                                String sql = args[0].toString();
                                if (sql.startsWith("SELECT \""))
                                    yield columns(schema, contents, queries, sql);
                                // A fixture table is a few blocks: the row audit reads it whole.
                                if (sql.startsWith("SELECT pg_relation_size("))
                                    yield rows(List.of(JsonParser.parseString("{\"blocks\":1}").getAsJsonObject()), true);
                                if (sql.equals("SELECT pg_export_snapshot()"))
                                    yield rows(List.of(JsonParser.parseString("{\"snapshot\":\"fixture\"}").getAsJsonObject()), true);
                                var table =
                                        schema.tables().stream()
                                                .filter(
                                                        t ->
                                                                sql.contains(
                                                                        "FROM \"" + t.name()
                                                                                + "\""))
                                                .findFirst()
                                                .orElseThrow(() -> new AssertionError(sql));
                                queries[0]++;
                                var data =
                                        new ArrayList<>(
                                                contents.getOrDefault(table.name(), List.of()));
                                var pk = SourceRow.primary(table);
                                Comparator<JsonObject> order =
                                        pk.type().equals("integer")
                                                ? Comparator.comparingLong(
                                                        r -> r.get(pk.name()).getAsLong())
                                                : (a, b) ->
                                                        Arrays.compareUnsigned(
                                                                a.get(pk.name())
                                                                        .getAsString()
                                                                        .getBytes(
                                                                                StandardCharsets
                                                                                        .UTF_8),
                                                                b.get(pk.name())
                                                                        .getAsString()
                                                                        .getBytes(
                                                                                StandardCharsets
                                                                                        .UTF_8));
                                data.sort(order);
                                yield rows(data);
                            }
                            default -> throw new AssertionError(method);
                        });
    }

    private static ResultSet rows(List<JsonObject> data) {
        return rows(data, false);
    }

    /** {@code firstColumn}: an index reads the row's only value, not the row as JSON. */
    private static ResultSet rows(List<JsonObject> data, boolean firstColumn) {
        int[] cursor = {-1};
        return proxy(
                ResultSet.class,
                (r, method, args) ->
                        switch (method.getName()) {
                            case "next" -> ++cursor[0] < data.size();
                            case "close" -> null;
                            case "getString" -> {
                                if (args[0] instanceof Integer && firstColumn)
                                    yield data.get(cursor[0]).entrySet().iterator().next().getValue().getAsString();
                                if (args[0] instanceof Integer)
                                    yield JSON.toJson(data.get(cursor[0]));
                                var value = data.get(cursor[0]).get(args[0].toString());
                                yield value == null || value.isJsonNull()
                                        ? null
                                        : value.getAsString();
                            }
                            case "getObject" -> {
                                if (!firstColumn) throw new AssertionError(method);
                                yield data.get(cursor[0]).entrySet().iterator().next().getValue().getAsDouble();
                            }
                            case "getDouble" -> {
                                if (!firstColumn) throw new AssertionError(method);
                                yield data.get(cursor[0]).entrySet().iterator().next().getValue().getAsDouble();
                            }
                            case "getLong" -> {
                                if (!firstColumn) throw new AssertionError(method);
                                yield data.get(cursor[0]).entrySet().iterator().next().getValue().getAsLong();
                            }
                            default -> throw new AssertionError(method);
                        });
    }

    static Path capture(Path directory, List<List<JsonObject>> transactions, Set<Integer> aborted)
            throws Exception {
        var client = new ArrayList<String>();
        var server = new ArrayList<String>();
        client.add(JSON.toJson(Map.of("event", "connection", "pid", 1)));
        for (int i = 0; i < transactions.size(); i++) {
            String app = "fixture-" + i;
            client.add(
                    JSON.toJson(
                            Map.of(
                                    "event",
                                    "request_start",
                                    "application",
                                    app,
                                    "sql",
                                    "SELECT 1")));
            client.add(JSON.toJson(Map.of("event", "request_return", "application", app)));
            server.add(
                    JSON.toJson(
                            Map.of(
                                    "event",
                                    "executor_start",
                                    "application",
                                    app,
                                    "pid",
                                    1,
                                    "sql",
                                    "SELECT 1")));
            for (var change : transactions.get(i)) {
                var event = change.deepCopy();
                event.addProperty("event", "row_change");
                event.addProperty("xid", i + 1);
                event.addProperty("pid", 1);
                event.addProperty("application", app);
                server.add(JSON.toJson(event));
            }
            server.add(
                    JSON.toJson(Map.of("event", "executor_run_end", "application", app, "pid", 1)));
            server.add(JSON.toJson(Map.of("event", "executor_end", "application", app, "pid", 1)));
            client.add(
                    JSON.toJson(
                            Map.of(
                                    "event",
                                    aborted.contains(i) ? "rollback" : "commit",
                                    "application",
                                    app)));
            server.add(
                    JSON.toJson(
                            Map.of(
                                    "event",
                                    aborted.contains(i)
                                            ? "transaction_abort"
                                            : "transaction_commit",
                                    "application",
                                    app,
                                    "pid",
                                    1,
                                    "xid",
                                    i + 1,
                                    // PostgreSQL's completion counter: the position that orders
                                    // a key's changes across sessions.
                                    "csn",
                                    (long) i + 1)));
        }
        client.add(
                JSON.toJson(
                        Map.of(
                                "event",
                                "close",
                                "application",
                                "fixture-" + (transactions.size() - 1))));
        Path jdbc = writeTrace(directory.resolve("jdbc-fixture.jsonl.zst"), client);
        Path raw = writeTrace(directory.resolve("pg-1.jsonl.zst"), server);
        complete(directory, jdbc, raw);
        return raw;
    }

    /**
     * Publishes the completion manifest a real capture writes, binding one JDBC/PG trace pair and
     * the audit the run records for it.
     */
    static void complete(Path directory, Path client, Path server) throws Exception {
        // A session the audit rejects fails its run, as a real capture does.
        Map<String, Object> audit;
        String status = "complete";
        try {
            audit = RequestCaptureAudit.audit(client, server, true);
        } catch (java.io.IOException rejected) {
            audit = Map.of("status", "failed", "message", String.valueOf(rejected.getMessage()));
            status = "failed";
        }
        Files.writeString(
                directory.resolve("capture-run.json"),
                JSON.toJson(
                        Map.of(
                                "status",
                                status,
                                "session_order",
                                "sequential",
                                "sessions",
                                List.of(
                                        Map.of(
                                                "pg_file", server.getFileName().toString(),
                                                "pg_sha256",
                                                        org.sibench.generation.BaselineGenerator
                                                                .sha256(server),
                                                "jdbc_file", client.getFileName().toString(),
                                                "jdbc_sha256",
                                                        org.sibench.generation.BaselineGenerator
                                                                .sha256(client),
                                                "bindings", true,
                                                "audit", audit)))));
    }

    /**
     * Publishes the manifest a concurrent capture writes: several backend traces, no JDBC pair, so
     * each session is audited on its own server events.
     */
    static void completeConcurrent(Path directory, List<Path> servers) throws Exception {
        var sessions = new ArrayList<Map<String, Object>>();
        for (Path server : servers)
            sessions.add(
                    Map.of(
                            "pg_file",
                            server.getFileName().toString(),
                            "pg_sha256",
                            org.sibench.generation.BaselineGenerator.sha256(server),
                            "audit",
                            ServerCaptureAudit.audit(server)));
        Files.writeString(
                directory.resolve("capture-run.json"),
                JSON.toJson(
                        Map.of(
                                "status",
                                "complete",
                                "session_order",
                                "concurrent",
                                "sessions",
                                sessions)));
    }

    static JsonObject change(String table, String operation, JsonObject before, JsonObject after) {
        var result = new JsonObject();
        result.addProperty("relation", table);
        result.addProperty("operation", operation);
        result.add("old", before == null ? JsonNull.INSTANCE : before);
        result.add("new", after == null ? JsonNull.INSTANCE : after);
        return result;
    }

    static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(
                Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }

    static Path writeTrace(Path file, Iterable<String> lines) throws Exception {
        try (var out = new com.github.luben.zstd.ZstdOutputStream(Files.newOutputStream(file))) {
            for (String line : lines) {
                out.write(line.getBytes(StandardCharsets.UTF_8));
                out.write('\n');
            }
        }
        bump(file);
        return file;
    }

    static Path writeTrace(Path file, byte[] bytes) throws Exception {
        try (var out = new com.github.luben.zstd.ZstdOutputStream(Files.newOutputStream(file))) {
            out.write(bytes);
        }
        bump(file);
        return file;
    }

    /** FileDigests keys on size and mtime; make a test rewrite observably newer. */
    private static void bump(Path file) throws Exception {
        Files.setLastModifiedTime(
                file,
                java.nio.file.attribute.FileTime.fromMillis(
                        System.currentTimeMillis() + 5_000 + WRITES.incrementAndGet()));
    }

    static List<String> readTrace(Path file) throws Exception {
        var lines = new ArrayList<String>();
        try (var input = ServerTrace.open(file)) {
            for (String line; (line = input.readLine()) != null; ) lines.add(line);
        }
        return lines;
    }
}
