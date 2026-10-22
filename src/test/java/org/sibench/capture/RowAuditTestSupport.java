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

    private RowAuditTestSupport() {}

    static Path baseline(Path fixture, SourceSchema schema, Map<String, List<JsonObject>> rows)
            throws Exception {
        Path directory = Files.createDirectory(fixture.resolve("baseline"));
        var original =
                CaptureConfig.read(Path.of(""), Path.of("config/hybench_tp_subset_sf1.yaml"))
                        .layout();
        var config =
                new GenerationConfig(
                        original.root(),
                        original.document(),
                        original.benchmark(),
                        original.seed(),
                        original.targetBytes(),
                        original.maxRows(),
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
        Files.writeString(directory.resolve("locators.tsv"), "");
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

    static Connection connection(
            SourceSchema schema, Map<String, List<JsonObject>> contents, int[] queries) {
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
                            default -> throw new AssertionError(method);
                        });
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
                            case "executeQuery" -> {
                                if (!transaction[0] || !readOnly[0])
                                    throw new AssertionError("Not a read-only transaction");
                                String sql = args[0].toString();
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
        int[] cursor = {-1};
        return proxy(
                ResultSet.class,
                (r, method, args) ->
                        switch (method.getName()) {
                            case "next" -> ++cursor[0] < data.size();
                            case "close" -> null;
                            case "getString" -> {
                                if (args[0] instanceof Integer)
                                    yield JSON.toJson(data.get(cursor[0]));
                                var value = data.get(cursor[0]).get(args[0].toString());
                                yield value == null || value.isJsonNull()
                                        ? null
                                        : value.getAsString();
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
                                    i + 1)));
        }
        client.add(
                JSON.toJson(
                        Map.of(
                                "event",
                                "close",
                                "application",
                                "fixture-" + (transactions.size() - 1))));
        Path jdbc = directory.resolve("jdbc-fixture.jsonl");
        Files.write(jdbc, client);
        Path raw = directory.resolve("pg-1.jsonl");
        Files.write(raw, server);
        complete(directory, jdbc, raw);
        return raw;
    }

    /** Publishes the completion manifest a real capture writes, binding one JDBC/PG trace pair. */
    static void complete(Path directory, Path client, Path server) throws java.io.IOException {
        Files.writeString(
                directory.resolve("capture-run.json"),
                JSON.toJson(
                        Map.of(
                                "status",
                                "complete",
                                "session_order",
                                "single",
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
                                                "bindings", true)))));
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
}
