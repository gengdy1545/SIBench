package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** One source-state audit per capture; consumers reuse its digest-bound row transitions. */
public final class RowStateAudit {
    private static final Gson JSON =
            new GsonBuilder().serializeNulls().setPrettyPrinting().create();
    private static final Gson EVENTS = new GsonBuilder().serializeNulls().create();

    record Key(String table, String value) {}

    record Change(JsonObject source, String session, long position) {}

    record Chain(JsonObject last, List<Change> changes) {}

    private RowStateAudit() {}

    public static Path defaultReport(Path raw) {
        return (Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent())
                .resolve("row-state-audit.json");
    }

    public static Map<String, Object> audit(
            String jdbc, String user, SourceSchema schema, Path baseline, Path raw, Path report)
            throws Exception {
        if (Files.exists(report)) return reuse(schema, baseline, raw, report);
        Path transitions =
                report.toAbsolutePath().resolveSibling(report.getFileName() + ".changes.jsonl");
        if (Files.exists(transitions)
                || Files.exists(report.resolveSibling(report.getFileName() + ".partial")))
            throw new IOException("Unfinished row audit output exists; use a fresh report path");
        try (Connection connection =
                DriverManager.getConnection(
                        jdbc, user, System.getenv().getOrDefault("PGPASSWORD", ""))) {
            return audit(connection, schema, baseline, raw, report);
        }
    }

    static Map<String, Object> audit(
            Connection connection, SourceSchema schema, Path baseline, Path raw, Path report)
            throws Exception {
        if (Files.exists(report)) return reuse(schema, baseline, raw, report);
        CaptureRun.verifyInput(resolveRaw(raw));
        PgBaselineVerify.verifyFiles(baseline);
        if (!connection.getAutoCommit())
            throw new IOException("Row audit requires a fresh autocommit connection");
        Map<String, String> inputs = inputHashes(raw);
        var tables = new LinkedHashMap<String, SourceSchema.Table>();
        for (var table : schema.tables()) tables.put(table.name(), table);
        var changes = new LinkedHashMap<Key, List<Change>>();
        long committed = 0, discarded = 0, commits = 0, aborts = 0;
        List<Path> sources = sourceFiles(raw);
        for (Path source : sources) {
            var pending = new ArrayList<Change>();
            long xid = 0, position = 0;
            try (var reader = Files.newBufferedReader(source)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    position++;
                    JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                    String type = event.get("event").getAsString();
                    if (type.equals("row_change")) {
                        var table = tables.get(event.get("relation").getAsString());
                        if (table == null) throw new IOException("Unknown source row table");
                        validateChange(table, event);
                        long current = event.get("xid").getAsLong();
                        if (!pending.isEmpty() && xid != current)
                            throw new IOException("Interleaved row transactions in one session");
                        xid = current;
                        pending.add(new Change(event, source.getFileName().toString(), position));
                    } else if (type.equals("transaction_commit")
                            || type.equals("transaction_abort")) {
                        if (!pending.isEmpty() && xid != event.get("xid").getAsLong())
                            throw new IOException("Row transaction boundary XID mismatch");
                        if (type.equals("transaction_commit")) {
                            commits++;
                            for (Change change : pending) {
                                var table =
                                        tables.get(change.source().get("relation").getAsString());
                                JsonObject row = SourceRow.image(change.source(), "new");
                                if (row == null) row = SourceRow.image(change.source(), "old");
                                changes.computeIfAbsent(
                                                new Key(table.name(), SourceRow.key(table, row)),
                                                ignored -> new ArrayList<>())
                                        .add(change);
                            }
                            committed += pending.size();
                        } else {
                            aborts++;
                            discarded += pending.size();
                        }
                        pending.clear();
                    }
                }
            }
            if (!pending.isEmpty()) throw new IOException("Unfinished source row changes");
        }
        List<JsonObject> files =
                Files.readAllLines(baseline.resolve("files.jsonl")).stream()
                        .map(line -> JsonParser.parseString(line).getAsJsonObject())
                        .toList();
        var initial = new HashMap<Key, JsonObject>();
        var baselineCounts = new LinkedHashMap<String, Long>();
        for (var table : schema.tables()) baselineCounts.put(table.name(), 0L);
        for (var file : files) {
            var table = tables.get(file.get("table").getAsString());
            if (table == null) throw new IOException("Unknown baseline table");
            long count = 0;
            try (var reader = reader(baseline.resolve(file.get("path").getAsString()))) {
                Group group;
                while ((group = reader.read()) != null) {
                    count++;
                    JsonObject row = SourceRow.json(group);
                    Key key = new Key(table.name(), SourceRow.key(table, row));
                    if (changes.containsKey(key) && initial.put(key, row) != null)
                        throw new IOException("Duplicate modified baseline key: " + key);
                }
            }
            if (count != file.get("rows").getAsLong())
                throw new IOException("Baseline row count mismatch");
            baselineCounts.merge(table.name(), count, Long::sum);
        }
        var chains = new LinkedHashMap<Key, Chain>();
        for (var entry : changes.entrySet())
            chains.put(
                    entry.getKey(),
                    chain(
                            tables.get(entry.getKey().table()),
                            initial.get(entry.getKey()),
                            entry.getValue(),
                            sources.size() == 1));

        var tableReports = new ArrayList<Map<String, Object>>();
        connection.setReadOnly(true);
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.setAutoCommit(false);
        try {
            for (var table : schema.tables()) {
                var replacements = new HashMap<String, JsonObject>();
                long mutations = 0;
                for (var entry : chains.entrySet()) {
                    if (!entry.getKey().table().equals(table.name())) continue;
                    replacements.put(entry.getKey().value(), entry.getValue().last());
                    mutations += entry.getValue().changes().size();
                }
                long finalRows = compareFinal(connection, table, baseline, files, replacements);
                tableReports.add(
                        Map.of(
                                "table",
                                table.name(),
                                "baseline_rows",
                                baselineCounts.get(table.name()),
                                "final_rows",
                                finalRows,
                                "committed_mutations",
                                mutations,
                                "changed_keys",
                                replacements.size()));
            }
            connection.commit();
        } catch (Exception error) {
            try {
                connection.rollback();
            } catch (SQLException rollback) {
                error.addSuppressed(rollback);
            }
            throw error;
        }
        // Publish transitions first; only the final report makes the audit consumable.
        Path transitions =
                report.toAbsolutePath().resolveSibling(report.getFileName() + ".changes.jsonl");
        try (var output = Files.newBufferedWriter(transitions, StandardOpenOption.CREATE_NEW)) {
            for (var chain : chains.values()) {
                for (var change : chain.changes()) {
                    JsonObject value = change.source().deepCopy();
                    value.addProperty("source_session", change.session());
                    value.addProperty("source_position", change.position());
                    output.write(EVENTS.toJson(value));
                    output.newLine();
                }
            }
        }
        if (!inputs.equals(inputHashes(raw)))
            throw new IOException("Source changed during row audit");
        var result = new LinkedHashMap<String, Object>();
        result.put("schema_version", 1);
        result.put("status", "passed_source_row_state");
        result.put("benchmark", schema.benchmark());
        result.put("source_schema", schema.tables());
        result.put("source_schema_sha256", schema.fingerprints());
        result.put("source_inputs_sha256", inputs);
        result.put(
                "baseline_verified_manifest_sha256",
                BaselineGenerator.sha256(baseline.resolve("verified-manifest.json")));
        result.put("changes_file", transitions.getFileName().toString());
        result.put("changes_sha256", BaselineGenerator.sha256(transitions));
        result.put("commits", commits);
        result.put("aborts", aborts);
        result.put("committed_row_changes", committed);
        result.put("discarded_aborted_row_changes", discarded);
        result.put("tables", tableReports);
        result.put("ordering", sources.size() == 1 ? "source_session" : "per_key_causal");
        result.put(
                "limitations",
                List.of(
                        "Not a global concurrent commit, query snapshot or lost-event proof",
                        "Aborted images are shape/type/identity checked; their effects are"
                            + " discarded"));
        Path partial = report.resolveSibling(report.getFileName() + ".partial");
        Files.writeString(partial, JSON.toJson(result), StandardOpenOption.CREATE_NEW);
        Files.move(partial, report, StandardCopyOption.ATOMIC_MOVE);
        return result;
    }

    static void validateChange(SourceSchema.Table table, JsonObject change) throws IOException {
        SourceRow.requireStablePrimaryKey(table, change);
        JsonObject before = SourceRow.image(change, "old"), after = SourceRow.image(change, "new");
        String operation = change.get("operation").getAsString();
        if (!(operation.equals("insert") && before == null && after != null
                || operation.equals("update") && before != null && after != null
                || operation.equals("delete") && before != null && after == null))
            throw new IOException("Invalid source row operation/images: " + operation);
        SourceRow.values(table, before);
        SourceRow.values(table, after);
    }

    static Chain chain(
            SourceSchema.Table table, JsonObject initial, List<Change> input, boolean sourceOrdered)
            throws IOException {
        var pending = new ArrayList<>(input);
        var ordered = new ArrayList<Change>();
        JsonObject state = initial;
        while (!pending.isEmpty()) {
            Change selected = null;
            var sessions = new HashSet<String>();
            Map<String, String> current = SourceRow.values(table, state);
            for (var change : pending) {
                if (!sessions.add(change.session())) continue;
                if (Objects.equals(
                        current,
                        SourceRow.values(table, SourceRow.image(change.source(), "old")))) {
                    selected = change;
                    break;
                }
                if (sourceOrdered) break;
            }
            if (selected == null)
                throw new IOException("Source before-image chain mismatch: " + table.name());
            state = SourceRow.image(selected.source(), "new");
            ordered.add(selected);
            pending.remove(selected);
        }
        return new Chain(state, List.copyOf(ordered));
    }

    private static long compareFinal(
            Connection connection,
            SourceSchema.Table table,
            Path baseline,
            List<JsonObject> files,
            Map<String, JsonObject> replacements)
            throws Exception {
        // Numeric/UTF-8 ordering is explicit on both sides, independent of PG's default collation.
        var primary = SourceRow.primary(table);
        Comparator<String> keys =
                primary.type().equals("integer")
                        ? Comparator.comparingLong(Long::parseLong)
                        : (a, b) ->
                                Arrays.compareUnsigned(
                                        a.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                        b.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var additions = new TreeMap<String, JsonObject>(keys);
        replacements.forEach(
                (key, row) -> {
                    if (row != null) additions.put(key, row);
                });
        var cursors = new ArrayList<Cursor>();
        var queue = new PriorityQueue<Cursor>((a, b) -> keys.compare(a.key, b.key));
        Throwable original = null;
        try {
            for (var file : files) {
                if (!file.get("table").getAsString().equals(table.name())) continue;
                Cursor cursor =
                        new Cursor(
                                table,
                                reader(baseline.resolve(file.get("path").getAsString())),
                                keys);
                cursors.add(cursor);
                if (cursor.advance()) queue.add(cursor);
            }
            String order =
                    quote(primary.name())
                            + (primary.type().equals("integer") ? "" : " COLLATE \"C\"");
            long count = 0;
            String previous = null;
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(8192);
                try (ResultSet actual =
                        statement.executeQuery(
                                "SELECT row_to_json(t)::text FROM "
                                        + quote(table.name())
                                        + " t ORDER BY "
                                        + order)) {
                    while (!queue.isEmpty() || !additions.isEmpty()) {
                        JsonObject expected;
                        String key;
                        if (!additions.isEmpty()
                                && (queue.isEmpty()
                                        || keys.compare(additions.firstKey(), queue.peek().key)
                                                < 0)) {
                            var entry = additions.pollFirstEntry();
                            key = entry.getKey();
                            expected = entry.getValue();
                        } else {
                            Cursor cursor = queue.remove();
                            key = cursor.key;
                            if (key.equals(previous))
                                throw new IOException("Duplicate baseline primary key");
                            previous = key;
                            expected =
                                    replacements.containsKey(key)
                                            ? replacements.get(key)
                                            : cursor.row;
                            additions.remove(key);
                            if (cursor.advance()) queue.add(cursor);
                        }
                        if (expected == null) continue;
                        if (!actual.next()
                                || !Objects.equals(
                                        SourceRow.values(table, expected),
                                        SourceRow.values(
                                                table,
                                                JsonParser.parseString(actual.getString(1))
                                                        .getAsJsonObject())))
                            throw new IOException(
                                    "Final source row mismatch: " + table.name() + "/" + key);
                        count++;
                    }
                    if (actual.next())
                        throw new IOException("Uncaptured final source row: " + table.name());
                }
            }
            return count;
        } catch (Exception error) {
            original = error;
            throw error;
        } finally {
            IOException failure = null;
            for (var cursor : cursors) {
                try {
                    cursor.reader.close();
                } catch (IOException error) {
                    if (failure == null) failure = error;
                    else failure.addSuppressed(error);
                }
            }
            if (failure != null) {
                if (original != null) original.addSuppressed(failure);
                else throw failure;
            }
        }
    }

    private static final class Cursor {
        final SourceSchema.Table table;
        final ParquetReader<Group> reader;
        final Comparator<String> keys;
        String key;
        JsonObject row;

        Cursor(SourceSchema.Table table, ParquetReader<Group> reader, Comparator<String> keys) {
            this.table = table;
            this.reader = reader;
            this.keys = keys;
        }

        boolean advance() throws IOException {
            Group group = reader.read();
            if (group == null) return false;
            row = SourceRow.json(group);
            String next = SourceRow.key(table, row);
            if (key != null && keys.compare(key, next) >= 0)
                throw new IOException(
                        "Baseline file primary keys are not ordered: " + table.name());
            key = next;
            return true;
        }
    }

    public static Map<String, Object> reuse(
            SourceSchema schema, Path baseline, Path raw, Path report) throws Exception {
        if (!Files.exists(report))
            throw new IOException(
                    "Source row audit required before compilation/maintenance: " + report);
        JsonObject value = JsonParser.parseString(Files.readString(report)).getAsJsonObject();
        if (!value.get("status").getAsString().equals("passed_source_row_state")
                || value.get("schema_version").getAsInt() != 1
                || !schema.benchmark().equals(value.get("benchmark").getAsString())
                || !JSON.toJsonTree(schema.tables()).equals(value.get("source_schema"))
                || !JSON.toJsonTree(schema.fingerprints()).equals(value.get("source_schema_sha256"))
                || !JSON.toJsonTree(inputHashes(raw)).equals(value.get("source_inputs_sha256"))
                || !BaselineGenerator.sha256(baseline.resolve("verified-manifest.json"))
                        .equals(value.get("baseline_verified_manifest_sha256").getAsString()))
            throw new IOException("Row audit does not match source/baseline/schema");
        PgBaselineVerify.verifyFiles(baseline);
        Path changes = changesFile(report, value);
        if (!BaselineGenerator.sha256(changes).equals(value.get("changes_sha256").getAsString()))
            throw new IOException("Audited row transitions changed");
        var result = new LinkedHashMap<String, Object>();
        for (var entry : value.entrySet()) result.put(entry.getKey(), entry.getValue());
        return result;
    }

    static Path changesFile(Path report, JsonObject value) throws IOException {
        String name = value.get("changes_file").getAsString();
        if (!Path.of(name).getFileName().toString().equals(name))
            throw new IOException("Invalid audited row transitions path");
        return report.toAbsolutePath().resolveSibling(name);
    }

    static Path resolveRaw(Path raw) throws IOException {
        if (!Files.isDirectory(raw)) return raw;
        JsonObject run =
                JsonParser.parseString(Files.readString(raw.resolve("capture-run.json")))
                        .getAsJsonObject();
        return run.has("sessions") ? raw : raw.resolve(run.get("pg_file").getAsString());
    }

    private static List<Path> sourceFiles(Path raw) throws IOException {
        Path directory = Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent();
        Path manifest = directory.resolve("capture-run.json");
        if (Files.exists(manifest)) {
            JsonObject run = JsonParser.parseString(Files.readString(manifest)).getAsJsonObject();
            if (run.has("sessions")) {
                var files = new ArrayList<Path>();
                for (var session : run.getAsJsonArray("sessions"))
                    files.add(
                            directory.resolve(
                                    session.getAsJsonObject().get("pg_file").getAsString()));
                return files;
            }
            return List.of(directory.resolve(run.get("pg_file").getAsString()));
        }
        return List.of(raw);
    }

    private static Map<String, String> inputHashes(Path raw) throws IOException {
        Path directory = Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent();
        var hashes = new TreeMap<String, String>();
        Path manifest = directory.resolve("capture-run.json");
        if (Files.exists(manifest))
            hashes.put("capture-run.json", BaselineGenerator.sha256(manifest));
        for (Path source : sourceFiles(raw))
            hashes.put(source.getFileName().toString(), BaselineGenerator.sha256(source));
        try (var paths = Files.list(directory)) {
            for (Path path :
                    paths.filter(p -> p.getFileName().toString().startsWith("jdbc-")).toList())
                hashes.put(path.getFileName().toString(), BaselineGenerator.sha256(path));
        }
        return hashes;
    }

    private static ParquetReader<Group> reader(Path path) throws IOException {
        return ParquetReader.builder(
                        new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri()))
                .build();
    }

    private static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
