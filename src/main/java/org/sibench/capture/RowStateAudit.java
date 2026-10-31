package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;
import org.sibench.state.ExternalSort;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
        String password = System.getenv().getOrDefault("PGPASSWORD", "");
        try (Connection connection = DriverManager.getConnection(jdbc, user, password)) {
            return audit(
                    connection,
                    () -> DriverManager.getConnection(jdbc, user, password),
                    schema,
                    baseline,
                    raw,
                    report,
                    SORT_MEMORY_BYTES);
        }
    }

    /**
     * Compares PostgreSQL's final rows with the captured state row by row in key order, which
     * names the first differing row; the default compares order-independent digests instead.
     */
    static final String EXACT = "sibench.audit.exact";

    /** Bytes of committed row changes held in memory before the audit sorts them out of core. */
    static final long SORT_MEMORY_BYTES = 256L << 20;

    static Map<String, Object> audit(
            Connection connection, SourceSchema schema, Path baseline, Path raw, Path report)
            throws Exception {
        return audit(connection, null, schema, baseline, raw, report, SORT_MEMORY_BYTES);
    }

    static Map<String, Object> audit(
            Connection connection,
            SourceSchema schema,
            Path baseline,
            Path raw,
            Path report,
            long sortMemory)
            throws Exception {
        return audit(connection, null, schema, baseline, raw, report, sortMemory);
    }

    /**
     * Committed changes are sorted by (table, primary key, session, position) outside memory, then
     * merged in key order with the baseline rows, so memory holds one key's changes at a time
     * however many rows changed; the rows that yields are compared with PostgreSQL's final table
     * ({@link FinalState}). {@code workers}, when given, open connections that read that table in
     * parallel.
     */
    static Map<String, Object> audit(
            Connection connection,
            PgBaselineVerify.Connections workers,
            SourceSchema schema,
            Path baseline,
            Path raw,
            Path report,
            long sortMemory)
            throws Exception {
        if (Files.exists(report)) return reuse(schema, baseline, raw, report);
        CaptureRun.verifyInput(resolveRaw(raw));
        PgBaselineVerify.verifyFiles(baseline);
        if (!connection.getAutoCommit())
            throw new IOException("Row audit requires a fresh autocommit connection");
        Map<String, String> inputs = inputHashes(raw, false);
        var tables = new LinkedHashMap<String, SourceSchema.Table>();
        var tableOrder = new HashMap<String, Integer>();
        for (var table : schema.tables()) {
            tableOrder.put(table.name(), tables.size());
            tables.put(table.name(), table);
        }
        long committed = 0, discarded = 0, commits = 0, aborts = 0;
        List<Path> sources = sourceFiles(raw);
        Path transitions =
                report.toAbsolutePath().resolveSibling(report.getFileName() + ".changes.jsonl");
        Path transitionsPartial =
                transitions.resolveSibling(transitions.getFileName() + ".partial");
        Path work = report.toAbsolutePath().resolveSibling(report.getFileName() + ".work");
        if (Files.exists(transitionsPartial) || Files.exists(work))
            throw new IOException("Unfinished row audit output exists; use a fresh report path");
        try (var sort = new ExternalSort(work, sortMemory)) {
            // Sessions are read concurrently; every sort key is distinct, so the sorted changes do
            // not depend on the order they were added in.
            var pool = java.util.concurrent.Executors.newFixedThreadPool(
                    Math.max(1, Math.min(sources.size(), Runtime.getRuntime().availableProcessors())));
            var collected = new ArrayList<java.util.concurrent.Future<long[]>>();
            try {
                for (int sourceIndex = 0; sourceIndex < sources.size(); sourceIndex++) {
                    int own = sourceIndex;
                    collected.add(pool.submit(() -> collect(sources.get(own), own, tables, tableOrder, sort)));
                }
                for (var future : collected) {
                    long[] counts;
                    try {
                        counts = future.get();
                    } catch (java.util.concurrent.ExecutionException failed) {
                        if (failed.getCause() instanceof Exception error) throw error;
                        throw failed;
                    }
                    committed += counts[0];
                    discarded += counts[1];
                    commits += counts[2];
                    aborts += counts[3];
                }
            } finally {
                pool.shutdownNow();
            }
            List<JsonObject> files =
                    Files.readAllLines(baseline.resolve("files.jsonl")).stream()
                            .map(line -> JsonParser.parseString(line).getAsJsonObject())
                            .toList();
            for (var file : files)
                if (!tables.containsKey(file.get("table").getAsString()))
                    throw new IOException("Unknown baseline table");
            var tableReports = new ArrayList<Map<String, Object>>();
            boolean complete = false;
            try (var groups = new Groups(sort.sorted(), tables, tableOrder, sources);
                    var output =
                            Files.newBufferedWriter(
                                    transitionsPartial, StandardOpenOption.CREATE_NEW)) {
                connection.setReadOnly(true);
                connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                connection.setAutoCommit(false);
                try {
                    for (var table : schema.tables()) {
                        var counts =
                                compareFinal(
                                        connection,
                                        table,
                                        baseline,
                                        files,
                                        groups,
                                        sources.size() == 1,
                                        output,
                                        workers);
                        tableReports.add(
                                Map.of(
                                        "table",
                                        table.name(),
                                        "baseline_rows",
                                        counts.baselineRows(),
                                        "final_rows",
                                        counts.finalRows(),
                                        "committed_mutations",
                                        counts.mutations(),
                                        "changed_keys",
                                        counts.changedKeys()));
                    }
                    if (groups.hasNext())
                        throw new IOException("Committed changes of an unknown table or key");
                    connection.commit();
                    complete = true;
                } catch (Exception error) {
                    try {
                        connection.rollback();
                    } catch (SQLException rollback) {
                        error.addSuppressed(rollback);
                    }
                    throw error;
                }
            } finally {
                if (!complete) Files.deleteIfExists(transitionsPartial);
            }
            // Publish transitions first; only the final report makes the audit consumable.
            Files.move(transitionsPartial, transitions, StandardCopyOption.ATOMIC_MOVE);
            if (!inputs.equals(inputHashes(raw, true)))
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
    }

    private static final String EVENT = "{\"event\":\"";

    /**
     * One session's committed row changes into the sort; {committed, discarded, commits, aborts}.
     * Only row changes and transaction records are parsed: a line naming another event type in
     * its first field is counted and passed over.
     */
    private static long[] collect(
            Path source,
            int sourceIndex,
            Map<String, SourceSchema.Table> tables,
            Map<String, Integer> tableOrder,
            ExternalSort sort)
            throws IOException {
        long committed = 0, discarded = 0, commits = 0, aborts = 0;
        var pending = new ArrayList<Change>();
        long xid = 0, position = 0;
        try (var reader = ServerTrace.open(source)) {
            String line;
            while ((line = reader.readLine()) != null) {
                position++;
                if (line.startsWith(EVENT)
                        && !line.startsWith("row_change\"", EVENT.length())
                        && !line.startsWith("transaction_commit\"", EVENT.length())
                        && !line.startsWith("transaction_abort\"", EVENT.length()))
                    continue;
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
                        // Concurrent sessions interleave by PostgreSQL's completion
                        // counter, not by the session a change came from: a change's
                        // before-image is the row as of its own commit, so the chain of
                        // one key only lines up in that order.
                        // Sessions that never overlap need no counter: then the order is
                        // the session order. Concurrent compilation requires one.
                        long csn = event.has("csn") ? event.get("csn").getAsLong() : 0;
                        for (Change change : pending) {
                            var table =
                                    tables.get(change.source().get("relation").getAsString());
                            JsonObject row = SourceRow.image(change.source(), "new");
                            if (row == null) row = SourceRow.image(change.source(), "old");
                            byte[] key =
                                    sortKey(
                                            tableOrder.get(table.name()),
                                            table,
                                            SourceRow.key(table, row),
                                            csn,
                                            sourceIndex,
                                            change.position());
                            byte[] value = EVENTS.toJson(change.source()).getBytes(StandardCharsets.UTF_8);
                            synchronized (sort) {
                                sort.add(key, value);
                            }
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
        return new long[] {committed, discarded, commits, aborts};
    }

    /**
     * Sort key of one committed change: table, the primary key in PostgreSQL's btree order (the
     * order of the baseline files and of the final scan), then the change's place in the capture.
     */
    /**
     * (table, primary key, commit position, session, position). The commit position is
     * PostgreSQL's transaction completion counter: it is what orders the changes of one key when
     * several sessions write it, and it is distinct for every writing transaction.
     */
    private static byte[] sortKey(
            int table, SourceSchema.Table schemaTable, String key, long csn, int session,
            long position)
            throws IOException {
        var primary = SourceRow.primary(schemaTable);
        IndexKey encoded =
                primary.type().equals("integer")
                        ? IndexKey.ofInt(Long.parseLong(key))
                        : IndexKey.ofText(key);
        return ByteBuffer.allocate(4 + encoded.length() + 8 + 4 + 8)
                .putInt(table)
                .put(encoded.bytes())
                .putLong(csn)
                .putInt(session)
                .putLong(position)
                .array();
    }

    /** One primary key's committed changes, in capture order within each session. */
    private record KeyChanges(String key, List<Change> changes) {}

    /** The sorted changes as groups of one key, table after table in schema order. */
    private static final class Groups implements AutoCloseable {
        private final ExternalSort.Sorted sorted;
        private final Map<String, SourceSchema.Table> tables;
        private final List<String> tableNames;
        private final List<Path> sources;
        private KeyChanges next;
        private int nextTable = -1;

        Groups(
                ExternalSort.Sorted sorted,
                Map<String, SourceSchema.Table> tables,
                Map<String, Integer> order,
                List<Path> sources)
                throws IOException {
            this.sorted = sorted;
            this.tables = tables;
            this.sources = sources;
            var names = new String[order.size()];
            order.forEach((name, index) -> names[index] = name);
            this.tableNames = List.of(names);
            load();
        }

        boolean hasNext() {
            return next != null;
        }

        int indexOf(String table) {
            return tableNames.indexOf(table);
        }

        int nextTable() {
            return nextTable;
        }

        KeyChanges peek() {
            return next;
        }

        KeyChanges take() throws IOException {
            KeyChanges result = next;
            load();
            return result;
        }

        private void load() throws IOException {
            if (!sorted.hasNext()) {
                next = null;
                nextTable = -1;
                return;
            }
            byte[] first = sorted.peekKey();
            int table = ByteBuffer.wrap(first).getInt();
            int prefixLength = first.length - 20;
            var changes = new ArrayList<Change>();
            String key = null;
            while (sorted.hasNext()
                    && Arrays.equals(
                            first,
                            0,
                            prefixLength,
                            sorted.peekKey(),
                            0,
                            sorted.peekKey().length - 20)) {
                ByteBuffer tail = ByteBuffer.wrap(sorted.peekKey(), prefixLength, 20);
                tail.getLong();
                int session = tail.getInt();
                long position = tail.getLong();
                JsonObject event =
                        JsonParser.parseString(
                                        new String(sorted.peekValue(), StandardCharsets.UTF_8))
                                .getAsJsonObject();
                var schemaTable = tables.get(tableNames.get(table));
                JsonObject row = SourceRow.image(event, "new");
                if (row == null) row = SourceRow.image(event, "old");
                key = SourceRow.key(schemaTable, row);
                changes.add(new Change(event, sources.get(session).getFileName().toString(), position));
                sorted.advance();
            }
            next = new KeyChanges(key, changes);
            nextTable = table;
        }

        @Override
        public void close() throws IOException {
            sorted.close();
        }
    }

    private record TableCounts(long baselineRows, long finalRows, long mutations, long changedKeys) {}

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

    /**
     * Merges the table's baseline rows (files may interleave; each is in key order) and the
     * committed changes of its keys. Every key in either input yields the row the capture says it
     * should now be; PostgreSQL's final table must hold exactly those rows, which its digest shows
     * (or, with {@link #EXACT}, its ordered scan row by row).
     */
    private static TableCounts compareFinal(
            Connection connection,
            SourceSchema.Table table,
            Path baseline,
            List<JsonObject> files,
            Groups groups,
            boolean sourceOrdered,
            Writer transitions,
            PgBaselineVerify.Connections workers)
            throws Exception {
        // Numeric/UTF-8 ordering is explicit on both sides, independent of PG's default collation.
        var primary = SourceRow.primary(table);
        Comparator<String> keys =
                primary.type().equals("integer")
                        ? Comparator.comparingLong(Long::parseLong)
                        : (a, b) ->
                                Arrays.compareUnsigned(
                                        a.getBytes(StandardCharsets.UTF_8),
                                        b.getBytes(StandardCharsets.UTF_8));
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
                                keys,
                                file.get("rows").getAsLong());
                cursors.add(cursor);
                if (cursor.advance()) queue.add(cursor);
            }
            int tableIndex = tableIndex(groups, table);
            String order =
                    quote(primary.name())
                            + (primary.type().equals("integer") ? "" : " COLLATE \"C\"");
            long count = 0, mutations = 0, changedKeys = 0, baselineRows = 0;
            String previous = null;
            boolean exact = Boolean.getBoolean(EXACT);
            var captured = new FinalState.Digest();
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(8192);
                // Columns as text, compared column by column: no JSON round trip on either side.
                try (ResultSet actual =
                        exact
                                ? statement.executeQuery(
                                        "SELECT * FROM "
                                                + quote(table.name())
                                                + " t ORDER BY "
                                                + order)
                                : null) {
                    while (!queue.isEmpty() || groups.nextTable() == tableIndex) {
                        boolean hasGroup = groups.nextTable() == tableIndex;
                        KeyChanges changes = null;
                        JsonObject expected;
                        String key;
                        if (hasGroup
                                && (queue.isEmpty()
                                        || keys.compare(groups.peek().key(), queue.peek().key)
                                                <= 0)) {
                            changes = groups.take();
                            key = changes.key();
                            JsonObject initial = null;
                            if (!queue.isEmpty() && keys.compare(key, queue.peek().key) == 0) {
                                Cursor cursor = queue.remove();
                                if (key.equals(previous))
                                    throw new IOException("Duplicate baseline primary key");
                                previous = key;
                                initial = cursor.row;
                                if (cursor.advance()) queue.add(cursor);
                            }
                            Chain chain = chain(table, initial, changes.changes(), sourceOrdered);
                            for (var change : chain.changes()) {
                                JsonObject value = change.source().deepCopy();
                                value.addProperty("source_session", change.session());
                                value.addProperty("source_position", change.position());
                                transitions.write(EVENTS.toJson(value));
                                transitions.write('\n');
                            }
                            mutations += chain.changes().size();
                            changedKeys++;
                            expected = chain.last();
                        } else {
                            Cursor cursor = queue.remove();
                            key = cursor.key;
                            if (key.equals(previous))
                                throw new IOException("Duplicate baseline primary key");
                            previous = key;
                            expected = cursor.row;
                            if (cursor.advance()) queue.add(cursor);
                        }
                        if (expected == null) continue;
                        if (!exact) captured.add(table, SourceRow.values(table, expected));
                        else if (!actual.next()
                                || !Objects.equals(
                                        SourceRow.values(table, expected), finalRow(table, actual)))
                            throw new IOException(
                                    "Final source row mismatch: " + table.name() + "/" + key);
                        count++;
                    }
                    if (exact && actual.next())
                        throw new IOException("Uncaptured final source row: " + table.name());
                }
            }
            if (!exact) {
                var postgres = FinalState.scan(connection, workers, table);
                if (!captured.same(postgres))
                    throw new IOException(
                            "Final source rows differ from the captured state: "
                                    + table.name()
                                    + ": captured "
                                    + captured
                                    + ", PostgreSQL "
                                    + postgres
                                    + "; audit again with -D"
                                    + EXACT
                                    + "=true to name the first differing row");
            }
            for (var cursor : cursors) {
                if (cursor.count != cursor.expectedRows)
                    throw new IOException("Baseline row count mismatch");
                baselineRows += cursor.count;
            }
            return new TableCounts(baselineRows, count, mutations, changedKeys);
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

    /** One final PG row in the comparable form {@link SourceRow#values} gives an image. */
    static Map<String, String> finalRow(SourceSchema.Table table, ResultSet row)
            throws Exception {
        var values = new java.util.LinkedHashMap<String, String>();
        for (var column : table.columns()) {
            String text = row.getString(column.name());
            if (text == null && !column.nullable())
                throw new IOException("Null required column: " + table.name() + "." + column.name());
            values.put(column.name(), SourceRow.normalize(column, text));
        }
        return values;
    }

    /** Position of the table in schema order, which is how the sorted changes are grouped. */
    private static int tableIndex(Groups groups, SourceSchema.Table table) {
        return groups.indexOf(table.name());
    }

    private static final class Cursor {
        final SourceSchema.Table table;
        final ParquetReader<Group> reader;
        final Comparator<String> keys;
        final long expectedRows;
        long count;
        String key;
        JsonObject row;

        Cursor(
                SourceSchema.Table table,
                ParquetReader<Group> reader,
                Comparator<String> keys,
                long expectedRows) {
            this.table = table;
            this.reader = reader;
            this.keys = keys;
            this.expectedRows = expectedRows;
        }

        boolean advance() throws IOException {
            Group group = reader.read();
            if (group == null) return false;
            count++;
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
                || !JSON.toJsonTree(inputHashes(raw, false)).equals(value.get("source_inputs_sha256"))
                || !BaselineGenerator.sha256(baseline.resolve("verified-manifest.json"))
                        .equals(value.get("baseline_verified_manifest_sha256").getAsString()))
            throw new IOException("Row audit does not match source/baseline/schema");
        PgBaselineVerify.verifyFiles(baseline);
        Path changes = changesFile(report, value);
        if (!FileDigests.sha256(changes).equals(value.get("changes_sha256").getAsString()))
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

    /** {@code fresh} reads every file again instead of trusting this process's earlier digests. */
    private static Map<String, String> inputHashes(Path raw, boolean fresh) throws IOException {
        Path directory = Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent();
        var hashes = new TreeMap<String, String>();
        Path manifest = directory.resolve("capture-run.json");
        if (Files.exists(manifest))
            hashes.put("capture-run.json", digest(manifest, fresh));
        for (Path source : sourceFiles(raw))
            hashes.put(source.getFileName().toString(), digest(source, fresh));
        try (var paths = Files.list(directory)) {
            for (Path path :
                    paths.filter(p -> p.getFileName().toString().startsWith("jdbc-")).toList())
                hashes.put(path.getFileName().toString(), digest(path, fresh));
        }
        return hashes;
    }

    private static String digest(Path file, boolean fresh) throws IOException {
        return fresh ? BaselineGenerator.sha256(file) : FileDigests.sha256(file);
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
