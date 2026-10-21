package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.generation.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/** Maps supported single-session reads and committed changes; aborts remain in the raw trace. */
public final class IndexTraceCompiler {
    private static final Map<String, Integer> TABLE_IDS = new LinkedHashMap<>();

    static {
        int i = 1;
        for (String name : PgBaselineExport.TABLES.keySet()) TABLE_IDS.put(name, i++);
    }

    private IndexTraceCompiler() {}

    private static final class Search {
        final JsonObject captured;
        long count;

        Search(
                long event,
                long key,
                int table,
                int index,
                long readTimestamp,
                long precedingMutations,
                long workUnitId) {
            this.captured = new JsonObject();
            captured.addProperty("event", "search");
            captured.addProperty("source_event", event);
            captured.addProperty("table", table);
            captured.addProperty("index", index);
            captured.addProperty("key", key);
            captured.addProperty("read_timestamp", readTimestamp);
            captured.addProperty("preceding_mutations", precedingMutations);
            captured.addProperty("work_unit_id", workUnitId);
        }
    }

    private static final class Output implements AutoCloseable {
        final Path path;
        final ParquetDataFile writer;
        final MessageType schema;
        final long fileId;
        long rows;

        Output(Path path, MessageType schema, GenerationConfig config, String table, long fileId)
                throws IOException {
            this.path = path;
            this.fileId = fileId;
            var fields = new ArrayList<>(schema.getFields());
            fields.add(
                    org.apache.parquet.schema.Types.required(
                                    org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64)
                            .named("_sib_mutation_ordinal"));
            this.schema = new MessageType(schema.getName(), fields);
            writer =
                    new ParquetDataFile(
                            path,
                            new TableData(
                                    table,
                                    0,
                                    0,
                                    PgBaselineExport.TABLES.get(table),
                                    this.schema,
                                    null),
                            config,
                            Map.of("sibench.source", "committed-pg-row-changes"));
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4)
            throw new IllegalArgumentException(
                    "IndexTraceCompiler ROOT CAPTURE_CONFIG RAW_RUNTIME_PG OUTPUT");
        CaptureConfig capture = CaptureConfig.read(Path.of(args[0]), Path.of(args[1]));
        compile(capture, Path.of(args[2]), Path.of(args[3]));
    }

    public static void compile(CaptureConfig capture, Path raw, Path output) throws Exception {
        CaptureRun.verifyInput(raw);
        Path rawDirectory = Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent();
        Path runManifest = rawDirectory.resolve("capture-run.json");
        boolean htapInput =
                Files.exists(runManifest)
                        && JsonParser.parseString(Files.readString(runManifest))
                                .getAsJsonObject()
                                .has("sessions");
        if (htapInput || capture != null && capture.mode().equals("runhtap"))
            throw new IOException(
                    "Native runHTAP requires concurrent snapshot/range/variable-key projection; use"
                            + " the lossless PgTraceBlocks container, not the single-session"
                            + " compiler");
        Path baseline = capture.layout().output();
        PgBaselineVerify.verifyFiles(baseline);
        Path verified = baseline.resolve("verified-manifest.json");
        JsonObject verification =
                JsonParser.parseString(Files.readString(verified)).getAsJsonObject();
        Files.createDirectory(output);
        Files.writeString(
                output.resolve("INCOMPLETE"),
                "Compilation not committed\n",
                StandardOpenOption.CREATE_NEW);
        Map<String, MessageType> schemas = new HashMap<>();
        IndexCatalog catalog = IndexCatalog.read(capture.source());
        var identities = new IndexReferenceReplay(true, false);
        long maximumRowId = 0, maximumFileId = 0, baselineCount, baselineRows = 0;
        try (var operations = new IndexOperationTrace.Writer(output.resolve("baseline.ops"))) {
            for (String line : Files.readAllLines(baseline.resolve("files.jsonl"))) {
                JsonObject file = JsonParser.parseString(line).getAsJsonObject();
                String table = file.get("table").getAsString();
                long fileId = file.get("file_id").getAsLong();
                maximumFileId = Math.max(maximumFileId, fileId);
                long position = 0;
                try (ParquetReader<Group> reader =
                        ParquetReader.builder(
                                        new GroupReadSupport(),
                                        new org.apache.hadoop.fs.Path(
                                                baseline.resolve(file.get("path").getAsString())
                                                        .toUri()))
                                .build()) {
                    Group row;
                    while ((row = reader.read()) != null) {
                        schemas.putIfAbsent(
                                table, new MessageType(table, row.getType().getFields()));
                        long key = row.getLong(PgBaselineExport.TABLES.get(table), 0),
                                rowId = row.getLong("_sib_row_id", 0);
                        maximumRowId = Math.max(maximumRowId, rowId);
                        Operation primary =
                                new Operation(
                                                0,
                                                IndexOperationTrace.INSERT,
                                                TABLE_IDS.get(table),
                                                0,
                                                0,
                                                key,
                                                0,
                                                fileId,
                                                position++,
                                                rowId,
                                                0,
                                                0,
                                                0)
                                        .withWorkUnitId(
                                                1
                                                        + baselineRows++
                                                                / IndexOperationTrace
                                                                        .BLOCK_RECORDS);
                        identities.accept(primary);
                        IndexMutation.emit(
                                primary, Map.of(), catalog.keys(table, row), operations::append);
                    }
                }
                if (position != file.get("rows").getAsLong())
                    throw new IOException("Baseline manifest row mismatch");
            }
            baselineCount = operations.count();
        }
        final long firstIncrementalFile = maximumFileId;
        Map<String, Output> writers = new LinkedHashMap<>();
        Map<String, Search> scans = new LinkedHashMap<>();
        List<JsonObject> pending = new ArrayList<>();
        long event = 0, version = 0, operationsCount, mutations = 0, searches = 0, rawInserts = 0;
        long inserts = 0, updates = 0, deletes = 0, commits = 0;
        long rawMutations = 0, aborts = 0, discardedMutations = 0, discardedIndexInserts = 0;
        long pendingIndexInserts = 0;
        long pendingMutations = 0, discardedSearches = 0, localSearches = 0;
        int expectedPid = 0;
        long pendingXid = 0;
        boolean transactionOpen = false;
        List<Map<String, Object>> files = new ArrayList<>();
        Map<String, Long> requestUnits = new LinkedHashMap<>();
        Set<Long> emittedUnits = new HashSet<>();
        try (var reader = Files.newBufferedReader(raw);
                var operations = new IndexOperationTrace.Writer(output.resolve("workload.ops"));
                var units =
                        Files.newBufferedWriter(
                                output.resolve("work-units.jsonl"),
                                StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = reader.readLine()) != null) {
                event++;
                JsonObject e = JsonParser.parseString(line).getAsJsonObject();
                int pid = e.get("pid").getAsInt();
                if (expectedPid == 0) expectedPid = pid;
                else if (pid != expectedPid)
                    throw new IOException("Single-session projection only");
                String type = e.get("event").getAsString();
                String application = e.get("application").getAsString();
                if (type.equals("executor_start")) {
                    if (requestUnits.containsKey(application))
                        throw new IOException("Duplicate source request identity");
                    long id = requestUnits.size() + 1L;
                    requestUnits.put(application, id);
                    JsonObject identity = new JsonObject();
                    identity.addProperty("work_unit_id", id);
                    identity.addProperty("application", application);
                    identity.addProperty("pid", pid);
                    identity.addProperty("source_event", event);
                    units.write(identity.toString());
                    units.newLine();
                }
                if (type.equals("executor_start")
                        || type.equals("scan_rescan")
                        || type.equals("row_change")
                        || type.equals("index_insert_attempt")) transactionOpen = true;
                if (type.equals("scan_rescan")) {
                    String index = e.get("relation").getAsString();
                    var definition = catalog.named(index);
                    int table = definition.tableId(), indexId = definition.indexId();
                    JsonArray keys = e.getAsJsonArray("keys");
                    if (keys.size() != 1)
                        throw new IOException("Only single equality scan supported");
                    JsonObject k = keys.get(0).getAsJsonObject();
                    if (k.get("strategy").getAsInt() != 3 || k.get("flags").getAsInt() != 0)
                        throw new IOException("Unsupported scan predicate");
                    String id = e.get("scan").getAsString();
                    Search search =
                            new Search(
                                    event,
                                    indexId == 0
                                            ? k.get("value").getAsLong()
                                            : definition.type().equals("int")
                                                    ? k.get("value").getAsLong()
                                                    : 0,
                                    table,
                                    indexId,
                                    version,
                                    pendingMutations,
                                    requireWorkUnit(requestUnits, application));
                    if (indexId > 0)
                        search.captured.addProperty(
                                "indexed_value",
                                definition.normalize(
                                        k.get("value").isJsonNull()
                                                ? null
                                                : k.get("value").getAsString()));
                    if (scans.put(id, search) != null)
                        throw new IOException("Repeated live scan identity");
                    pending.add(search.captured);
                } else if (type.equals("bitmap_candidate") || type.equals("heap_visibility")) {
                    Search search = scans.get(e.get("scan").getAsString());
                    if (search == null) throw new IOException("Unmatched candidate event");
                    if (e.get("found").getAsBoolean()) search.count++;
                } else if (type.equals("scan_end")) {
                    Search search = scans.remove(e.get("scan").getAsString());
                    if (search == null) throw new IOException("Unmatched scan end");
                    search.captured.addProperty("count", search.count);
                } else if (type.equals("row_change")) {
                    long xid = e.get("xid").getAsLong();
                    if (pendingMutations != 0 && pendingXid != xid)
                        throw new IOException("Interleaved transactions unsupported");
                    pendingXid = xid;
                    e.addProperty("source_event", event);
                    e.addProperty("work_unit_id", requireWorkUnit(requestUnits, application));
                    pending.add(e);
                    rawMutations++;
                    pendingMutations++;
                } else if (type.equals("index_insert_attempt")) {
                    rawInserts++;
                    pendingIndexInserts++;
                } else if (type.equals("transaction_abort")) {
                    if (!scans.isEmpty()) throw new IOException("Abort with unfinished scans");
                    if (pendingMutations != 0 && pendingXid != e.get("xid").getAsLong())
                        throw new IOException("Abort XID mismatch");
                    aborts++;
                    discardedMutations += pendingMutations;
                    discardedIndexInserts += pendingIndexInserts;
                    for (JsonObject item : pending) {
                        if (!item.get("event").getAsString().equals("search")) continue;
                        if (item.get("preceding_mutations").getAsLong() != 0) {
                            discardedSearches++;
                            continue;
                        }
                        operations.append(
                                IndexOperationTrace.search(
                                                item.get("source_event").getAsLong(),
                                                item.get("table").getAsInt(),
                                                item.get("index").getAsInt(),
                                                item.get("key").getAsLong(),
                                                item.get("read_timestamp").getAsLong(),
                                                item.get("count").getAsLong(),
                                                0)
                                        .withWorkUnitId(item.get("work_unit_id").getAsLong())
                                        .withIndex(
                                                item.get("index").getAsInt(),
                                                item.has("indexed_value")
                                                                && !item.get("indexed_value")
                                                                        .isJsonNull()
                                                        ? item.get("indexed_value").getAsString()
                                                        : null,
                                                IndexOperationTrace.SEARCH));
                        emittedUnits.add(item.get("work_unit_id").getAsLong());
                        searches++;
                    }
                    pending.clear();
                    pendingXid = 0;
                    pendingIndexInserts = 0;
                    pendingMutations = 0;
                    transactionOpen = false;
                } else if (type.equals("transaction_commit")) {
                    if (!scans.isEmpty()) throw new IOException("Commit with unfinished scans");
                    if (pendingMutations != 0 && pendingXid != e.get("xid").getAsLong())
                        throw new IOException("Commit XID mismatch");
                    version = Math.incrementExact(version);
                    commits++;
                    long transactionId = event, ordinal = 0;
                    operations.append(
                            IndexOperationTrace.transaction(
                                    event,
                                    IndexOperationTrace.TRANSACTION_BEGIN,
                                    transactionId,
                                    version,
                                    0));
                    for (JsonObject change : pending) {
                        long workUnitId = change.get("work_unit_id").getAsLong();
                        emittedUnits.add(workUnitId);
                        if (change.get("event").getAsString().equals("search")) {
                            long preceding = change.get("preceding_mutations").getAsLong();
                            boolean local = preceding != 0;
                            operations.append(
                                    IndexOperationTrace.transactionSearch(
                                                    change.get("source_event").getAsLong(),
                                                    change.get("table").getAsInt(),
                                                    change.get("index").getAsInt(),
                                                    change.get("key").getAsLong(),
                                                    local
                                                            ? version
                                                            : change.get("read_timestamp")
                                                                    .getAsLong(),
                                                    change.get("count").getAsLong(),
                                                    transactionId,
                                                    preceding,
                                                    local)
                                            .withWorkUnitId(workUnitId)
                                            .withIndex(
                                                    change.get("index").getAsInt(),
                                                    change.has("indexed_value")
                                                                    && !change.get("indexed_value")
                                                                            .isJsonNull()
                                                            ? change.get("indexed_value")
                                                                    .getAsString()
                                                            : null,
                                                    IndexOperationTrace.SEARCH));
                            searches++;
                            if (local) localSearches++;
                            continue;
                        }
                        ordinal++;
                        String table = change.get("relation").getAsString();
                        int tableId = IndexCatalog.tableId(table);
                        String operation = change.get("operation").getAsString();
                        JsonObject oldValue =
                                change.has("old") && change.get("old").isJsonObject()
                                        ? change.getAsJsonObject("old")
                                        : null;
                        if (!operation.equals("insert") && oldValue == null)
                            throw new IOException("Missing before-image for row change");
                        Map<Integer, String> before =
                                operation.equals("insert")
                                        ? Map.of()
                                        : catalog.keys(table, oldValue);
                        if (operation.equals("delete")) {
                            long key =
                                    change.getAsJsonObject("old")
                                            .get(PgBaselineExport.TABLES.get(table))
                                            .getAsLong();
                            var previous = identities.lookupPrimary(tableId, key, version);
                            if (previous == null) throw new IOException("Deletion of absent row");
                            Operation primary =
                                    new Operation(
                                                    change.get("source_event").getAsLong(),
                                                    IndexOperationTrace.LOGICAL_DELETE,
                                                    TABLE_IDS.get(table),
                                                    0,
                                                    0,
                                                    key,
                                                    version,
                                                    0,
                                                    0,
                                                    previous.value(),
                                                    0,
                                                    0,
                                                    0,
                                                    transactionId,
                                                    ordinal)
                                            .withWorkUnitId(workUnitId);
                            identities.accept(primary);
                            IndexMutation.emit(primary, before, Map.of(), operations::append);
                            deletes++;
                            mutations++;
                            continue;
                        }
                        if (!Set.of("insert", "update").contains(operation))
                            throw new IOException("Unsupported change kind");
                        JsonObject value = change.getAsJsonObject("new");
                        long key = value.get(PgBaselineExport.TABLES.get(table)).getAsLong();
                        var previous = identities.lookupPrimary(tableId, key, version);
                        if (operation.equals("update")
                                && (previous == null
                                        || oldValue.get(PgBaselineExport.TABLES.get(table))
                                                        .getAsLong()
                                                != key))
                            throw new IOException(
                                    "Update requires an existing unchanged primary key");
                        if (operation.equals("insert") && previous != null)
                            throw new IOException("Insertion of existing primary key");
                        long rowId =
                                operation.equals("update")
                                        ? previous.value()
                                        : (maximumRowId = Math.incrementExact(maximumRowId));
                        Output destination = writers.get(table);
                        if (destination == null) {
                            if (!schemas.containsKey(table))
                                throw new IOException("Missing baseline table schema");
                            destination =
                                    new Output(
                                            output.resolve(table + "-versions.parquet"),
                                            schemas.get(table),
                                            capture.layout(),
                                            table,
                                            firstIncrementalFile + tableId);
                            writers.put(table, destination);
                        }
                        Group row = new SimpleGroupFactory(destination.schema).newGroup();
                        for (var field : destination.schema.getFields()) {
                            String name = field.getName();
                            if (name.startsWith("_sib_")) continue;
                            JsonElement v = value.get(name);
                            if (v == null || v.isJsonNull()) continue;
                            switch (field.asPrimitiveType().getPrimitiveTypeName()) {
                                case INT64 -> row.add(name, v.getAsLong());
                                case DOUBLE -> row.add(name, (double) v.getAsFloat());
                                default -> row.add(name, v.getAsString());
                            }
                        }
                        row.add("_sib_row_id", rowId);
                        row.add("_sib_timestamp", version);
                        row.add("_sib_mutation_ordinal", ordinal);
                        Operation primary =
                                new Operation(
                                                change.get("source_event").getAsLong(),
                                                IndexOperationTrace.UPSERT,
                                                TABLE_IDS.get(table),
                                                0,
                                                operation.equals("insert")
                                                        ? IndexOperationTrace.BUSINESS_INSERT
                                                        : IndexOperationTrace.BUSINESS_UPDATE,
                                                key,
                                                version,
                                                destination.fileId,
                                                destination.rows++,
                                                rowId,
                                                0,
                                                0,
                                                0,
                                                transactionId,
                                                ordinal)
                                        .withWorkUnitId(workUnitId);
                        identities.accept(primary);
                        IndexMutation.emit(
                                primary, before, catalog.keys(table, value), operations::append);
                        if (destination.writer.write(row))
                            throw new IOException(
                                    "PoC incremental file reached rollover limit; extend compiler"
                                            + " before larger run");
                        mutations++;
                        if (operation.equals("insert")) inserts++;
                        else updates++;
                    }
                    operations.append(
                            IndexOperationTrace.transaction(
                                    event,
                                    IndexOperationTrace.TRANSACTION_COMMIT,
                                    transactionId,
                                    version,
                                    ordinal));
                    pending.clear();
                    pendingXid = 0;
                    pendingIndexInserts = 0;
                    pendingMutations = 0;
                    transactionOpen = false;
                } else if (!Set.of(
                                "executor_start",
                                "executor_end",
                                "executor_run_end",
                                "scan_next",
                                "bitmap_end",
                                "result_row")
                        .contains(type)) {
                    throw new IOException("Unsupported raw event: " + type);
                }
                if (pending.size() > org.sibench.replay.ReplayProjection.MAX_TRANSACTION_OPERATIONS)
                    throw new IOException("Source transaction exceeds operation buffer limit");
            }
            if (transactionOpen || !pending.isEmpty() || !scans.isEmpty())
                throw new IOException("Truncated runtime capture");
            for (var entry : writers.entrySet()) {
                Output writer = entry.getValue();
                var footer = writer.writer.finish();
                writer.close();
                long decoded = 0;
                try (ParquetReader<Group> check =
                        ParquetReader.builder(
                                        new GroupReadSupport(),
                                        new org.apache.hadoop.fs.Path(writer.path.toUri()))
                                .build()) {
                    while (check.read() != null) decoded++;
                }
                if (decoded != writer.rows)
                    throw new IOException("Incremental Parquet row mismatch");
                files.add(
                        Map.of(
                                "table",
                                entry.getKey(),
                                "file_id",
                                writer.fileId,
                                "path",
                                writer.path.getFileName().toString(),
                                "rows",
                                writer.rows,
                                "sha256",
                                BaselineGenerator.sha256(writer.path),
                                "row_groups",
                                footer.getBlocks().stream().map(b -> b.getRowCount()).toList()));
            }
            operationsCount = operations.count();
        } finally {
            for (Output writer : writers.values()) writer.close();
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("status", "complete_for_declared_projection");
        report.put("operation_trace_version", IndexOperationTrace.VERSION);
        report.put("secondary_operations_explicit", true);
        report.put("index_definitions", catalog.describe());
        report.put("secondary_index_coverage", "all_source_definitions");
        report.put("source_directory", capture.source().toAbsolutePath().toString());
        report.put("source_schema_sha256", IndexCatalog.fingerprints(capture.source()));
        report.put("baseline_directory", baseline.toAbsolutePath().toString());
        report.put("baseline_operations", baselineCount);
        report.put("baseline_rows", verification.get("rows").getAsLong());
        report.put(
                "baseline_work_units",
                (baselineRows + IndexOperationTrace.BLOCK_RECORDS - 1)
                        / IndexOperationTrace.BLOCK_RECORDS);
        report.put(
                "baseline_work_unit_rule",
                "consecutive batches of 4096 source rows; index expansion shares a unit");
        report.put("source_work_units", requestUnits.size());
        report.put("workload_work_units", emittedUnits.size());
        report.put(
                "work_unit_rule",
                "one audited source SQL request, numbered before abort/secondary filtering");
        report.put(
                "work_units_sha256", BaselineGenerator.sha256(output.resolve("work-units.jsonl")));
        report.put("workload_operations", operationsCount);
        report.put("searches", searches);
        report.put("logical_row_version_writes", mutations);
        report.put("change_delivery", "committed_only");
        report.put("raw_row_changes", rawMutations);
        report.put("discarded_aborted_row_changes", discardedMutations);
        report.put("aborted_transactions", aborts);
        report.put("discarded_aborted_index_insert_attempts", discardedIndexInserts);
        report.put("discarded_aborted_transaction_local_searches", discardedSearches);
        report.put("transaction_local_searches", localSearches);
        report.put("ordering", "original_single_session_transaction_and_operation_order");
        report.put(
                "business_changes",
                Map.of("insert", inserts, "update", updates, "delete", deletes));
        report.put("committed_transactions", commits);
        report.put("transaction_control_records", commits * 2);
        report.put("workload_index_operations", operationsCount - commits * 2);
        report.put("index_version_writes", operationsCount - searches - commits * 2);
        report.put("secondary_index_scope", "all secondary indexes in pinned source DDL");
        report.put("raw_index_insert_attempts", rawInserts);
        report.put("incremental_files", files);
        report.put("raw_runtime_sha256", BaselineGenerator.sha256(raw));
        report.put("baseline_verified_manifest_sha256", BaselineGenerator.sha256(verified));
        report.put(
                "baseline_trace_sha256", BaselineGenerator.sha256(output.resolve("baseline.ops")));
        report.put(
                "workload_trace_sha256", BaselineGenerator.sha256(output.resolve("workload.ops")));
        report.put(
                "scope",
                "single-session commit-version projection; preserves supported observed equality"
                    + " searches; complete source index maintenance; row writes are service-version"
                    + " projection, not PG physical index writes");
        report.put(
                "version_rule",
                "SIBench assigns one commit version in original single-session commit order,"
                    + " without reordering; transaction-local mutation ordinal preserves all row"
                    + " changes; source boundaries group ordered operations and are not index"
                    + " transactions");
        report.put(
                "read_timestamp_rule",
                "pre-write searches use the preceding commit; transaction-local searches use their"
                        + " source commit version and preceding mutation position; single-session"
                        + " only");
        report.put("snapshot_searches", 0);
        report.put(
                "limitations",
                List.of(
                        "Not PG physical-index equivalence",
                        "No concurrent snapshot projection or inferred cross-session ordering",
                        "Searches occupy native scan-start positions; candidates finalized at scan"
                                + " end",
                        "No historical lookup or Rewrite events in this native trace",
                        "Single-column equality keys only; ordered range/composite encoding not"
                            + " implemented",
                        "Incremental rollover and disk-backed compilation state not implemented"));
        Files.writeString(
                output.resolve("manifest.json"),
                new GsonBuilder().setPrettyPrinting().create().toJson(report),
                StandardOpenOption.CREATE_NEW);
        Files.delete(output.resolve("INCOMPLETE"));
    }

    private static long requireWorkUnit(Map<String, Long> units, String application)
            throws IOException {
        Long id = units.get(application);
        if (id == null)
            throw new IOException("Index operation without source SQL request identity");
        return id;
    }
}
