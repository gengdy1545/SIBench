package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.sibench.capture.AccessProjection.Decision;
import org.sibench.capture.AccessUnits.Request;
import org.sibench.capture.AccessUnits.Unit;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.generation.*;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Compiles a completed native capture, sequential or concurrent sessions, into the canonical
 * trace: every audited access becomes the searches that reproduce its native result or fails the
 * compilation with its reason, and every committed row change becomes a complete index
 * maintenance unit; aborts stay in the raw trace.
 */
public final class IndexTraceCompiler {
    private IndexTraceCompiler() {}

    /**
     * A source access placed at its scan-start position and completed when its unit, or its
     * BitmapAnd group, settles: the searches of its decision, emitted together.
     */
    private static final class Search {
        final long event, readTimestamp, precedingMutations, workUnitId;
        Decision decision;

        Search(long event, long readTimestamp, long preceding, long unit) {
            this.event = event;
            this.readTimestamp = readTimestamp;
            this.precedingMutations = preceding;
            this.workUnitId = unit;
        }
    }

    private record Change(JsonObject row, long event, long workUnitId) {}

    private static final class Output implements AutoCloseable {
        final Path path;
        final ParquetDataFile writer;
        final MessageType schema;
        final long fileId;
        long rows;

        Output(
                Path path,
                MessageType schema,
                GenerationConfig config,
                String table,
                String primaryKey,
                long fileId)
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
                            new TableData(table, 0, 0, primaryKey, this.schema, null),
                            config,
                            Map.of("sibench.source", "committed-pg-row-changes"));
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    /** Resolves a HyBench or native-client capture profile; a null audit uses the default. */
    public static Map<String, Object> compileProfile(
            Path root, Path profile, Path output, Path auditReport) throws Exception {
        var capture = CaptureProfile.read(root, profile);
        return compile(
                capture.schema(),
                capture.layout(),
                capture.raw(),
                output,
                auditReport == null ? RowStateAudit.defaultReport(capture.raw()) : auditReport);
    }

    /** The session traces of one capture and the order the capture recorded for them. */
    record Sessions(String order, List<Path> files) {}

    static Sessions sessions(Path raw) throws Exception {
        CaptureRun.verifyInput(raw);
        Path directory = Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent();
        JsonObject run =
                JsonParser.parseString(Files.readString(directory.resolve("capture-run.json")))
                        .getAsJsonObject();
        String order = run.get("session_order").getAsString();
        if (!Set.of("sequential", "concurrent").contains(order))
            throw new IOException("Unsupported session order: " + order);
        var files = new ArrayList<Path>();
        for (var session : run.getAsJsonArray("sessions"))
            files.add(directory.resolve(session.getAsJsonObject().get("pg_file").getAsString()));
        if (!Files.isDirectory(raw) && !files.equals(List.of(raw.toAbsolutePath())))
            throw new IOException("Compile the capture directory of a multi-session run");
        return new Sessions(order, files);
    }

    public static Map<String, Object> compile(
            SourceSchema sourceSchema,
            GenerationConfig layout,
            Path raw,
            Path output,
            Path auditReport)
            throws Exception {
        Sessions sessions = sessions(raw);
        boolean concurrent = sessions.order().equals("concurrent");
        Path baseline = layout.output();
        RowStateAudit.reuse(sourceSchema, baseline, raw, auditReport);
        Path verified = baseline.resolve("verified-manifest.json");
        JsonObject verification =
                JsonParser.parseString(Files.readString(verified)).getAsJsonObject();
        Path preflightFile = baseline.resolve("schema-preflight.json");
        boolean cCollation =
                AccessLedger.cCollation(
                        JsonParser.parseString(Files.readString(preflightFile)).getAsJsonObject());
        IndexCatalog catalog = IndexCatalog.fromSchema(sourceSchema);
        Files.createDirectory(output);
        Files.writeString(
                output.resolve("INCOMPLETE"),
                "Compilation not committed\n",
                StandardOpenOption.CREATE_NEW);
        Map<String, MessageType> schemas = new HashMap<>();
        var identities = new IndexReferenceReplay(true, false);
        long maximumRowId = 0, maximumFileId = 0, baselineCount, baselineRows = 0;
        RunTiming.current().phase("baseline");
        try (var operations = new IndexOperationTrace.Writer(output.resolve("baseline.ops"))) {
            for (String line : Files.readAllLines(baseline.resolve("files.jsonl"))) {
                JsonObject file = JsonParser.parseString(line).getAsJsonObject();
                String table = file.get("table").getAsString();
                int tableId = catalog.tableId(table);
                long fileId = file.get("file_id").getAsLong();
                maximumFileId = Math.max(maximumFileId, fileId);
                long position = 0;
                try (ParquetReader<Group> reader = reader(baseline.resolve(file.get("path").getAsString()))) {
                    Group row;
                    while ((row = reader.read()) != null) {
                        schemas.putIfAbsent(
                                table, new MessageType(table, row.getType().getFields()));
                        long rowId = row.getLong("_sib_row_id", 0);
                        maximumRowId = Math.max(maximumRowId, rowId);
                        Operation primary =
                                IndexOperationTrace.write(
                                                0,
                                                IndexOperationTrace.INSERT,
                                                tableId,
                                                0,
                                                catalog.primaryKey(table, row),
                                                0,
                                                fileId,
                                                position++,
                                                rowId)
                                        .withWorkUnitId(
                                                1 + baselineRows++ / IndexOperationTrace.BLOCK_RECORDS);
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
        var state = new Compilation(catalog, cCollation, identities, schemas, layout, output);
        state.concurrent = concurrent;
        state.maximumRowId = maximumRowId;
        state.firstIncrementalFile = maximumFileId;
        var sessionReports = new ArrayList<Map<String, Object>>();
        RunTiming.current().phase("workload");
        try (var operations = new IndexOperationTrace.Writer(output.resolve("workload.ops"));
                var units =
                        Files.newBufferedWriter(
                                output.resolve("work-units.jsonl"),
                                StandardOpenOption.CREATE_NEW)) {
            state.operations = operations;
            state.units = units;
            if (concurrent) sessionReports.addAll(state.merge(sessions.files()));
            else for (Path session : sessions.files()) sessionReports.add(state.session(session));
            state.finishFiles();
        } finally {
            for (Output writer : state.writers.values()) writer.close();
        }
        RunTiming.current()
                .phase("report")
                .note("session_order", sessions.order())
                .note("sessions", sessions.files().size())
                .note("source_events", state.event)
                .note("workload_operations", state.operationsCount)
                .note("searched_accesses", state.searches)
                .note("search_operations", state.partSearches);
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("status", "complete_for_declared_projection");
        report.put("operation_trace_version", IndexOperationTrace.VERSION);
        report.put("secondary_operations_explicit", true);
        report.putAll(catalog.provenance());
        report.put("secondary_index_coverage", "all_source_definitions");
        report.put("key_encoding", "order_preserving_v1");
        report.put("text_intervals_ordered", cCollation);
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
        report.put("session_order", sessions.order());
        report.put("sessions", sessionReports);
        report.put("source_work_units", state.requestUnits);
        report.put("workload_work_units", state.emittedUnits.size());
        report.put(
                "work_unit_rule",
                "one audited top-level source SQL execution, numbered in source order before"
                        + " abort/secondary filtering");
        report.put(
                "work_units_sha256", BaselineGenerator.sha256(output.resolve("work-units.jsonl")));
        report.put("workload_operations", state.operationsCount);
        report.put("searched_accesses", state.searches);
        report.put("search_operations", state.partSearches);
        report.put("searched_accesses_by_shape", state.shapes);
        report.put("excluded_access_units", state.excluded);
        report.put("logical_row_version_writes", state.mutations);
        report.put("change_delivery", "committed_only");
        report.put("raw_row_changes", state.rawMutations);
        report.put("discarded_aborted_row_changes", state.discardedMutations);
        report.put("aborted_transactions", state.aborts);
        report.put("utility_only_transactions", state.utilityOnly);
        report.put("discarded_aborted_index_insert_attempts", state.discardedIndexInserts);
        report.put("discarded_aborted_transaction_local_searches", state.discardedSearches);
        report.put("transaction_local_searches", state.localSearches);
        report.put(
                "ordering",
                concurrent
                        ? "postgresql_transaction_completion_counter_then_operation_order"
                        : "original_session_transaction_and_operation_order");
        var changes = new LinkedHashMap<String, Object>();
        changes.put("insert", state.inserts);
        changes.put("update", state.updates);
        changes.put("delete", state.deletes);
        report.put("business_changes", changes);
        report.put("committed_transactions", state.commits);
        report.put("transaction_control_records", state.commits * 2);
        report.put("workload_index_operations", state.operationsCount - state.commits * 2);
        report.put(
                "index_version_writes",
                state.operationsCount - state.partSearches - state.commits * 2);
        report.put("secondary_index_scope", "all secondary indexes in pinned source DDL");
        report.put("raw_index_insert_attempts", state.rawInserts);
        report.put("incremental_files", state.files);
        report.put("baseline_verified_manifest_sha256", BaselineGenerator.sha256(verified));
        report.put("baseline_schema_preflight_sha256", BaselineGenerator.sha256(preflightFile));
        report.put("row_state_audit_sha256", BaselineGenerator.sha256(auditReport));
        report.put(
                "baseline_trace_sha256", BaselineGenerator.sha256(output.resolve("baseline.ops")));
        report.put(
                "workload_trace_sha256", BaselineGenerator.sha256(output.resolve("workload.ops")));
        report.put(
                "scope",
                (concurrent ? "concurrent-session" : "sequential-session")
                        + " commit-version projection; every observed"
                        + " index access unit is an ordered point/range search or fails the"
                        + " compilation; complete source index maintenance; row writes are"
                        + " service-version projection, not PG physical index writes");
        report.put(
                "version_rule",
                "SIBench assigns one commit version per committed source transaction that ran a"
                    + " request, in original session order; transactions that ran no request"
                    + " (connection setup, request labels) carry no version; transaction-local"
                    + " mutation ordinal preserves all row changes; source boundaries group"
                    + " ordered operations and are not index transactions");
        report.put(
                "read_timestamp_rule",
                "pre-write searches use the preceding commit; transaction-local searches use their"
                        + " source commit version and preceding mutation position"
                        + (concurrent
                                ? "; concurrent sessions instead use the commit PostgreSQL's"
                                        + " transaction completion counter made visible to the"
                                        + " statement's own snapshot, so a search may read a"
                                        + " version older than the commit before it"
                                : "; sessions do not overlap"));
        report.put("snapshot_searches", state.snapshotSearches);
        var crosscheck = new LinkedHashMap<String, Object>();
        crosscheck.put("searches_checked", state.crossChecked);
        crosscheck.put("searches_without_usable_snapshot", state.crossCheckUnverifiable);
        crosscheck.put(
                "rule",
                "every transaction the completion counter calls visible to a snapshot must not"
                        + " appear as running in that snapshot's own xip list; a disagreement"
                        + " fails the compilation because one read timestamp cannot express it");
        report.put("snapshot_crosscheck", crosscheck);
        report.put(
                "search_rule",
                "scan keys intersect to one interval of order-preserving encoded values, an = ANY"
                    + " array to a union of points; expected count is the visible rows of the index"
                    + " result; a primary-key scan stopped by its consumer is limited to the rows"
                    + " consumed; a secondary scan stopped early is its fully consumed interval"
                    + " before the last value plus one pinned (value, PK) search per returned row"
                    + " at that value; a BitmapAnd is an intersection group whose count is the"
                    + " visible rows of the intersection");
        report.put(
                "limitations",
                List.of(
                        "Not PG physical-index equivalence",
                        concurrent
                                ? "Concurrent order is PostgreSQL's transaction completion"
                                        + " counter; it is not derived from pid, xid magnitude,"
                                        + " file order or wall clock"
                                : "Sessions ran one after another; no cross-session order is"
                                        + " inferred",
                        "Searches occupy native scan-start positions; counts finalized when the"
                                + " access unit settles",
                        "No historical lookup or Rewrite events in this native trace",
                        "Incremental rollover and disk-backed compilation state not implemented"));
        Files.writeString(
                output.resolve("manifest.json"),
                new GsonBuilder().setPrettyPrinting().create().toJson(report),
                StandardOpenOption.CREATE_NEW);
        Files.delete(output.resolve("INCOMPLETE"));
        return report;
    }

    private static ParquetReader<Group> reader(Path path) throws IOException {
        return ParquetReader.builder(
                        new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri()))
                .build();
    }

    /** Compilation state carried across sessions: versions, work units, RowIDs and files. */
    private static final class Compilation implements AccessUnits.Listener {
        final IndexCatalog catalog;
        final boolean cCollation;
        final IndexReferenceReplay identities;
        final Map<String, MessageType> schemas;
        final GenerationConfig layout;
        final Path output;
        final Map<String, Output> writers = new LinkedHashMap<>();
        final Map<Unit, Search> live = new IdentityHashMap<>();
        final Map<AccessUnits.BitmapGroup, Search> liveGroups = new IdentityHashMap<>();
        final List<Object> pending = new ArrayList<>();
        final Set<Long> emittedUnits = new HashSet<>();
        final Map<String, Long> shapes = new TreeMap<>(), excluded = new TreeMap<>();
        final List<Map<String, Object>> files = new ArrayList<>();
        IndexOperationTrace.Writer operations;
        BufferedWriter units;
        long maximumRowId, firstIncrementalFile, event, version, operationsCount;
        long requestUnits, currentUnit, mutations, searches, rawInserts, inserts, updates;
        long deletes, commits, rawMutations, aborts, discardedMutations, discardedIndexInserts;
        long pendingIndexInserts, pendingMutations, discardedSearches, localSearches, utilityOnly;
        long pendingXid;
        long snapshotSearches, crossChecked, crossCheckUnverifiable, partSearches;
        boolean transactionRequest, transactionOpen;
        boolean concurrent;
        /**
         * Committed writers (transactions with an XID) in completion-counter order: the counter,
         * the version the compiler gave it and its PostgreSQL XID. A search reads the version of the last commit
         * whose counter is at or before its snapshot's counter, so the arrays stay sorted by
         * counter and the lookup is a binary search.
         */
        long[] commitCsns = new long[1024], commitVersions = new long[1024], commitXids = new long[1024];
        int commitCount;
        final Map<Long, Long> commitCounterByXid = new HashMap<>();
        int pid;
        String sessionFile;

        Compilation(
                IndexCatalog catalog,
                boolean cCollation,
                IndexReferenceReplay identities,
                Map<String, MessageType> schemas,
                GenerationConfig layout,
                Path output) {
            this.catalog = catalog;
            this.cCollation = cCollation;
            this.identities = identities;
            this.schemas = schemas;
            this.layout = layout;
            this.output = output;
        }

        /** One backend trace of a sequential-session capture. */
        Map<String, Object> session(Path file) throws IOException {
            long firstEvent = event + 1, sessionRequests = requestUnits;
            pid = 0;
            sessionFile = file.getFileName().toString();
            var tracker = new AccessUnits(this);
            tracker.beginSession();
            try (var reader = Files.newBufferedReader(file)) {
                String line;
                while ((line = reader.readLine()) != null) line(line, tracker);
            }
            tracker.endSession();
            finishSession();
            var report = report(file, pid, event - firstEvent + 1, requestUnits - sessionRequests);
            report.put("first_source_event", firstEvent);
            return report;
        }

        /**
         * Backend traces of one concurrent capture, merged into completion-counter order. Each
         * chunk is one transaction, so scans never cross a chunk boundary and the access tracker
         * can start fresh on every one: scan identities are per-backend pointers and would
         * otherwise collide between sessions.
         */
        List<Map<String, Object>> merge(List<Path> files) throws IOException {
            var events = new long[files.size()];
            var requests = new long[files.size()];
            var backends = new int[files.size()];
            try (SessionMerge merge = SessionMerge.open(files)) {
                while (merge.hasNext()) {
                    var chunk = merge.next();
                    long firstEvent = event + 1, sessionRequests = requestUnits;
                    pid = 0;
                    sessionFile = files.get(chunk.session()).getFileName().toString();
                    var tracker = new AccessUnits(this);
                    tracker.beginSession();
                    for (String line : chunk.lines()) line(line, tracker);
                    tracker.endSession();
                    finishSession();
                    events[chunk.session()] += event - firstEvent + 1;
                    requests[chunk.session()] += requestUnits - sessionRequests;
                    backends[chunk.session()] = pid;
                }
            }
            var reports = new ArrayList<Map<String, Object>>();
            for (int session = 0; session < files.size(); session++)
                reports.add(report(files.get(session), backends[session], events[session], requests[session]));
            return reports;
        }

        private void finishSession() throws IOException {
            if (transactionOpen || !pending.isEmpty() || !live.isEmpty() || !liveGroups.isEmpty())
                throw new IOException("Truncated runtime capture: " + sessionFile);
        }

        private Map<String, Object> report(Path file, int backend, long events, long requests)
                throws IOException {
            var report = new LinkedHashMap<String, Object>();
            report.put("pg_file", file.getFileName().toString());
            report.put("sha256", BaselineGenerator.sha256(file));
            report.put("pid", backend);
            report.put("source_events", events);
            report.put("source_requests", requests);
            return report;
        }

        private void line(String line, AccessUnits tracker) throws IOException {
            event++;
            RunTiming.current().advance(1);
            JsonObject e = JsonParser.parseString(line).getAsJsonObject();
            int eventPid = e.get("pid").getAsInt();
            if (pid == 0) pid = eventPid;
            else if (pid != eventPid)
                throw new IOException("One session trace must contain one backend");
            String type = e.get("event").getAsString();
            tracker.accept(e);
            switch (type) {
                case "executor_start" -> {
                    transactionOpen = true;
                    if (e.get("depth").getAsInt() <= 1) transactionRequest = true;
                }
                case "scan_rescan", "index_insert_attempt" -> transactionOpen = true;
                case "row_change" -> change(e);
                case "transaction_abort" -> abort(e);
                case "transaction_commit" -> commit(e);
                default -> {}
            }
            if (type.equals("index_insert_attempt")) {
                rawInserts++;
                pendingIndexInserts++;
            }
            if (pending.size() > org.sibench.replay.ReplayProjection.MAX_TRANSACTION_OPERATIONS)
                throw new IOException("Source transaction exceeds operation buffer limit");
        }

        @Override
        public void requestStarted(Request request, JsonObject e) throws IOException {
            currentUnit = ++requestUnits;
            JsonObject identity = new JsonObject();
            identity.addProperty("work_unit_id", currentUnit);
            identity.addProperty("application", request.application);
            identity.addProperty("pid", pid);
            identity.addProperty("request", request.ordinal);
            identity.addProperty("source_event", event);
            units.write(identity.toString());
            units.newLine();
        }

        @Override
        public void unitStarted(Unit unit, JsonObject e) throws IOException {
            if (unit.source == AccessUnits.Source.PLANNER_PROBE) return;
            // A BitmapAnd group is one access, placed where its first input started.
            if (unit.group != null && unit.group.members.get(0) != unit) return;
            Search search = new Search(event, readTimestampFor(unit), pendingMutations, currentUnit);
            if (unit.group != null) liveGroups.put(unit.group, search);
            else live.put(unit, search);
            pending.add(search);
        }

        /**
         * The version a search reads. Sessions that never overlap read the commit before their
         * own; concurrent sessions read the last writer their snapshot's completion counter saw,
         * which is the commit set PostgreSQL actually made visible to that statement. A reader's
         * commit changes no data, so it is never a read point.
         */
        private long readTimestampFor(Unit unit) throws IOException {
            // Sessions that never overlap read the commit before their own, exactly as before.
            if (!concurrent) return version;
            if (unit.snapshotCsn < 0)
                throw new IOException(
                        "Concurrent compilation needs a snapshot completion counter on "
                                + unit.scan.index
                                + " in "
                                + sessionFile
                                + " request "
                                + unit.request.ordinal
                                + ": "
                                + unit.request.sql);
            int low = 0, high = commitCount - 1, best = -1;
            while (low <= high) {
                int middle = (low + high) >>> 1;
                if (commitCsns[middle] <= unit.snapshotCsn) {
                    best = middle;
                    low = middle + 1;
                } else high = middle - 1;
            }
            return best < 0 ? 0 : commitVersions[best];
        }

        private void recordCommit(long csn, long xid, long commitVersion) {
            if (commitCount == commitCsns.length) {
                int size = commitCount * 2;
                commitCsns = java.util.Arrays.copyOf(commitCsns, size);
                commitVersions = java.util.Arrays.copyOf(commitVersions, size);
                commitXids = java.util.Arrays.copyOf(commitXids, size);
            }
            commitCsns[commitCount] = csn;
            commitVersions[commitCount] = commitVersion;
            commitXids[commitCount] = xid;
            commitCount++;
            if (xid != 0) commitCounterByXid.put(xid, csn);
        }

        /**
         * Cross-checks the completion counter against the snapshot it describes: no transaction
         * the counter calls visible may appear as running in that snapshot. The other direction is
         * what PostgreSQL guarantees by listing every running XID in the snapshot, so this is the
         * one that could actually be wrong, and a disagreement means one ReadTimestamp cannot
         * express the native visible set.
         */
        private void crossCheck(Unit unit) throws IOException {
            if (!concurrent) return;
            if (unit.snapshotXip == null || unit.snapshotSuboverflowed) {
                crossCheckUnverifiable++;
                return;
            }
            crossChecked++;
            for (long xid : unit.snapshotXip) {
                Long csn = commitCounterByXid.get(xid);
                if (csn != null && csn <= unit.snapshotCsn)
                    throw new IOException(
                            "Snapshot counter "
                                    + unit.snapshotCsn
                                    + " calls XID "
                                    + xid
                                    + " visible, but the snapshot still reports it running"
                                    + " (counter "
                                    + csn
                                    + "); one read timestamp cannot express this snapshot");
            }
            int last = lastCommitAtOrBefore(unit.snapshotCsn);
            if (last >= 0 && commitXids[last] != 0 && commitXids[last] >= unit.snapshotXmax)
                throw new IOException(
                        "Snapshot counter "
                                + unit.snapshotCsn
                                + " calls XID "
                                + commitXids[last]
                                + " visible, but that XID is at or after the snapshot's xmax "
                                + unit.snapshotXmax);
        }

        private int lastCommitAtOrBefore(long csn) {
            int low = 0, high = commitCount - 1, best = -1;
            while (low <= high) {
                int middle = (low + high) >>> 1;
                if (commitCsns[middle] <= csn) {
                    best = middle;
                    low = middle + 1;
                } else high = middle - 1;
            }
            return best;
        }

        @Override
        public void unitSettled(Unit unit) throws IOException {
            if (unit.group != null) return;
            Decision decision = AccessProjection.decide(unit, catalog, cCollation);
            Search search = live.remove(unit);
            if (decision.excluded()) {
                excluded.merge(decision.reason(), 1L, Long::sum);
                if (search != null) pending.remove(search);
                return;
            }
            if (!decision.projected()) throw unsupported(decision, unit);
            search.decision = decision;
            shapes.merge(decision.shape(), 1L, Long::sum);
            crossCheck(unit);
        }

        /**
         * Inputs that started before their BitmapAnd bound them each took a search position; the
         * group keeps the first one and drops the others.
         */
        @Override
        public void groupFormed(AccessUnits.BitmapGroup group) throws IOException {
            Search first = null;
            for (Unit unit : group.members) {
                Search search = live.remove(unit);
                if (search == null) throw new IOException("Bitmap input without a search position");
                if (first == null) first = search;
                else pending.remove(search);
            }
            liveGroups.put(group, first);
        }

        @Override
        public void groupSettled(AccessUnits.BitmapGroup group) throws IOException {
            Decision decision = AccessProjection.decideGroup(group, catalog, cCollation);
            Search search = liveGroups.remove(group);
            if (!decision.projected()) throw unsupported(decision, group.members.get(0));
            search.decision = decision;
            shapes.merge(decision.shape(), 1L, Long::sum);
            for (Unit unit : group.members) crossCheck(unit);
        }

        private IOException unsupported(Decision decision, Unit unit) {
            return new IOException(
                    "Unsupported access ("
                            + decision.reason()
                            + ") on "
                            + unit.scan.index
                            + " in "
                            + sessionFile
                            + " request "
                            + unit.request.ordinal
                            + ": "
                            + unit.request.sql);
        }

        private void change(JsonObject e) throws IOException {
            String relation = e.get("relation").getAsString();
            var table =
                    catalog.schema().tables().stream()
                            .filter(t -> t.name().equals(relation))
                            .findFirst()
                            .orElseThrow(() -> new IOException("Unknown source row table"));
            SourceRow.requireStablePrimaryKey(table, e);
            long xid = e.get("xid").getAsLong();
            if (pendingMutations != 0 && pendingXid != xid)
                throw new IOException("Interleaved transactions unsupported");
            pendingXid = xid;
            transactionOpen = true;
            pending.add(new Change(e, event, currentUnit));
            rawMutations++;
            pendingMutations++;
        }

        private void requireSettled() throws IOException {
            if (!live.isEmpty() || !liveGroups.isEmpty())
                throw new IOException("Transaction boundary with unfinished scans");
        }

        private void reset() {
            pending.clear();
            pendingXid = 0;
            pendingIndexInserts = 0;
            pendingMutations = 0;
            transactionOpen = false;
            transactionRequest = false;
        }

        private void abort(JsonObject e) throws IOException {
            requireSettled();
            if (pendingMutations != 0 && pendingXid != e.get("xid").getAsLong())
                throw new IOException("Abort XID mismatch");
            long previousVersion = version;
            if (!transactionRequest) utilityOnly++;
            else aborts++;
            discardedMutations += pendingMutations;
            discardedIndexInserts += pendingIndexInserts;
            for (Object item : pending) {
                if (!(item instanceof Search search)) continue;
                if (search.precedingMutations != 0) {
                    discardedSearches++;
                    continue;
                }
                boolean behind = search.readTimestamp < previousVersion;
                emit(search, search.readTimestamp, behind ? IndexOperationTrace.SNAPSHOT_SEARCH : 0, 0, 0);
                if (behind) snapshotSearches++;
                emittedUnits.add(search.workUnitId);
                searches++;
            }
            reset();
        }

        private void commit(JsonObject e) throws IOException {
            requireSettled();
            if (pendingMutations != 0 && pendingXid != e.get("xid").getAsLong())
                throw new IOException("Commit XID mismatch");
            if (!transactionRequest) {
                // Connection setup and the capture's request labels run no executor.
                if (!pending.isEmpty()) throw new IOException("Index work outside any request");
                utilityOnly++;
                reset();
                return;
            }
            long previousVersion = version;
            version = Math.incrementExact(version);
            commits++;
            // Only a writer changes what a snapshot sees, so only writers are read points: a
            // snapshot reads the last writer at or below its counter. Sessions that never
            // overlap read the previous commit and need no positions.
            if (concurrent && e.get("xid").getAsLong() != 0)
                recordCommit(e.get("csn").getAsLong(), e.get("xid").getAsLong(), version);
            long transactionId = event, ordinal = 0;
            append(
                    IndexOperationTrace.transaction(
                            event, IndexOperationTrace.TRANSACTION_BEGIN, transactionId, version, 0));
            for (Object item : pending) {
                if (item instanceof Search search) {
                    boolean local = search.precedingMutations != 0;
                    boolean behind = !local && search.readTimestamp < previousVersion;
                    emit(
                            search,
                            local ? version : search.readTimestamp,
                            local
                                    ? IndexOperationTrace.TRANSACTION_LOCAL_SEARCH
                                    : behind ? IndexOperationTrace.SNAPSHOT_SEARCH : 0,
                            transactionId,
                            search.precedingMutations);
                    emittedUnits.add(search.workUnitId);
                    searches++;
                    if (local) localSearches++;
                    else if (behind) snapshotSearches++;
                    continue;
                }
                Change change = (Change) item;
                emittedUnits.add(change.workUnitId());
                write(change, transactionId, ++ordinal);
            }
            append(
                    IndexOperationTrace.transaction(
                            event,
                            IndexOperationTrace.TRANSACTION_COMMIT,
                            transactionId,
                            version,
                            ordinal));
            reset();
        }

        /**
         * The searches of one source access, in decision order. A group's members carry the group
         * flag, their positions and the group's count; independent searches carry their own.
         */
        private void emit(
                Search search, long readTimestamp, int flags, long transactionId, long preceding)
                throws IOException {
            Decision decision = search.decision;
            int size = decision.mode() == AccessProjection.Mode.INDEPENDENT ? 0 : decision.parts().size();
            int group =
                    switch (decision.mode()) {
                        case INTERSECTION -> IndexOperationTrace.INTERSECT_GROUP;
                        case UNION -> IndexOperationTrace.UNION_GROUP;
                        case INDEPENDENT -> 0;
                    };
            int position = 0;
            for (AccessProjection.Part part : decision.parts()) {
                append(
                        IndexOperationTrace.search(
                                        search.event,
                                        part.table(),
                                        part.index(),
                                        part.range(),
                                        part.pinned(),
                                        readTimestamp,
                                        size == 0 ? part.count() : decision.count(),
                                        flags | group,
                                        size == 0 ? 0 : position++,
                                        size)
                                .withTransaction(transactionId, preceding)
                                .withWorkUnitId(search.workUnitId));
                partSearches++;
            }
        }

        private void append(Operation operation) throws IOException {
            operations.append(operation);
            operationsCount++;
        }

        private void write(Change item, long transactionId, long ordinal) throws IOException {
            JsonObject change = item.row();
            String table = change.get("relation").getAsString();
            int tableId = catalog.tableId(table);
            String operation = change.get("operation").getAsString();
            JsonObject oldValue =
                    change.has("old") && change.get("old").isJsonObject()
                            ? change.getAsJsonObject("old")
                            : null;
            if (!operation.equals("insert") && oldValue == null)
                throw new IOException("Missing before-image for row change");
            Map<Integer, IndexKey> before =
                    operation.equals("insert") ? Map.of() : catalog.keys(table, oldValue);
            if (operation.equals("delete")) {
                IndexKey key = catalog.primaryKey(table, oldValue);
                var previous = identities.lookupPrimary(tableId, key, version);
                if (previous == null) throw new IOException("Deletion of absent row");
                Operation primary =
                        IndexOperationTrace.write(
                                        item.event(),
                                        IndexOperationTrace.LOGICAL_DELETE,
                                        tableId,
                                        0,
                                        key,
                                        version,
                                        0,
                                        0,
                                        previous.value())
                                .withTransaction(transactionId, ordinal)
                                .withWorkUnitId(item.workUnitId());
                identities.accept(primary);
                IndexMutation.emit(primary, before, Map.of(), this::append);
                deletes++;
                mutations++;
                return;
            }
            if (!Set.of("insert", "update").contains(operation))
                throw new IOException("Unsupported change kind");
            JsonObject value = change.getAsJsonObject("new");
            IndexKey key = catalog.primaryKey(table, value);
            var previous = identities.lookupPrimary(tableId, key, version);
            if (operation.equals("update")
                    && (previous == null || !catalog.primaryKey(table, oldValue).equals(key)))
                throw new IOException("Update requires an existing unchanged primary key");
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
                                layout,
                                table,
                                catalog.primary(tableId).column(),
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
                    IndexOperationTrace.write(
                                    item.event(),
                                    IndexOperationTrace.UPSERT,
                                    tableId,
                                    operation.equals("insert")
                                            ? IndexOperationTrace.BUSINESS_INSERT
                                            : IndexOperationTrace.BUSINESS_UPDATE,
                                    key,
                                    version,
                                    destination.fileId,
                                    destination.rows++,
                                    rowId)
                            .withTransaction(transactionId, ordinal)
                            .withWorkUnitId(item.workUnitId());
            identities.accept(primary);
            IndexMutation.emit(primary, before, catalog.keys(table, value), this::append);
            if (destination.writer.write(row))
                throw new IOException(
                        "Incremental file reached rollover limit; extend compiler before larger run");
            mutations++;
            if (operation.equals("insert")) inserts++;
            else updates++;
        }

        void finishFiles() throws IOException {
            for (var entry : writers.entrySet()) {
                Output writer = entry.getValue();
                var footer = writer.writer.finish();
                writer.close();
                long decoded = 0;
                try (ParquetReader<Group> check = reader(writer.path)) {
                    while (check.read() != null) decoded++;
                }
                if (decoded != writer.rows) throw new IOException("Incremental Parquet row mismatch");
                var file = new LinkedHashMap<String, Object>();
                file.put("table", entry.getKey());
                file.put("file_id", writer.fileId);
                file.put("path", writer.path.getFileName().toString());
                file.put("rows", writer.rows);
                file.put("sha256", BaselineGenerator.sha256(writer.path));
                file.put(
                        "row_groups",
                        footer.getBlocks().stream().map(b -> b.getRowCount()).toList());
                files.add(file);
            }
        }
    }
}
