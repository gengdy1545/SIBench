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
import org.sibench.state.RowIdentities;
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
    /** In-memory entries of the row-identity store before it spills a sorted run to disk. */
    static final int IDENTITY_MEMORY_ENTRIES = 1_000_000;

    /** Committed writers kept in order to place a snapshot among them. */
    static final int COMMIT_WINDOW = 4_000_000;

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

    /**
     * What vouches for a capture beyond the compilation's own checks. The row changes: either the
     * capture's row-state audit ({@code rowAudit}) or a certification from a fully verified
     * capture of the same build ({@code certification}, see {@link Certification}). The index
     * accesses: PostgreSQL's index counters before and after the native run, which the ledger
     * built in the same pass must match; without them the completeness check is left to a
     * separate {@code check-capture-completeness}.
     */
    public record Checks(
            Path rowAudit, Path certification, Path countersBefore, Path countersAfter, Path runJar) {
        public Checks {
            if (rowAudit != null && certification != null)
                throw new IllegalArgumentException("A capture is verified by its row audit or a certification, not both");
            if ((countersBefore == null) != (countersAfter == null))
                throw new IllegalArgumentException("Index counters come as a before and after pair");
        }

        /** Checks without the capture's run jar, which only a certified compilation reads. */
        public Checks(Path rowAudit, Path certification, Path countersBefore, Path countersAfter) {
            this(rowAudit, certification, countersBefore, countersAfter, null);
        }

        public static Checks rowAudit(Path report) {
            return new Checks(report, null, null, null);
        }
    }

    /** Resolves a HyBench or native-client capture profile; a null row audit uses the default. */
    public static Map<String, Object> compileProfile(Path root, Path profile, Path output, Checks checks)
            throws Exception {
        var capture = CaptureProfile.read(root, profile);
        if (checks.certification() == null && checks.rowAudit() == null)
            checks = new Checks(
                    RowStateAudit.defaultReport(capture.raw()), null, checks.countersBefore(), checks.countersAfter());
        else if (checks.certification() != null && checks.runJar() == null)
            checks = new Checks(
                    null, checks.certification(), checks.countersBefore(), checks.countersAfter(),
                    Certification.runJar(capture.artifacts()));
        return compile(capture.schema(), capture.layout(), capture.raw(), output, checks);
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
        return compile(sourceSchema, layout, raw, output, Checks.rowAudit(auditReport));
    }

    public static Map<String, Object> compile(
            SourceSchema sourceSchema, GenerationConfig layout, Path raw, Path output, Checks checks)
            throws Exception {
        return compile(sourceSchema, layout, raw, output, checks, IDENTITY_MEMORY_ENTRIES, COMMIT_WINDOW);
    }

    /**
     * {@code identityMemory} bounds the in-memory entries of the row-identity store and {@code
     * commitWindow} the committed writers kept to place snapshots.
     */
    static Map<String, Object> compile(
            SourceSchema sourceSchema,
            GenerationConfig layout,
            Path raw,
            Path output,
            Path auditReport,
            int identityMemory,
            int commitWindow)
            throws Exception {
        return compile(sourceSchema, layout, raw, output, Checks.rowAudit(auditReport), identityMemory, commitWindow);
    }

    static Map<String, Object> compile(
            SourceSchema sourceSchema,
            GenerationConfig layout,
            Path raw,
            Path output,
            Checks checks,
            int identityMemory,
            int commitWindow)
            throws Exception {
        Sessions sessions = sessions(raw);
        boolean concurrent = sessions.order().equals("concurrent");
        Path baseline = layout.output();
        Path auditReport = checks.rowAudit();
        JsonObject certificate = null;
        if (checks.certification() != null) {
            // The certification vouches for the row changes; the baseline is still checked here.
            certificate =
                    Certification.require(
                            checks.certification(),
                            raw,
                            checks.runJar(),
                            IndexCatalog.fromSchema(sourceSchema).provenance());
            PgBaselineVerify.verifyFiles(baseline);
        } else RowStateAudit.reuse(sourceSchema, baseline, raw, auditReport);
        Path verified = baseline.resolve("verified-manifest.json");
        JsonObject verification =
                JsonParser.parseString(Files.readString(verified)).getAsJsonObject();
        Path preflightFile = baseline.resolve("schema-preflight.json");
        boolean cCollation =
                AccessLedger.cCollation(
                        JsonParser.parseString(Files.readString(preflightFile)).getAsJsonObject());
        IndexCatalog catalog = IndexCatalog.fromSchema(sourceSchema);
        Files.createDirectory(output);
        RunTiming.current().measure(output);
        Files.writeString(
                output.resolve("INCOMPLETE"),
                "Compilation not committed\n",
                StandardOpenOption.CREATE_NEW);
        Map<String, MessageType> schemas = new HashMap<>();
        // Row identities spill to disk beyond a bounded number of in-memory entries.
        var identities = new RowIdentities(output.resolve("state"), identityMemory, 4);
        long maximumRowId = 0, maximumFileId = 0, baselineCount, baselineRows = 0;
        String baselineDigest, workloadDigest;
        RunTiming.current().phase("baseline");
        // The baseline load is derived again by every reader (BaselineOperations), so only its
        // count and the digest of the file it would form are recorded.
        var baselineOperations = new WriterThread(new IndexOperationTrace.Writer(null), "baseline-writer");
        try (var operations = baselineOperations) {
            var extent =
                    BaselineOperations.derive(
                            baseline,
                            catalog,
                            operations,
                            new BaselineOperations.Rows() {
                                RowIdentities.Baseline identity;

                                @Override
                                public void file(String table, int tableId) throws IOException {
                                    identity = identities.baseline(tableId);
                                }

                                @Override
                                public void row(String table, org.apache.parquet.schema.GroupType type, Operation primary)
                                        throws IOException {
                                    if (!schemas.containsKey(table)) schemas.put(table, new MessageType(table, type.getFields()));
                                    identity.add(primary.key(), primary.rowId());
                                }

                                @Override
                                public void fileFinished() throws IOException {
                                    try (var finished = identity) {
                                        finished.finish();
                                    }
                                }
                            });
            maximumRowId = extent.maximumRowId();
            maximumFileId = extent.maximumFileId();
            baselineRows = extent.rows();
            baselineCount = operations.count();
        }
        baselineDigest = baselineOperations.sha256();
        var state = new Compilation(catalog, cCollation, identities, schemas, layout, output);
        state.concurrent = concurrent;
        state.commitWindow = new CommitWindow(commitWindow);
        state.maximumRowId = maximumRowId;
        state.firstIncrementalFile = maximumFileId;
        var sessionReports = new ArrayList<Map<String, Object>>();
        RunTiming.current().phase("workload");
        var workloadOperations = new WriterThread(new IndexOperationTrace.Writer(output.resolve("workload.ops")), "workload-writer");
        var workUnits = WorkUnits.create(output);
        try (var operations = workloadOperations;
                var units = workUnits.lines()) {
            state.operations = operations;
            state.units = units;
            if (concurrent) sessionReports.addAll(state.merge(sessions.files()));
            else for (Path session : sessions.files()) sessionReports.add(state.session(session));
            state.finishFiles();
        } finally {
            for (Output writer : state.writers.values()) writer.close();
            identities.close();
        }
        workloadDigest = workloadOperations.sha256();
        // The spill files are working state, not an artifact: a finished trace has none.
        deleteTree(output.resolve("state"));
        RunTiming.current().phase("ledger");
        Map<String, Object> ledgerReport = state.ledger.report(sourceSchema);
        Path ledgerFile = output.resolve("access-ledger.json");
        AccessLedger.write(ledgerReport, ledgerFile);
        JsonObject ledger = JsonParser.parseString(Files.readString(ledgerFile)).getAsJsonObject();
        String completeness = "not_checked";
        if (checks.countersBefore() != null) {
            var result =
                    CaptureCompleteness.compare(
                            ledger,
                            JsonParser.parseString(Files.readString(checks.countersBefore())).getAsJsonObject(),
                            JsonParser.parseString(Files.readString(checks.countersAfter())).getAsJsonObject());
            Files.writeString(
                    output.resolve("capture-completeness.json"),
                    new GsonBuilder().serializeNulls().setPrettyPrinting().create().toJson(result),
                    StandardOpenOption.CREATE_NEW);
            completeness = (String) result.get("status");
            if (!completeness.equals("consistent"))
                throw new IOException(
                        "PG index counters disagree with the access ledger on "
                                + result.get("inconsistent_indexes")
                                + "; see capture-completeness.json");
        }
        if (certificate != null) Certification.requireCovered(certificate, ledger, state.changesByTable);
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
        report.put("workload_work_units", state.emittedUnits);
        report.put(
                "work_unit_rule",
                "one audited top-level source SQL execution, numbered in source order before"
                        + " abort/secondary filtering");
        report.put(
                "work_units_sha256", HexFormat.of().formatHex(workUnits.digest().digest()));
        report.put("workload_operations", state.operationsCount);
        report.put("searched_accesses", state.searches);
        report.put("search_operations", state.partSearches);
        report.put("searched_accesses_by_shape", state.shapes);
        report.put("excluded_access_units", state.excluded);
        report.put("logical_row_version_writes", state.mutations);
        // Committed row writes as a share of the baseline: the replay volume a capture delivers.
        report.put("row_write_ratio", (double) state.mutations / verification.get("rows").getAsLong());
        report.put("change_delivery", "committed_only");
        report.put("raw_row_changes", state.rawMutations);
        report.put("discarded_aborted_row_changes", state.discardedMutations);
        report.put("aborted_transactions", state.aborts);
        report.put("utility_only_transactions", state.utilityOnly);
        report.put("discarded_aborted_index_insert_attempts", state.discardedIndexInserts);
        report.put("discarded_aborted_transaction_local_searches", state.discardedSearches);
        report.put("transaction_local_searches", state.localSearches);
        report.put(
                "transaction_local_read_model",
                "statement_snapshot_plus_own_writes_up_to_mutation_ordinal");
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
        report.put("business_changes_by_table", state.changesByTable);
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
        var verificationReport = new LinkedHashMap<String, Object>();
        if (certificate != null) {
            verificationReport.put("mode", "certified");
            verificationReport.put("certification", checks.certification().toAbsolutePath().toString());
            verificationReport.put("certification_sha256", BaselineGenerator.sha256(checks.certification()));
        } else {
            verificationReport.put("mode", "row_audit");
            verificationReport.put("row_state_audit_sha256", BaselineGenerator.sha256(auditReport));
            report.put("row_state_audit_sha256", verificationReport.get("row_state_audit_sha256"));
        }
        report.put("verification", verificationReport);
        report.put("access_ledger_sha256", BaselineGenerator.sha256(ledgerFile));
        report.put("capture_completeness", completeness);
        report.put("baseline_trace_sha256", baselineDigest);
        report.put("workload_trace_sha256", workloadDigest);
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
                    + " (connection setup, request labels) carry no version; a transaction-local"
                    + " search's version is that transaction's commit and its mutation ordinal"
                    + " preserves the read position; source boundaries group ordered operations"
                    + " and are not index transactions");
        report.put(
                "read_timestamp_rule",
                "every search reads the statement's own snapshot"
                        + (concurrent
                                ? " (the last writer PostgreSQL's transaction completion counter"
                                        + " made visible to it, so it may be older than the commit"
                                        + " before it)"
                                : " (the preceding commit; sessions do not overlap)")
                        + "; a transaction-local search also names its transaction's commit version"
                        + " and mutation ordinal, so a reader overlays only this transaction's"
                        + " writes up to that ordinal and never another transaction's later commit");
        report.put("snapshot_searches", state.snapshotSearches);
        var crosscheck = new LinkedHashMap<String, Object>();
        crosscheck.put("searches_checked", state.crossChecked);
        crosscheck.put("searches_without_usable_snapshot", state.crossCheckUnverifiable);
        crosscheck.put(
                "rule",
                "every transaction the completion counter calls visible to a snapshot must not"
                        + " appear as running in that snapshot's own xip list; a disagreement"
                        + " fails the compilation because one read timestamp cannot express it");
        var window = new LinkedHashMap<String, Object>();
        window.put("limit", commitWindow);
        window.put("evicted_commits", state.commitWindow.evicted());
        crosscheck.put("commit_window", window);
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
                        "Disk-backed compilation state not implemented"));
        Files.writeString(
                output.resolve("manifest.json"),
                new GsonBuilder().setPrettyPrinting().create().toJson(report),
                StandardOpenOption.CREATE_NEW);
        Files.delete(output.resolve("INCOMPLETE"));
        return report;
    }

    private static void deleteTree(Path directory) throws IOException {
        if (!Files.exists(directory)) return;
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList())
                Files.delete(path);
        }
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
        final RowIdentities identities;
        final Map<String, MessageType> schemas;
        final GenerationConfig layout;
        final Path output;
        /** The open incremental file of each table; a finished one leaves this map. */
        final Map<String, Output> writers = new LinkedHashMap<>();
        final Map<String, Integer> fileSequence = new HashMap<>();
        long nextRolloverFile;
        final Map<Unit, Search> live = new IdentityHashMap<>();
        final Map<AccessUnits.BitmapGroup, Search> liveGroups = new IdentityHashMap<>();
        /** The open transaction's searches and changes, in source order; see {@link #drain()}. */
        final java.util.ArrayDeque<Object> pending = new java.util.ArrayDeque<>();
        /**
         * Set when the open transaction's outcome is known before its events are read (the
         * concurrent merge reads ahead to its final record): whether it commits, and that record's
         * event ordinal, which is the transaction's identity. Its operations are then written as
         * soon as they and everything before them are settled, instead of at the final record, so
         * a transaction of millions of searches never has to be held.
         */
        Boolean knownCommit;
        long knownEvent;
        /** Of a known commit: its BEGIN was written, and the change ordinal reached so far. */
        boolean beginWritten;
        long streamedOrdinal;
        /** Work units that carried an operation; ids only grow, so the last one suffices. */
        long emittedUnits, lastEmittedUnit;
        final Map<String, Long> shapes = new TreeMap<>(), excluded = new TreeMap<>();
        /** The capture's access ledger, built from the same events in the same pass. */
        final AccessLedger ledger;
        final List<Map<String, Object>> files = new ArrayList<>();
        /** Committed changes per table and operation: the change classes a capture exercised. */
        final Map<String, Map<String, Long>> changesByTable = new TreeMap<>();
        WriterThread operations;
        BufferedWriter units;
        long maximumRowId, firstIncrementalFile, event, version, operationsCount;
        long requestUnits, currentUnit, mutations, searches, rawInserts, inserts, updates;
        long deletes, commits, rawMutations, aborts, discardedMutations, discardedIndexInserts;
        long pendingIndexInserts, pendingMutations, discardedSearches, localSearches, utilityOnly;
        long pendingXid;
        long snapshotSearches, crossChecked, crossCheckUnverifiable, partSearches;
        boolean transactionRequest, transactionOpen;
        boolean concurrent;
        /** The latest committed writers by completion counter; see {@link CommitWindow}. */
        CommitWindow commitWindow;
        int pid;
        String sessionFile;

        Compilation(
                IndexCatalog catalog,
                boolean cCollation,
                RowIdentities identities,
                Map<String, MessageType> schemas,
                GenerationConfig layout,
                Path output)
                throws IOException {
            this.catalog = catalog;
            this.cCollation = cCollation;
            this.ledger = new AccessLedger(catalog.schema(), cCollation);
            this.identities = identities;
            this.schemas = schemas;
            this.layout = layout;
            this.output = output;
        }

        /** One backend trace of a sequential-session capture. */
        Map<String, Object> session(Path file) throws IOException {
            long firstEvent = event + 1, sessionRequests = requestUnits, unitsBefore = ledger.settled();
            pid = 0;
            sessionFile = file.getFileName().toString();
            var tracker = new AccessUnits(this);
            tracker.beginSession();
            try (var reader = ServerTrace.open(file)) {
                String line;
                while ((line = reader.readLine()) != null) line(line, tracker);
            }
            AccessUnits.Detail detail = tracker.detail();
            tracker.endSession();
            ledger.absorb(tracker);
            finishSession();
            ledger.input(file, pid, detail, event - firstEvent + 1, ledger.settled() - unitsBefore);
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
            var accessUnits = new long[files.size()];
            // Each file's first part names its capture format; later parts inherit it.
            var details = new AccessUnits.Detail[files.size()];
            // Each session's events are tracked here, in the merge's own order: moving the
            // per-session tracking onto threads of its own cost more in calls and queueing than
            // it saved on the one ordered thread, so tracking stays in-line.
            try (SessionMerge merge = SessionMerge.open(files)) {
                while (merge.hasNext()) {
                    var chunk = merge.next();
                    long firstEvent = event + 1, sessionRequests = requestUnits, unitsBefore = ledger.settled();
                    pid = 0;
                    sessionFile = files.get(chunk.session()).getFileName().toString();
                    var tracker = new AccessUnits(this);
                    if (details[chunk.session()] == null) tracker.beginSession();
                    else tracker.continueSession(details[chunk.session()]);
                    // The final record is line lineCount, so its ordinal is known now.
                    knownCommit = chunk.committed();
                    knownEvent = event + chunk.lineCount();
                    for (Event event; (event = merge.event()) != null; ) event(event, tracker);
                    details[chunk.session()] = tracker.detail();
                    tracker.endSession();
                    ledger.absorb(tracker);
                    finishSession();
                    accessUnits[chunk.session()] += ledger.settled() - unitsBefore;
                    events[chunk.session()] += event - firstEvent + 1;
                    requests[chunk.session()] += requestUnits - sessionRequests;
                    backends[chunk.session()] = pid;
                }
            }
            var reports = new ArrayList<Map<String, Object>>();
            for (int session = 0; session < files.size(); session++) {
                if (details[session] == null)
                    throw new IOException(
                            "Session trace has no capture_start: " + files.get(session));
                var detail = details[session];
                ledger.input(files.get(session), backends[session], detail, events[session], accessUnits[session]);
                reports.add(report(files.get(session), backends[session], events[session], requests[session]));
            }
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
            report.put("sha256", FileDigests.sha256(file));
            report.put("pid", backend);
            report.put("source_events", events);
            report.put("source_requests", requests);
            return report;
        }

        private void line(String line, AccessUnits tracker) throws IOException {
            event(Event.parse(line), tracker);
        }

        /** One event, routed by {@code tracker} and claimed once by the ledger. */
        private void event(Event e, AccessUnits tracker) throws IOException {
            event++;
            RunTiming.current().advance(1);
            int eventPid = e.pid();
            if (pid == 0) pid = eventPid;
            else if (pid != eventPid)
                throw new IOException("One session trace must contain one backend");
            String type = e.type();
            boolean routed = tracker.accept(e);
            ledger.event(e, routed);
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
            if (knownCommit != null && transactionOpen) drain();
            if (pending.size() > org.sibench.replay.ReplayProjection.MAX_TRANSACTION_OPERATIONS)
                throw new IOException("Source transaction exceeds operation buffer limit");
        }

        @Override
        public void requestStarted(Request request, Event e) throws IOException {
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
        public void unitStarted(Unit unit, Event e) throws IOException {
            started(unit, unit.group, unit.group != null && unit.group.members.get(0) == unit);
        }

        /** {@code group} and {@code first}: the unit's BitmapAnd group as it started, and whether it was the group's first input. */
        private void started(Unit unit, AccessUnits.BitmapGroup group, boolean first) throws IOException {
            if (unit.source == AccessUnits.Source.PLANNER_PROBE) return;
            // A BitmapAnd group is one access, placed where its first input started.
            if (group != null && !first) return;
            Search search = new Search(event, readTimestampFor(unit), pendingMutations, currentUnit);
            if (group != null) liveGroups.put(group, search);
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
            return commitWindow.versionAt(unit.snapshotCsn);
        }

        private void recordCommit(long csn, long xid, long commitVersion) {
            commitWindow.add(csn, xid, commitVersion);
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
                Long csn = commitWindow.counterOf(xid);
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
            long last = commitWindow.xidAt(unit.snapshotCsn);
            if (last != 0 && last >= unit.snapshotXmax)
                throw new IOException(
                        "Snapshot counter "
                                + unit.snapshotCsn
                                + " calls XID "
                                + last
                                + " visible, but that XID is at or after the snapshot's xmax "
                                + unit.snapshotXmax);
        }

        @Override
        public void requestFinished(Request request) throws IOException {
            ledger.request(request);
        }

        @Override
        public void unitSettled(Unit unit) throws IOException {
            settled(unit, unit.group);
        }

        private void settled(Unit unit, AccessUnits.BitmapGroup group) throws IOException {
            ledger.unit(unit);
            if (group != null) return;
            Decision decision = AccessProjection.decide(unit, catalog, cCollation);
            ledger.record(unit, decision);
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
            formed(group, group.members);
        }

        /** {@code members}: the group's inputs as it formed; later ones join it as they start. */
        private void formed(AccessUnits.BitmapGroup group, List<Unit> members) throws IOException {
            Search first = null;
            for (Unit unit : members) {
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
            ledger.group(group, decision);
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

        private void change(Event e) throws IOException {
            String relation = e.get("relation").getAsString();
            var table =
                    catalog.schema().tables().stream()
                            .filter(t -> t.name().equals(relation))
                            .findFirst()
                            .orElseThrow(() -> new IOException("Unknown source row table"));
            // Row changes are few next to index events; they keep the Gson form SourceRow reads.
            JsonObject row = e.json();
            SourceRow.requireStablePrimaryKey(table, row);
            long xid = e.get("xid").getAsLong();
            if (pendingMutations != 0 && pendingXid != xid)
                throw new IOException("Interleaved transactions unsupported");
            pendingXid = xid;
            transactionOpen = true;
            pending.add(new Change(row, event, currentUnit));
            rawMutations++;
            pendingMutations++;
        }

        private void requireSettled() throws IOException {
            if (!live.isEmpty() || !liveGroups.isEmpty())
                throw new IOException("Transaction boundary with unfinished scans");
        }

        /**
         * Writes the settled prefix of a transaction whose outcome is known, exactly as the final
         * record would write it: an abort keeps the searches before its first change and drops the
         * rest, a commit writes its BEGIN, searches and changes with its known identity.
         */
        private void drain() throws IOException {
            while (!pending.isEmpty()) {
                Object item = pending.peekFirst();
                if (item instanceof Search search && search.decision == null) return;
                if (!transactionRequest) throw new IOException("Index work outside any request");
                pending.pollFirst();
                if (knownCommit) {
                    if (!beginWritten) {
                        append(IndexOperationTrace.transaction(
                                knownEvent, IndexOperationTrace.TRANSACTION_BEGIN, knownEvent,
                                Math.addExact(version, 1), 0));
                        beginWritten = true;
                    }
                    if (item instanceof Search search) emitCommitted(search, version, Math.addExact(version, 1), knownEvent);
                    else {
                        Change change = (Change) item;
                        emitted(change.workUnitId());
                        write(change, knownEvent, ++streamedOrdinal, Math.addExact(version, 1));
                    }
                } else if (item instanceof Search search) emitAborted(search, version);
            }
        }

        private void reset() {
            pending.clear();
            knownCommit = null;
            beginWritten = false;
            streamedOrdinal = 0;
            pendingXid = 0;
            pendingIndexInserts = 0;
            pendingMutations = 0;
            transactionOpen = false;
            transactionRequest = false;
        }

        private void abort(Event e) throws IOException {
            requireSettled();
            if (pendingMutations != 0 && pendingXid != e.get("xid").getAsLong())
                throw new IOException("Abort XID mismatch");
            long previousVersion = version;
            if (!transactionRequest) utilityOnly++;
            else aborts++;
            if (knownCommit != null && (knownCommit || knownEvent != event))
                throw new IOException("Transaction outcome differs from the merge's read-ahead");
            discardedMutations += pendingMutations;
            discardedIndexInserts += pendingIndexInserts;
            for (Object item : pending)
                if (item instanceof Search search) emitAborted(search, previousVersion);
            reset();
        }

        /** A search of an aborted transaction: kept before its first change, dropped after. */
        private void emitAborted(Search search, long previousVersion) throws IOException {
            if (search.precedingMutations != 0) {
                discardedSearches++;
                return;
            }
            boolean behind = search.readTimestamp < previousVersion;
            emit(search, search.readTimestamp, behind ? IndexOperationTrace.SNAPSHOT_SEARCH : 0, 0, 0, 0);
            if (behind) snapshotSearches++;
            emitted(search.workUnitId);
            searches++;
        }

        /**
         * A search of a committed transaction. A transaction-local search keeps its statement
         * snapshot and names the transaction's own version; a reader overlays that transaction's
         * writes up to the search's mutation ordinal, so other transactions that committed after
         * the snapshot stay invisible.
         */
        private void emitCommitted(Search search, long previousVersion, long commitVersion, long transactionId)
                throws IOException {
            boolean local = search.precedingMutations != 0;
            boolean behind = !local && search.readTimestamp < previousVersion;
            emit(
                    search,
                    search.readTimestamp,
                    local
                            ? IndexOperationTrace.TRANSACTION_LOCAL_SEARCH
                            : behind ? IndexOperationTrace.SNAPSHOT_SEARCH : 0,
                    transactionId,
                    search.precedingMutations,
                    local ? commitVersion : 0);
            emitted(search.workUnitId);
            searches++;
            if (local) localSearches++;
            else if (behind) snapshotSearches++;
        }

        private void commit(Event e) throws IOException {
            requireSettled();
            if (pendingMutations != 0 && pendingXid != e.get("xid").getAsLong())
                throw new IOException("Commit XID mismatch");
            if (knownCommit != null && (!knownCommit || knownEvent != event))
                throw new IOException("Transaction outcome differs from the merge's read-ahead");
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
            long transactionId = event, ordinal = streamedOrdinal;
            if (!beginWritten)
                append(
                        IndexOperationTrace.transaction(
                                event, IndexOperationTrace.TRANSACTION_BEGIN, transactionId, version, 0));
            for (Object item : pending) {
                if (item instanceof Search search) {
                    emitCommitted(search, previousVersion, version, transactionId);
                    continue;
                }
                Change change = (Change) item;
                emitted(change.workUnitId());
                write(change, transactionId, ++ordinal, version);
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
                Search search,
                long readTimestamp,
                int flags,
                long transactionId,
                long preceding,
                long localVersion)
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
                                .withVersion(localVersion)
                                .withWorkUnitId(search.workUnitId));
                partSearches++;
            }
        }

        /**
         * Counts a work unit once. Units are numbered as their requests start, and a request lies
         * inside one transaction that is compiled whole, so the ids arrive in order.
         */
        private void emitted(long unit) throws IOException {
            if (unit < lastEmittedUnit)
                throw new IOException("Work units were emitted out of order: " + unit);
            if (unit > lastEmittedUnit) {
                emittedUnits++;
                lastEmittedUnit = unit;
            }
        }

        private void append(Operation operation) throws IOException {
            operations.append(operation);
            operationsCount++;
        }

        private void write(Change item, long transactionId, long ordinal, long commitVersion) throws IOException {
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
                long previous = identities.rowId(tableId, key);
                if (previous == 0) throw new IOException("Deletion of absent row");
                Operation primary =
                        IndexOperationTrace.write(
                                        item.event(),
                                        IndexOperationTrace.LOGICAL_DELETE,
                                        tableId,
                                        0,
                                        key,
                                        commitVersion,
                                        0,
                                        0,
                                        previous)
                                .withTransaction(transactionId, ordinal)
                                .withWorkUnitId(item.workUnitId());
                identities.delete(tableId, key);
                IndexMutation.emit(primary, before, Map.of(), this::append);
                changed(table, operation);
                deletes++;
                mutations++;
                return;
            }
            if (!Set.of("insert", "update").contains(operation))
                throw new IOException("Unsupported change kind");
            JsonObject value = change.getAsJsonObject("new");
            IndexKey key = catalog.primaryKey(table, value);
            long previous = identities.rowId(tableId, key);
            if (operation.equals("update")
                    && (previous == 0 || !catalog.primaryKey(table, oldValue).equals(key)))
                throw new IOException("Update requires an existing unchanged primary key");
            if (operation.equals("insert") && previous != 0)
                throw new IOException("Insertion of existing primary key");
            long rowId =
                    operation.equals("update")
                            ? previous
                            : (maximumRowId = Math.incrementExact(maximumRowId));
            Output destination = writers.get(table);
            if (destination == null) {
                destination = open(table, tableId);
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
            row.add("_sib_timestamp", commitVersion);
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
                                    commitVersion,
                                    destination.fileId,
                                    destination.rows++,
                                    rowId)
                            .withTransaction(transactionId, ordinal)
                            .withWorkUnitId(item.workUnitId());
            identities.put(tableId, key, rowId);
            IndexMutation.emit(primary, before, catalog.keys(table, value), this::append);
            // A file that reached its target size is closed; the table's next row opens another.
            if (destination.writer.write(row)) {
                writers.remove(table);
                finish(table, destination);
            }
            mutations++;
            changed(table, operation);
            if (operation.equals("insert")) inserts++;
            else updates++;
        }

        /**
         * The table's next incremental file. The first uses the baseline's last FileID plus the
         * table's id; rollover files take the FileIDs after
         * the last table's, in the order they are opened, so every FileID is unique and int64.
         */
        private Output open(String table, int tableId) throws IOException {
            if (!schemas.containsKey(table)) throw new IOException("Missing baseline table schema");
            int sequence = fileSequence.merge(table, 1, Integer::sum);
            long fileId;
            String name;
            if (sequence == 1) {
                fileId = Math.addExact(firstIncrementalFile, tableId);
                name = table + "-versions.parquet";
            } else {
                if (nextRolloverFile == 0)
                    nextRolloverFile =
                            Math.addExact(
                                    firstIncrementalFile,
                                    catalog.definitions().stream()
                                                    .mapToInt(IndexCatalog.Definition::tableId)
                                                    .max()
                                                    .orElse(0)
                                            + 1L);
                fileId = nextRolloverFile++;
                name = "%s-versions-%06d.parquet".formatted(table, sequence);
            }
            return new Output(
                    output.resolve(name),
                    schemas.get(table),
                    layout,
                    table,
                    catalog.primary(tableId).column(),
                    fileId);
        }

        private void changed(String table, String operation) {
            changesByTable.computeIfAbsent(table, ignored -> new TreeMap<>()).merge(operation, 1L, Long::sum);
        }

        private void finish(String table, Output writer) throws IOException {
            var footer = writer.writer.finish();
            writer.close();
            long rows = footer.getBlocks().stream().mapToLong(block -> block.getRowCount()).sum();
            if (rows != writer.rows) throw new IOException("Incremental Parquet row mismatch");
            var file = new LinkedHashMap<String, Object>();
            file.put("table", table);
            file.put("file_id", writer.fileId);
            file.put("path", writer.path.getFileName().toString());
            file.put("rows", writer.rows);
            file.put("size_bytes", Files.size(writer.path));
            file.put("file_size", writer.writer.sizing());
            file.put("sha256", BaselineGenerator.sha256(writer.path));
            file.put(
                    "row_groups", footer.getBlocks().stream().map(b -> b.getRowCount()).toList());
            files.add(file);
        }

        void finishFiles() throws IOException {
            for (var entry : writers.entrySet()) finish(entry.getKey(), entry.getValue());
            writers.clear();
        }
    }
}
