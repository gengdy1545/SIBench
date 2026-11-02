package org.sibench.capture;

import com.google.gson.*;

import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Converts a TiDB/TiKV capture into canonical index operations and checks every converted read
 * against an independent MVCC evaluation. TiKV records the MVCC entries each coprocessor scan or
 * point get actually read, with the request's start_ts and task ID; TiDB records statements,
 * retries, statement and transaction rollbacks, commits with their commit_ts, pessimistic lock
 * reads and the row and index mutations it buffered. Unsupported facts fail with their reason
 * instead of being approximated.
 */
public final class TidbCapture {
    static final long MAX_TS = -1L; // uint64 max, TiDB's autocommit point-get optimization

    private TidbCapture() {}

    /** One table of the schema file: integer or common (clustered) handle and its indexes. */
    record Table(long id, int canonical, String name, boolean intHandle, List<String> pkTypes, Map<Long, Index> indexes) {}

    record Index(long id, String name, boolean unique, List<String> types) {}

    /** One observed index entry: its search component, the row's PK and the version read. */
    record Entry(IndexKey value, IndexKey pk, long version) {}

    /** A converted read with what the source read; version -1 when the source does not report it. */
    record Read(Operation search, List<Entry> observed, String origin) {}

    private record Version(long commitTs, long startTs) {}

    public static Map<String, Object> convert(Path capture, Path schemaFile, long baselineTs, Path output)
            throws IOException {
        var state = new State(readSchema(schemaFile), baselineTs);
        try (var files = Files.list(capture)) {
            var sorted = files.sorted().toList();
            for (Path file : sorted)
                if (file.getFileName().toString().startsWith("tidb")) state.tidb(file);
            for (Path file : sorted)
                if (file.getFileName().toString().startsWith("tikv")) state.tikv(file);
        }
        state.finish();
        Files.createDirectory(output);
        String baselineSha, workloadSha;
        try (var writer = new IndexOperationTrace.Writer(output.resolve("baseline.ops"))) {
            for (Operation o : state.baseline) writer.append(o);
            writer.close();
            baselineSha = writer.sha256();
        }
        try (var writer = new IndexOperationTrace.Writer(output.resolve("workload.ops"))) {
            for (Operation o : state.workload) writer.append(o);
            writer.close();
            workloadSha = writer.sha256();
        }
        var check = TidbCaptureOracle.check(state.baseline, state.workload, state.reads);
        var report = new LinkedHashMap<String, Object>();
        report.put("status", check.get("mismatches").equals(0L) && state.rejected.isEmpty() ? "consistent" : "inconsistent");
        report.put("baseline_ts", Long.toUnsignedString(baselineTs));
        report.put("baseline_operations", state.baseline.size());
        report.put("workload_operations", state.workload.size());
        report.put("baseline_trace_sha256", baselineSha);
        report.put("workload_trace_sha256", workloadSha);
        report.put("committed_writer_transactions", state.versions.size());
        report.put("counts", state.counts);
        report.put("rejected", state.rejected);
        report.put("oracle", check);
        report.put(
                "read_point_rule",
                "a TiKV read at start_ts T sees the writer transactions with commit_ts <= T; versions number"
                        + " those commits in commit_ts order; a pessimistic lock read uses for_update_ts, or the"
                        + " conflict commit_ts it reports; a max-ts autocommit point get takes the version it read");
        report.put(
                "scope",
                "storage-level reads TiKV performed for user statements of the captured workload; TiDB-local"
                        + " reads (memory buffer, lock cache, UnionScan merge) are counted, not converted");
        Files.writeString(output.resolve("tidb-capture-report.json"),
                new GsonBuilder().setPrettyPrinting().serializeNulls().create().toJson(report));
        return report;
    }

    static Map<Long, Table> readSchema(Path file) throws IOException {
        var tables = new TreeMap<Long, Table>();
        int canonical = 0;
        for (var element : JsonParser.parseString(Files.readString(file)).getAsJsonObject().getAsJsonArray("tables")) {
            var t = element.getAsJsonObject();
            var indexes = new TreeMap<Long, Index>();
            for (var i : t.getAsJsonArray("indexes")) {
                var index = i.getAsJsonObject();
                indexes.put(index.get("id").getAsLong(), new Index(index.get("id").getAsLong(),
                        index.get("name").getAsString(), index.get("unique").getAsBoolean(), strings(index, "types")));
            }
            tables.put(t.get("id").getAsLong(), new Table(t.get("id").getAsLong(), ++canonical,
                    t.get("name").getAsString(), t.get("int_handle").getAsBoolean(), strings(t, "pk_types"), indexes));
        }
        return tables;
    }

    private static List<String> strings(JsonObject o, String name) {
        var r = new ArrayList<String>();
        for (var e : o.getAsJsonArray(name)) r.add(e.getAsString());
        return r;
    }

    /** TiDB prints an integer handle as its value and a common handle as {value, ...}. */
    static String handle(String text) throws IOException {
        if (!text.startsWith("{")) return text;
        if (!text.endsWith("}") || text.contains(", ")) throw new IOException("Composite handle is not supported: " + text);
        return text.substring(1, text.length() - 1);
    }

    static IndexKey key(String type, Object component) throws IOException {
        if (component == null) return IndexKey.NULL;
        return switch (type) {
            case "int" -> IndexKey.ofInt(component instanceof Long l ? l : Long.parseLong(TidbKeys.text(component)));
            case "text" -> IndexKey.ofText(TidbKeys.text(component));
            default -> throw new IOException("Unsupported column type " + type);
        };
    }

    private static final class State {
        final Map<Long, Table> tables;
        final long baselineTs;
        final Map<Long, JsonObject> statements = new HashMap<>();
        final Set<Long> rolledBackTasks = new HashSet<>();
        final Map<Long, List<JsonObject>> mutations = new LinkedHashMap<>(); // by txn start_ts
        final Map<Long, Long> commits = new HashMap<>(); // start_ts -> commit_ts of writers
        final Set<Long> rolledBack = new HashSet<>();
        final List<JsonObject> locks = new ArrayList<>();
        final Map<Long, JsonObject> copRequests = new LinkedHashMap<>(); // by request ID
        final Map<Long, JsonObject> copEnds = new HashMap<>();
        final List<JsonObject> unionScans = new ArrayList<>();
        final List<JsonObject> localGets = new ArrayList<>();
        final List<JsonObject> batchResults = new ArrayList<>();
        final Map<Long, Long> attemptNs = new HashMap<>(); // statement task -> start of the attempt
        final Map<Long, Long> versionOfStart = new HashMap<>();
        final Map<Long, Map<Long, List<Operation>>> pendingLocal = new HashMap<>(); // version -> ordinal -> searches
        final Set<Long> absorbed = new HashSet<>();
        final List<JsonObject> tikv = new ArrayList<>();
        final List<Version> versions = new ArrayList<>();
        final List<Operation> baseline = new ArrayList<>(), workload = new ArrayList<>();
        final List<Read> reads = new ArrayList<>();
        final Map<String, Long> counts = new TreeMap<>();
        final Map<String, Long> rejected = new TreeMap<>();
        final Map<Long, Long> workUnits = new HashMap<>();
        long event, rowIds;

        State(Map<Long, Table> tables, long baselineTs) {
            this.tables = tables;
            this.baselineTs = baselineTs;
        }

        void count(String name) {
            counts.merge(name, 1L, Long::sum);
        }

        void reject(String reason) {
            rejected.merge(reason, 1L, Long::sum);
        }

        void tidb(Path file) throws IOException {
            try (var lines = Files.newBufferedReader(file)) {
                for (String line; (line = lines.readLine()) != null; ) {
                    var e = JsonParser.parseString(line).getAsJsonObject();
                    switch (e.get("ev").getAsString()) {
                        case "stmt_begin" -> {
                            statements.put(e.get("task").getAsLong(), e);
                            attemptNs.put(e.get("task").getAsLong(), e.get("ns").getAsLong());
                        }
                        case "stmt_rollback" -> rolledBackTasks.add(e.get("task").getAsLong());
                        case "stmt_retry" -> {
                            // A retry is a new attempt of the same statement under a new task ID.
                            var statement = statements.get(e.get("old_task").getAsLong());
                            if (statement != null) {
                                statements.put(e.get("task").getAsLong(), statement);
                                attemptNs.put(e.get("task").getAsLong(), e.get("ns").getAsLong());
                            }
                            count("statement_retries");
                        }
                        case "row", "index_put", "index_delete" -> {
                            // Rejected writes leave no state; statement rollback discards the rest.
                            if (e.get("err").getAsString().isEmpty())
                                mutations.computeIfAbsent(e.get("start_ts").getAsLong(), k -> new ArrayList<>()).add(e);
                        }
                        case "commit" -> {
                            if (e.get("read_only").getAsBoolean() || !e.get("err").getAsString().isEmpty()) break;
                            var info = JsonParser.parseString(e.get("info").getAsString()).getAsJsonObject();
                            long start = e.get("start_ts").getAsLong(), committed = info.get("start_ts").getAsLong();
                            // TiDB re-executes a conflicting autocommit transaction in a new one; only
                            // the new transaction commits, so the first one's writes never apply.
                            if (committed != start) {
                                if (committed < start) throw new IOException("Commit info belongs to an older transaction: " + e);
                                count("transaction_level_retries");
                            }
                            commits.put(committed, info.get("commit_ts").getAsLong());
                        }
                        case "rollback" -> rolledBack.add(e.get("start_ts").getAsLong());
                        case "lock" -> locks.add(e);
                        case "local_get" -> {
                            if (e.get("src").getAsString().equals("membuffer")) localGets.add(e);
                            else count("tidb_lock_cache_point_reads");
                        }
                        case "union_scan" -> unionScans.add(e);
                        case "cop" -> copRequests.put(e.get("req").getAsLong(), e);
                        case "cop_end" -> copEnds.put(e.get("req").getAsLong(), e);
                        case "batch_get_result" -> batchResults.add(e);
                        case "stmt_end" -> {}
                        default -> throw new IOException("Unknown TiDB capture event: " + line);
                    }
                }
            }
        }

        void tikv(Path file) throws IOException {
            try (var lines = Files.newBufferedReader(file)) {
                for (String line; (line = lines.readLine()) != null; ) {
                    var e = JsonParser.parseString(line).getAsJsonObject();
                    if (!e.has("task_id")) {
                        reject("tikv_record_without_task_" + e.get("kind").getAsString());
                        continue;
                    }
                    long task = e.get("task_id").getAsLong();
                    var cop = e.get("kind").getAsString().equals("cop_dag") ? copRequests.get(task) : null;
                    long statement = cop != null ? cop.get("stmt").getAsLong() : task;
                    if (statements.containsKey(statement)) {
                        e.addProperty("stmt", statement);
                        tikv.add(e);
                    } else count("tikv_records_outside_user_statements");
                }
            }
        }

        long[] commitOrder;

        long version(long commitTs) throws IOException {
            return version(commitTs, -1, null);
        }

        /**
         * The version of a writer's commit_ts. Async commit and 1PC let non-conflicting writers
         * share a commit_ts; only one of them can have written a given row, which resolves it.
         */
        long version(long commitTs, long table, String handle) throws IOException {
            if (Long.compareUnsigned(commitTs, baselineTs) <= 0) return 0;
            int after = (int) readPoint(commitTs);
            if (after == 0 || commitOrder[after - 1] != commitTs)
                throw new IOException("Read a version without a captured commit: " + Long.toUnsignedString(commitTs));
            int first = after;
            while (first >= 2 && commitOrder[first - 2] == commitTs) first--;
            if (first == after) return after;
            long found = -1;
            for (int v = first; v <= after; v++) {
                for (var m : mutations.get(versions.get(v - 1).startTs()))
                    if (m.get("table").getAsLong() == table && handle(m.get("handle").getAsString()).equals(handle)) {
                        if (found >= 0 && found != v) throw new IOException("Two writers of one row share commit_ts " + commitTs);
                        found = v;
                    }
            }
            if (found < 0) throw new IOException("No writer sharing commit_ts " + commitTs + " wrote the row read");
            count("versions_resolved_among_equal_commit_ts");
            return found;
        }

        /** The last version a snapshot at {@code ts} sees. */
        long readPoint(long ts) {
            if (ts == MAX_TS) throw new IllegalArgumentException("max-ts read has no snapshot");
            int lo = 0, hi = commitOrder.length;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (commitOrder[mid] <= ts) lo = mid + 1;
                else hi = mid;
            }
            return lo;
        }

        long versionOfLock(long startTs) throws IOException {
            Long commit = commits.get(startTs);
            if (commit == null) throw new IOException("Read through a lock of an uncommitted transaction " + startTs);
            if (Long.compareUnsigned(commit, baselineTs) <= 0) return 0;
            for (int v = 0; v < versions.size(); v++) if (versions.get(v).startTs() == startTs) return v + 1;
            throw new IOException("Read through a lock of a transaction outside the workload " + startTs);
        }

        long unit(long task) {
            return workUnits.computeIfAbsent(task, t -> (long) workUnits.size() + 1);
        }

        void finish() throws IOException {
            // Writer transactions after the baseline snapshot, in commit_ts order.
            for (var commit : commits.entrySet()) {
                long start = commit.getKey(), ts = commit.getValue();
                boolean wrote = mutations.getOrDefault(start, List.of()).stream()
                        .anyMatch(m -> statements.containsKey(m.get("task").getAsLong()) && tables.containsKey(m.get("table").getAsLong()));
                if (!wrote) continue;
                if (Long.compareUnsigned(ts, baselineTs) <= 0) continue;
                if (Long.compareUnsigned(start, baselineTs) < 0)
                    throw new IOException("Writer transaction spans the baseline snapshot: start_ts " + start);
                versions.add(new Version(ts, start));
            }
            versions.sort(Comparator.comparingLong(Version::commitTs).thenComparingLong(Version::startTs));
            commitOrder = versions.stream().mapToLong(Version::commitTs).toArray();
            for (int i = 0; i < versions.size(); i++) versionOfStart.put(versions.get(i).startTs(), (long) i + 1);
            for (int i = 1; i < versions.size(); i++)
                if (versions.get(i).commitTs() == versions.get(i - 1).commitTs()) count("equal_commit_ts_writers");
            var baselineReads = new ArrayList<JsonObject>();
            var workloadReads = new ArrayList<JsonObject>();
            for (var record : tikv) {
                long ts = record.get("start_ts").getAsLong();
                if (ts == baselineTs) baselineReads.add(record);
                else if (Long.compareUnsigned(ts, baselineTs) > 0) workloadReads.add(record);
                else count("tikv_reads_before_baseline");
            }
            completeness();
            baseline(baselineReads);
            var pending = new TreeMap<Long, List<Operation>>(); // snapshot searches by read point
            localReads(pending);
            for (var group : groupByRequest(workloadReads).values()) searches(group, pending);
            lockReads(pending);
            writes(pending);
        }

        /**
         * Source completeness: every key range TiDB sent to TiKV for a user statement must be
         * covered, in scan order, by the ranges or keys TiKV recorded reading for that task, up to
         * the end of the range or a piece that stopped before exhausting its Region (a satisfied
         * limit leaves later Regions unread).
         */
        void completeness() throws IOException {
            var read = new HashMap<Long, List<String[]>>(); // lo, hi, "stopped" when not exhausted
            for (var record : tikv) {
                if (!record.get("kind").getAsString().equals("cop_dag")) continue;
                var list = read.computeIfAbsent(record.get("task_id").getAsLong(), k -> new ArrayList<>());
                for (var r : record.getAsJsonArray("reads")) {
                    var o = r.getAsJsonObject();
                    if (o.has("scan")) {
                        var scan = o.getAsJsonObject("scan");
                        list.add(new String[] {scan.get("lo").getAsString(), scan.get("hi").getAsString(),
                                scan.get("exhausted").getAsBoolean() ? "" : "stopped"});
                    } else {
                        // TiKV serves a range [k, PrefixNext(k)) as a point get of k.
                        String k = o.getAsJsonObject("get").get("k").getAsString();
                        list.add(new String[] {k, prefixNext(k), ""});
                    }
                }
            }
            for (var request : copRequests.values()) {
                long task = request.get("req").getAsLong();
                if (!statements.containsKey(request.get("stmt").getAsLong())) continue;
                boolean desc = request.get("desc").getAsBoolean();
                var pieces = read.getOrDefault(task, List.of());
                for (var range : request.getAsJsonArray("ranges")) {
                    String lo = range.getAsJsonArray().get(0).getAsString(), hi = range.getAsJsonArray().get(1).getAsString();
                    if (space(lo, hi) == null) continue;
                    String reached = desc ? hi : lo;
                    boolean stopped = false, moved = true;
                    while (moved && !stopped && (desc ? reached.compareTo(lo) > 0 : reached.compareTo(hi) < 0)) {
                        moved = false;
                        for (var p : pieces) {
                            boolean covers = desc ? p[1].compareTo(reached) >= 0 && p[0].compareTo(reached) < 0
                                    : p[0].compareTo(reached) <= 0 && p[1].compareTo(reached) > 0;
                            if (!covers) continue;
                            reached = desc ? p[0] : p[1];
                            stopped = !p[2].isEmpty();
                            moved = true;
                            break;
                        }
                    }
                    if (!stopped && (desc ? reached.compareTo(lo) > 0 : reached.compareTo(hi) < 0)) {
                        reject("cop_range_not_read_by_tikv");
                        throw new IOException("TiKV recorded no read of " + lo + ".." + hi + " for task " + task);
                    }
                    count(stopped ? "cop_ranges_covered_until_scan_stopped" : "cop_ranges_covered");
                }
            }
        }

        static String prefixNext(String hex) {
            byte[] key = TidbKeys.hex(hex);
            for (int i = key.length - 1; i >= 0; i--)
                if (++key[i] != 0) return HexFormat.of().formatHex(key);
            return hex + "00";
        }

        /**
         * The reads of one request: every coprocessor request has its own task ID (TiDB allocates
         * one per request while capturing). Point reads share their statement's task ID, so each
         * stays a unit of its own, told apart by record.
         */
        Map<Object, List<JsonObject>> groupByRequest(List<JsonObject> records) {
            var r = new LinkedHashMap<Object, List<JsonObject>>();
            int single = 0;
            for (var e : records)
                r.computeIfAbsent(e.get("kind").getAsString().equals("cop_dag") ? (Object) e.get("task_id").getAsLong() : (Object) ("p" + single++),
                        k -> new ArrayList<>()).add(e);
            return r;
        }

        /** Baseline: complete scans of every record and index space at the baseline snapshot. */
        void baseline(List<JsonObject> records) throws IOException {
            var pieces = new HashMap<String, List<JsonObject[]>>();
            for (var record : records) {
                for (var read : record.getAsJsonArray("reads")) {
                    var scan = read.getAsJsonObject().getAsJsonObject("scan");
                    if (scan == null) throw new IOException("Baseline read is not a scan");
                    var space = space(scan.get("lo").getAsString(), scan.get("hi").getAsString());
                    if (space != null) pieces.computeIfAbsent(space.label(), k -> new ArrayList<>()).add(new JsonObject[] {record, scan});
                }
            }
            long rows = 0;
            for (var table : tables.values()) {
                for (long index : spaces(table)) {
                    var space = new Space(table, index);
                    var chains = chains(pieces.getOrDefault(space.label(), List.of()));
                    if (chains.size() != 1) throw new IOException("Baseline of " + table.name() + " index " + index + " is not one complete scan");
                    var chain = chains.get(0);
                    var first = chain.get(0)[1];
                    var last = chain.get(chain.size() - 1)[1];
                    if (bound(space, first.get("lo").getAsString(), true) != UNBOUNDED
                            || bound(space, last.get("hi").getAsString(), false) != UNBOUNDED
                            || !last.get("exhausted").getAsBoolean())
                        throw new IOException("Baseline of " + table.name() + " index " + index + " does not cover its key space");
                    long entries = 0;
                    for (var piece : chain) {
                        for (var o : piece[1].getAsJsonArray("entries")) {
                            var e = entry(space, o.getAsJsonObject());
                            if (e.version() != 0) throw new IOException("Baseline entry newer than the snapshot");
                            long unit = 1 + rowIds / IndexOperationTrace.BLOCK_RECORDS;
                            var write = IndexOperationTrace.write(event++, IndexOperationTrace.UPSERT, table.canonical(), 0, e.pk(), 0, 0, 0, ++rowIds);
                            baseline.add((index == 0 ? write : write.withIndex((int) index, e.value(), IndexOperationTrace.UPSERT)).withWorkUnitId(unit));
                            entries++;
                        }
                    }
                    if (index == 0) rows += entries;
                    counts.put("baseline_entries_" + table.name() + "_" + index, entries);
                }
            }
            counts.put("baseline_rows", rows);
        }

        List<Long> spaces(Table table) {
            var r = new ArrayList<Long>();
            r.add(0L);
            r.addAll(table.indexes().keySet());
            return r;
        }

        private record Space(Table table, long index) {
            String label() {
                return table.id() + "/" + index;
            }
        }

        /** The table and index a key range reads, or null for a table outside the schema. */
        Space space(String lo, String hi) throws IOException {
            for (String bound : new String[] {lo, hi}) {
                byte[] key = TidbKeys.hex(bound);
                if (key.length < 11 || key[0] != 't') continue;
                long id = TidbKeys.decodeInt(key, 1);
                Table table = tables.get(id);
                if (table == null) return null;
                var parsed = TidbKeys.parse(key, table.intHandle());
                if (parsed == null || parsed.index() < 0) continue;
                if (parsed.index() != 0 && !table.indexes().containsKey(parsed.index()))
                    throw new IOException("Index " + parsed.index() + " of " + table.name() + " is not in the schema");
                return new Space(table, parsed.index());
            }
            throw new IOException("Range names no table key space: " + lo + ".." + hi);
        }

        String lastKeyNext(JsonObject scan) {
            var entries = scan.getAsJsonArray("entries");
            return entries.isEmpty() ? scan.get("lo").getAsString()
                    : entries.get(entries.size() - 1).getAsJsonObject().get("k").getAsString() + "00";
        }

        Entry entry(Space space, JsonObject o) throws IOException {
            byte[] key = TidbKeys.hex(o.get("k").getAsString());
            var parsed = TidbKeys.parse(key, space.table().intHandle());
            if (parsed == null || parsed.table() != space.table().id() || parsed.index() != space.index() || !parsed.exact())
                throw new IOException("Entry outside its scan space: " + o);
            var c = parsed.components();
            var pkTypes = space.table().pkTypes();
            IndexKey value, pk;
            if (space.index() == 0) {
                if (c.size() != pkTypes.size()) throw new IOException("Record key has " + c.size() + " handle components");
                pk = pk(pkTypes, c);
                value = pk;
            } else {
                var index = space.table().indexes().get(space.index());
                if (index.types().size() != 1) throw new IOException("Multi-column index " + index.name() + " is not supported");
                if (index.unique() && c.size() == 1) {
                    reject("unique_index_entry_pk_in_value");
                    throw new IOException("Unique index entry keeps its handle in the value: " + o);
                }
                if (c.size() != 1 + pkTypes.size()) throw new IOException("Index key has " + c.size() + " components");
                value = key(index.types().get(0), c.get(0));
                pk = pk(pkTypes, c.subList(1, c.size()));
            }
            String handle = TidbKeys.text(space.index() == 0 ? c.get(0) : c.get(c.size() - 1));
            long version = switch (o.get("vk").getAsInt()) {
                case 1 -> version(o.get("vt").getAsLong(), space.table().id(), handle);
                case 2 -> versionOfLock(o.get("vt").getAsLong());
                default -> throw new IOException("Read entry without its version: " + o);
            };
            return new Entry(value, pk, version);
        }

        IndexKey pk(List<String> types, List<Object> components) throws IOException {
            if (types.size() != 1) throw new IOException("Composite primary keys are not supported");
            return key(types.get(0), components.get(0));
        }

        /** Converts one statement's TiKV reads: point gets one by one; contiguous scan pieces joined. */
        void searches(List<JsonObject> records, Map<Long, List<Operation>> pending) throws IOException {
            var scans = new ArrayList<JsonObject[]>();
            for (var record : records) {
                for (var read : record.getAsJsonArray("reads")) {
                    var r = read.getAsJsonObject();
                    if (r.has("get")) get(record, r.getAsJsonObject("get"), pending);
                    else scans.add(new JsonObject[] {record, r.getAsJsonObject("scan")});
                }
            }
            for (var chain : chains(scans)) scan(chain, pending);
        }

        /**
         * Joins the pieces of one logical scan: a following Region (previous piece exhausted at
         * the next piece's start) or a paging continuation (next piece starts right after the
         * previous piece's last key). Overlapping pieces are duplicate reads and fail.
         */
        List<List<JsonObject[]>> chains(List<JsonObject[]> scans) throws IOException {
            var forward = new ArrayList<JsonObject[]>();
            var backward = new ArrayList<JsonObject[]>();
            for (var s : scans) (s[1].get("desc").getAsBoolean() ? backward : forward).add(s);
            forward.sort(Comparator.comparing(s -> s[1].get("lo").getAsString()));
            backward.sort(Comparator.comparing((JsonObject[] s) -> s[1].get("hi").getAsString()).reversed());
            var chains = new ArrayList<List<JsonObject[]>>();
            for (var list : List.of(forward, backward)) {
                List<JsonObject[]> current = null;
                for (var s : list) {
                    if (current != null && joins(current.get(current.size() - 1)[1], s[1])) current.add(s);
                    else {
                        if (current != null && overlaps(current.get(current.size() - 1)[1], s[1]))
                            throw new IOException("Overlapping TiKV reads of one statement: " + s[1].get("lo"));
                        current = new ArrayList<>();
                        current.add(s);
                        chains.add(current);
                    }
                }
            }
            return chains;
        }

        boolean joins(JsonObject a, JsonObject b) {
            boolean desc = a.get("desc").getAsBoolean();
            if (desc) {
                var entries = a.getAsJsonArray("entries");
                String next = a.get("exhausted").getAsBoolean() ? a.get("lo").getAsString()
                        : entries.isEmpty() ? null : entries.get(entries.size() - 1).getAsJsonObject().get("k").getAsString();
                return b.get("hi").getAsString().equals(next) && a.get("lo").getAsString().compareTo(b.get("lo").getAsString()) >= 0;
            }
            String next = a.get("exhausted").getAsBoolean() ? a.get("hi").getAsString() : lastKeyNext(a);
            return b.get("lo").getAsString().equals(next);
        }

        boolean overlaps(JsonObject a, JsonObject b) {
            return a.get("lo").getAsString().compareTo(b.get("hi").getAsString()) < 0
                    && b.get("lo").getAsString().compareTo(a.get("hi").getAsString()) < 0;
        }

        void scan(List<JsonObject[]> chain, Map<Long, List<Operation>> pending) throws IOException {
            var first = chain.get(0);
            var last = chain.get(chain.size() - 1);
            var record = first[0];
            boolean desc = first[1].get("desc").getAsBoolean();
            String lo = desc ? last[1].get("lo").getAsString() : first[1].get("lo").getAsString();
            String hi = desc ? first[1].get("hi").getAsString() : last[1].get("hi").getAsString();
            var space = space(lo, hi);
            if (space == null) {
                count("reads_outside_schema_tables");
                return;
            }
            var observed = new ArrayList<Entry>();
            for (var piece : chain) {
                if (piece[0].get("start_ts").getAsLong() != record.get("start_ts").getAsLong())
                    throw new IOException("One scan read at two timestamps");
                for (var e : piece[1].getAsJsonArray("entries")) observed.add(entry(space, e.getAsJsonObject()));
            }
            boolean exhausted = last[1].get("exhausted").getAsBoolean();
            var lower = bound(space, lo, true);
            var upper = bound(space, hi, false);
            if (lower == null || upper == null) {
                reject("scan_bound_inside_equal_index_values");
                return;
            }
            long limit = exhausted ? 0 : observed.size();
            if (!exhausted && observed.isEmpty()) {
                reject("unexhausted_scan_without_entries");
                return;
            }
            var range = new Range(lower.key, lower.inclusive, upper.key, upper.inclusive, desc, limit);
            long ts = record.get("start_ts").getAsLong();
            if (ts == MAX_TS) {
                reject("max_ts_scan");
                return;
            }
            emit(record, space, range, readPoint(ts), observed, record.get("executors").getAsString(), pending);
            count(chain.size() > 1 ? "scans_joined_from_pieces" : "scans_single_piece");
        }

        private record Bound(IndexKey key, boolean inclusive) {}

        static final Bound UNBOUNDED = new Bound(null, false);

        /**
         * Translates a TiKV range bound into an inclusive or exclusive bound on the scan's first
         * component. Returns null for a bound between entries of one indexed value, which a range
         * over indexed values cannot express.
         */
        Bound bound(Space space, String text, boolean lower) throws IOException {
            byte[] key = TidbKeys.hex(text);
            var parsed = TidbKeys.parse(key, space.table().intHandle());
            if (parsed == null || parsed.table() != space.table().id() || parsed.index() != space.index())
                return UNBOUNDED;
            var c = parsed.components();
            String type = space.index() == 0 ? space.table().pkTypes().get(0)
                    : space.table().indexes().get(space.index()).types().get(0);
            if (c.isEmpty()) {
                if (parsed.exact()) return UNBOUNDED;
                int flag = key[key.length - parsed.rest()] & 0xFF;
                if (!lower && flag >= TidbKeys.MAX_FLAG) return UNBOUNDED;
                // A Region boundary at a component's type flag sits before every non-NULL value.
                if (lower && parsed.rest() == 1 && (flag == TidbKeys.BYTES || flag == TidbKeys.INT)) return UNBOUNDED;
                throw new IOException("Undecodable range bound " + text);
            }
            if (space.index() != 0 && c.size() > 1) {
                // TiDB's split keys and full-value bounds pad the handle with its minimum.
                if (c.size() != 2 || !Long.valueOf(Long.MIN_VALUE).equals(c.get(1)) || !parsed.exact()) return null;
                return new Bound(key(type, c.get(0)), lower);
            }
            if (c.get(0) == null) {
                // TiDB sorts NULL first and SIBench last: a NULL lower bound is a scan from the start,
                // exact only while the column holds no NULL, which the oracle checks.
                if (lower && parsed.exact()) {
                    count("null_lower_bounds_as_unbounded");
                    return UNBOUNDED;
                }
                throw new IOException("NULL range bound is not supported: " + text);
            }
            if (space.index() == 0 && space.table().intHandle() && c.get(0) instanceof Long l
                    && (l == Long.MIN_VALUE && lower || l == Long.MAX_VALUE && !lower && !parsed.exact()))
                return UNBOUNDED;
            var value = key(type, c.get(0));
            // An exact bound sits before every entry with that value; trailing bytes sit after.
            return lower ? new Bound(value, parsed.exact()) : new Bound(value, !parsed.exact());
        }

        void get(JsonObject record, JsonObject get, Map<Long, List<Operation>> pending) throws IOException {
            byte[] key = TidbKeys.hex(get.get("k").getAsString());
            if (key.length < 11 || key[0] != 't') {
                count("point_reads_outside_tables");
                return;
            }
            var space = space(get.get("k").getAsString(), get.get("k").getAsString());
            if (space == null) {
                count("reads_outside_schema_tables");
                return;
            }
            var parsed = TidbKeys.parse(key, space.table().intHandle());
            if (space.index() != 0) {
                reject("unique_index_point_get");
                return;
            }
            var pk = pk(space.table().pkTypes(), parsed.components());
            var observed = new ArrayList<Entry>();
            boolean found = !get.get("v").isJsonNull();
            if (found) observed.add(entry(space, get));
            long ts = record.get("start_ts").getAsLong();
            long readPoint;
            if (ts == MAX_TS) {
                if (!found) {
                    reject("max_ts_point_get_miss_read_point_unobserved");
                    return;
                }
                readPoint = observed.get(0).version();
                count("max_ts_point_gets_read_point_from_version");
            } else {
                readPoint = readPoint(ts);
            }
            emit(record, space, Range.equal(pk), readPoint, observed, record.get("kind").getAsString(), pending);
        }

        /** Pessimistic lock reads return the row at for_update_ts (or at the conflict) without its version. */
        void lockReads(Map<Long, List<Operation>> pending) throws IOException {
            for (var lock : locks) {
                long task = lock.get("task").getAsLong();
                if (!statements.containsKey(task) || !lock.get("return_values").getAsBoolean()) continue;
                if (!lock.get("err").getAsString().isEmpty()) {
                    count("lock_reads_failed");
                    continue;
                }
                for (var value : lock.getAsJsonObject("values").entrySet()) {
                    var space = space(value.getKey(), value.getKey());
                    if (space == null) continue;
                    if (space.index() != 0) {
                        reject("unique_index_lock_read");
                        continue;
                    }
                    var v = value.getValue().getAsJsonObject();
                    if (v.get("already_locked").getAsBoolean()) {
                        count("lock_reads_already_locked");
                        continue;
                    }
                    long conflict = v.get("conflict_ts").getAsLong();
                    long ts = conflict != 0 ? conflict : lock.get("for_update_ts").getAsLong();
                    var parsed = TidbKeys.parse(TidbKeys.hex(value.getKey()), space.table().intHandle());
                    var pk = pk(space.table().pkTypes(), parsed.components());
                    var observed = v.get("exists").getAsBoolean() ? List.of(new Entry(pk, pk, -1)) : List.<Entry>of();
                    long readPoint = conflict != 0 ? version(conflict, space.table().id(), TidbKeys.text(parsed.components().get(0))) : readPoint(ts);
                    var fake = new JsonObject();
                    fake.addProperty("task_id", task);
                    emit(fake, space, Range.equal(pk), readPoint, observed, "pessimistic_lock", pending);
                    count(conflict != 0 ? "lock_reads_with_conflict" : "lock_reads");
                }
            }
        }

        void emit(JsonObject record, Space space, Range range, long readPoint, List<Entry> observed, String origin,
                Map<Long, List<Operation>> pending) {
            var search = IndexOperationTrace.search(event++, space.table().canonical(), (int) space.index(), range,
                    readPoint, observed.size(), IndexOperationTrace.SNAPSHOT_SEARCH)
                    .withWorkUnitId(unit(record.get("task_id").getAsLong()));
            reads.add(new Read(search, observed, origin));
            pending.computeIfAbsent(readPoint, k -> new ArrayList<>()).add(search);
            count("searches");
        }

        /** Writes of committed transactions in version order, each followed by the reads of its snapshot. */
        void writes(TreeMap<Long, List<Operation>> pending) throws IOException {
            for (var s : pending.headMap(1L).values()) workload.addAll(s);
            for (int v = 1; v <= versions.size(); v++) {
                long start = versions.get(v - 1).startTs();
                if (rolledBack.contains(start)) throw new IOException("Transaction both committed and rolled back: " + start);
                workload.add(IndexOperationTrace.transaction(event++, IndexOperationTrace.TRANSACTION_BEGIN, v, v, 0));
                long ordinal = 0;
                for (var m : mutations.get(start)) {
                    long task = m.get("task").getAsLong();
                    var table = tables.get(m.get("table").getAsLong());
                    if (table == null || !statements.containsKey(task)) continue;
                    if (rolledBackTasks.contains(task)) {
                        count("discarded_statement_rollback_mutations");
                        continue;
                    }
                    long unit = unit(task);
                    switch (m.get("ev").getAsString()) {
                        case "row" -> {
                            ordinal++;
                            var pk = key(table.pkTypes().get(0), handle(m.get("handle").getAsString()));
                            String op = m.get("op").getAsString();
                            int kind = op.equals("delete") ? IndexOperationTrace.LOGICAL_DELETE : IndexOperationTrace.UPSERT;
                            int flags = op.equals("insert") ? IndexOperationTrace.BUSINESS_INSERT
                                    : op.equals("update") ? IndexOperationTrace.BUSINESS_UPDATE : 0;
                            workload.add(IndexOperationTrace.write(event++, kind, table.canonical(), flags, pk, v, 0, 0,
                                    kind == IndexOperationTrace.UPSERT ? ++rowIds : 0)
                                    .withTransaction(v, ordinal).withWorkUnitId(unit));
                            count("row_" + op);
                        }
                        case "index_put", "index_delete" -> {
                            if (m.get("ev").getAsString().equals("index_put") && m.get("untouched").getAsBoolean()) {
                                count("untouched_index_entries");
                                continue;
                            }
                            var index = table.indexes().get(m.get("index").getAsLong());
                            if (index == null) throw new IOException("Unknown index in mutation: " + m);
                            var values = m.getAsJsonArray("values");
                            if (values.size() != 1) throw new IOException("Multi-column index mutation: " + m);
                            var value = key(index.types().get(0), values.get(0).isJsonNull() ? null : values.get(0).getAsString());
                            var pk = key(table.pkTypes().get(0), handle(m.get("handle").getAsString()));
                            int kind = m.get("ev").getAsString().equals("index_put") ? IndexOperationTrace.UPSERT : IndexOperationTrace.LOGICAL_DELETE;
                            workload.add(IndexOperationTrace.write(event++, kind, table.canonical(), 0, pk, v, 0, 0, 0)
                                    .withIndex((int) index.id(), value, kind).withTransaction(v, ordinal).withWorkUnitId(unit));
                            count(m.get("ev").getAsString());
                        }
                        default -> throw new IllegalStateException();
                    }
                }
                workload.add(IndexOperationTrace.transaction(event++, IndexOperationTrace.TRANSACTION_COMMIT, v, v, ordinal));
                for (var s : pending.getOrDefault((long) v, List.of())) workload.add(s);
            }
        }
    }
}
