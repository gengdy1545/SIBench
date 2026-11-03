package org.sibench.capture;

import com.google.gson.*;
import org.sibench.generation.GenerationConfig;
import org.sibench.schema.SourceSchema;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Feeds retained, hand-checkable source fixtures into the production projection engine. */
final class CaptureFixtureCompiler {
    private CaptureFixtureCompiler() {}

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
        if (auditReport != null) RowStateAudit.reuse(sourceSchema, layout.output(), raw, auditReport);
        return compile(sourceSchema, layout, raw, output, new IndexTraceCompiler.Checks(null, null));
    }

    public static Map<String, Object> compile(
            SourceSchema sourceSchema, GenerationConfig layout, Path raw, Path output, IndexTraceCompiler.Checks checks)
            throws Exception {
        return compile(sourceSchema, layout, raw, output, checks, IndexTraceCompiler.IDENTITY_MEMORY_ENTRIES, IndexTraceCompiler.COMMIT_WINDOW);
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
        if (auditReport != null) RowStateAudit.reuse(sourceSchema, layout.output(), raw, auditReport);
        return compile(sourceSchema, layout, raw, output, new IndexTraceCompiler.Checks(null, null), identityMemory, commitWindow);
    }

    static Map<String, Object> compile(
            SourceSchema sourceSchema,
            GenerationConfig layout,
            Path raw,
            Path output,
            IndexTraceCompiler.Checks checks,
            int identityMemory,
            int commitWindow)
            throws Exception {
        return compile(sourceSchema, layout, raw, output, checks, identityMemory, commitWindow, TraceOutput.FULL);
    }

    public static Map<String, Object> compile(SourceSchema sourceSchema, GenerationConfig layout, Path raw,
            Path output, IndexTraceCompiler.Checks checks, TraceOutput strategy) throws Exception {
        return compile(sourceSchema, layout, raw, output, checks, IndexTraceCompiler.IDENTITY_MEMORY_ENTRIES, IndexTraceCompiler.COMMIT_WINDOW, strategy);
    }

    private static Map<String, Object> compile(SourceSchema schema, GenerationConfig layout, Path raw,
            Path output, IndexTraceCompiler.Checks checks, int identityMemory, int commitWindow,
            TraceOutput strategy) throws Exception {
        Sessions sessions = sessions(raw);
        return IndexTraceCompiler.generate(schema, layout, raw, output, checks, identityMemory,
                commitWindow, strategy, sessions.order(), state -> {
                    if (sessions.order().equals("concurrent")) return merge(state, sessions.files());
                    var reports = new ArrayList<Map<String, Object>>();
                    for (Path file : sessions.files()) reports.add(session(state, file));
                    return reports;
                });
    }

    /** One backend trace of a sequential-session capture. */
    static Map<String, Object> session(IndexTraceCompiler.Compilation state, Path file) throws IOException {
        long firstEvent = state.event + 1, sessionRequests = state.requestUnits, unitsBefore = state.ledger.settled();
        state.pid = 0;
        state.sessionFile = file.getFileName().toString();
        var tracker = new AccessUnits(state);
        tracker.beginSession();
        try (var reader = ServerTrace.open(file)) {
            String line;
            while ((line = reader.readLine()) != null) state.event(Event.parse(line), tracker);
        }
        AccessUnits.Detail detail = tracker.detail();
        tracker.endSession();
        state.ledger.absorb(tracker);
        state.finishSession();
        state.ledger.input(file, state.pid, detail, state.event - firstEvent + 1, state.ledger.settled() - unitsBefore);
        var report = report(file, state.pid, state.event - firstEvent + 1, state.requestUnits - sessionRequests);
        report.put("first_source_event", firstEvent);
        return report;
    }

    /**
     * Backend traces of one concurrent capture, merged into completion-counter order. Each
     * chunk is one transaction, so scans never cross a chunk boundary and the access tracker
     * can start fresh on every one: scan identities are per-backend pointers and would
     * otherwise collide between sessions.
     */
    static List<Map<String, Object>> merge(IndexTraceCompiler.Compilation state, List<Path> files) throws IOException {
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
                long firstEvent = state.event + 1, sessionRequests = state.requestUnits, unitsBefore = state.ledger.settled();
                state.pid = 0;
                state.sessionFile = files.get(chunk.session()).getFileName().toString();
                var tracker = new AccessUnits(state);
                if (details[chunk.session()] == null) tracker.beginSession();
                else tracker.continueSession(details[chunk.session()]);
                // The final record is line lineCount, so its ordinal is known now.
                state.knownCommit = chunk.committed();
                state.knownEvent = state.event + chunk.lineCount();
                for (Event item; (item = merge.event()) != null; ) state.event(item, tracker);
                details[chunk.session()] = tracker.detail();
                tracker.endSession();
                state.ledger.absorb(tracker);
                state.finishSession();
                accessUnits[chunk.session()] += state.ledger.settled() - unitsBefore;
                events[chunk.session()] += state.event - firstEvent + 1;
                requests[chunk.session()] += state.requestUnits - sessionRequests;
                backends[chunk.session()] = state.pid;
            }
        }
        var reports = new ArrayList<Map<String, Object>>();
        for (int session = 0; session < files.size(); session++) {
            if (details[session] == null)
                throw new IOException(
                        "Session trace has no capture_start: " + files.get(session));
            var detail = details[session];
            state.ledger.input(files.get(session), backends[session], detail, events[session], accessUnits[session]);
            reports.add(report(files.get(session), backends[session], events[session], requests[session]));
        }
        return reports;
    }

    private static Map<String, Object> report(Path file, int backend, long events, long requests)
            throws IOException {
        var report = new LinkedHashMap<String, Object>();
        report.put("pg_file", file.getFileName().toString());
        report.put("sha256", FileDigests.sha256(file));
        report.put("pid", backend);
        report.put("source_events", events);
        report.put("source_requests", requests);
        return report;
    }

}
