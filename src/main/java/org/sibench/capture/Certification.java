package org.sibench.capture;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A fully verified capture vouches for later captures of the same build, so the checks that
 * prove the method, rather than one run, are not repeated on every capture.
 *
 * <p>A certifying capture passed every check: the client/server session audits, the row-state
 * audit against the baseline and PostgreSQL's final state, the access ledger against PG's own
 * index counters, and the compilation's structural checks. Its certificate records the capture
 * build that ran (the capture code of the run's jar, see {@link CaptureCode}, the server, the
 * trigger library and the plan-relevant server parameters), the source and index definitions, the
 * capture format, and what the run exercised: every access path (source, index, scan kind,
 * projection decision) and every committed change class (table, operation). The compiler is not
 * part of the build: compiling with a changed compiler keeps the certificate.
 *
 * <p>A certified capture skips the row-state audit. It still runs its own session audits, ledger
 * partition, counter check and compilation checks, and it must stay inside the certificate: the
 * same build, source, index definitions and format, and no access path or change class the
 * certifying capture did not exercise. Anything new fails the compilation until a capture that
 * exercises it is certified.
 */
public final class Certification {
    /** 2: the build is the capture code of the run's jar, no longer the whole jar. */
    public static final int VERSION = 2;
    /** Where native-capture.sh keeps the immutable copy of the jar a capture ran. */
    static final String RUN_JAR = "native/sibench-run.jar";
    public static final String FILE = "certification.json";

    private Certification() {}

    /** Certifies a compiled capture whose full verification passed. */
    public static Map<String, Object> certify(CaptureProfile capture) throws Exception {
        Path artifacts = capture.artifacts(), trace = artifacts.resolve("trace");
        Path output = artifacts.resolve(FILE);
        if (Files.exists(output)) throw new IOException("Certification already exists: " + output);
        if (Files.exists(trace.resolve("INCOMPLETE")) || !Files.exists(trace.resolve("manifest.json")))
            throw new IOException("Only a compiled trace can be certified: " + trace);
        CaptureRun.verifyInput(capture.raw());
        JsonObject manifest = json(trace.resolve("manifest.json"));
        JsonObject verification = manifest.getAsJsonObject("verification");
        if (verification == null || !verification.get("mode").getAsString().equals("row_audit"))
            throw new IOException("Only a capture verified by its row-state audit can certify");
        Path audit = RowStateAudit.defaultReport(capture.raw());
        if (!BaselineGenerator.sha256(audit).equals(verification.get("row_state_audit_sha256").getAsString()))
            throw new IOException("The trace was compiled against a different row audit");
        RowStateAudit.reuse(capture.schema(), capture.baseline(), capture.raw(), audit);
        if (!manifest.has("capture_completeness")
                || !manifest.get("capture_completeness").getAsString().equals("consistent"))
            throw new IOException("Only a capture whose ledger matched PG's index counters can certify");
        Path ledgerFile = trace.resolve("access-ledger.json");
        if (!BaselineGenerator.sha256(ledgerFile).equals(manifest.get("access_ledger_sha256").getAsString()))
            throw new IOException("Access ledger changed after compilation");
        JsonObject ledger = json(ledgerFile);
        JsonObject run = json(capture.raw().resolve("capture-run.json"));
        var result = new LinkedHashMap<String, Object>();
        result.put("schema_version", VERSION);
        result.put("status", "certified");
        result.put("benchmark", manifest.get("benchmark").getAsString());
        result.put("build", captureBuild(run, runJar(capture.artifacts())));
        result.put("source", source(manifest));
        result.put("capture_detail", details(ledger));
        var coverage = new LinkedHashMap<String, Object>();
        coverage.put("access_paths", accessPaths(ledger));
        coverage.put("row_changes", changeClasses(manifest));
        result.put("coverage", coverage);
        var evidence = new LinkedHashMap<String, Object>();
        evidence.put("artifacts", artifacts.toString());
        evidence.put("capture_run_sha256", BaselineGenerator.sha256(capture.raw().resolve("capture-run.json")));
        evidence.put("row_state_audit_sha256", verification.get("row_state_audit_sha256").getAsString());
        evidence.put("access_ledger_sha256", manifest.get("access_ledger_sha256").getAsString());
        evidence.put("trace_manifest_sha256", BaselineGenerator.sha256(trace.resolve("manifest.json")));
        evidence.put("row_write_ratio", manifest.get("row_write_ratio"));
        evidence.put("searched_accesses", manifest.get("searched_accesses"));
        result.put("certified_capture", evidence);
        result.put(
                "rule",
                "a capture of the same build, source, index definitions and capture detail whose access"
                        + " paths and committed change classes are all listed here may skip the"
                        + " row-state audit; it keeps its session audits, ledger, counter check and"
                        + " compilation checks");
        Files.writeString(
                output,
                new GsonBuilder().setPrettyPrinting().create().toJson(result),
                StandardOpenOption.CREATE_NEW);
        return result;
    }

    /**
     * The certificate a capture relies on, once its build, source and index definitions match;
     * coverage is checked when the capture's ledger is complete, see {@link #requireCovered}.
     */
    static JsonObject require(Path certificate, Path raw, Path runJar, Map<String, Object> provenance)
            throws IOException {
        JsonObject value = json(certificate);
        if (!value.get("status").getAsString().equals("certified"))
            throw new IOException("Not a certification: " + certificate);
        if (value.get("schema_version").getAsInt() != VERSION)
            throw new IOException(
                    "Certification of schema "
                            + value.get("schema_version")
                            + ", this build reads "
                            + VERSION
                            + "; certify the capture again with certify-capture: "
                            + certificate);
        if (runJar == null)
            throw new IOException("A certified compilation needs the jar the capture ran");
        JsonObject run = json((Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent()).resolve("capture-run.json"));
        JsonObject build = new GsonBuilder().create().toJsonTree(captureBuild(run, runJar)).getAsJsonObject();
        if (!build.equals(value.get("build")))
            throw new IOException(
                    "The capture's build differs from the certified build "
                            + certificate
                            + differences(build, value.getAsJsonObject("build")));
        JsonObject source = new GsonBuilder().create().toJsonTree(provenance).getAsJsonObject();
        if (!source(source).equals(value.getAsJsonObject("source")))
            throw new IOException("The capture's source or index definitions differ from the certification");
        return value;
    }

    /** The run's jar copy under a capture's artifacts directory. */
    static Path runJar(Path artifacts) {
        return artifacts.resolve(RUN_JAR);
    }

    /**
     * The part of a capture's recorded build a certificate vouches for: the capture code of the jar
     * it ran instead of that jar's digest, plus everything else the run recorded (server, trigger
     * library, plan-relevant parameters). The jar must be the very one the run recorded.
     */
    static Map<String, Object> captureBuild(JsonObject run, Path runJar) throws IOException {
        if (!run.has("build") || !CaptureBuild.complete(map(run.getAsJsonObject("build"))))
            throw new IOException("The capture recorded no complete build; run it from the packaged jar");
        var recorded = map(run.getAsJsonObject("build"));
        if (!Files.isRegularFile(runJar))
            throw new IOException("The jar the capture ran is missing: " + runJar);
        if (!FileDigests.sha256(runJar).equals(recorded.get("sibench_jar_sha256")))
            throw new IOException("This jar is not the one the capture ran: " + runJar);
        var result = new LinkedHashMap<String, Object>();
        result.put("capture_code", CaptureCode.identity(runJar));
        for (var entry : new java.util.TreeMap<>(recorded).entrySet())
            if (!entry.getKey().equals("sibench_jar_sha256")) result.put(entry.getKey(), entry.getValue());
        return result;
    }

    /** The build fields and capture classes that differ, for the failure message. */
    private static String differences(JsonObject actual, JsonObject certified) {
        var changed = new TreeSet<String>();
        if (certified == null) return "";
        for (String key : union(actual.keySet(), certified.keySet())) {
            JsonElement a = actual.get(key), c = certified.get(key);
            if (java.util.Objects.equals(a, c)) continue;
            if (!key.equals("capture_code") || a == null || c == null) {
                changed.add(key);
                continue;
            }
            JsonObject ac = a.getAsJsonObject(), cc = c.getAsJsonObject();
            for (String part : union(ac.keySet(), cc.keySet())) {
                if (java.util.Objects.equals(ac.get(part), cc.get(part))) continue;
                if (!part.equals("classes") || !ac.has(part) || !cc.has(part)) {
                    changed.add("capture_code." + part);
                    continue;
                }
                JsonObject x = ac.getAsJsonObject(part), y = cc.getAsJsonObject(part);
                for (String name : union(x.keySet(), y.keySet()))
                    if (!java.util.Objects.equals(x.get(name), y.get(name))) changed.add(name);
            }
        }
        return changed.isEmpty() ? "" : ": " + String.join(", ", changed);
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        var result = new TreeSet<String>(a);
        result.addAll(b);
        return result;
    }

    /** Fails on anything the certifying capture did not exercise. */
    static void requireCovered(JsonObject certificate, JsonObject ledger, Map<String, ?> changesByTable)
            throws IOException {
        var problems = new ArrayList<String>();
        var details = new TreeSet<String>(details(ledger));
        var certifiedDetails = strings(certificate.getAsJsonArray("capture_detail"));
        if (!certifiedDetails.containsAll(details))
            problems.add("capture detail " + details + " not in " + certifiedDetails);
        JsonObject coverage = certificate.getAsJsonObject("coverage");
        var paths = new TreeSet<>(accessPaths(ledger));
        paths.removeAll(strings(coverage.getAsJsonArray("access_paths")));
        if (!paths.isEmpty()) problems.add("access paths " + paths);
        var changes = new TreeSet<>(changeClasses(changesByTable));
        changes.removeAll(strings(coverage.getAsJsonArray("row_changes")));
        if (!changes.isEmpty()) problems.add("row changes " + changes);
        if (!problems.isEmpty())
            throw new IOException(
                    "Capture is outside its certification ("
                            + String.join("; ", problems)
                            + "); certify a fully verified capture that exercises them");
    }

    /** source|index|scan kind|decision of every access path the ledger reports. */
    static List<String> accessPaths(JsonObject ledger) {
        var result = new TreeSet<String>();
        for (JsonElement template : ledger.getAsJsonArray("templates"))
            for (JsonElement item : template.getAsJsonObject().getAsJsonArray("access_paths")) {
                JsonObject path = item.getAsJsonObject();
                result.add(String.join(
                        "|",
                        path.get("source").getAsString(),
                        path.get("index").getAsString(),
                        path.get("kind").getAsString(),
                        path.get("decision").getAsString()));
            }
        return List.copyOf(result);
    }

    private static List<String> changeClasses(JsonObject manifest) {
        return changeClasses(map(manifest.getAsJsonObject("business_changes_by_table")));
    }

    /** table|operation of every committed change class with at least one change. */
    static List<String> changeClasses(Map<String, ?> byTable) {
        var result = new TreeSet<String>();
        for (var table : byTable.entrySet())
            for (var operation : ((Map<?, ?>) table.getValue()).entrySet())
                if (((Number) operation.getValue()).longValue() > 0)
                    result.add(table.getKey() + "|" + operation.getKey());
        return List.copyOf(result);
    }

    private static List<String> details(JsonObject ledger) {
        var result = new TreeSet<String>();
        for (JsonElement input : ledger.getAsJsonArray("inputs"))
            result.add(input.getAsJsonObject().get("detail").getAsString());
        return List.copyOf(result);
    }

    /** What must not change between a certification and the captures it vouches for. */
    private static JsonObject source(JsonObject provenance) {
        var result = new JsonObject();
        for (String field : List.of("benchmark", "source_commit", "source_schema_sha256", "index_definitions"))
            if (provenance.has(field)) result.add(field, provenance.get(field));
        return result;
    }

    private static Set<String> strings(com.google.gson.JsonArray array) {
        var result = new TreeSet<String>();
        for (JsonElement item : array) result.add(item.getAsString());
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(JsonObject value) {
        return new com.google.gson.Gson().fromJson(value, Map.class);
    }

    private static JsonObject json(Path file) throws IOException {
        return JsonParser.parseString(Files.readString(file)).getAsJsonObject();
    }
}
