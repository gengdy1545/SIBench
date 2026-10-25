package org.sibench.cli;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.capture.AccessLedger;
import org.sibench.capture.CaptureCompleteness;
import org.sibench.capture.CaptureProfile;
import org.sibench.capture.IndexTraceCompiler;
import org.sibench.capture.LedgerComparison;
import org.sibench.capture.RelocationCheck;
import org.sibench.capture.RowStateAudit;
import org.sibench.capture.RunTiming;
import org.sibench.config.ConfigValues;
import org.sibench.generation.BaselineGenerator;
import org.sibench.replay.ReferenceReplay;
import org.sibench.adapters.IndexServer;
import org.sibench.adapters.RocksDbBackend;
import org.sibench.adapters.ServiceConfig;
import org.sibench.adapters.ServiceReplay;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayMatrixValidation;
import org.sibench.replay.SnapshotLookupValidation;
import org.sibench.schema.PgSchemaCheck;
import org.sibench.schema.SourceSchema;
import org.sibench.workloads.ScaleInspector;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Sibench {
    private Sibench() {}

    private static void printServiceHelp(PrintStream out) {
        out.println(
                "       java -jar build/sibench/sibench.jar plan-write-ratio PROFILE CALIBRATION_TRACE"
                        + " RATIO   (native parameters for committed row writes = RATIO x baseline)");
        out.println(
                "       java -jar build/sibench/sibench.jar index-server SERVICE_CONFIG"
                        + "   (serves a fresh RocksDB index over the SIBench index protocol)");
        out.println(
                "       java -jar build/sibench/sibench.jar replay-service REPLAY_CONFIG"
                        + " NORMALIZED_DIRECTORY REPORT [--reference REFERENCE_REPORT]"
                        + "   (target.service: grpc endpoint or in-process rocksdb path)");
    }

    public static void main(String[] args) {
        // Close before exiting: System.exit() never returns, so a finally block would not run.
        int status = run(args, System.out, System.err);
        RunTiming.current().close();
        System.exit(status);
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("--help") || args[0].equals("-h")) {
            printHelp(out);
            return 0;
        }
        try {
            if (!List.of(
                            "inspect-scale",
                            "prepare",
                            "replay",
                            "check-replay",
                            "validate-snapshots",
                            "check-schema",
                            "audit-rows",
                            "access-ledger",
                            "compare-ledgers",
                            "snapshot-index-counters",
                            "check-capture-completeness",
                            "compile-trace",
                            "certify-capture",
                            "validate-replay-matrix",
                            "check-relocation",
                            "index-server",
                            "replay-service",
                            "plan-write-ratio")
                    .contains(args[0])) {
                throw new IllegalArgumentException("Unknown command: " + args[0]);
            }
            Path root = Path.of("").toAbsolutePath();
            List<String> profiles = new ArrayList<>();
            boolean dryRun = false;
            String jdbc = null, user = null, reference = null, certification = null;
            String countersBefore = null, countersAfter = null;
            for (int i = 1; i < args.length; i++) {
                if (args[i].equals("--help") || args[i].equals("-h")) {
                    printHelp(out);
                    return 0;
                } else if (args[i].equals("--dry-run") && args[0].equals("prepare")) {
                    dryRun = true;
                } else if (List.of("check-schema", "audit-rows", "snapshot-index-counters")
                                .contains(args[0])
                        && args[i].equals("--jdbc")) {
                    if (++i == args.length)
                        throw new IllegalArgumentException("--jdbc requires a URL");
                    jdbc = args[i];
                } else if (List.of("check-schema", "audit-rows", "snapshot-index-counters")
                                .contains(args[0])
                        && args[i].equals("--user")) {
                    if (++i == args.length)
                        throw new IllegalArgumentException("--user requires a role");
                    user = args[i];
                } else if (args[0].equals("compile-trace") && args[i].equals("--certification")) {
                    if (++i == args.length)
                        throw new IllegalArgumentException("--certification requires a certificate");
                    certification = args[i];
                } else if (args[0].equals("compile-trace") && args[i].equals("--counters")) {
                    if (i + 2 >= args.length)
                        throw new IllegalArgumentException("--counters requires BEFORE AFTER snapshots");
                    countersBefore = args[++i];
                    countersAfter = args[++i];
                } else if (args[0].equals("replay-service") && args[i].equals("--reference")) {
                    if (++i == args.length)
                        throw new IllegalArgumentException("--reference requires a report");
                    reference = args[i];
                } else if (args[i].equals("--root")) {
                    if (++i == args.length) {
                        throw new IllegalArgumentException("--root requires a path");
                    }
                    root = Path.of(args[i]).toAbsolutePath();
                } else if (args[i].equals("--timing")) {
                    if (++i == args.length)
                        throw new IllegalArgumentException("--timing requires a path");
                    RunTiming.open(Path.of(args[i]).toAbsolutePath()).phase(args[0]);
                } else if (args[i].startsWith("-")) {
                    throw new IllegalArgumentException("Unknown option: " + args[i]);
                } else {
                    profiles.add(args[i]);
                }
            }
            root = root.toRealPath();
            if (args[0].equals("audit-rows")) {
                if ((profiles.size() != 3 && profiles.size() != 4) || jdbc == null || user == null)
                    throw new IllegalArgumentException(
                            "audit-rows requires PROFILE BASELINE RAW [REPORT] --jdbc URL --user"
                                + " ROLE");
                if (!jdbc.startsWith("jdbc:postgresql:"))
                    throw new IllegalArgumentException("audit-rows requires a PostgreSQL JDBC URL");
                var document = ConfigValues.parse(Files.readString(root.resolve(profiles.get(0))));
                Path raw = root.resolve(profiles.get(2));
                var report =
                        RowStateAudit.audit(
                                jdbc,
                                user,
                                SourceSchema.read(root, document),
                                root.resolve(profiles.get(1)),
                                raw,
                                profiles.size() == 4
                                        ? root.resolve(profiles.get(3))
                                        : RowStateAudit.defaultReport(raw));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(report));
                return 0;
            }
            if (args[0].equals("access-ledger")) {
                if (profiles.size() != 2 && profiles.size() != 3)
                    throw new IllegalArgumentException("access-ledger requires PROFILE RAW [REPORT]");
                var document = ConfigValues.parse(Files.readString(root.resolve(profiles.get(0))));
                Path raw = root.resolve(profiles.get(1));
                var report =
                        AccessLedger.audit(
                                SourceSchema.read(root, document),
                                raw,
                                profiles.size() == 3
                                        ? root.resolve(profiles.get(2))
                                        : AccessLedger.defaultReport(raw));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(report));
                return 0;
            }
            if (args[0].equals("compare-ledgers")) {
                if (profiles.size() != 2 && profiles.size() != 3)
                    throw new IllegalArgumentException(
                            "compare-ledgers requires REPORT_A REPORT_B [OUTPUT]");
                var report =
                        LedgerComparison.compare(
                                json(root.resolve(profiles.get(0))),
                                json(root.resolve(profiles.get(1))));
                if (profiles.size() == 2) {
                    out.println(new GsonBuilder().setPrettyPrinting().create().toJson(report));
                    return 0;
                }
                Path output = root.resolve(profiles.get(2));
                if (Files.exists(output))
                    throw new IOException("Ledger comparison already exists: " + output);
                Files.writeString(
                        output,
                        new GsonBuilder().setPrettyPrinting().create().toJson(report),
                        StandardOpenOption.CREATE_NEW);
                return 0;
            }
            if (args[0].equals("snapshot-index-counters")) {
                if (profiles.size() != 2 || jdbc == null || user == null)
                    throw new IllegalArgumentException(
                            "snapshot-index-counters requires PROFILE OUTPUT --jdbc URL --user"
                                + " ROLE");
                if (!jdbc.startsWith("jdbc:postgresql:"))
                    throw new IllegalArgumentException(
                            "snapshot-index-counters requires a PostgreSQL JDBC URL");
                if (!Files.exists(root.resolve(profiles.get(0))))
                    throw new IOException("Unknown capture profile: " + profiles.get(0));
                var report =
                        CaptureCompleteness.snapshot(
                                jdbc, user, root.resolve(profiles.get(1)), profiles.get(0));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(report));
                return 0;
            }
            if (args[0].equals("check-capture-completeness")) {
                if (profiles.size() != 4)
                    throw new IllegalArgumentException(
                            "check-capture-completeness requires PROFILE LEDGER BEFORE AFTER");
                if (!Files.exists(root.resolve(profiles.get(0))))
                    throw new IOException("Unknown capture profile: " + profiles.get(0));
                var report =
                        CaptureCompleteness.compare(
                                json(root.resolve(profiles.get(1))),
                                json(root.resolve(profiles.get(2))),
                                json(root.resolve(profiles.get(3))));
                out.println(
                        new GsonBuilder()
                                .serializeNulls()
                                .setPrettyPrinting()
                                .disableHtmlEscaping()
                                .create()
                                .toJson(report));
                if (!"consistent".equals(report.get("status"))) {
                    err.println(
                            "Index counters disagree with the access ledger: "
                                    + report.get("inconsistent_indexes"));
                    return 1;
                }
                return 0;
            }
            if (args[0].equals("validate-replay-matrix")) {
                if (profiles.size() != 2)
                    throw new IllegalArgumentException(
                            "validate-replay-matrix requires NORMALIZED_DIRECTORY OUTPUT_DIRECTORY");
                ReplayMatrixValidation.validate(
                        root.resolve(profiles.get(0)), root.resolve(profiles.get(1)));
                out.println(Files.readString(root.resolve(profiles.get(1)).resolve("matrix.json")));
                return 0;
            }
            if (args[0].equals("check-relocation")) {
                if (profiles.size() != 3)
                    throw new IllegalArgumentException(
                            "check-relocation requires PROFILE NORMALIZED_DIRECTORY OUTPUT_DIRECTORY");
                Path output = root.resolve(profiles.get(2));
                RelocationCheck.validate(
                        CaptureProfile.read(root, Path.of(profiles.get(0))).layout(),
                        root.resolve(profiles.get(1)),
                        output);
                out.println(Files.readString(output.resolve("report.json")));
                return 0;
            }
            if (args[0].equals("compile-trace")) {
                if (profiles.size() != 2 && profiles.size() != 3)
                    throw new IllegalArgumentException(
                            "compile-trace requires PROFILE OUTPUT [ROW_AUDIT] [--certification"
                                + " CERTIFICATE] [--counters BEFORE AFTER]");
                var checks =
                        new IndexTraceCompiler.Checks(
                                profiles.size() == 3 ? root.resolve(profiles.get(2)) : null,
                                certification == null ? null : root.resolve(certification),
                                countersBefore == null ? null : root.resolve(countersBefore),
                                countersAfter == null ? null : root.resolve(countersAfter));
                var report =
                        IndexTraceCompiler.compileProfile(
                                root, Path.of(profiles.get(0)), root.resolve(profiles.get(1)), checks);
                var summary = new LinkedHashMap<String, Object>();
                for (String key :
                        List.of(
                                "status",
                                "benchmark",
                                "session_order",
                                "baseline_operations",
                                "workload_operations",
                                "searched_accesses",
                                "search_operations",
                                "searched_accesses_by_shape",
                                "excluded_access_units",
                                "committed_transactions",
                                "row_write_ratio",
                                "utility_only_transactions",
                                "business_changes",
                                "verification",
                                "capture_completeness"))
                    summary.put(key, report.get(key));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(summary));
                return 0;
            }
            if (args[0].equals("certify-capture")) {
                if (profiles.size() != 1)
                    throw new IllegalArgumentException("certify-capture requires PROFILE");
                var report =
                        org.sibench.capture.Certification.certify(
                                org.sibench.capture.CaptureProfile.read(root, Path.of(profiles.get(0))));
                var summary = new LinkedHashMap<String, Object>(report);
                summary.remove("coverage");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(summary));
                return 0;
            }
            if (args[0].equals("check-schema")) {
                if (profiles.size() != 1 || jdbc == null || user == null)
                    throw new IllegalArgumentException(
                            "check-schema requires PROFILE --jdbc URL --user ROLE");
                if (!jdbc.startsWith("jdbc:postgresql:"))
                    throw new IllegalArgumentException(
                            "check-schema requires a PostgreSQL JDBC URL");
                var document = ConfigValues.parse(Files.readString(root.resolve(profiles.get(0))));
                try {
                    out.println(
                            PgSchemaCheck.json(PgSchemaCheck.check(root, document, jdbc, user)));
                    return 0;
                } catch (PgSchemaCheck.Mismatch mismatch) {
                    out.println(PgSchemaCheck.json(mismatch.report()));
                    err.println(mismatch.getMessage());
                    return 1;
                }
            }
            if (args[0].equals("plan-write-ratio")) {
                if (profiles.size() != 3)
                    throw new IllegalArgumentException(
                            "plan-write-ratio requires PROFILE CALIBRATION_TRACE RATIO");
                var plan =
                        org.sibench.capture.WriteRatioPlan.plan(
                                root,
                                Path.of(profiles.get(0)),
                                root.resolve(profiles.get(1)),
                                Double.parseDouble(profiles.get(2)));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(plan));
                return 0;
            }
            if (args[0].equals("index-server")) {
                if (profiles.size() != 1)
                    throw new IllegalArgumentException("index-server requires SERVICE_CONFIG");
                var service = ServiceConfig.ofServer(root, Path.of(profiles.get(0)));
                try (var backend = RocksDbBackend.create(service.path(), service.value());
                        var server = IndexServer.start(service.port(), backend, service.ordered())) {
                    out.println("index-server " + backend.describe().getName() + " on port " + server.port());
                    out.flush();
                    server.awaitTermination();
                }
                return 0;
            }
            if (args[0].equals("replay-service")) {
                if (profiles.size() != 3)
                    throw new IllegalArgumentException(
                            "replay-service requires REPLAY_CONFIG NORMALIZED_DIRECTORY REPORT [--reference REPORT]");
                var document = ConfigValues.parse(Files.readString(root.resolve(profiles.get(0))));
                ReplayConfig config = ReplayConfig.from(document);
                ServiceConfig service = ServiceConfig.ofReplay(root, document);
                if (service == null)
                    throw new IllegalArgumentException("replay-service needs target.service in its config");
                var result =
                        ServiceReplay.run(
                                root.resolve(profiles.get(1)),
                                config,
                                service,
                                reference == null ? null : root.resolve(reference),
                                root.resolve(profiles.get(2)));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(result.get("result")));
                return 0;
            }
            if (List.of("replay", "check-replay", "validate-snapshots").contains(args[0])) {
                boolean check = args[0].equals("check-replay");
                if (profiles.size() != (check ? 2 : 3))
                    throw new IllegalArgumentException(
                            args[0]
                                    + " requires REPLAY_CONFIG NORMALIZED_DIRECTORY"
                                    + (check ? "" : " REPORT"));
                ReplayConfig config = ReplayConfig.read(root, Path.of(profiles.get(0)));
                Path directory = root.resolve(profiles.get(1));
                var result =
                        check
                                ? ReferenceReplay.check(directory, config)
                                : args[0].equals("validate-snapshots")
                                        ? SnapshotLookupValidation.run(
                                                directory, config, root.resolve(profiles.get(2)))
                                        : ReferenceReplay.run(
                                                directory, config, root.resolve(profiles.get(2)));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(result));
                return 0;
            }
            if (args[0].equals("prepare")) {
                if (profiles.size() != 1)
                    throw new IllegalArgumentException(
                            "prepare requires exactly one configuration");
                var prepared = BaselineGenerator.prepare(root, Path.of(profiles.get(0)));
                var result =
                        dryRun
                                ? BaselineGenerator.describe(prepared)
                                : BaselineGenerator.generate(prepared);
                out.println(
                        new GsonBuilder()
                                .serializeNulls()
                                .setPrettyPrinting()
                                .create()
                                .toJson(result));
                return 0;
            }
            if (profiles.isEmpty()) {
                profiles.addAll(ScaleInspector.DEFAULT_PROFILES);
            }
            List<Map<String, Object>> reports = new ArrayList<>();
            for (String profile : profiles) {
                reports.add(ScaleInspector.inspect(root, Path.of(profile)));
            }
            Map<String, Object> report = new LinkedHashMap<>();
            report.put("status", "profiles_valid_generation_pending");
            report.put("workspace_free_bytes", Files.getFileStore(root).getUsableSpace());
            report.put("profiles", reports);
            out.println(
                    new GsonBuilder()
                            .serializeNulls()
                            .setPrettyPrinting()
                            .disableHtmlEscaping()
                            .create()
                            .toJson(report));
            return 0;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            err.println("SIBench interrupted");
            return 1;
        } catch (Exception error) {
            err.println("SIBench failed: " + error.getMessage());
            if (System.getProperty("sibench.trace") != null) error.printStackTrace(err);
            return 1;
        }
    }

    private static JsonObject json(Path path) throws IOException {
        return JsonParser.parseString(Files.readString(path)).getAsJsonObject();
    }

    private static void printHelp(PrintStream out) {
        printServiceHelp(out);
        out.println(
                "Usage: java -jar build/sibench/sibench.jar inspect-scale [--root PATH] [PROFILE"
                        + " ...]");
        out.println("Root defaults to the working directory. Profiles are relative to root.");
        out.println(
                "Every command accepts --timing FILE: its phase wall clock, event count and the"
                    + " facts noted by the command are appended to that one file, so a whole run"
                    + " leaves a single timing artifact.");
        out.println(
                "       java -jar build/sibench/sibench.jar check-schema [--root PATH]"
                        + " PROFILE --jdbc URL --user ROLE");
        out.println(
                "       java -jar build/sibench/sibench.jar audit-rows [--root PATH]"
                        + " PROFILE BASELINE RAW [REPORT] --jdbc URL --user ROLE");
        out.println(
                "audit-rows verifies source row state once; unchanged inputs reuse the report.");
        out.println(
                "       java -jar build/sibench/sibench.jar access-ledger [--root PATH]"
                        + " PROFILE RAW [REPORT]");
        out.println(
                "access-ledger partitions raw PG index events into access units; it never touches PG.");
        out.println(
                "       java -jar build/sibench/sibench.jar compare-ledgers [--root PATH]"
                        + " REPORT_A REPORT_B [OUTPUT]");
        out.println(
                "compare-ledgers aligns two access-ledger reports by query_id and reports plan and"
                    + " access-path drift.");
        out.println(
                "       java -jar build/sibench/sibench.jar snapshot-index-counters [--root PATH]"
                        + " PROFILE OUTPUT --jdbc URL --user ROLE");
        out.println(
                "snapshot-index-counters waits for other sessions to leave and the counters to"
                    + " settle before reading them.");
        out.println(
                "       java -jar build/sibench/sibench.jar check-capture-completeness [--root PATH]"
                        + " PROFILE LEDGER BEFORE AFTER");
        out.println(
                "check-capture-completeness cross-checks a ledger against the PG index counter"
                    + " deltas of two snapshots; it never touches PG and exits 1 when they disagree.");
        out.println(
                "       java -jar build/sibench/sibench.jar compile-trace [--root PATH]"
                        + " PROFILE OUTPUT [ROW_AUDIT] [--certification CERTIFICATE]"
                        + " [--counters BEFORE AFTER]");
        out.println(
                "compile-trace turns a completed capture into baseline/workload traces and its access"
                        + " ledger in one pass; any access it cannot project fails the compilation."
                        + " --counters requires the ledger to match PG's index counters;"
                        + " --certification replaces the row audit by a certified build's coverage.");
        out.println(
                "       java -jar build/sibench/sibench.jar certify-capture [--root PATH] PROFILE");
        out.println(
                "certify-capture writes ARTIFACTS/certification.json for a compiled capture whose row"
                        + " audit and counter check passed.");
        out.println(
                "       java -jar build/sibench/sibench.jar validate-replay-matrix [--root PATH]"
                        + " NORMALIZED_DIRECTORY OUTPUT_DIRECTORY");
        out.println(
                "validate-replay-matrix replays 1/64/128 clients x both dispatch modes x secondary"
                    + " indexes on/off and requires identical operation order, state and results.");
        out.println(
                "       java -jar build/sibench/sibench.jar check-relocation [--root PATH]"
                        + " PROFILE NORMALIZED_DIRECTORY OUTPUT_DIRECTORY");
        out.println(
                "check-relocation rewrites the latest live rows of modified tables and checks exact"
                    + " Locator relocation with RowIDs unchanged.");
        out.println(
                "check-schema reads the live PG catalog; it never creates or repairs"
                        + " tables/indexes.");
        out.println(
                "       java -jar build/sibench/sibench.jar prepare [--root PATH] [--dry-run]"
                        + " PROFILE");
        out.println(
                "prepare writes baseline Parquet files; --dry-run only validates and describes"
                        + " schemas.");
        out.println(
                "       java -jar build/sibench/sibench.jar check-replay [--root PATH]"
                        + " REPLAY_CONFIG NORMALIZED_DIRECTORY");
        out.println(
                "       java -jar build/sibench/sibench.jar replay [--root PATH]"
                        + " REPLAY_CONFIG NORMALIZED_DIRECTORY REPORT");
        out.println(
                "replay runs the in-process reference once; no real backend or trace repetition.");
        out.println(
                "       java -jar build/sibench/sibench.jar validate-snapshots [--root PATH]"
                        + " REPLAY_CONFIG NORMALIZED_DIRECTORY REPORT");
        out.println(
                "validate-snapshots runs derived history correctness probes, not native workload or"
                        + " performance.");
    }
}
