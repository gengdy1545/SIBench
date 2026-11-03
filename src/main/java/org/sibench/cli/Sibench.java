package org.sibench.cli;

import org.sibench.replay.ReplayInput;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.capture.AccessLedger;
import org.sibench.capture.CaptureCompleteness;
import org.sibench.capture.CaptureProfile;
import org.sibench.capture.LedgerComparison;
import org.sibench.capture.RowStateAudit;
import org.sibench.capture.RunTiming;
import org.sibench.config.ConfigValues;
import org.sibench.adapters.IndexServer;
import org.sibench.adapters.ServiceConfig;
import org.sibench.adapters.ServiceReplay;
import org.sibench.replay.ReplayConfig;
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
        out.println("       java -jar build/sibench/sibench.jar postgres-config CONFIG OUTPUT");
        out.println(
                "       java -jar build/sibench/sibench.jar plan-write-ratio PROFILE CALIBRATION_TRACE"
                        + " RATIO   (native parameters for committed row writes = RATIO x baseline)");
        out.println(
                "       java -jar build/sibench/sibench.jar plan-operations PROFILE CALIBRATION_TRACE"
                        + " REPLAY_CONFIG   (native parameters for the replay configuration's operation budget)");
        out.println(
                "       java -jar build/sibench/sibench.jar index-server SERVICE_CONFIG"
                        + "   (serves a new or reopened RocksDB / rocksdb-cloud index over the SIBench index protocol)");
        out.println(
                "       java -jar build/sibench/sibench.jar replay-service REPLAY_CONFIG"
                        + " TRACE_DIRECTORY REPORT"
                        + "   (target.service: grpc endpoint or in-process rocksdb path)");
        out.println(
                "       java -jar build/sibench/sibench.jar verify-service REPLAY_CONFIG"
                        + " TRACE_DIRECTORY REPORT"
                        + "   (snapshot searches against an index already holding the trace)");
        out.println(
                "       java -jar build/sibench/sibench.jar analyze-locality TRACE CONFIG OUTPUT"
                        + "   (static baseline layout, update windows and overlap estimate)");
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
                            "postgres-config",
                            "check-replay",
                            "check-schema",
                            "audit-rows",
                            "audit-baseline",
                            "access-ledger",
                            "compare-ledgers",
                            "snapshot-index-counters",
                            "check-capture-completeness",
                            "index-server",
                            "replay-service",
                            "verify-service",
                            "plan-write-ratio",
                            "plan-operations",
                            "analyze-locality")
                    .contains(args[0])) {
                throw new IllegalArgumentException("Unknown command: " + args[0]);
            }
            Path root = Path.of("").toAbsolutePath();
            List<String> profiles = new ArrayList<>();
            String jdbc = null, user = null;
            for (int i = 1; i < args.length; i++) {
                if (args[i].equals("--help") || args[i].equals("-h")) {
                    printHelp(out);
                    return 0;
                } else if (List.of("check-schema", "audit-rows", "audit-baseline", "snapshot-index-counters")
                                .contains(args[0])
                        && args[i].equals("--jdbc")) {
                    if (++i == args.length)
                        throw new IllegalArgumentException("--jdbc requires a URL");
                    jdbc = args[i];
                } else if (List.of("check-schema", "audit-rows", "audit-baseline", "snapshot-index-counters")
                                .contains(args[0])
                        && args[i].equals("--user")) {
                    if (++i == args.length)
                        throw new IllegalArgumentException("--user requires a role");
                    user = args[i];
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
            if (args[0].equals("audit-baseline")) {
                if (profiles.size() != 2 || jdbc == null || user == null || !jdbc.startsWith("jdbc:postgresql:"))
                    throw new IllegalArgumentException("audit-baseline requires PROFILE BASELINE --jdbc URL --user ROLE");
                var document = ConfigValues.parse(Files.readString(root.resolve(profiles.get(0))));
                org.sibench.capture.PgBaselineVerify.verify(jdbc, user, SourceSchema.read(root, document), root.resolve(profiles.get(1)));
                out.println(Files.readString(root.resolve(profiles.get(1)).resolve("verified-manifest.json")));
                return 0;
            }
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
                        org.sibench.capture.CapturePlan.writeRatio(
                                root,
                                Path.of(profiles.get(0)),
                                root.resolve(profiles.get(1)),
                                Double.parseDouble(profiles.get(2)));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(plan));
                return 0;
            }
            if (args[0].equals("plan-operations")) {
                if (profiles.size() != 3)
                    throw new IllegalArgumentException(
                            "plan-operations requires PROFILE CALIBRATION_TRACE REPLAY_CONFIG");
                var plan =
                        org.sibench.capture.CapturePlan.operations(
                                root, Path.of(profiles.get(0)), root.resolve(profiles.get(1)), Path.of(profiles.get(2)));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(plan));
                return 0;
            }
            if (args[0].equals("postgres-config")) {
                if (profiles.size() != 2)
                    throw new IllegalArgumentException("postgres-config requires CONFIG OUTPUT");
                org.sibench.config.SibenchConfig.writePostgres(
                        root.resolve(profiles.get(0)), root.resolve(profiles.get(1)));
                out.println("PostgreSQL config written: " + profiles.get(1));
                return 0;
            }
            if (args[0].equals("index-server")) {
                if (profiles.size() != 1)
                    throw new IllegalArgumentException("index-server requires SERVICE_CONFIG");
                var service = ServiceConfig.ofServer(root, Path.of(profiles.get(0)));
                RunTiming.current().measure(service.path());
                try (var backend = service.backend();
                        var server = IndexServer.start(service.port(), backend)) {
                    out.println("index-server " + backend.describe().getName() + " on port " + server.port());
                    out.flush();
                    server.awaitTermination();
                }
                return 0;
            }
            if (args[0].equals("replay-service")) {
                if (profiles.size() != 3)
                    throw new IllegalArgumentException(
                            "replay-service requires REPLAY_CONFIG TRACE_DIRECTORY REPORT");
                var document = org.sibench.config.SibenchConfig.read(root.resolve(profiles.get(0)));
                ReplayConfig config = ReplayConfig.from(document);
                ServiceConfig service = ServiceConfig.ofReplay(root, document);
                var result =
                        ServiceReplay.run(
                                root.resolve(profiles.get(1)),
                                config,
                                service,
                                root.resolve(profiles.get(2)));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(result.get("result")));
                return 0;
            }
            if (args[0].equals("verify-service")) {
                if (profiles.size() != 3)
                    throw new IllegalArgumentException(
                            "verify-service requires REPLAY_CONFIG TRACE_DIRECTORY REPORT");
                var document = org.sibench.config.SibenchConfig.read(root.resolve(profiles.get(0)));
                ServiceConfig service = ServiceConfig.ofReplay(root, document);
                var result =
                        ServiceReplay.verify(
                                root.resolve(profiles.get(1)),
                                ReplayConfig.from(document),
                                service,
                                root.resolve(profiles.get(2)));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(result.get("result")));
                return 0;
            }
            if (args[0].equals("analyze-locality")) {
                if (profiles.size() != 3)
                    throw new IllegalArgumentException(
                            "analyze-locality requires TRACE CONFIG OUTPUT_DIRECTORY");
                org.sibench.analysis.UpdateLocality.run(
                        root.resolve(profiles.get(0)),
                        root.resolve(profiles.get(1)),
                        root.resolve(profiles.get(2)));
                out.println(Files.readString(root.resolve(profiles.get(2)).resolve("summary.json")));
                return 0;
            }
            if (args[0].equals("check-replay")) {
                if (profiles.size() != 2)
                    throw new IllegalArgumentException("check-replay requires REPLAY_CONFIG TRACE_DIRECTORY");
                var document = org.sibench.config.SibenchConfig.read(root.resolve(profiles.get(0)));
                var result = ReplayInput.check(root.resolve(profiles.get(1)), ReplayConfig.from(document));
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(result));
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
            report.put("status", "profiles_valid");
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
        out.println("       java -jar build/sibench/sibench.jar audit-baseline [--root PATH]"
                + " PROFILE BASELINE --jdbc URL --user ROLE");
        out.println("audit-baseline independently compares Parquet values with the unchanged initial PostgreSQL database.");

        out.println(
                "check-schema reads the live PG catalog; it never creates or repairs"
                        + " tables/indexes.");
        out.println(
                "       java -jar build/sibench/sibench.jar check-replay [--root PATH]"
                        + " REPLAY_CONFIG TRACE_DIRECTORY");

    }
}
