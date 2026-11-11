package org.sibench.cli;

import org.sibench.replay.ReplayInput;
import com.google.gson.GsonBuilder;

import org.sibench.capture.RunTiming;
import org.sibench.adapters.IndexServer;
import org.sibench.adapters.ServiceConfig;
import org.sibench.adapters.ServiceReplay;
import org.sibench.replay.ReplayConfig;
import org.sibench.workloads.ScaleInspector;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Sibench {
    private Sibench() {}

    private static void printServiceHelp(PrintStream out) {
        out.println("       java -jar build/sibench/sibench.jar generate [--root PATH] CONFIG");
        out.println("       java -jar build/sibench/sibench.jar prepare-pixels-baseline CONFIG CSV_INPUT_DIRECTORY OUTPUT_DIRECTORY");
        out.println("       java -jar build/sibench/sibench.jar prepare-pixels-inputs CONFIG CSV_INPUT_DIRECTORY PIXELS_BASELINE_DIRECTORY");
        out.println("       java -jar build/sibench/sibench.jar check-pixels-baseline INPUT_DIRECTORY");
        out.println("       java -jar build/sibench/sibench.jar prepare-csv [--root PATH] CONFIG OUTPUT_DIRECTORY");
        out.println("       java -jar build/sibench/sibench.jar check-csv-input INPUT_DIRECTORY");
        out.println("       java -jar build/sibench/sibench.jar publish-input STORAGE_CONFIG PROFILE OBJECT_PATH INPUT_DIRECTORY");
        out.println("       java -jar build/sibench/sibench.jar fetch-input STORAGE_CONFIG PROFILE OBJECT_PATH MANIFEST_SHA256 OUTPUT_DIRECTORY");
        out.println("       java -jar build/sibench/sibench.jar prepare-cdc [--root PATH] CONFIG CSV_INPUT_DIRECTORY");
        out.println("       java -jar build/sibench/sibench.jar check-cdc-input INPUT_DIRECTORY");
        out.println("       java -jar build/sibench/sibench.jar pixels-experiment CONFIG CDC_INPUT_DIRECTORY PIXELS_BASELINE_DIRECTORY OUTPUT_DIRECTORY");
        out.println(
                "       java -jar build/sibench/sibench.jar pixels-host ADDRESS:PORT BUILD WORK_ROOT TOKEN_FILE"
                        + "   (Pixels side of CDC generation for a remote pixels_host)");
        out.println(
                "       java -jar build/sibench/sibench.jar plan-operations PROFILE CALIBRATION_TRACE"
                        + " REPLAY_CONFIG   (native parameters for the replay configuration's operation budget)");
        out.println(
                "       java -jar build/sibench/sibench.jar index-server SERVICE_CONFIG"
                        + "   (serves a new or reopened RocksDB / rocksdb-cloud / SlateDB index)");
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
                            "generate",
                            "prepare-pixels-baseline",
                            "prepare-pixels-inputs",
                            "check-pixels-baseline",
                            "prepare-csv",
                            "check-csv-input",
                            "publish-input",
                            "fetch-input",
                            "prepare-cdc",
                            "check-cdc-input",
                            "pixels-experiment",
                            "pixels-host",
                            "check-replay",
                            "index-server",
                            "replay-service",
                            "verify-service",
                            "plan-operations",
                            "analyze-locality")
                    .contains(args[0])) {
                throw new IllegalArgumentException("Unknown command: " + args[0]);
            }
            Path root = Path.of("").toAbsolutePath();
            List<String> profiles = new ArrayList<>();
            for (int i = 1; i < args.length; i++) {
                if (args[i].equals("--help") || args[i].equals("-h")) {
                    printHelp(out);
                    return 0;
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
            if (args[0].equals("pixels-experiment")) {
                if (profiles.size() != 4) throw new IllegalArgumentException("pixels-experiment requires CONFIG CDC_INPUT_DIRECTORY PIXELS_BASELINE_DIRECTORY OUTPUT_DIRECTORY");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(org.sibench.capture.PixelsExperiment.run(root,
                        Path.of(profiles.get(0)), Path.of(profiles.get(1)), Path.of(profiles.get(2)), Path.of(profiles.get(3)))));
                return 0;
            }
            if (args[0].equals("publish-input") || args[0].equals("fetch-input")) {
                boolean publish = args[0].equals("publish-input");
                if (profiles.size() != (publish ? 4 : 5)) throw new IllegalArgumentException(
                        publish ? "publish-input requires STORAGE_CONFIG PROFILE OBJECT_PATH INPUT_DIRECTORY"
                                : "fetch-input requires STORAGE_CONFIG PROFILE OBJECT_PATH MANIFEST_SHA256 OUTPUT_DIRECTORY");
                var settings = org.sibench.config.ObjectStore.of(root.resolve(profiles.get(0)), profiles.get(1), profiles.get(2));
                try (var store = new org.sibench.capture.InputStore(settings)) {
                    var result = publish ? store.publish(root.resolve(profiles.get(3)))
                            : store.fetch(profiles.get(3), root.resolve(profiles.get(4)));
                    out.println(new GsonBuilder().setPrettyPrinting().create().toJson(result));
                }
                return 0;
            }
            if (args[0].equals("prepare-pixels-baseline")) {
                if (profiles.size() != 3) throw new IllegalArgumentException("prepare-pixels-baseline requires CONFIG CSV_INPUT_DIRECTORY OUTPUT_DIRECTORY");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(org.sibench.capture.PixelsBaseline.prepare(root,
                        Path.of(profiles.get(0)), Path.of(profiles.get(1)), Path.of(profiles.get(2)))));
                return 0;
            }
            if (args[0].equals("prepare-pixels-inputs")) {
                if (profiles.size() != 3) throw new IllegalArgumentException("prepare-pixels-inputs requires CONFIG CSV_INPUT_DIRECTORY PIXELS_BASELINE_DIRECTORY");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(org.sibench.capture.PixelsBaseline.prepareInputs(root,
                        Path.of(profiles.get(0)), Path.of(profiles.get(1)), Path.of(profiles.get(2)))));
                return 0;
            }
            if (args[0].equals("check-pixels-baseline")) {
                if (profiles.size() != 1) throw new IllegalArgumentException("check-pixels-baseline requires INPUT_DIRECTORY");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(org.sibench.capture.PixelsBaseline.check(root.resolve(profiles.get(0)))));
                return 0;
            }
            if (args[0].equals("prepare-csv")) {
                if (profiles.size() != 2) throw new IllegalArgumentException("prepare-csv requires CONFIG OUTPUT_DIRECTORY");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(
                        org.sibench.capture.CsvInput.prepare(root, Path.of(profiles.get(0)), Path.of(profiles.get(1)))));
                return 0;
            }
            if (args[0].equals("check-csv-input")) {
                if (profiles.size() != 1) throw new IllegalArgumentException("check-csv-input requires INPUT_DIRECTORY");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(
                        org.sibench.capture.CsvInput.check(root.resolve(profiles.get(0)))));
                return 0;
            }
            if (args[0].equals("prepare-cdc")) {
                if (profiles.size() != 2) throw new IllegalArgumentException("prepare-cdc requires CONFIG CSV_INPUT_DIRECTORY");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(
                        org.sibench.capture.CdcPreparation.run(root, Path.of(profiles.get(0)), Path.of(profiles.get(1)))));
                return 0;
            }
            if (args[0].equals("check-cdc-input")) {
                if (profiles.size() != 1) throw new IllegalArgumentException("check-cdc-input requires INPUT_DIRECTORY");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(
                        org.sibench.capture.CdcInput.check(root.resolve(profiles.get(0)))));
                return 0;
            }
            if (args[0].equals("generate")) {
                if (profiles.size() != 1) throw new IllegalArgumentException("generate requires one runtime CONFIG");
                out.println(new GsonBuilder().setPrettyPrinting().create().toJson(
                        org.sibench.capture.Generation.run(root,Path.of(profiles.get(0)))));
                return 0;
            }
            if (args[0].equals("pixels-host")) {
                if (profiles.size() != 4)
                    throw new IllegalArgumentException("pixels-host requires ADDRESS:PORT BUILD WORK_ROOT TOKEN_FILE");
                org.sibench.capture.PixelsHost.serve(profiles.get(0), Path.of(profiles.get(1)).toAbsolutePath(),
                        Path.of(profiles.get(2)).toAbsolutePath(), Path.of(profiles.get(3)));
                return 0;
            }
            root = root.toRealPath();
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
            if (args[0].equals("index-server")) {
                if (profiles.size() != 1)
                    throw new IllegalArgumentException("index-server requires SERVICE_CONFIG");
                var service = ServiceConfig.ofServer(root, Path.of(profiles.get(0)));
                RunTiming.current().measure(service.path());
                var backend = service.backend();
                IndexServer server;
                try { server = IndexServer.start(service.port(), backend); }
                catch (Exception failure) {
                    try { backend.close(); } catch (Exception close) { failure.addSuppressed(close); }
                    throw failure;
                }
                Throwable failed = null;
                var closing = new java.util.concurrent.atomic.AtomicBoolean();
                var shutdown = new Thread(() -> {
                    if (!closing.compareAndSet(false, true)) return;
                    try { server.close(); backend.close(); }
                    catch (Exception error) { err.println("Index service shutdown failed: " + error); }
                }, "sibench-index-shutdown");
                Runtime.getRuntime().addShutdownHook(shutdown);
                try {
                    out.println("index-server " + backend.describe().name() + " on port " + server.port());
                    out.flush();
                    server.awaitTermination();
                } catch (Exception | Error error) {
                    failed = error;
                    throw error;
                } finally {
                    try { Runtime.getRuntime().removeShutdownHook(shutdown); }
                    catch (IllegalStateException shuttingDown) { /* The shutdown hook owns cleanup. */ }
                    // A failed shutdown leaves native calls alive; keep their handles valid.
                    try { if (closing.compareAndSet(false, true)) { server.close(); backend.close(); } }
                    catch (Exception close) {
                        if (failed == null) throw close;
                        failed.addSuppressed(close);
                    }
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
                "       java -jar build/sibench/sibench.jar check-replay [--root PATH]"
                        + " REPLAY_CONFIG TRACE_DIRECTORY");

    }
}
