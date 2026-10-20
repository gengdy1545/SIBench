package org.sibench.cli;

import com.google.gson.GsonBuilder;

import org.sibench.generation.BaselineGenerator;
import org.sibench.workloads.ScaleInspector;
import org.sibench.replay.ReferenceReplay;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.SnapshotLookupValidation;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Sibench {
    private Sibench() {}

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("--help") || args[0].equals("-h")) {
            printHelp(out);
            return 0;
        }
        try {
            if (!List.of("inspect-scale", "prepare", "replay", "check-replay", "validate-snapshots").contains(args[0])) {
                throw new IllegalArgumentException("Unknown command: " + args[0]);
            }
            Path root = Path.of("").toAbsolutePath();
            List<String> profiles = new ArrayList<>();
            boolean dryRun = false;
            for (int i = 1; i < args.length; i++) {
                if (args[i].equals("--help") || args[i].equals("-h")) {
                    printHelp(out);
                    return 0;
                } else if (args[i].equals("--dry-run") && args[0].equals("prepare")) {
                    dryRun = true;
                } else if (args[i].equals("--root")) {
                    if (++i == args.length) {
                        throw new IllegalArgumentException("--root requires a path");
                    }
                    root = Path.of(args[i]).toAbsolutePath();
                } else if (args[i].startsWith("-")) {
                    throw new IllegalArgumentException("Unknown option: " + args[i]);
                } else {
                    profiles.add(args[i]);
                }
            }
            root = root.toRealPath();
            if (List.of("replay", "check-replay", "validate-snapshots").contains(args[0])) {
                boolean check = args[0].equals("check-replay");
                if (profiles.size() != (check ? 2 : 3))
                    throw new IllegalArgumentException(
                            args[0] + " requires REPLAY_CONFIG NORMALIZED_DIRECTORY"
                                    + (check ? "" : " REPORT"));
                ReplayConfig config = ReplayConfig.read(root, Path.of(profiles.get(0)));
                Path directory = root.resolve(profiles.get(1));
                var result = check
                        ? ReferenceReplay.check(directory, config)
                        : args[0].equals("validate-snapshots")
                                ? SnapshotLookupValidation.run(directory, config, root.resolve(profiles.get(2)))
                                : ReferenceReplay.run(directory, config, root.resolve(profiles.get(2)));
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
        } catch (IOException
                | IllegalArgumentException
                | ArithmeticException
                | YAMLException error) {
            err.println("SIBench failed: " + error.getMessage());
            return 1;
        }
    }

    private static void printHelp(PrintStream out) {
        out.println(
                "Usage: java -jar build/sibench/sibench.jar inspect-scale [--root PATH] [PROFILE"
                        + " ...]");
        out.println("Root defaults to the working directory. Profiles are relative to root.");
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
        out.println("replay runs the in-process reference once; no real backend or trace repetition.");
        out.println("       java -jar build/sibench/sibench.jar validate-snapshots [--root PATH]"
                + " REPLAY_CONFIG NORMALIZED_DIRECTORY REPORT");
        out.println("validate-snapshots runs derived history correctness probes, not native workload or performance.");
    }
}
