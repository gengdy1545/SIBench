package org.sibench.cli;

import com.google.gson.GsonBuilder;

import org.sibench.generation.BaselineGenerator;
import org.sibench.workloads.ScaleInspector;
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
            if (!args[0].equals("inspect-scale") && !args[0].equals("prepare")) {
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
            err.println("Scale validation interrupted");
            return 1;
        } catch (IOException
                | IllegalArgumentException
                | ArithmeticException
                | YAMLException error) {
            err.println("Scale validation failed: " + error.getMessage());
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
    }
}
