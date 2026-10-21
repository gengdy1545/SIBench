package org.sibench.capture;

import com.google.gson.*;

import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Exact compiled payload equality and structural JSON manifest equality across JVMs. */
public final class CompilationValidation {
    private CompilationValidation() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IllegalArgumentException("CompilationValidation FIRST SECOND REPORT");
        compare(Path.of(args[0]), Path.of(args[1]), Path.of(args[2]));
    }

    public static void compare(Path first, Path second, Path report) throws IOException {
        if (Files.exists(report)) throw new IOException("Compilation comparison report exists");
        for (Path directory : List.of(first, second))
            if (Files.exists(directory.resolve("INCOMPLETE")))
                throw new IOException("Incomplete compilation");
        SortedSet<String> names = new TreeSet<>();
        try (var files = Files.list(first)) {
            for (Path file : files.toList()) names.add(file.getFileName().toString());
        }
        try (var files = Files.list(second)) {
            if (!names.equals(
                    new TreeSet<>(files.map(path -> path.getFileName().toString()).toList())))
                throw new IOException("Compiled file sets differ");
        }
        List<Map<String, Object>> compared = new ArrayList<>();
        for (String name : names) {
            Path a = first.resolve(name), b = second.resolve(name);
            if (name.equals("manifest.json")) {
                if (!JsonParser.parseString(Files.readString(a))
                        .equals(JsonParser.parseString(Files.readString(b))))
                    throw new IOException("Compiled manifests differ structurally");
            } else if (Files.mismatch(a, b) != -1) {
                throw new IOException("Compiled payload differs: " + name);
            }
            compared.add(
                    Map.of(
                            "path",
                            name,
                            "first_sha256",
                            BaselineGenerator.sha256(a),
                            "second_sha256",
                            BaselineGenerator.sha256(b),
                            "comparison",
                            name.equals("manifest.json") ? "structural_json" : "exact_bytes"));
        }
        Files.writeString(
                report,
                new GsonBuilder()
                        .setPrettyPrinting()
                        .create()
                        .toJson(
                                Map.of(
                                        "status",
                                        "passed",
                                        "files",
                                        compared,
                                        "scope",
                                        "Exact Trace/Parquet/work-unit bytes and all manifest"
                                            + " values; JSON object property order is not a"
                                            + " cross-JVM contract.")),
                StandardOpenOption.CREATE_NEW);
    }
}
