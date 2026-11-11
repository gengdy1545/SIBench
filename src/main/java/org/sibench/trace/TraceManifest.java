package org.sibench.trace;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.sibench.adapters.ValueKind;

/** Required identities and coverage counts shared by Trace consumers. */
public final class TraceManifest {
    private TraceManifest() {}

    public static boolean secondaryIndexes(JsonObject manifest) throws IOException {
        var scope = manifest.get("secondary_index_coverage");
        if (scope == null || !scope.isJsonPrimitive() || !scope.getAsJsonPrimitive().isString())
            throw new IOException("Trace must declare its exported index coverage");
        return switch (scope.getAsString()) {
            case "all_source_definitions" -> true;
            case "primary_only" -> false;
            default -> throw new IOException("Unknown exported index coverage: " + scope);
        };
    }

    public static void requireValue(Path directory,ValueKind kind)throws IOException {
        var manifest=read(directory);
        if(!manifest.getAsJsonArray("value_kinds").contains(new com.google.gson.JsonPrimitive(kind.name().toLowerCase(java.util.Locale.ROOT))))
            throw new IOException("Trace does not provide "+kind+" values");
    }

    public static JsonObject read(Path directory) throws IOException {
        if (Files.exists(directory.resolve("INCOMPLETE"))
                || !Files.isRegularFile(directory.resolve("manifest.json")))
            throw new IOException("Incomplete Trace directory");
        var manifest = JsonParser.parseString(Files.readString(directory.resolve("manifest.json")))
                .getAsJsonObject();
        if(!manifest.has("value_kinds")||!manifest.get("value_kinds").isJsonArray()
                ||manifest.getAsJsonArray("value_kinds").isEmpty())throw new IOException("Missing Trace value capabilities");
        var kinds=new java.util.HashSet<String>();
        for(var kind:manifest.getAsJsonArray("value_kinds")) {
            if(!kind.isJsonPrimitive()||!kind.getAsJsonPrimitive().isString()
                    ||!List.of("row_id","locator").contains(kind.getAsString())||!kinds.add(kind.getAsString()))
                throw new IOException("Invalid Trace value capabilities");
        }
        var output = manifest.get("output");
        if (output == null || !output.isJsonPrimitive() || !output.getAsJsonPrimitive().isString()
                || !output.getAsString().equals("proto"))
            throw new IOException("Missing or unsupported Trace output strategy");
        for (String name : List.of("baseline", "workload"))
            if (!manifest.has(name + "_trace_sha256"))
                throw new IOException("Trace identity missing: " + name);
        for (String name : List.of("baseline_rows", "source_work_units",
                "baseline_operations", "workload_operations", "last_resolved_version")) {
            var value = manifest.get(name);
            if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber())
                throw new IOException("Missing or invalid Trace coverage count: " + name);
            try {
                if (value.getAsBigDecimal().longValueExact() < 0)
                    throw new IOException("Negative Trace coverage count: " + name);
            } catch (ArithmeticException | NumberFormatException invalid) {
                throw new IOException("Invalid int64 Trace coverage count: " + name, invalid);
            }
        }
        return manifest;
    }
}
