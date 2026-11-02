package org.sibench.trace;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Required identities and coverage counts shared by Trace consumers. */
public final class TraceManifest {
    private TraceManifest() {}

    public static JsonObject read(Path directory) throws IOException {
        if (Files.exists(directory.resolve("INCOMPLETE"))
                || !Files.isRegularFile(directory.resolve("manifest.json")))
            throw new IOException("Incomplete Trace directory");
        var manifest = JsonParser.parseString(Files.readString(directory.resolve("manifest.json")))
                .getAsJsonObject();
        for (String name : List.of("baseline", "workload"))
            if (!manifest.has(name + "_trace_sha256"))
                throw new IOException("Trace identity missing: " + name);
        for (String name : List.of("baseline_rows", "source_work_units",
                "baseline_operations", "workload_operations")) {
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
