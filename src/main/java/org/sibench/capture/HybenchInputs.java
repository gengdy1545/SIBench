package org.sibench.capture;

import com.google.gson.JsonParser;
import org.sibench.config.Digests;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

/** Generated upstream inputs: COPY files and the lists used by workload clients. */
final class HybenchInputs {
    /** One upstream load statement: {@code \copy table from 'Data_1x/file.csv' CSV DELIMITER ','}. */
    private static final Pattern COPY =
            Pattern.compile("\\\\copy\\s+(\\w+)\\s+from\\s+'Data_1x/([\\w.]+)'\\s+CSV\\s+DELIMITER\\s+','\\s*;?", Pattern.CASE_INSENSITIVE);

    /**
     * The generated files the workload clients read when they start ({@code Client.init}). Upstream
     * only logs a missing one and leaves its id list null, so every AP client then fails on its
     * first query; the load stage checks them before workload execution.
     */
    static final java.util.List<String> RUNTIME_INPUTS = java.util.List.of("Related_transfer_bids", "Related_checking_bids");

    private HybenchInputs() {}

    static void record(Path data, Path loadScript, Path manifest) throws IOException {
        var names = new java.util.LinkedHashSet<String>();
        for (String line : Files.readAllLines(loadScript)) {
            if (line.isBlank()) continue;
            String[] input = copyStatement(line);
            if (input == null || !names.add(input[1]))
                throw new IOException("Invalid or repeated upstream load input: " + line);
        }
        names.addAll(RUNTIME_INPUTS);
        var hashes = new java.util.LinkedHashMap<String, String>();
        for (String name : names) {
            Path file = data.resolve(name);
            if (!Files.isRegularFile(file, java.nio.file.LinkOption.NOFOLLOW_LINKS))
                throw new IOException("Missing generated HyBench input: " + file);
            hashes.put(name, Digests.sha256(file));
        }
        Files.writeString(manifest, new com.google.gson.GsonBuilder().setPrettyPrinting().create()
                .toJson(java.util.Map.of("data_sha256", hashes)), java.nio.file.StandardOpenOption.CREATE_NEW);
    }

    /**
     * The client's runtime inputs are the files bound by this load's manifest.
     */
    static void requireRuntimeInputs(Path data, Path manifest) throws Exception {
        var recorded = JsonParser.parseString(Files.readString(manifest))
                .getAsJsonObject().getAsJsonObject("data_sha256");
        for (String name : RUNTIME_INPUTS) {
            Path file = data.resolve(name);
            if (!Files.isRegularFile(file))
                throw new IllegalStateException(
                        "HyBench runtime input missing: " + file + "; the work directory must hold the Data_ directory"
                                + " its database was generated with");
            if (!recorded.has(name) || !Digests.sha256(file).equals(recorded.get(name).getAsString()))
                throw new IllegalStateException("HyBench runtime input differs from the load manifest: " + file);
        }
    }

    /** Deletes the loaded CSV files of {@code Data_<sf>}, keeping the client's runtime inputs. */
    static void dropLoadInputs(Path data, Path loadScript) throws IOException {
        for (String line : Files.readAllLines(loadScript)) {
            if (line.isBlank()) continue;
            String[] loaded = copyStatement(line);
            if (loaded == null) throw new IOException("Unexpected upstream load statement: " + line);
            Path file = data.resolve(loaded[1]);
            if (!Files.isRegularFile(file)) continue;
            Files.delete(file);
        }
    }

    /** The table and file of one upstream load statement, or null when it is not one. */
    static String[] copyStatement(String line) {
        var match = COPY.matcher(line.strip());
        return match.matches() ? new String[] {match.group(1), match.group(2)} : null;
    }

}
