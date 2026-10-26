package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

import org.sibench.generation.GenerationConfig;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;

/**
 * The export stage of a capture: its baseline is the still database's, exported and verified once.
 * A capture's copy of the still cluster holds exactly the still database's rows until its native
 * run (prepare only renames the database), so the first capture of a still database exports from
 * its copy into the still directory ({@code baseline/}, with the layout it was written with) and
 * every capture's baseline directory is hard links to those files, checked against their digests.
 * A profile whose data layout differs is refused rather than given another baseline.
 *
 * <p>Export and verification are checks every capture runs, not part of the capture code a
 * certification covers ({@link CaptureCode}); nothing in that code refers to this class.
 *
 * <pre>SharedBaseline ROOT PROFILE [TIMING]</pre>
 */
public final class SharedBaseline {
    private SharedBaseline() {}

    /** Writes and verifies a baseline into the given layout's directory. */
    @FunctionalInterface
    interface Export {
        void into(GenerationConfig layout) throws Exception;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 && args.length != 3) throw new IllegalArgumentException("SharedBaseline ROOT PROFILE [TIMING]");
        var timing = RunTiming.open(args.length == 3 ? Path.of(args[2]).toAbsolutePath() : null);
        try {
            timing.phase("export");
            Path root = Path.of(args[0]);
            Path profile = Path.of(args[1]);
            String benchmark =
                    org.sibench.config.ConfigValues.string(
                            org.sibench.config.ConfigValues.parse(Files.readString(root.resolve(profile))), "benchmark");
            if (benchmark.equals("hybench")) {
                var config = CaptureConfig.read(root, profile);
                var schema = SourceSchema.hybench(config.source());
                link(root, config.cloneFrom(), config.layout(), layout -> {
                    PgBaselineExport.export(config.jdbc(), layout.output(), layout);
                    PgBaselineVerify.verify(config.jdbc(), config.user(), schema, layout.output());
                });
            } else {
                var config = NativeCaptureConfig.read(root, profile);
                var schema = SourceSchema.read(config.root(), config.document());
                link(config.root(), config.cloneFrom(), config.layout(), layout -> {
                    PgBaselineExport.export(config.jdbc(), layout.output(), layout);
                    PgBaselineVerify.verify(config.jdbc(), config.user(), schema, layout.output());
                });
            }
        } finally {
            timing.close();
        }
    }

    /**
     * The capture's baseline ({@code layout.output()}) as hard links to the still database's
     * verified baseline, exported once through {@code export} when there is none yet.
     */
    static void link(Path root, String base, GenerationConfig layout, Export export) throws Exception {
        Path directory = StillBase.directory(root, base), shared = directory.resolve("baseline");
        Path identity = directory.resolve("baseline-layout.json");
        var expected = new Gson().toJsonTree(identity(layout));
        if (!Files.exists(identity)) {
            Path partial = directory.resolve("baseline.partial");
            // An export that failed before leaves its partial directory; it holds nothing reusable.
            if (Files.exists(partial)) deleteTree(partial);
            export.into(withOutput(layout, partial));
            if (Files.exists(shared)) throw new IllegalStateException("Shared baseline without its layout: " + shared);
            Files.move(partial, shared, StandardCopyOption.ATOMIC_MOVE);
            Files.writeString(identity, expected.toString(), StandardOpenOption.CREATE_NEW);
            System.out.println("Exported the shared baseline of " + base);
        } else if (!JsonParser.parseString(Files.readString(identity)).equals(expected))
            throw new IllegalStateException(
                    "The profile's data_layout differs from the shared baseline of " + base + "; name another clone_from");
        Path target = Files.createDirectory(layout.output());
        try (var files = Files.list(shared)) {
            for (Path file : files.sorted().toList()) Files.createLink(target.resolve(file.getFileName()), file);
        }
        // The links are the shared files, so what the shared baseline recorded holds for them;
        // what this check reads is recorded for the next capture.
        BaselineDigests.merge(shared, target);
        PgBaselineVerify.verifyFiles(target);
        BaselineDigests.merge(target, shared);
        System.out.println("Linked the shared baseline of " + base + " into " + target);
    }

    /** The same layout written to another directory. */
    static GenerationConfig withOutput(GenerationConfig l, Path directory) {
        return new GenerationConfig(
                l.root(), l.document(), l.benchmark(), l.seed(), l.targetBytes(), l.maxRows(), l.groupBytes(),
                l.pageBytes(), l.dictionaryBytes(), l.dictionary(), l.codec(), l.codecLevel(), l.buckets(),
                l.baselineTimestamp(), directory, l.minFreeBytes());
    }

    /** What decides the bytes of a Parquet file written with a layout, besides its rows. */
    static java.util.Map<String, Object> identity(GenerationConfig layout) {
        var result = new java.util.LinkedHashMap<String, Object>();
        result.put("target_bytes", layout.targetBytes());
        result.put("max_rows", layout.maxRows());
        result.put("group_bytes", layout.groupBytes());
        result.put("page_bytes", layout.pageBytes());
        result.put("dictionary_bytes", layout.dictionaryBytes());
        result.put("dictionary", layout.dictionary());
        result.put("codec", layout.codec().name());
        result.put("codec_level", layout.codecLevel());
        return result;
    }

    private static void deleteTree(Path directory) throws IOException {
        try (var walk = Files.walk(directory)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
        }
    }
}
