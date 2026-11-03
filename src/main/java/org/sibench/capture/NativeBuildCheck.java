package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/** The native build must match its pinned sources plus the exact admission-stop patch. */
public final class NativeBuildCheck {
    static final String HYBENCH_DIRECTORY = "build/benchmarks/HyBench-2024";
    private static final Set<String> SOURCE_SUFFIXES = Set.of(".java", ".c", ".h", ".lua", ".am", ".ac");

    private NativeBuildCheck() {}

    /** Each entry is a pinned source file or a directory searched recursively. */
    static void require(Path pinned, Path build, List<String> entries) throws IOException {
        require(pinned, build, entries, java.util.Map.of());
    }

    private static void require(Path pinned, Path build, List<String> entries,
            java.util.Map<String, String> patched) throws IOException {
        for (String directory : entries) {
            Path expected = pinned.resolve(directory);
            if (!Files.exists(expected)) throw new IOException("Pinned source is missing: " + directory);
            long checked = 0;
            try (var files = Files.walk(expected)) {
                for (Path file : files.filter(Files::isRegularFile).sorted().toList()) {
                    String name = file.getFileName().toString();
                    if (SOURCE_SUFFIXES.stream().noneMatch(name::endsWith)) continue;
                    Path copy = build.resolve(pinned.relativize(file));
                    if (!Files.isRegularFile(copy)
                            || !FileDigests.read(copy).equals(patched.getOrDefault(
                                    pinned.relativize(file).toString(), FileDigests.read(file))))
                        throw new IOException(
                                "Build copy differs from the pinned source: " + pinned.relativize(file));
                    checked++;
                }
            }
            if (checked == 0) throw new IOException("No source files checked in " + directory);
        }
    }
    private record Build(Path root, String benchmark, Path source, Path directory, String commit) {}

    private static Path stopPatch(Build build) {
        String name = switch (build.benchmark()) {
            case "hybench" -> "hybench-1.0-snapshot-stop.patch";
            case "ycsb" -> "ycsb-0.18.0-snapshot-stop.patch";
            case "sysbench" -> "sysbench-1.1.0-stop.patch";
            default -> throw new IllegalArgumentException("Unsupported native benchmark: " + build.benchmark());
        };
        return build.root().resolve("scripts/patches").resolve(name);
    }

    /** Derive expected patched bytes from the pin; a patch digest alone cannot certify source. */
    private static java.util.Map<String, String> patchedSources(Build build) throws IOException {
        Path patch = stopPatch(build), expected = Files.createTempDirectory("sibench-native-source-");
        var names = new java.util.ArrayList<String>();
        try {
            for (String line : Files.readAllLines(patch)) {
                if (!line.startsWith("--- a/")) continue;
                String name = line.substring(6).split("\t", 2)[0];
                Path relative = Path.of(name);
                if (relative.isAbsolute() || !relative.normalize().equals(relative) || name.startsWith(".."))
                    throw new IOException("Invalid native patch path: " + name);
                Path target = expected.resolve(relative);
                Files.createDirectories(target.getParent());
                Files.copy(build.source().resolve(relative), target);
                names.add(name);
            }
            if (names.isEmpty()) throw new IOException("Native stop patch has no source inputs");
            var process = new ProcessBuilder("patch", "--batch", "--forward", "--silent", "-p1",
                    "-d", expected.toString(), "-i", patch.toAbsolutePath().toString())
                    .redirectErrorStream(true).start();
            try {
                if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS))
                    throw new IOException("Native stop patch check timed out");
                if (process.exitValue() != 0)
                    throw new IOException("Native stop patch does not apply: " +
                            new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("Native stop patch check interrupted", interrupted);
            } finally { if (process.isAlive()) process.destroyForcibly(); }
            var hashes = new java.util.HashMap<String, String>();
            for (String name : names) hashes.put(name, FileDigests.read(expected.resolve(name)));
            return hashes;
        } finally {
            try (var files = Files.walk(expected)) {
                for (Path file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(file);
            }
        }
    }

    private static Build build(Path root, Path profile) throws Exception {
        var document = org.sibench.config.ConfigValues.parse(Files.readString(root.resolve(profile)));
        org.sibench.schema.SourceSchema.read(root, document);
        String benchmark = org.sibench.config.ConfigValues.string(document, "benchmark");
        var source = org.sibench.config.ConfigValues.object(document, "source");
        Path directory = benchmark.equals("hybench") ? root.resolve(HYBENCH_DIRECTORY)
                : root.resolve(org.sibench.config.ConfigValues.string(
                        org.sibench.config.ConfigValues.object(document, "native"), "build_directory"));
        requireDirectory(root, directory);
        return new Build(root, benchmark, root.resolve(org.sibench.config.ConfigValues.string(source, "path")),
                directory.toAbsolutePath().normalize(), org.sibench.config.ConfigValues.string(source, "commit"));
    }

    private static void requireDirectory(Path root, Path directory) throws IOException {
        Path parent = root.toRealPath().resolve("build/benchmarks");
        Path normalized = directory.toAbsolutePath().normalize();
        if (!normalized.startsWith(parent) || normalized.equals(parent))
            throw new IOException("Native builds must be under build/benchmarks: " + directory);
        Path existing = normalized;
        while (!Files.exists(existing)) existing = existing.getParent();
        if (!existing.toRealPath().startsWith(root.toRealPath()))
            throw new IOException("Native build escapes the repository: " + directory);
    }

    static Path ycsbLibraries(Path build) throws IOException {
        try (var entries = Files.list(build.resolve("jdbc/target"))) {
            var libraries = entries.filter(p -> p.getFileName().toString().startsWith("ycsb-jdbc-binding-"))
                    .map(p -> p.resolve("lib")).filter(Files::isDirectory).toList();
            if (libraries.size() != 1) throw new IOException("Expected one built YCSB JDBC distribution in " + build);
            return libraries.get(0);
        }
    }

    private static java.util.Map<String, Object> identity(Build build) throws IOException {
        require(build.source(), build.directory(), List.of("."), patchedSources(build));
        var hashes = new java.util.TreeMap<String, String>();
        var roots = switch (build.benchmark()) {
            case "hybench" -> List.of(build.directory().resolve("target/classes"),
                    build.directory().resolve("lib"), build.directory().resolve("pom.xml"));
            case "ycsb" -> List.of(ycsbLibraries(build.directory()));
            case "sysbench" -> List.of(build.directory().resolve("src/sysbench"));
            default -> throw new IOException("Unsupported native build: " + build.benchmark());
        };
        for (Path root : roots) {
            if (!Files.exists(root)) throw new IOException("Native client is not built: " + root);
            try (var files = Files.walk(root)) {
                var artifacts = files.filter(Files::isRegularFile).sorted().toList();
                if (artifacts.isEmpty()) throw new IOException("Native build is empty: " + root);
                for (Path file : artifacts) hashes.put(build.directory().relativize(file).toString(), FileDigests.sha256(file));
            }
        }
        return java.util.Map.of("benchmark", build.benchmark(), "source_commit", build.commit(), "artifacts", hashes,
                "native_stop_patch_sha256", FileDigests.sha256(stopPatch(build)));
    }

    static java.util.Map<String, Object> verify(Path root, Path profile) throws Exception {
        Build build = build(root, profile);
        return verify(build);
    }

    private static java.util.Map<String, Object> verify(Build build) throws IOException {
        Path manifest = build.directory().resolve("native-build.json");
        if (!Files.isRegularFile(manifest)) throw new IOException("Missing native-build.json; rebuild with scripts/build-benchmarks.sh");
        var actual = identity(build);
        var gson = new com.google.gson.Gson();
        if (!com.google.gson.JsonParser.parseString(Files.readString(manifest)).equals(gson.toJsonTree(actual)))
            throw new IOException("Native client differs from its recorded build: " + build.directory());
        if (build.benchmark().equals("hybench")) {
            try {
                Path loaded = Path.of(Class.forName("com.hybench.ConfigLoader", false,
                        NativeBuildCheck.class.getClassLoader()).getProtectionDomain().getCodeSource().getLocation().toURI());
                if (!loaded.toRealPath().equals(build.directory().resolve("target/classes").toRealPath()))
                    throw new IOException("HyBench classes are loaded from a different build: " + loaded);
            } catch (ClassNotFoundException | java.net.URISyntaxException e) {
                throw new IOException("HyBench build is absent from the classpath", e);
            }
        }
        return actual;
    }

    static java.util.Map<String, Object> verify(
            Path root, String benchmark, Path source, Path directory, String commit) throws IOException {
        requireDirectory(root, directory);
        return verify(new Build(root, benchmark, source, directory, commit));
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("NativeBuildCheck paths|record|verify ROOT CONFIG");
        Path root = Path.of(args[1]).toRealPath();
        Build build = build(root, Path.of(args[2]));
        switch (args[0]) {
            case "paths" -> {
                NativeProcess.assignment("BENCHMARK", build.benchmark());
                NativeProcess.assignment("SOURCE", build.source());
                NativeProcess.assignment("BUILD", build.directory());
                NativeProcess.assignment("NATIVE_STOP_PATCH", stopPatch(build));
            }
            case "record" -> Files.writeString(build.directory().resolve("native-build.json"),
                    new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(identity(build)) + "\n",
                    java.nio.file.StandardOpenOption.CREATE_NEW);
            case "verify" -> verify(build);
            default -> throw new IllegalArgumentException("Unknown build action: " + args[0]);
        }
    }

}
