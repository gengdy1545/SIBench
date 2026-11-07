package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/** The native build must match its pinned sources, project adapters and declared patches. */
public final class NativeBuildCheck {
    static java.util.Map<String,Object> verifyRocks(Path root, Path build) throws IOException {
        var recorded = org.sibench.config.ConfigValues.parse(Files.readString(build.resolve("engine-build.json")));
        var inputs = org.sibench.config.ConfigValues.object(recorded, "inputs");
        var expected = new java.util.TreeMap<String,Object>();
        expected.put("bridge_sha256", FileDigests.sha256(root.resolve("src/main/native/rocksdb/timestamps_jni.cc")));
        expected.put("build_script_sha256", FileDigests.sha256(root.resolve("scripts/build-rocksdb.sh")));
        Path headers = root.resolve("index/rocksdb-cloud/include");
        var hashes = new java.util.TreeMap<String,String>();
        try (var paths = Files.walk(headers)) {
            for (Path file : paths.filter(p -> Files.isRegularFile(p) && p.toString().endsWith(".h")).sorted().toList())
                hashes.put(headers.relativize(file).toString(), FileDigests.sha256(file));
        }
        expected.put("headers", hashes);
        if (!inputs.equals(expected)) throw new IOException("RocksDB bridge inputs differ from verified build");
        var artifacts = org.sibench.config.ConfigValues.object(recorded, "artifacts");
        if (!artifacts.containsKey("libsibench_rocksdb.so") || !artifacts.containsKey("librocksdbjni-linux64.so"))
            throw new IOException("Incomplete RocksDB bridge build");
        for (var artifact : artifacts.entrySet()) {
            Path name = Path.of(artifact.getKey());
            if (name.getNameCount() != 1 || name.isAbsolute() || name.toString().equals("..")
                    || !FileDigests.sha256(build.resolve(name)).equals(artifact.getValue()))
                throw new IOException("RocksDB runtime artifact differs: " + name);
        }
        try (var runtime = org.rocksdb.RocksDB.class.getResourceAsStream("/librocksdbjni-linux64.so")) {
            if (runtime == null) throw new IOException("Missing Maven RocksDB runtime");
            var digest = org.sibench.config.Digests.digest();
            byte[] block = new byte[1 << 20];
            for (int count; (count = runtime.read(block)) != -1;) digest.update(block, 0, count);
            if (!java.util.HexFormat.of().formatHex(digest.digest()).equals(artifacts.get("librocksdbjni-linux64.so")))
                throw new IOException("Bridge RocksDB runtime differs from Maven dependency");
        }
        return recorded;
    }
    private static final Set<String> SOURCE_SUFFIXES = Set.of(".java", ".c", ".cc", ".h", ".lua", ".am", ".ac", ".xml", ".sql", ".toml");

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
            Path inventory=Files.createTempFile("sibench-source-inventory-",".txt");
            try {
                var git=new ProcessBuilder("git","-C",pinned.toString(),"ls-files","-z","--",directory)
                        .redirectOutput(inventory.toFile()).redirectError(ProcessBuilder.Redirect.DISCARD).start();
                try {
                    if(!git.waitFor(30,java.util.concurrent.TimeUnit.SECONDS)||git.exitValue()!=0)
                        throw new IOException("Cannot read pinned source inventory");
                }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}
                finally{if(git.isAlive())git.destroyForcibly();}
                for (String relative : Files.readString(inventory).split("\u0000")) {
                    if(relative.isEmpty())continue;
                    Path file=pinned.resolve(relative);
                    String name = file.getFileName().toString();
                    if (SOURCE_SUFFIXES.stream().noneMatch(name::endsWith)) continue;
                    Path copy = build.resolve(pinned.relativize(file));
                    if (!Files.isRegularFile(copy)
                            || !FileDigests.sha256(copy).equals(patched.getOrDefault(
                                    pinned.relativize(file).toString(), FileDigests.sha256(file))))
                        throw new IOException(
                                "Build copy differs from the pinned source: " + pinned.relativize(file));
                    checked++;
                }
            }finally{Files.deleteIfExists(inventory);}
            if (checked == 0) throw new IOException("No source files checked in " + directory);
        }
    }
    private record Build(Path root, String benchmark, Path source, Path directory, String commit) {}

    private static Path stopPatch(Build build) {
        String name = switch (build.benchmark()) {
            case "hybench" -> "hybench-1.0-snapshot.patch";
            case "ycsb" -> "ycsb-0.18.0-snapshot-stop.patch";
            case "sysbench" -> "sysbench-1.1.0.patch";
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
            for (String name : names) hashes.put(name, FileDigests.sha256(expected.resolve(name)));
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
        Path directory = root.resolve(org.sibench.config.ConfigValues.string(
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
        Path libraries = build.resolve("sibench-api/lib");
        if (!Files.isRegularFile(libraries.resolve("sibench-ycsb.jar")))
            throw new IOException("Missing YCSB API binding; rebuild with scripts/build-benchmarks.sh: " + build);
        return libraries;
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
        var identity = new java.util.LinkedHashMap<String, Object>();
        identity.put("benchmark", build.benchmark());
        identity.put("source_commit", build.commit());
        identity.put("artifacts", hashes);
        identity.put("native_stop_patch_sha256", FileDigests.sha256(stopPatch(build)));
        if (build.benchmark().equals("ycsb")) identity.put("api_binding_sha256",
                FileDigests.sha256(build.root().resolve("src/main/native/ycsb/YcsbBinding.java")));
        if (build.benchmark().equals("sysbench")) {
            String driver = FileDigests.sha256(build.root().resolve("src/main/native/sysbench/drv_duckdb.cc"));
            if (!driver.equals(FileDigests.sha256(build.directory().resolve("src/drivers/duckdb/drv_duckdb.cc"))))
                throw new IOException("Built sysbench driver differs from the project source");
            identity.put("duckdb_driver_sha256", driver);
            String retry = FileDigests.sha256(build.root().resolve("src/main/native/sysbench/retryable_error.hpp"));
            if (!retry.equals(FileDigests.sha256(build.directory().resolve("src/drivers/duckdb/retryable_error.hpp"))))
                throw new IOException("Built sysbench retry policy differs from the project source");
            identity.put("duckdb_retry_policy_sha256", retry);
            identity.put("duckdb_error_types_sha256", FileDigests.sha256(build.root().resolve(
                    "third_party/duckdb/src/include/duckdb/common/exception.hpp")));
        }
        return identity;
    }

    static java.util.Map<String, Object> verify(Path root, Path profile) throws Exception {
        Build build = build(root, profile);
        var result = verify(build);
        if (build.benchmark().equals("hybench")) requireLoadedHybench(build.directory());
        return result;
    }

    private static java.util.Map<String, Object> verify(Build build) throws IOException {
        Path manifest = build.directory().resolve("native-build.json");
        if (!Files.isRegularFile(manifest)) throw new IOException("Missing native-build.json; rebuild with scripts/build-benchmarks.sh");
        var actual = identity(build);
        var gson = new com.google.gson.Gson();
        if (!com.google.gson.JsonParser.parseString(Files.readString(manifest)).equals(gson.toJsonTree(actual)))
            throw new IOException("Native client differs from its recorded build: " + build.directory());
        return actual;
    }

    /** Check in the worker JVM that actually loads HyBench, not the generation orchestrator. */
    static void requireLoadedHybench(Path build) throws IOException {
            try {
                Path loaded = Path.of(Class.forName("com.hybench.ConfigLoader", false,
                        NativeBuildCheck.class.getClassLoader()).getProtectionDomain().getCodeSource().getLocation().toURI());
                if (!loaded.toRealPath().equals(build.resolve("target/classes").toRealPath()))
                    throw new IOException("HyBench classes are loaded from a different build: " + loaded);
            } catch (ClassNotFoundException | java.net.URISyntaxException e) {
                throw new IOException("HyBench build is absent from the classpath", e);
            }
    }

    static java.util.Map<String, Object> verify(
            Path root, String benchmark, Path source, Path directory, String commit) throws IOException {
        requireDirectory(root, directory);
        return verify(new Build(root, benchmark, source, directory, commit));
    }

    static Path engineDirectory(Path root, java.util.Map<String, Object> runtime) throws IOException {
        return engineDirectory(root,runtime,"duckdb");
    }

    static Path engineDirectory(Path root, java.util.Map<String, Object> runtime,String engine) throws IOException {
        root = root.toRealPath();
        Path directory = root.resolve(org.sibench.config.ConfigValues.string(
                org.sibench.config.ConfigValues.object(runtime, engine), "build_directory")).normalize();
        Path parent = root.resolve("build/services");
        if (!directory.startsWith(parent) || directory.equals(parent))
            throw new IOException(engine+" builds must be under build/services");
        Path existing = directory;
        while (!Files.exists(existing)) existing = existing.getParent();
        if (!existing.toRealPath().startsWith(root))
            throw new IOException(engine+" build escapes the repository");
        return directory;
    }

    private static String git(Path directory, String... arguments) throws IOException {
        var command = new java.util.ArrayList<>(List.of("git", "-C", directory.toString()));
        command.addAll(List.of(arguments));
        var process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        try {
            if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS) || process.exitValue() != 0)
                throw new IOException("Cannot verify DuckDB source: " + command);
            return new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).strip();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new IOException(interrupted);
        } finally { if (process.isAlive()) process.destroyForcibly(); }
    }

    private static java.util.Map<String, String> tree(Path directory) throws IOException {
        var hashes = new java.util.TreeMap<String, String>();
        try (var files = Files.walk(directory)) {
            for (Path file : files.filter(Files::isRegularFile).toList())
                hashes.put(directory.relativize(file).toString(), FileDigests.sha256(file));
        }
        if (hashes.isEmpty()) throw new IOException("Empty build source inventory: " + directory);
        return hashes;
    }

    private static java.util.Map<String, Object> engineInputs(Path root) throws IOException {
        return engineInputs(root,"duckdb","duckdb");
    }

    private static java.util.Map<String,Object> engineInputs(Path root,String engine,String sourceName)throws IOException {
        Path source = root.resolve("third_party/"+sourceName);
        String pin = git(root, "rev-parse", ":third_party/"+sourceName);
        if (!pin.matches("[0-9a-f]{40}") || !pin.equals(git(source, "rev-parse", "HEAD"))
                || !git(source, "status", "--porcelain=v1", "--untracked-files=all").isEmpty())
            throw new IOException(engine+" source must be clean and at its gitlink");
        return java.util.Map.of("source_commit", pin,
                "native_sources", tree(root.resolve("src/main/native/"+engine)),
                "build_script_sha256", FileDigests.sha256(root.resolve("scripts/build-"+engine+".sh")));
    }

    private static void record(Path file, Object value) throws IOException {
        Files.writeString(file, new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(value) + "\n",
                java.nio.file.StandardOpenOption.CREATE_NEW);
    }

    private static void requireRecord(Path file, Object actual, String boundary) throws IOException {
        if (!Files.isRegularFile(file)
                || !com.google.gson.JsonParser.parseString(Files.readString(file)).equals(new com.google.gson.Gson().toJsonTree(actual)))
            throw new IOException(boundary + " differs from the recorded build: " + file);
    }

    private static java.util.Map<String, String> engineArtifacts(Path build) throws IOException {
        var hashes = new java.util.TreeMap<String, String>();
        for (String name : List.of("duckdb_host", "src/libduckdb.so", "duckdb", "CMakeCache.txt",
                "compile_commands.json", "compiler-version.txt", "source-files.json"))
            hashes.put(name, FileDigests.sha256(build.resolve(name)));
        if (!Files.isExecutable(build.resolve("duckdb_host"))) throw new IOException("DuckDB host is not executable");
        return hashes;
    }

    /** The build receipt is written only after compilation succeeds and all inputs remain unchanged. */
    static void recordEngine(Path root, Path build) throws IOException {
        var inputs = engineInputs(root);
        requireRecord(build.resolve("engine-inputs.json"), inputs, "DuckDB build inputs");
        requireRecord(build.resolve("source-files.json"), tree(build.resolve("source")), "Prepared DuckDB source");
        if (!FileDigests.sha256(build.resolve("duckdb_host.cpp")).equals(
                FileDigests.sha256(root.resolve("src/main/native/duckdb/duckdb_host.cpp"))))
            throw new IOException("Compiled host source differs from project input");
        record(build.resolve("engine-build.json"), java.util.Map.of("inputs", inputs, "artifacts", engineArtifacts(build)));
    }

    /** Generation verifies immutable build inputs and the artifacts it will actually execute. */
    static java.util.Map<String, Object> verifyEngine(Path root, Path build) throws IOException {
        var identity = java.util.Map.<String, Object>of("inputs", engineInputs(root), "artifacts", engineArtifacts(build));
        requireRecord(build.resolve("engine-build.json"), identity, "DuckDB engine");
        return identity;
    }

    private static java.util.Map<String,Object> postgresIdentity(Path root,Path build)throws IOException {
        var artifacts=new java.util.TreeMap<String,String>();
        for(var entry:tree(build.resolve("runtime")).entrySet())artifacts.put("runtime/"+entry.getKey(),entry.getValue());
        for(String file:List.of("source-files.json","compiler-version.txt","source/config.status"))
            artifacts.put(file,FileDigests.sha256(build.resolve(file)));
        for(String file:List.of("bin/postgres","bin/initdb","bin/pg_ctl","lib/row_capture.so"))
            if(!Files.isRegularFile(build.resolve("runtime/"+file)))throw new IOException("Missing PostgreSQL artifact: "+file);
        return java.util.Map.of("inputs",engineInputs(root,"postgres","postgresql"),"artifacts",artifacts);
    }

    static java.util.Map<String,Object> verifyPostgres(Path root,Path build)throws IOException {
        var identity=postgresIdentity(root,build);
        requireRecord(build.resolve("engine-build.json"),identity,"PostgreSQL engine");return identity;
    }

    private static void postgresBuild(String action,Path root,Path build)throws IOException {
        switch(action) {
            case "postgres-path" -> NativeProcess.assignment("BUILD",build);
            case "postgres-inputs" -> record(build.resolve("engine-inputs.json"),engineInputs(root,"postgres","postgresql"));
            case "postgres-prepared" -> record(build.resolve("source-files.json"),tree(build.resolve("source")));
            case "postgres-verify" -> verifyPostgres(root,build);
            case "postgres-record" -> {
                requireRecord(build.resolve("engine-inputs.json"),engineInputs(root,"postgres","postgresql"),"PostgreSQL inputs");
                var sources=com.google.gson.JsonParser.parseString(Files.readString(build.resolve("source-files.json"))).getAsJsonObject();
                for(var entry:sources.entrySet()) {
                    Path relative=Path.of(entry.getKey());
                    if(relative.isAbsolute()||!relative.normalize().equals(relative)||relative.startsWith("..")
                            ||!entry.getValue().getAsString().equals(FileDigests.sha256(build.resolve("source").resolve(relative))))
                        throw new IOException("Prepared PostgreSQL source changed: "+entry.getKey());
                }
                record(build.resolve("engine-build.json"),postgresIdentity(root,build));
            }
            default -> throw new IllegalArgumentException("Unknown PostgreSQL build action: "+action);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("NativeBuildCheck paths|record|verify|engine-path|engine-inputs|engine-prepared|engine-record|postgres-path|postgres-inputs|postgres-prepared|postgres-record|postgres-verify ROOT CONFIG");
        Path root = Path.of(args[1]).toRealPath();
        if(args[0].startsWith("postgres-")) {
            var runtime=org.sibench.config.SibenchConfig.read(root.resolve(args[2]));
            postgresBuild(args[0],root,engineDirectory(root,runtime,"postgres"));return;
        }
        if (args[0].startsWith("engine-")) {
            var runtime = org.sibench.config.SibenchConfig.read(root.resolve(args[2]));
            Path directory = engineDirectory(root, runtime);
            switch (args[0]) {
                case "engine-path" -> NativeProcess.assignment("BUILD", directory);
                case "engine-inputs" -> record(directory.resolve("engine-inputs.json"), engineInputs(root));
                case "engine-prepared" -> record(directory.resolve("source-files.json"), tree(directory.resolve("source")));
                case "engine-record" -> recordEngine(root, directory);
                default -> throw new IllegalArgumentException("Unknown engine build action: " + args[0]);
            }
            return;
        }
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
