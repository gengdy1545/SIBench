package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.sibench.config.PixelsConfig;

/**
 * The Pixels side of CDC generation: etcd, coordinator and Retina for one run, schema setup and
 * LOAD. The generator drives it over one authenticated connection, also when Pixels runs locally.
 * A failed run keeps its directory and logs; only an explicit delete after publication removes it.
 */
public final class PixelsHost {
    private static final Gson GSON = new Gson();
    private static final Duration SETUP = Duration.ofMinutes(5), STOP = Duration.ofMinutes(2);
    private static final long MAX_UPLOAD_BYTES = 4L << 30;

    private PixelsHost() {}

    /** {@code pixels-host ADDRESS:PORT BUILD WORK_ROOT TOKEN_FILE}: serves generator sessions one at a time. */
    public static void serve(String listen, Path build, Path workRoot, Path tokenFile) throws Exception {
        int colon = listen.lastIndexOf(':');
        var address = InetAddress.getByName(listen.substring(0, colon));
        String token = Files.readString(tokenFile).strip();
        if (token.length() < 16) throw new IOException("Pixels host token must have at least 16 characters");
        PixelsBuild.verifyFiles(build);
        try (var server = new ServerSocket(Integer.parseInt(listen.substring(colon + 1)), 1, address)) {
            System.out.println("pixels-host on " + server.getLocalSocketAddress());
            while (true) try (var socket = server.accept()) {
                // A controller lost without closing its connection must still end the session and its cleanup.
                socket.setKeepAlive(true);
                socket.setOption(jdk.net.ExtendedSocketOptions.TCP_KEEPIDLE, 60);
                socket.setOption(jdk.net.ExtendedSocketOptions.TCP_KEEPINTERVAL, 10);
                socket.setOption(jdk.net.ExtendedSocketOptions.TCP_KEEPCOUNT, 6);
                new Session(build, workRoot, token, socket).run();
            } catch (Exception failure) {
                failure.printStackTrace();
            }
        }
    }

    /** Starts a session server on loopback for a local deployment; the run directory is {@code run}. */
    static Client local(Path build, Path run, JsonObject expected) throws IOException {
        var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        String token = java.util.UUID.randomUUID().toString();
        var thread = new Thread(() -> {
            try (server; var socket = server.accept()) {
                new Session(build, run.getParent(), token, socket).run();
            } catch (Exception failure) {
                failure.printStackTrace();
            }
        }, "sibench-pixels-host");
        thread.setDaemon(true);
        thread.start();
        return Client.connect(server.getInetAddress().getHostAddress() + ":" + server.getLocalPort(), token, expected);
    }

    /** The generator's view of one Pixels host session. */
    static final class Client implements AutoCloseable {
        private static final java.util.concurrent.ScheduledThreadPoolExecutor DEADLINES = deadlines();
        private final Socket socket;
        private final DataInputStream input;
        private final DataOutputStream output;
        private JsonObject identity;

        private static java.util.concurrent.ScheduledThreadPoolExecutor deadlines() {
            var executor = new java.util.concurrent.ScheduledThreadPoolExecutor(1, task -> {
                var thread = new Thread(task, "pixels-host-deadline");
                thread.setDaemon(true);
                return thread;
            });
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        }

        private Client(Socket socket) throws IOException {
            this.socket = socket;
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
        }

        static Client connect(String endpoint, String token, JsonObject expected) throws IOException {
            return connect(endpoint, token, expected, Duration.ofSeconds(30));
        }

        static Client connect(String endpoint, String token, JsonObject expected, Duration timeout) throws IOException {
            int colon = endpoint.lastIndexOf(':');
            var socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(endpoint.substring(0, colon), Integer.parseInt(endpoint.substring(colon + 1))),
                        Math.toIntExact(timeout.toMillis()));
                socket.setTcpNoDelay(true);
                var client = new Client(socket);
                client.identity = client.exchange(Map.of("token", token), null, timeout).getAsJsonObject("build");
                PixelsBuild.requireHost(expected, client.identity);
                return client;
            } catch (IOException | RuntimeException failure) {
                try { socket.close(); } catch (IOException close) { failure.addSuppressed(close); }
                throw failure;
            }
        }

        JsonObject identity() { return identity.deepCopy(); }

        /** The address of this host as Pixels sees the generator, used for the trace endpoint. */
        String localAddress() { return socket.getLocalAddress().getHostAddress(); }

        synchronized JsonObject call(Map<String, ?> request) throws IOException {
            Duration timeout = switch (request.get("op").toString()) {
                case "start" -> Duration.ofMinutes(30);
                case "stop" -> STOP.multipliedBy(4);
                case "check" -> Duration.ofSeconds(30);
                default -> SETUP;
            };
            return call(request, timeout);
        }

        synchronized JsonObject call(Map<String, ?> request, Duration timeout) throws IOException {
            var bounded = new LinkedHashMap<String, Object>(request);
            bounded.put("timeout_ms", timeout.toMillis());
            return exchange(bounded, null, timeout);
        }

        synchronized void upload(String table, String name, Path file, Duration timeout) throws IOException {
            long length = Files.size(file);
            exchange(Map.of("op", "upload", "table", table, "name", name, "bytes", length,
                    "timeout_ms", timeout.toMillis()), file, timeout);
        }

        /** Closing the socket bounds blocked writes as well as response reads. */
        private JsonObject exchange(Map<String, ?> request, Path file, Duration timeout) throws IOException {
            var expired = new java.util.concurrent.atomic.AtomicBoolean();
            var deadline = DEADLINES.schedule(() -> {
                expired.set(true);
                try { socket.close(); } catch (IOException ignored) { }
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
            try {
                send(request);
                if (file != null) try (var data = Files.newInputStream(file)) {
                    if (data.transferTo(output) != ((Number) request.get("bytes")).longValue())
                        throw new IOException("Load input changed during upload: " + file);
                    output.flush();
                }
                return receive();
            } catch (IOException failure) {
                if (expired.get()) throw new IOException("Pixels host request timed out: " + (request.containsKey("op") ? request.get("op") : "handshake"), failure);
                throw failure;
            } finally {
                deadline.cancel(false);
            }
        }

        private void send(Map<String, ?> request) throws IOException {
            byte[] bytes = GSON.toJson(request).getBytes(StandardCharsets.UTF_8);
            output.writeInt(bytes.length);
            output.write(bytes);
            output.flush();
        }

        private JsonObject receive() throws IOException {
            var response = message(input);
            if (response.has("error")) throw new IOException("Pixels host: " + response.get("error").getAsString());
            return response;
        }

        @Override public void close() throws IOException { socket.close(); }
    }

    private static final class Session {
        private final Path build, workRoot;
        private final String token;
        private final Socket socket;
        private final DataInputStream input;
        private final DataOutputStream output;
        private final Map<String, Process> processes = new LinkedHashMap<>();
        private Path run, config;
        private Map<String, String> environment;
        private Duration requestTimeout = SETUP;
        private long requestDeadline;
        private boolean stopped;
        private String dataUri, storageUri;
        private boolean retainData;

        Session(Path build, Path workRoot, String token, Socket socket) throws IOException {
            this.build = build;
            this.workRoot = workRoot;
            this.token = token;
            this.socket = socket;
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
        }

        void run() throws Exception {
            // A terminated generator or agent JVM must not leave Pixels daemons behind.
            var cleanup = new Thread(() -> processes.values().forEach(Process::destroyForcibly), "sibench-pixels-host-cleanup");
            Runtime.getRuntime().addShutdownHook(cleanup);
            try {
                socket.setSoTimeout(30_000);
                var hello = request();
                if (!MessageDigest.isEqual(hello.get("token").getAsString().getBytes(StandardCharsets.UTF_8),
                        token.getBytes(StandardCharsets.UTF_8))) throw new IOException("Invalid Pixels host token");
                reply(Map.of("build", PixelsBuild.identity(build)));
                socket.setSoTimeout(0);
                while (true) {
                    JsonObject request;
                    try {
                        request = request();
                    } catch (java.io.EOFException closed) {
                        return;
                    }
                    long timeout = request.get("timeout_ms").getAsLong();
                    if (timeout <= 0 || timeout > TimeUnit.HOURS.toMillis(24)) throw new IOException("Invalid host request timeout");
                    var deadline = Client.DEADLINES.schedule(() -> {
                        try { socket.close(); } catch (IOException ignored) { }
                    }, timeout, TimeUnit.MILLISECONDS);
                    try {
                        reply(handle(request));
                    } catch (Exception failure) {
                        failure.printStackTrace(System.err);
                        reply(Map.of("error", String.valueOf(failure)));
                        // Failed uploads leave an unread body; never parse it as another request.
                        return;
                    } finally { deadline.cancel(false); }
                }
            } finally {
                // An abandoned session must not leave daemons running; their state and logs stay.
                for (var process : processes.values()) if (process.isAlive()) process.destroyForcibly();
                try {
                    Runtime.getRuntime().removeShutdownHook(cleanup);
                } catch (IllegalStateException shuttingDown) {
                    // The hook is already running.
                }
            }
        }

        private Map<String, ?> handle(JsonObject request) throws Exception {
            long timeout = request.get("timeout_ms").getAsLong();
            if (timeout <= 0 || timeout > TimeUnit.HOURS.toMillis(24)) throw new IOException("Invalid host request timeout");
            requestTimeout = Duration.ofMillis(timeout);
            requestDeadline = System.nanoTime() + requestTimeout.toNanos();
            return switch (request.get("op").getAsString()) {
                case "start" -> start(request);
                case "create" -> create(request);
                case "upload" -> upload(request);
                case "load" -> load(request);
                case "export-baseline", "import-baseline" -> baseline(request);
                case "start-query" -> startQuery(request);
                case "check" -> check();
                case "stop" -> stop();
                case "delete" -> delete();
                default -> throw new IOException("Unknown Pixels host operation");
            };
        }

        private Map<String, ?> create(JsonObject request) throws Exception {
            String table = request.get("table").getAsString();
            if (!table.matches("[A-Za-z0-9_]+")) throw new IOException("Invalid table name");
            return Map.of("tables", setup("create", request.get("schema").getAsString(), dataUri,
                    write("spec-" + table + ".tsv", request.get("spec").getAsString()).toString(), run.resolve("tables-" + table + ".tsv").toString()));
        }

        private Map<String, ?> start(JsonObject request) throws Exception {
            if (run != null) throw new IOException("Pixels run already started in this session");
            String name = request.get("run").getAsString();
            if (!name.matches("[A-Za-z0-9._-]+") || name.equals(".") || name.equals("..")) throw new IOException("Invalid run name");
            Files.createDirectories(workRoot);
            Path created = workRoot.toRealPath().resolve(name);
            Files.createDirectory(created);
            run = created;
            stopped = false;
            storageUri = null;
            retainData = false;
            String address = socket.getLocalAddress().getHostAddress();
            var ports = new LinkedHashMap<String, Integer>();
            for (String port : List.of("etcd", "etcd_peer", "metadata", "trans", "retina", "node", "query", "index", "scaling", "worker"))
                ports.put(port, freePort(address));
            Path home = Files.createDirectories(run.resolve("home/etc"));
            Files.createSymbolicLink(run.resolve("home/lib"), build.resolve("home/lib"));
            Files.writeString(home.resolve("retina"), address + "\n");
            var generated = new LinkedHashMap<String, String>();
            dataUri = "file://" + run.resolve("data");
            if (request.has("data_storage")) {
                var storage = org.sibench.config.ConfigValues.parse(request.get("data_storage").toString());
                var profile = org.sibench.config.ConfigValues.object(storage, "profile");
                String prefix = org.sibench.config.ConfigValues.string(storage, "prefix") + "/runs/" + java.util.UUID.randomUUID();
                var connection = org.sibench.config.ObjectStore.of("pixels", profile, prefix);
                if (!connection.virtualHosted() || !prefix.matches("[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*"))
                    throw new IOException("Invalid Pixels COS storage prefix/addressing");
                storageUri = "cos://" + connection.bucket() + "/" + prefix + "/";
                dataUri = storageUri + "data";
                generated.put("enabled.storage.schemes", "file,cos");
                generated.put("cos.region", connection.region()); generated.put("cos.endpoint", connection.endpoint());
                generated.put("cos.access.key", ""); generated.put("cos.secret.key", "");
                for (String key : List.of("access_key_env", "secret_key_env", "session_token_env"))
                    if (profile.containsKey(key)) generated.put("sibench.cos." + key.replace('_', '.'),
                            org.sibench.config.ConfigValues.string(profile, key));
                retainData = request.has("retain_data") && request.get("retain_data").getAsBoolean();
            }
            generated.put("pixels.var.dir", run.resolve("var") + "/");
            generated.put("metadata.db.url", "jdbc:derby:" + run.resolve("var/pixels_metadata") + ";create=true");
            for (String server : List.of("metadata", "trans", "retina", "node", "index"))
                generated.put(server + ".server.host", address);
            generated.put("query.schedule.server.host", address);
            generated.put("worker.coordinate.server.host", address);
            generated.put("scaling.metrics.server.host", address);
            for (var port : Map.of("metadata.server.port", "metadata", "trans.server.port", "trans", "retina.server.port", "retina",
                    "node.server.port", "node", "query.schedule.server.port", "query", "index.server.port", "index",
                    "scaling.metrics.server.port", "scaling", "worker.coordinate.server.port", "worker", "etcd.port", "etcd").entrySet())
                generated.put(port.getKey(), Integer.toString(ports.get(port.getValue())));
            generated.put("etcd.hosts", address);
            generated.put("retina.enable", "true");
            generated.put("index.server.enabled", "true");
            generated.put("enabled.single.point.index.schemes", "rocksdb");
            generated.put("enabled.main.index.scheme", "sqlite");
            generated.put("retina.storage.gc.enabled", "false");
            generated.put("retina.upsert-mode.enabled", "false");
            for (var path : Map.of("index.rocksdb.data.path", "index/rocksdb", "index.sqlite.path", "index/sqlite",
                    "index.mapdb.path", "index/mapdb", "index.base.location", "index/base", "cache.base.location", "cache",
                    "index.rocksdb.stats.path", "index/stats").entrySet())
                generated.put(path.getKey(), run.resolve(path.getValue()).toString());
            for (var folder : Map.of("retina.buffer.object.storage.folder", "retina-buffer", "retina.offload.checkpoint.dir", "offload",
                    "retina.recovery.checkpoint.dir", "recovery", "retina.storage.gc.journal.dir", "gc-journal").entrySet())
                generated.put(folder.getKey(), storageUri == null ? "file://" + run.resolve(folder.getValue()) + "/"
                        : storageUri + folder.getValue() + "/");
            Map<String, String> overlay = new LinkedHashMap<>();
            for (var entry : request.getAsJsonObject("overlay").entrySet()) overlay.put(entry.getKey(), entry.getValue().getAsString());
            config = home.resolve("pixels.properties");
            Files.writeString(config, properties(Files.readString(build.resolve("defaults/pixels.properties")), overlay, generated),
                    StandardOpenOption.CREATE_NEW);
            Files.createDirectories(run.resolve("var"));
            environment = Map.of("PIXELS_HOME", run.resolve("home").toString(), "PIXELS_CONFIG", config.toString(), "HOSTNAME", address);

            launch("etcd", List.of(build.resolve("etcd/etcd").toString(), "--data-dir", run.resolve("etcd").toString(),
                    "--listen-client-urls", "http://" + address + ":" + ports.get("etcd"),
                    "--advertise-client-urls", "http://" + address + ":" + ports.get("etcd"),
                    "--listen-peer-urls", "http://" + address + ":" + ports.get("etcd_peer"),
                    "--initial-advertise-peer-urls", "http://" + address + ":" + ports.get("etcd_peer"),
                    "--initial-cluster", "default=http://" + address + ":" + ports.get("etcd_peer")), Map.of());
            await(address, ports.get("etcd"));
            if (storageUri != null) setup("init-storage", storageUri);
            if (request.has("baseline_max_timestamp")) setup("seed-timestamps", request.get("baseline_max_timestamp").getAsString());
            setup("init-meta");
            var memory = request.getAsJsonObject("memory");
            launch("coordinator", daemon(memory.get("coordinator_heap").getAsString(), "coordinator", List.of(), build.resolve("jars/pixels-daemon.jar").toString()),
                    Map.of());
            await(address, ports.get("metadata"));
            var trace = request.get("trace").getAsBoolean()
                    ? List.of("-Dsibench.trace.endpoint=" + request.get("trace_endpoint").getAsString(),
                            "-Dsibench.trace.token=" + request.get("trace_token").getAsString()) : List.<String>of();
            launch("retina", daemon(memory.get("retina_heap").getAsString(), "retina", trace,
                    build.resolve("jars/sibench-pixels.jar") + ":" + build.resolve("jars/pixels-daemon.jar")),
                    // As upstream run-class.sh: the Retina library may link the JDK's libjawt.so.
                    Map.of("LD_LIBRARY_PATH", run.resolve("home/lib") + ":" + Path.of(System.getProperty("java.home"), "lib") + ":"
                            + Path.of(System.getProperty("java.home"), "lib/server"), "LD_PRELOAD", run.resolve("home/lib/libjemalloc.so").toString()));
            for (String port : List.of("retina", "index", "trans")) await(address, ports.get(port));
            return Map.of("properties", Files.readString(config), "address", address, "directory", run.toString(), "data_uri", dataUri);
        }

        private Map<String, ?> baseline(JsonObject request) throws Exception {
            if (storageUri == null || stopped) throw new IOException("Baseline operations require an active COS session");
            String operation = request.get("op").getAsString();
            Path result = run.resolve(operation + ".json");
            if (operation.equals("export-baseline")) {
                var names = new ArrayList<String>();
                for (var item : request.getAsJsonArray("tables")) {
                    String name = item.getAsString();
                    if (!name.matches("[A-Za-z0-9_]+")) throw new IOException("Invalid baseline table");
                    names.add(name);
                }
                setup(operation, "postgres", String.join(",", names), result.toString());
            } else {
                Path spec = write("import-spec.tsv", request.get("spec").getAsString());
                Path inventory = write("import-inventory.json", request.get("inventory").toString());
                setup(operation, "postgres", spec.toString(), inventory.toString(), result.toString());
            }
            return Map.of("baseline", JsonParser.parseString(Files.readString(result)));
        }

        private List<String> daemon(String heap, String role, List<String> options, String classpath) throws IOException {
            var command = new ArrayList<>(List.of(java(), "-Xmx" + heap));
            for (String line : Files.readAllLines(build.resolve("source/pixels/scripts/etc/" + role + "-jvm.config")))
                for (String option : line.trim().split("\\s+")) if (!option.isEmpty() && !option.startsWith("-Xmx")) command.add(option);
            command.addAll(options);
            command.addAll(List.of("-Dio.netty.leakDetection.level=advanced", "-Doperation=start", "-Drole=" + role, "-cp", classpath,
                    "io.pixelsdb.pixels.daemon.Pixels" + Character.toUpperCase(role.charAt(0)) + role.substring(1)));
            return command;
        }

        private Map<String, ?> startQuery(JsonObject request) throws Exception {
            if (run == null || stopped || processes.containsKey("trino")) throw new IOException("No fresh Pixels query session");
            var settings = org.sibench.config.ConfigValues.parse(request.get("settings").toString());
            Path deployment = Path.of(org.sibench.config.ConfigValues.string(settings, "deployment_directory")).toRealPath();
            var pixelsIdentity = PixelsBuild.identity(build).getAsJsonObject("java");
            var identity = TrinoHost.verify(deployment, pixelsIdentity.get("pixels_commit").getAsString());
            if (!identity.get("pixels_patch_sha256").equals(pixelsIdentity.get("pixels_patch_sha256")))
                throw new IOException("Trino connector and Pixels patches differ");
            String address = socket.getLocalAddress().getHostAddress();
            int port = freePort(address);
            Path query = Files.createDirectory(run.resolve("trino"));
            String javaHome = deployment.resolve(identity.get("java_home").getAsString()).toString();
            launch("trino", TrinoHost.command(deployment, query, address, port, settings, identity),
                    Map.of("JAVA_HOME", javaHome, "PATH", javaHome + "/bin:" + System.getenv("PATH")));
            String server = "http://" + address + ":" + port;
            var readiness = TrinoHost.await(server, processes.get("trino"), requestTimeout);
            return Map.of("server", server, "deployment", identity, "settings", settings, "readiness", readiness,
                    "jvm_options", Files.readAllLines(query.resolve("etc/jvm.config")),
                    "server_properties", Files.readString(query.resolve("etc/config.properties")));
        }

        private String setup(String... arguments) throws Exception {
            var command = new ArrayList<>(List.of(java(), "-Xmx4g", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp",
                    build.resolve("jars/sibench-pixels.jar") + ":" + build.resolve("jars/pixels-cli.jar") + ":"
                            + build.resolve("jars/pixels-daemon.jar"), "org.sibench.pixels.PixelsSetup"));
            command.addAll(List.of(arguments));
            String name = "setup-" + arguments[0] + "-" + (arguments[0].equals("create") ? Path.of(arguments[3]).getFileName()
                    : arguments.length > 2 ? arguments[2] : "");
            var builder = new ProcessBuilder(command).directory(run.toFile()).redirectErrorStream(true)
                    .redirectOutput(run.resolve(name.replaceAll("[^A-Za-z0-9_-]", "_") + ".log").toFile());
            builder.environment().putAll(environment);
            var process = builder.start();
            processes.put("setup", process);
            try {
                if (!process.waitFor(remainingMillis(), TimeUnit.MILLISECONDS))
                    throw new IOException("Pixels setup timed out: " + arguments[0]);
                if (process.exitValue() != 0) throw new IOException("Pixels setup failed: " + arguments[0] + "; see " + run);
            } finally {
                if (process.isAlive()) process.destroyForcibly();
                processes.remove("setup");
            }
            return arguments[0].equals("create") ? Files.readString(Path.of(arguments[4])) : "";
        }

        private Map<String, ?> upload(JsonObject request) throws IOException {
            String table = request.get("table").getAsString(), name = request.get("name").getAsString();
            if (!table.matches("[A-Za-z0-9_]+") || !name.matches("[A-Za-z0-9_.-]+") || name.equals(".") || name.equals(".."))
                throw new IOException("Invalid upload name");
            if (run == null || stopped) throw new IOException("No active Pixels run");
            long remaining = request.get("bytes").getAsLong();
            if (remaining < 0 || remaining > MAX_UPLOAD_BYTES) throw new IOException("Upload exceeds the 4 GiB file limit");
            Path directory = run;
            for (String part : List.of("load", table)) {
                if (Files.isSymbolicLink(directory)) throw new IOException("Symbolic upload directory: " + directory);
                directory = directory.resolve(part);
                if (!Files.exists(directory, java.nio.file.LinkOption.NOFOLLOW_LINKS)) Files.createDirectory(directory);
            }
            if (Files.isSymbolicLink(directory)) throw new IOException("Symbolic upload directory: " + directory);
            Path file = directory.resolve(name);
            boolean created = false;
            try (var target = Files.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
                created = true;
                byte[] buffer = new byte[1 << 20];
                while (remaining > 0) {
                    int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (read < 0) throw new IOException("Upload ended early");
                    target.write(buffer, 0, read);
                    remaining -= read;
                }
            } catch (IOException | RuntimeException failure) {
                if (created) try { Files.delete(file); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
            return Map.of();
        }

        private Map<String, ?> load(JsonObject request) throws Exception {
            var tables = new ArrayList<String>();
            for (var value : request.getAsJsonArray("tables")) {
                String table = value.getAsString();
                if (!table.matches("[A-Za-z0-9_]+")) throw new IOException("Invalid LOAD table");
                tables.add(table);
            }
            setup("load", request.get("schema").getAsString(), String.join(",", tables), run.resolve("load").toString(),
                    request.get("rows_per_file").getAsString(), request.get("threads").getAsString(), run.resolve("loaded-rows.tsv").toString());
            var rows = new LinkedHashMap<String, Long>();
            for (String line : Files.readAllLines(run.resolve("loaded-rows.tsv"))) {
                String[] fields = line.split("\t");
                if (fields.length != 2 || rows.put(fields[0], Long.parseLong(fields[1])) != null)
                    throw new IOException("Invalid native LOAD row counts");
            }
            Generation.deleteTree(run.resolve("load"));
            return Map.of("rows", rows);
        }

        private Map<String, ?> check() throws IOException {
            for (var process : processes.entrySet())
                if (!process.getValue().isAlive())
                    throw new IOException("Pixels " + process.getKey() + " exited with " + process.getValue().exitValue() + "; see " + run);
            return Map.of();
        }

        /** Normal shutdown in reverse start order; a SIGTERM exit (143) after the request is a normal stop. */
        private Map<String, ?> stop() throws Exception {
            check();
            var exits = new LinkedHashMap<String, Integer>();
            var names = new ArrayList<>(processes.keySet());
            java.util.Collections.reverse(names);
            for (String name : names) {
                var process = processes.get(name);
                process.destroy();
                if (!process.waitFor(Math.min(STOP.toMillis(), remainingMillis()), TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly();
                    throw new IOException("Pixels " + name + " did not stop; see " + run);
                }
                exits.put(name, process.exitValue());
                if (process.exitValue() != 0 && process.exitValue() != 143)
                    throw new IOException("Pixels " + name + " stopped with " + process.exitValue() + "; see " + run);
            }
            processes.clear();
            stopped = true;
            return Map.of("exits", exits);
        }

        private Map<String, ?> delete() throws IOException {
            if (run == null || !stopped || !processes.isEmpty()) throw new IOException("Pixels run has not stopped normally");
            if (storageUri != null && !retainData) {
                try { setup("delete-storage", storageUri); }
                catch (Exception failure) { throw new IOException("Cannot delete owned Pixels COS prefix: " + storageUri, failure); }
            }
            Generation.deleteTree(run);
            run = null;
            return Map.of();
        }

        private void launch(String name, List<String> command, Map<String, String> extra) throws IOException {
            var builder = new ProcessBuilder(command).directory(run.toFile()).redirectErrorStream(true)
                    .redirectOutput(run.resolve(name + ".log").toFile());
            builder.environment().putAll(environment);
            builder.environment().putAll(extra);
            Files.writeString(run.resolve(name + ".command"), command.stream()
                    .map(option -> option.replaceAll("(-Dsibench\\.[^=]*token=).*", "$1<redacted>"))
                    .collect(java.util.stream.Collectors.joining(" ")) + "\n", StandardOpenOption.CREATE_NEW);
            processes.put(name, builder.start());
        }

        private void await(String address, int port) throws Exception {
            long deadline = Math.min(System.nanoTime() + SETUP.toNanos(), requestDeadline);
            while (true) {
                check();
                try (var probe = new Socket()) {
                    probe.connect(new InetSocketAddress(address, port), (int) Math.min(1000, remainingMillis()));
                    return;
                } catch (IOException notReady) {
                    if (System.nanoTime() > deadline) throw new IOException("Pixels port " + port + " not ready; see " + run);
                    Thread.sleep(200);
                }
            }
        }

        private long remainingMillis() throws IOException {
            long remaining = requestDeadline - System.nanoTime();
            if (remaining <= 0) throw new IOException("Pixels host request deadline exceeded");
            return Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
        }

        private Path write(String name, String text) throws IOException {
            return Files.writeString(run.resolve(name), text, StandardOpenOption.CREATE_NEW);
        }

        private JsonObject request() throws IOException {
            return message(input);
        }

        private void reply(Map<String, ?> response) throws IOException {
            byte[] bytes = GSON.toJson(response).getBytes(StandardCharsets.UTF_8);
            output.writeInt(bytes.length);
            output.write(bytes);
            output.flush();
        }
    }

    private static JsonObject message(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > 16 * 1024 * 1024) throw new IOException("Invalid Pixels host message size");
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();
    }

    /**
     * Upstream defaults with overrides and generated values applied in place; every override must
     * name an upstream key, so a misspelled setting fails instead of being ignored.
     */
    static String properties(String defaults, Map<String, String> overrides, Map<String, String> generated) {
        PixelsConfig.pixelsProperties(overrides);
        var pending = new LinkedHashMap<>(generated);
        var unknown = new LinkedHashMap<>(overrides);
        var text = new StringBuilder();
        for (String line : defaults.lines().toList()) {
            int equals = line.indexOf('=');
            String key = equals < 0 || line.stripLeading().startsWith("#") ? null : line.substring(0, equals).strip();
            if (key != null && (pending.containsKey(key) || unknown.containsKey(key))) {
                String value = pending.containsKey(key) ? pending.remove(key) : unknown.get(key);
                unknown.remove(key);
                text.append(key).append('=').append(value).append('\n');
            } else text.append(line).append('\n');
        }
        if (!unknown.isEmpty()) throw new IllegalArgumentException("Unknown Pixels properties: " + unknown.keySet());
        pending.forEach((key, value) -> text.append(key).append('=').append(value).append('\n'));
        return text.toString();
    }

    private static int freePort(String address) throws IOException {
        try (var socket = new ServerSocket(0, 1, InetAddress.getByName(address))) {
            return socket.getLocalPort();
        }
    }

    private static String java() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }
}
