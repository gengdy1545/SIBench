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
                    try {
                        reply(handle(request));
                    } catch (Exception failure) {
                        reply(Map.of("error", String.valueOf(failure)));
                    }
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
            return switch (request.get("op").getAsString()) {
                case "start" -> start(request);
                case "create" -> create(request);
                case "upload" -> upload(request);
                case "load" -> load(request);
                case "check" -> check();
                case "stop" -> stop();
                case "delete" -> delete();
                default -> throw new IOException("Unknown Pixels host operation");
            };
        }

        private Map<String, ?> create(JsonObject request) throws Exception {
            String table = request.get("table").getAsString();
            if (!table.matches("[A-Za-z0-9_]+")) throw new IOException("Invalid table name");
            return Map.of("tables", setup("create", request.get("schema").getAsString(), run.resolve("data").toString(),
                    write("spec-" + table + ".tsv", request.get("spec").getAsString()).toString(), run.resolve("tables-" + table + ".tsv").toString()));
        }

        private Map<String, ?> start(JsonObject request) throws Exception {
            if (run != null) throw new IOException("Pixels run already started in this session");
            String name = request.get("run").getAsString();
            if (!name.matches("[A-Za-z0-9._-]+")) throw new IOException("Invalid run name");
            run = workRoot.resolve(name);
            Files.createDirectories(workRoot);
            Files.createDirectory(run);
            String address = socket.getLocalAddress().getHostAddress();
            var ports = new LinkedHashMap<String, Integer>();
            for (String port : List.of("etcd", "etcd_peer", "metadata", "trans", "retina", "node", "query", "index", "scaling", "worker"))
                ports.put(port, freePort(address));
            Path home = Files.createDirectories(run.resolve("home/etc"));
            Files.createSymbolicLink(run.resolve("home/lib"), build.resolve("home/lib"));
            Files.writeString(home.resolve("retina"), address + "\n");
            var generated = new LinkedHashMap<String, String>();
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
                generated.put(folder.getKey(), "file://" + run.resolve(folder.getValue()) + "/");
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
            setup("init-meta");
            var memory = request.getAsJsonObject("memory");
            launch("coordinator", daemon(memory.get("coordinator_heap").getAsString(), "coordinator", List.of(), build.resolve("jars/pixels-daemon.jar").toString()),
                    Map.of());
            await(address, ports.get("metadata"));
            var trace = List.of("-Dsibench.trace.endpoint=" + request.get("trace_endpoint").getAsString(),
                    "-Dsibench.trace.token=" + request.get("trace_token").getAsString());
            launch("retina", daemon(memory.get("retina_heap").getAsString(), "retina", trace,
                    build.resolve("jars/sibench-pixels.jar") + ":" + build.resolve("jars/pixels-daemon.jar")),
                    // As upstream run-class.sh: the Retina library may link the JDK's libjawt.so.
                    Map.of("LD_LIBRARY_PATH", run.resolve("home/lib") + ":" + Path.of(System.getProperty("java.home"), "lib") + ":"
                            + Path.of(System.getProperty("java.home"), "lib/server"), "LD_PRELOAD", run.resolve("home/lib/libjemalloc.so").toString()));
            for (String port : List.of("retina", "index", "trans")) await(address, ports.get(port));
            return Map.of("properties", Files.readString(config), "address", address);
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

        private String setup(String... arguments) throws Exception {
            var command = new ArrayList<>(List.of(java(), "-Xmx4g", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp",
                    build.resolve("jars/sibench-pixels.jar") + ":" + build.resolve("jars/pixels-cli.jar"), "org.sibench.pixels.PixelsSetup"));
            command.addAll(List.of(arguments));
            String name = "setup-" + arguments[0] + "-" + (arguments[0].equals("create") ? Path.of(arguments[3]).getFileName()
                    : arguments.length > 2 ? arguments[2] : "");
            var builder = new ProcessBuilder(command).directory(run.toFile()).redirectErrorStream(true)
                    .redirectOutput(run.resolve(name.replaceAll("[^A-Za-z0-9_-]", "_") + ".log").toFile());
            builder.environment().putAll(environment);
            var process = builder.start();
            processes.put("setup", process);
            try {
                if (!process.waitFor(requestTimeout.toMillis(), TimeUnit.MILLISECONDS))
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
            if (!table.matches("[A-Za-z0-9_]+") || !name.matches("[A-Za-z0-9_.-]+")) throw new IOException("Invalid upload name");
            Path file = Files.createDirectories(run.resolve("load").resolve(table)).resolve(name);
            long remaining = request.get("bytes").getAsLong();
            try (var target = Files.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
                byte[] buffer = new byte[1 << 20];
                while (remaining > 0) {
                    int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (read < 0) throw new IOException("Upload ended early");
                    target.write(buffer, 0, read);
                    remaining -= read;
                }
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
                    request.get("rows_per_file").getAsString(), request.get("threads").getAsString());
            Generation.deleteTree(run.resolve("load"));
            return Map.of();
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
                if (!process.waitFor(STOP.toSeconds(), TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    throw new IOException("Pixels " + name + " did not stop; see " + run);
                }
                exits.put(name, process.exitValue());
                if (process.exitValue() != 0 && process.exitValue() != 143)
                    throw new IOException("Pixels " + name + " stopped with " + process.exitValue() + "; see " + run);
            }
            processes.clear();
            return Map.of("exits", exits);
        }

        private Map<String, ?> delete() throws IOException {
            if (!processes.isEmpty()) throw new IOException("Pixels processes are still running");
            Generation.deleteTree(run);
            return Map.of();
        }

        private void launch(String name, List<String> command, Map<String, String> extra) throws IOException {
            var builder = new ProcessBuilder(command).directory(run.toFile()).redirectErrorStream(true)
                    .redirectOutput(run.resolve(name + ".log").toFile());
            builder.environment().putAll(environment);
            builder.environment().putAll(extra);
            Files.writeString(run.resolve(name + ".command"), String.join(" ", command) + "\n", StandardOpenOption.CREATE_NEW);
            processes.put(name, builder.start());
        }

        private void await(String address, int port) throws Exception {
            long deadline = System.nanoTime() + SETUP.toNanos();
            while (true) {
                check();
                try (var probe = new Socket()) {
                    probe.connect(new InetSocketAddress(address, port), 1000);
                    return;
                } catch (IOException notReady) {
                    if (System.nanoTime() > deadline) throw new IOException("Pixels port " + port + " not ready; see " + run);
                    Thread.sleep(200);
                }
            }
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
