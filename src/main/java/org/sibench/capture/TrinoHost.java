package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;
import com.google.gson.*;
import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.sibench.config.Digests;

/** Launch settings for a source-bound Pixels connector on the same host as local Pixels files. */
final class TrinoHost {
    private TrinoHost() {}

    static JsonObject verify(Path directory, String pixelsCommit) throws IOException {
        var receipt = JsonParser.parseString(Files.readString(directory.resolve("deployment.json"))).getAsJsonObject();
        if (!receipt.get("pixels_commit").getAsString().equals(pixelsCommit))
            throw new IOException("Trino connector and Pixels source pins differ");
        for (String key : List.of("server_directory", "java_home")) {
            Path relative = Path.of(receipt.get(key).getAsString());
            if (relative.isAbsolute() || !directory.resolve(relative).normalize().startsWith(directory))
                throw new IOException("Trino deployment location escapes its directory");
        }
        for (var entry : receipt.getAsJsonObject("files").entrySet()) {
            Path file = directory.resolve(entry.getKey()).normalize();
            if (!file.startsWith(directory) || !Digests.sha256(file).equals(entry.getValue().getAsString()))
                throw new IOException("Trino deployment file differs: " + entry.getKey());
        }
        Path server = directory.resolve(receipt.get("server_directory").getAsString());
        for (String folder : List.of("lib", "plugin")) try (var files = Files.walk(server.resolve(folder))) {
            for (Path file : files.filter(p -> p.getFileName().toString().endsWith(".jar")).toList())
                if (!receipt.getAsJsonObject("files").has(directory.relativize(file).toString()))
                    throw new IOException("Unlisted Trino deployment JAR: " + file);
        }
        return receipt;
    }

    static List<String> command(Path deployment, Path run, String address, int port, Map<String, Object> settings,
            JsonObject receipt) throws IOException {
        Path etc = Files.createDirectories(run.resolve("etc/catalog")).getParent();
        Path data = Files.createDirectories(run.resolve("data"));
        Files.writeString(etc.resolve("node.properties"), "node.environment=sibench\nnode.id=" + UUID.randomUUID()
                + "\nnode.internal-address=" + address + "\nnode.data-dir=" + data + "\n");
        Files.writeString(etc.resolve("config.properties"), "coordinator=true\nnode-scheduler.include-coordinator=true\n"
                + "http-server.http.port=" + port + "\ndiscovery.uri=http://" + address + ":" + port + "\n"
                + "query.max-memory=" + string(settings, "query_memory") + "\n"
                + "query.max-memory-per-node=" + string(settings, "query_memory") + "\n"
                + "query.max-total-memory=" + string(settings, "query_total_memory") + "\n"
                + "memory.heap-headroom-per-node=" + string(settings, "heap_headroom") + "\n"
                + "task.concurrency=8\ntask.max-worker-threads=" + positive(settings, "threads") + "\n"
                + "plugin.dir=" + deployment.resolve(receipt.get("server_directory").getAsString()).resolve("plugin")
                + "\ncatalog.config-dir=" + etc.resolve("catalog") + "\n");
        Files.writeString(etc.resolve("jvm.config"), "-server\n-Xmx" + string(settings, "heap").replace("B", "") + "\n"
                + "-XX:+UseG1GC\n-XX:G1HeapRegionSize=32M\n-XX:+ExplicitGCInvokesConcurrent\n"
                + "-XX:+ExitOnOutOfMemoryError\n-XX:ReservedCodeCacheSize=512M\n"
                + "-XX:ActiveProcessorCount=" + positive(settings, "threads") + "\n"
                + "-Djdk.attach.allowAttachSelf=true\n-XX:+EnableDynamicAgentLoading\n"
                + "-Dfile.encoding=UTF-8\n--add-modules=jdk.incubator.vector\n"
                + "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\n--add-opens=java.base/java.nio=ALL-UNNAMED\n");
        Files.writeString(etc.resolve("catalog/pixels.properties"),
                "connector.name=pixels\ncloud.function.switch=off\nclean.intermediate.result=true\n");
        // Own the Java process directly; the upstream launcher forks a child that outlives a killed launcher.
        var command = new ArrayList<String>();
        command.add(deployment.resolve(receipt.get("java_home").getAsString()).resolve("bin/java").toString());
        command.addAll(Files.readAllLines(etc.resolve("jvm.config")));
        var node = new Properties();
        try (var reader = Files.newBufferedReader(etc.resolve("node.properties"))) { node.load(reader); }
        node.stringPropertyNames().stream().sorted().forEach(key -> command.add("-D" + key + "=" + node.getProperty(key)));
        command.addAll(List.of("-Dconfig=" + etc.resolve("config.properties"), "-cp",
                deployment.resolve(receipt.get("server_directory").getAsString()).resolve("lib/*").toString(), "io.trino.server.TrinoServer"));
        return command;
    }

    static Map<String, Object> await(String server, Process process, Duration timeout) throws Exception {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        var client = new org.sibench.query.TrinoQueries(URI.create(server), "sibench", "system", "runtime", Duration.ofSeconds(10));
        long deadline = System.nanoTime() + Math.min(timeout.toNanos(), Duration.ofMinutes(1).toNanos());
        IOException lastFailure = null;
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) throw new IOException("Trino exited with " + process.exitValue());
            boolean started = false;
            try {
                var response = http.send(HttpRequest.newBuilder(URI.create(server + "/v1/info"))
                        .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
                started = response.statusCode() == 200 && !JsonParser.parseString(response.body()).getAsJsonObject().get("starting").getAsBoolean();
            } catch (IOException notReady) { /* Startup has a bounded deadline. */ }
            if (started) {
                try {
                    long[] active = {0};
                    var nodes = client.execute("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'", row -> active[0] = row.get(0).getAsLong());
                    if (active[0] > 0) {
                        long[] markers = {-1};
                        var probe = client.execute("SELECT count(*) FROM pixels.postgres.sibench_marker", row -> markers[0] = row.get(0).getAsLong());
                        return Map.of("active_nodes", active[0], "nodes_query_id", nodes.queryId(), "pixels_query_id", probe.queryId(), "marker_rows", markers[0]);
                    }
                } catch (IOException notReady) {
                    if (!notReady.getMessage().contains("NO_NODES_AVAILABLE")) throw notReady;
                    lastFailure = notReady;
                }
            }
            Thread.sleep(200);
        }
        throw new IOException("Trino worker/catalog startup timed out", lastFailure);
    }
}
