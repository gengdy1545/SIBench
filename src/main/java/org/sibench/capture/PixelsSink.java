package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** One patched pixels-sink JVM for CDC file capture or file application, with its authenticated report connection. */
final class PixelsSink {
    static final String SLOT = "sibench";
    private PixelsSink() {}

    record Started(Process process, Socket socket, BufferedReader reports) {}

    /** Starts {@code main} with the written sink properties; returns once the sink reported ready. */
    static Started start(Path build, Map<String, Object> settings, Path root, String properties, List<String> options,
            Map<String, String> environment, String main, String... arguments) throws Exception {
        Files.createDirectory(root);
        Path config = Files.writeString(root.resolve("sink.properties"), properties, StandardOpenOption.CREATE_NEW);
        // The bundled logging records every change event at DEBUG, twice; SIBench keeps INFO on stdout.
        Path logging = Files.writeString(root.resolve("log4j2.properties"), "status=warn\n"
                + "appender.console.type=Console\nappender.console.name=STDOUT\n"
                + "appender.console.layout.type=PatternLayout\nappender.console.layout.pattern=%d [%c]-[%p] %m%n\n"
                + "rootLogger.level=info\nrootLogger.appenderRef.stdout.ref=STDOUT\n", StandardOpenOption.CREATE_NEW);
        String token = UUID.randomUUID().toString();
        Process process = null;
        try (var listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            listener.setSoTimeout(30_000);
            var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                    "-Xmx" + string(object(settings, "memory"), "sink_heap"), "-Dlog4j2.configurationFile=" + logging,
                    "-Dsibench.report.endpoint=127.0.0.1:" + listener.getLocalPort(), "-Dsibench.report.token=" + token));
            command.addAll(options);
            command.addAll(List.of("-cp", build.resolve("jars/sibench-pixels.jar") + ":" + build.resolve("jars/pixels-sink.jar"),
                    main, config.toString()));
            command.addAll(List.of(arguments));
            var builder = new ProcessBuilder(command).directory(root.toFile()).redirectErrorStream(true)
                    .redirectOutput(root.resolve("sink.log").toFile());
            builder.environment().putAll(environment);
            process = builder.start();
            Socket socket = listener.accept();
            try {
                socket.setSoTimeout(30_000);
                var reports = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                if (!("T " + token).equals(reports.readLine()) || !"R".equals(reports.readLine()))
                    throw new IOException("Invalid pixels-sink handshake; see " + root.resolve("sink.log"));
                socket.setSoTimeout(0);
                return new Started(process, socket, reports);
            } catch (Exception | Error failure) {
                socket.close();
                throw failure;
            }
        } catch (Exception | Error failure) {
            if (process != null) process.destroyForcibly();
            throw failure;
        }
    }

    /** Sink writer settings; only file capture, given its source port, also gets the Debezium PostgreSQL source. */
    static String properties(Map<String, Object> overrides, Path root, Integer sourcePort) {
        var generated = new LinkedHashMap<String, String>();
        generated.put("sink.mode", sourcePort == null ? "retina" : "proto");
        generated.put("sink.retina.mode", "stream");
        generated.put("sink.trans.mode", "batch");
        generated.put("sink.storage.loop", "false");
        generated.put("sink.monitor.freshness.timestamp", "false");
        generated.put("sink.retina.trans.request.batch", "false");
        generated.put("sink.retina.embedded", "false");
        if (sourcePort != null) {
            generated.put("sink.datasource", "engine");
            generated.put("sink.datasource.engine.format", "connect");
            generated.put("sink.debezium.dialect", "postgresql");
            generated.put("debezium.name", "sibench");
            generated.put("debezium.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
            generated.put("debezium.provide.transaction.metadata", "true");
            generated.put("debezium.offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
            generated.put("debezium.offset.storage.file.filename", root.resolve("offsets.dat").toString());
            generated.put("debezium.offset.flush.interval.ms", "60000");
            generated.put("debezium.schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
            generated.put("debezium.schema.history.internal.file.filename", root.resolve("schema-history.dat").toString());
            generated.put("debezium.database.hostname", "127.0.0.1");
            generated.put("debezium.database.port", Integer.toString(sourcePort));
            generated.put("debezium.database.user", "sibench");
            generated.put("debezium.database.password", "");
            generated.put("debezium.database.dbname", "postgres");
            generated.put("debezium.plugin.name", "pgoutput");
            generated.put("debezium.slot.name", SLOT);
            generated.put("debezium.publication.name", SLOT);
            generated.put("debezium.publication.autocreate.mode", "disabled");
            generated.put("debezium.schema.include.list", "public");
            generated.put("debezium.snapshot.mode", "never");
            generated.put("debezium.topic.prefix", "sibench");
            generated.put("debezium.decimal.handling.mode", "precise");
            generated.put("debezium.time.precision.mode", "adaptive_time_microseconds");
            generated.put("debezium.key.converter", "org.apache.kafka.connect.json.JsonConverter");
            generated.put("debezium.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        }
        var text = new StringBuilder("# Generated for one SIBench run; overrides come from the engine configuration.\n");
        overrides.forEach((key, value) -> text.append(key).append('=').append(value).append('\n'));
        generated.forEach((key, value) -> text.append(key).append('=').append(value).append('\n'));
        return text.toString();
    }
}
