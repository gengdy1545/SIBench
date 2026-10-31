package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Starts upstream native clients as separate processes and waits for their sessions to drain. */
final class NativeProcess {
    /** Assignments consumed by the shell entry points, including paths containing quotes. */
    static void assignment(String name, Object value) {
        System.out.println(name + "='" + value.toString().replace("'", "'\\''") + "'");
    }
    private NativeProcess() {}

    record Result(int exit, Path out, Path err) {
        String stdout() throws IOException {
            return Files.readString(out);
        }
    }

    /** Stdout and stderr are kept next to the capture; an existing log is never overwritten. */
    static Result run(
            String name,
            List<String> command,
            Map<String, String> environment,
            Path directory,
            Path logs,
            Duration timeout)
            throws IOException, InterruptedException {
        Files.createDirectories(logs);
        Path out = logs.resolve(name + ".out"), err = logs.resolve(name + ".err");
        Files.writeString(
                logs.resolve(name + ".command"),
                String.join(" ", command.stream().map(argument ->
                        argument.startsWith("db.passwd=") ? "db.passwd=<redacted>" : argument).toList()) + "\n",
                StandardOpenOption.CREATE_NEW);
        var builder = new ProcessBuilder(command).directory(directory.toFile());
        builder.environment().putAll(environment);
        builder.redirectOutput(out.toFile()).redirectError(err.toFile());
        Process process = builder.start();
        if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            throw new IOException("Native client timed out: " + name);
        }
        return new Result(process.exitValue(), out, err);
    }

    /** Waits until no backend still carries a capture label, so its trace is complete. */
    static void drain(String adminJdbc, String user) throws SQLException, InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            try (Connection connection =
                            DriverManager.getConnection(
                                    adminJdbc, user, System.getenv().getOrDefault("PGPASSWORD", ""));
                    Statement statement = connection.createStatement();
                    ResultSet rows =
                            statement.executeQuery(
                                    "SELECT count(*) FROM pg_stat_activity WHERE application_name"
                                            + " LIKE 'sibench:%'")) {
                rows.next();
                if (rows.getLong(1) == 0) return;
            }
            if (System.nanoTime() > deadline)
                throw new SQLException("Capture sessions did not close within 60 seconds");
            Thread.sleep(100);
        }
    }

    static List<String> java(Path classpathLibraries, Path sibenchJar, String... arguments) {
        return java(classpathLibraries, sibenchJar, List.of(arguments));
    }

    static List<String> java(Path classpathLibraries, Path sibenchJar, List<String> arguments) {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx512m");
        command.add("-cp");
        command.add(classpathLibraries.toString() + "/*:" + sibenchJar);
        command.addAll(arguments);
        return command;
    }

    /** The jar (or class directory) that contains this code; native JVMs reuse it for the driver. */
    static Path self() throws IOException {
        try {
            return Path.of(NativeProcess.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (java.net.URISyntaxException error) {
            throw new IOException(error);
        }
    }
}
