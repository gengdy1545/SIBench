package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Starts upstream native clients with bounded execution and retained diagnostics. */
final class NativeProcess {
    @FunctionalInterface interface Check {void run()throws Exception;}
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
            throws Exception {
        return run(name,command,environment,directory,logs,timeout,()->{});
    }

    static Result run(String name,List<String> command,Map<String,String> environment,Path directory,
            Path logs,Duration timeout,Check check)throws Exception {
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
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            while (true) {
                check.run();
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new IOException("Native client timed out: " + name);
                if (process.waitFor(Math.min(remaining, TimeUnit.SECONDS.toNanos(1)), TimeUnit.NANOSECONDS)) {
                    check.run();
                    return new Result(process.exitValue(), out, err);
                }
            }
        } finally {
            if (process.isAlive()) {
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
            }
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

    static String runtimeClasspath() {
        return java.util.Arrays.stream(System.getProperty("java.class.path").split(java.io.File.pathSeparator))
                .map(path -> Path.of(path).toAbsolutePath().normalize().toString())
                .collect(java.util.stream.Collectors.joining(java.io.File.pathSeparator));
    }
}
