package org.sibench.generation;

import static org.junit.Assert.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/** Bounded native host lifecycle shared by explicit integration checks. */
public abstract class DuckDbHostFixture {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    @Rule public Timeout timeout = Timeout.seconds(30);
    protected Process host;
    protected Path root, socket, errors;

    @Before public void start() throws Exception {
        String executable = System.getProperty("sibench.duckdb.host");
        assertNotNull("Set sibench.duckdb.host to the instrumented native host", executable);
        root = temporary.getRoot().toPath(); socket = root.resolve("host.sock"); errors = root.resolve("host.log");
        host = new ProcessBuilder(executable, root.resolve("data").toString(), socket.toString(), "256MiB", "2")
                .redirectError(errors.toFile()).start();
        var ready = CompletableFuture.supplyAsync(() -> {
            try { return new BufferedReader(new InputStreamReader(host.getInputStream())).readLine(); }
            catch (IOException e) { throw new UncheckedIOException(e); }
        });
        assertEquals("READY", ready.get(15, TimeUnit.SECONDS));
    }

    @After public void stop() throws Exception {
        if (host == null) return;
        host.destroy();
        if (!host.waitFor(10, TimeUnit.SECONDS)) {
            host.destroyForcibly(); host.waitFor(5, TimeUnit.SECONDS);
            fail("DuckDB host did not stop within 10 seconds: " + Files.readString(errors));
        }
        assertEquals(Files.readString(errors), 0, host.exitValue());
    }

    protected Connection connect() throws SQLException { return DuckDbHostDriver.open(socket); }
}
