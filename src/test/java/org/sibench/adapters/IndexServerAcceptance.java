package org.sibench.adapters;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/** Real service acceptance process: stdin requests a clean close of both server and backend. */
public final class IndexServerAcceptance {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Expected SERVICE_CONFIG");
        var config = ServiceConfig.ofServer(Path.of("").toAbsolutePath(), Path.of(args[0]));
        try (var backend = config.backend(); var server = IndexServer.start(config.port(), backend)) {
            System.out.println("READY " + server.port());
            System.out.flush();
            var input = new java.io.BufferedReader(new java.io.InputStreamReader(System.in, StandardCharsets.UTF_8));
            if (!"close".equals(input.readLine())) throw new IllegalStateException("Missing clean-close request");
        }
        System.out.println("CLOSED");
    }
}
