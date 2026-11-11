package org.sibench.query;

import static org.junit.Assert.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class TrinoQueriesTest {
    @Test public void drainsEveryPageAndDistinguishesPhysicalInputFromProcessedBytes() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String origin = "http://127.0.0.1:" + server.getAddress().getPort();
        var headers = new AtomicBoolean();
        server.createContext("/", request -> {
            headers.set("pixels".equals(request.getRequestHeaders().getFirst("X-Trino-Catalog"))
                    && "UTC".equals(request.getRequestHeaders().getFirst("X-Trino-Time-Zone")));
            String body = switch (request.getRequestURI().getPath()) {
                case "/v1/statement" -> "{\"id\":\"q1\",\"nextUri\":\"" + origin + "/page\",\"data\":[[1]]}";
                case "/page" -> "{\"id\":\"q1\",\"data\":[[2],[3]],\"stats\":{\"processedBytes\":900}}";
                default -> "{\"queryStats\":{\"physicalInputDataSize\":\"10B\"}}";
            };
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            request.sendResponseHeaders(200, bytes.length);request.getResponseBody().write(bytes);request.close();
        });
        server.start();
        try {
            var rows = new ArrayList<Integer>();
            var result = new TrinoQueries(URI.create(origin), "test", "pixels", "postgres", Duration.ofSeconds(3))
                    .execute("SELECT 1", row -> rows.add(row.get(0).getAsInt()));
            assertEquals(List.of(1, 2, 3), rows);assertEquals(3, result.rows());assertTrue(headers.get());
            assertEquals("10B", result.queryStats().get("physicalInputDataSize").getAsString());
        } finally { server.stop(0); }
    }

    @Test public void errorsAndForeignContinuationsCannotBecomeSuccessfulSamples() throws Exception {
        for (String response : List.of("{\"id\":\"q1\",\"error\":{\"message\":\"bad SQL\"}}",
                "{\"id\":\"q1\",\"nextUri\":\"http://example.invalid/page\"}")) {
            var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", request -> {
                byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                request.sendResponseHeaders(200, bytes.length);request.getResponseBody().write(bytes);request.close();
            });
            server.start();
            try {
                var client = new TrinoQueries(URI.create("http://127.0.0.1:" + server.getAddress().getPort()), "test", "pixels", "postgres", Duration.ofSeconds(2));
                assertThrows(java.io.IOException.class, () -> client.execute("bad", row -> {}));
            } finally { server.stop(0); }
        }
    }
}
