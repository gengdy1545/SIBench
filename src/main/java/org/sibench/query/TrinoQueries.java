package org.sibench.query;

import com.google.gson.*;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Consumer;

/** Trino statement protocol: success means every result page was consumed without an error. */
public final class TrinoQueries {
    public record Result(String queryId, long rows, double seconds, JsonObject stats, JsonObject queryStats) {}
    private final URI server;
    private final String user, catalog, schema;
    private final Duration timeout;
    private final HttpClient http;

    public TrinoQueries(URI server, String user, String catalog, String schema, Duration timeout) {
        if (!java.util.Set.of("http", "https").contains(server.getScheme()) || server.getHost() == null
                || server.getUserInfo() != null || server.getQuery() != null || server.getFragment() != null
                || !(server.getPath().isEmpty() || server.getPath().equals("/")) || timeout.isNegative() || timeout.isZero())
            throw new IllegalArgumentException("Trino requires an HTTP(S) server origin and positive timeout");
        this.server = server;
        this.user = user;
        this.catalog = catalog;
        this.schema = schema;
        this.timeout = timeout;
        http = HttpClient.newBuilder().connectTimeout(timeout).followRedirects(HttpClient.Redirect.NEVER).build();
    }

    public Result execute(String sql, Consumer<JsonArray> row) throws Exception {
        long start = System.nanoTime(), deadline = start + timeout.toNanos(), rows = 0;
        URI next = null;
        String id = null;
        JsonObject stats = new JsonObject();
        try {
            var request = request(server.resolve("/v1/statement"), deadline)
                    .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8)).build();
            while (true) {
                var page = send(request);
                if (!page.has("id") || !page.get("id").isJsonPrimitive()) throw new IOException("Missing Trino query ID");
                String received = page.get("id").getAsString();
                if (id != null && !id.equals(received)) throw new IOException("Trino query ID changed between pages");
                id = received;
                if (page.has("nextUri")) next = sameOrigin(URI.create(page.get("nextUri").getAsString()));
                else next = null;
                if (page.has("error")) throw new IOException("Trino query " + id + " failed: " + page.get("error"));
                if (page.has("stats")) stats = page.getAsJsonObject("stats");
                if (page.has("data")) for (var value : page.getAsJsonArray("data")) {
                    row.accept(value.getAsJsonArray());
                    rows++;
                }
                if (next == null) break;
                request = request(next, deadline).GET().build();
            }
            // StatementStats.processedBytes is not physical Scan I/O. Keep the server's full metrics separately.
            double seconds = (System.nanoTime() - start) / 1e9;
            var info = send(request(server.resolve("/v1/query/" + id), deadline).GET().build());
            if (!info.has("queryStats")) throw new IOException("Trino query metrics missing for " + id);
            return new Result(id, rows, seconds, stats, info.getAsJsonObject("queryStats"));
        } catch (Exception | Error failure) {
            if (next != null) {
                // An interrupted caller still cancels the server query; the request has its own 5-second bound.
                boolean interrupted = Thread.interrupted();
                try {
                    http.send(request(next, System.nanoTime() + Duration.ofSeconds(5).toNanos()).DELETE().build(),
                            HttpResponse.BodyHandlers.discarding());
                } catch (Exception cancel) { failure.addSuppressed(cancel); }
                finally { if (interrupted) Thread.currentThread().interrupt(); }
            }
            throw failure;
        }
    }

    private JsonObject send(HttpRequest request) throws Exception {
        var response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() != 200) throw new IOException("Trino HTTP " + response.statusCode() + " at " + request.uri());
        try { return JsonParser.parseString(response.body()).getAsJsonObject(); }
        catch (RuntimeException malformed) { throw new IOException("Malformed Trino response", malformed); }
    }

    private HttpRequest.Builder request(URI uri, long deadline) throws IOException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new java.net.http.HttpTimeoutException("Trino query deadline exceeded");
        return HttpRequest.newBuilder(sameOrigin(uri)).timeout(Duration.ofNanos(remaining))
                .header("X-Trino-User", user).header("X-Trino-Source", "sibench")
                .header("X-Trino-Time-Zone", "UTC")
                .header("X-Trino-Catalog", catalog).header("X-Trino-Schema", schema);
    }

    private URI sameOrigin(URI uri) throws IOException {
        if (!server.getScheme().equalsIgnoreCase(uri.getScheme()) || !server.getHost().equalsIgnoreCase(uri.getHost())
                || port(server) != port(uri) || uri.getUserInfo() != null || uri.getFragment() != null)
            throw new IOException("Trino continuation escaped the configured server origin");
        return uri;
    }

    private static int port(URI uri) { return uri.getPort() != -1 ? uri.getPort() : uri.getScheme().equals("https") ? 443 : 80; }
}
