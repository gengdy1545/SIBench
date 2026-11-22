package org.sibench.adapters;

import static org.junit.Assert.*;
import static org.sibench.adapters.IndexServiceCheck.*;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.*;

/** Relive deployment checks reuse the common data oracle across maintenance and reopening. */
public class ReliveIntegrationCheck {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static URL required(String name) throws Exception {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Required property: " + name);
        return Path.of(value).toUri().toURL();
    }

    static void maintenance(String endpoint, String operation) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(endpoint + operation)).timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(response.body(), 200, response.statusCode());
    }

    @Test(timeout = 120000)
    public void grpcBridgePreservesVersionsPrivateReadsAndReopenedCloudFiles() throws Exception {
        try (var loader = new URLClassLoader(new URL[]{required("relive.test.classes"), required("relive.test.jar")},
                ClassLoader.getPlatformClassLoader())) {
            Class<?> stores = loader.loadClass("org.relive.TestStores");
            Method start = stores.getDeclaredMethod("server", Path.class, int.class);
            start.setAccessible(true);
            Method address = loader.loadClass("org.relive.IndexServer").getMethod("address");
            for (boolean maintain : List.of(true, false)) {
                Path directory = temporary.getRoot().toPath().resolve(maintain ? "sorted" : "active-segment");
                try (var first = (AutoCloseable) start.invoke(null, directory, 0);
                     var second = (AutoCloseable) start.invoke(null, directory, 0)) {
                    exercise((String) address.invoke(first), (String) address.invoke(second), maintain);
                }
                try (var reopened = (AutoCloseable) start.invoke(null, directory, 0)) {
                    checkReopened((String) address.invoke(reopened));
                }
            }
        }
    }

    /** Explicit real-deployment check; the caller owns fresh service state and process restart. */
    public static void main(String[] args) throws Exception {
        if (args.length == 3 && (args[0].equals("populate") || args[0].equals("populate-active")))
            exercise(args[1], args[2], args[0].equals("populate"));
        else if (args.length == 2 && args[0].equals("reopen")) checkReopened(args[1]);
        else throw new IllegalArgumentException("populate URL_A URL_B | populate-active URL_A URL_B | reopen URL");
        System.out.println("Relive gRPC " + args[0] + " semantic check passed");
    }

    private static void exercise(String endpoint, String otherEndpoint, boolean maintain) throws Exception {
        try (var left = new ReliveBackend(endpoint, ValueKind.ROW_ID);
             var right = new ReliveBackend(otherEndpoint, ValueKind.ROW_ID);
             var server = IndexTestSupport.start(0, left);
             var observer = IndexTestSupport.start(0, right)) {
            assertEquals(left.describe().location(), right.describe().location());
            var writerChannel = IndexServerTest.channel(server.port());
            var readerChannel = IndexServerTest.channel(server.port());
            var observerChannel = IndexServerTest.channel(observer.port());
            try {
                var writer = bound(writerChannel, "relive-integration", ValueKind.ROW_ID);
                var reader = bound(readerChannel, "relive-integration", ValueKind.ROW_ID);
                var other = bound(observerChannel, "relive-integration", ValueKind.ROW_ID);
                writer.put(put("a", 0, "base").getPut());
                writer.put(put("a", 20, "B").getPut());
                value(reader, "a", 25, "B");
                value(other, "a", 25, "B");
                if (maintain) maintenance(endpoint, "/flush");
                other.put(put("a", 10, "A").getPut());
                writer.delete(delete("a", 30).getDelete());
                if (maintain) maintenance(endpoint, "/flush");
                writer.put(put("a", 40, "C").getPut());
                batchAndConcurrentWrites(writer, other);
                history(reader);
                history(other);

                reads(writer, reader, other);
                for (var invalid : List.of(bound(writerChannel, "foreign", ValueKind.ROW_ID),
                        bound(writerChannel, "relive-integration", ValueKind.LOCATOR))) {
                    assertEquals(Status.Code.FAILED_PRECONDITION, assertThrows(StatusRuntimeException.class,
                            () -> invalid.put(put("rejected", 1, "must-not-exist").getPut())).getStatus().getCode());
                }
                assertFalse(reader.get(get("rejected", 45)).hasValue());
                if (maintain) {
                    maintenance(endpoint, "/flush");
                    maintenance(endpoint, "/compact");
                }
                history(reader);
                history(other);
                if (!maintain) {
                    var http = HttpClient.newHttpClient();
                    for (String url : List.of(endpoint, otherEndpoint)) {
                        var response = http.send(HttpRequest.newBuilder(URI.create(url + "/stats"))
                                .timeout(Duration.ofSeconds(10)).build(), HttpResponse.BodyHandlers.ofString());
                        assertEquals(response.body(), 200, response.statusCode());
                        var stats = com.google.gson.JsonParser.parseString(response.body()).getAsJsonObject();
                        assertEquals("Active-segment check requires no sorted files", 0, stats.get("runCount").getAsInt());
                        assertTrue(stats.get("activeSegments").getAsLong() > 0);
                        assertTrue(stats.get("activeSegmentBytes").getAsLong() > 0);
                        assertEquals(0, stats.get("sealedSegments").getAsInt());
                        assertEquals(0, stats.getAsJsonObject("objectStorage").get("uploadedObjectBytes").getAsLong());
                    }
                }
            } finally {
                writerChannel.shutdownNow(); readerChannel.shutdownNow(); observerChannel.shutdownNow();
            }
        }
    }

    private static void checkReopened(String endpoint) throws Exception {
        try (var backend = new ReliveBackend(endpoint, ValueKind.ROW_ID);
             var server = IndexTestSupport.start(0, backend)) {
            var channel = IndexServerTest.channel(server.port());
            try {
                var api = bound(channel, "relive-integration", ValueKind.ROW_ID);
                history(api);
                assertTrue(api.get(get("empty", 45)).hasValue());
                assertEquals(ByteString.EMPTY, api.get(get("empty", 45)).getValue());
                assertFalse(api.get(get("ab", 45)).hasValue());
                assertEquals(List.of(bytes("empty-key"), bytes("binary"), bytes("C"), bytes("duplicate"),
                        bytes("duplicate"), ByteString.EMPTY, bytes("sixty"), bytes("high")),
                        scan(api, ScanRequest.newBuilder().setReadTs(65).build()));
                assertEquals(Status.Code.FAILED_PRECONDITION, assertThrows(StatusRuntimeException.class,
                        () -> bound(channel, "foreign", ValueKind.ROW_ID).get(get("a", 45))).getStatus().getCode());
            } finally { channel.shutdownNow(); }
        }
    }
}
