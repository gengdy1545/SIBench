package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class InputStoreTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Map<String, byte[]> objects = new HashMap<>();
    private java.util.function.Consumer<String> beforeHead = key -> {};
    private int uploads;

    @Test public void publishesContentOnceAndRestoresAVerifiedIndependentInput() throws Exception {
        var input = CsvInputTest.fixture(temporary.newFolder("source").toPath());
        Files.writeString(input.output().resolve("private-settings.properties"), "secret=local-only");
        String original = CsvInput.open(input.output()).sha256();
        try (var store = store()) {
            var receipt = store.publish(input.output());
            int firstUploads = uploads;
            assertEquals(receipt, store.publish(input.output()));
            assertEquals(firstUploads + 1, uploads);
            Path destination = temporary.getRoot().toPath().resolve("download");
            String identity = receipt.get("manifest_sha256").toString();
            store.fetch(identity, destination);
            assertFalse(Files.exists(destination.resolve("private-settings.properties")));
            assertEquals(original, CsvInput.open(destination).sha256());
            assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> store.fetch(identity, destination));
            assertEquals(original, CsvInput.open(destination).sha256());
        }
    }

    @Test public void columnarManifestRoundTripPreservesRemoteBindingsWithoutCopyingData() throws Exception {
        Path input = PixelsBaselineTest.fixture(temporary.newFolder("csv").toPath(), temporary.newFolder("baseline").toPath());
        var original = PixelsBaseline.open(input);
        try (var store = store()) {
            var receipt = store.publish(input);
            assertEquals(3, receipt.get("files"));
            Path output = temporary.getRoot().toPath().resolve("cache");
            store.fetch(receipt.get("manifest_sha256").toString(), output);
            assertEquals(original.sha256(), PixelsBaseline.open(output).sha256());
            assertEquals(original.manifest().get("inventory"), PixelsBaseline.open(output).manifest().get("inventory"));
        }
    }

    @Test public void corruptRemoteObjectsCannotPublishALocalManifest() throws Exception {
        var input = CsvInputTest.fixture(temporary.newFolder("source").toPath());
        try (var store = store()) {
            var receipt = store.publish(input.output());
            var manifest = CsvInput.open(input.output()).manifest();
            String sha = manifest.getAsJsonObject("baseline_files").getAsJsonObject("customer.csv").get("sha256").getAsString();
            objects.put("inputs/objects/" + sha, "bad".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            Path destination = temporary.getRoot().toPath().resolve("failed");
            assertThrows(java.io.IOException.class, () -> store.fetch(receipt.get("manifest_sha256").toString(), destination));
            assertFalse(Files.exists(destination.resolve("manifest.json")));
        }
    }

    @Test public void inventoryFilesOutsideTheInputManifestAreRejected() throws Exception {
        var input = CsvInputTest.fixture(temporary.newFolder("source").toPath());
        try (var store = store()) {
            var receipt = store.publish(input.output());
            var inventory = com.google.gson.JsonParser.parseString(new String(objects.get("inputs/manifests/"
                    + receipt.get("manifest_sha256") + ".json"), java.nio.charset.StandardCharsets.UTF_8)).getAsJsonObject();
            var extra = inventory.getAsJsonArray("files").get(0).getAsJsonObject().deepCopy();
            extra.addProperty("path", "unbound/extra.bin");
            inventory.getAsJsonArray("files").add(extra);
            byte[] bytes = inventory.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            String identity = java.util.HexFormat.of().formatHex(org.sibench.config.Digests.digest().digest(bytes));
            objects.put("inputs/manifests/" + identity + ".json", bytes);
            Path destination = temporary.getRoot().toPath().resolve("unbound");
            assertTrue(assertThrows(java.io.IOException.class, () -> store.fetch(identity, destination))
                    .getMessage().contains("binds"));
            assertFalse(Files.exists(destination.resolve("manifest.json")));
        }
    }

    @Test public void traversalIsRejectedBeforeCreatingAnyOutput() throws Exception {
        var entry = new com.google.gson.JsonObject();
        entry.addProperty("path", "../outside");entry.addProperty("bytes", 0);entry.addProperty("sha256", "0".repeat(64));
        var files = new com.google.gson.JsonArray();files.add(entry);
        var document = new com.google.gson.JsonObject();
        document.addProperty("kind", "sibench_input_files");document.addProperty("status", "complete");document.add("files", files);
        byte[] bytes = document.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String identity = java.util.HexFormat.of().formatHex(org.sibench.config.Digests.digest().digest(bytes));
        objects.put("inputs/manifests/" + identity + ".json", bytes);
        Path output = temporary.getRoot().toPath().resolve("output");
        try (var store = store()) { assertThrows(java.io.IOException.class, () -> store.fetch(identity, output)); }
        assertFalse(Files.exists(output));
        assertFalse(Files.exists(temporary.getRoot().toPath().resolve("outside")));
    }

    @Test public void nonnumericSizesAreRejectedBeforeCreatingOutput() throws Exception {
        var entry = new com.google.gson.JsonObject();
        entry.addProperty("path", "manifest.json");
        entry.addProperty("bytes", "0");
        entry.addProperty("sha256", "0".repeat(64));
        var files = new com.google.gson.JsonArray(); files.add(entry);
        var document = new com.google.gson.JsonObject();
        document.addProperty("kind", "sibench_input_files"); document.addProperty("status", "complete");
        document.add("files", files);
        byte[] bytes = document.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String identity = java.util.HexFormat.of().formatHex(org.sibench.config.Digests.digest().digest(bytes));
        objects.put("inputs/manifests/" + identity + ".json", bytes);
        Path output = temporary.getRoot().toPath().resolve("output");
        try (var store = store()) { assertThrows(java.io.IOException.class, () -> store.fetch(identity, output)); }
        assertFalse(Files.exists(output));
    }

    @Test public void multipartStreamsExactRangesAndAbortsOnFailure() throws Exception {
        Path file = temporary.newFile("multipart").toPath();
        long size = (64L << 20) + 1;
        try (var data = new java.io.RandomAccessFile(file.toFile(), "rw")) { data.setLength(size); }
        String sha256 = org.sibench.config.Digests.sha256(file);
        for (String mode : new String[] {"complete", "fail", "changed"}) {
            var events = new java.util.ArrayList<String>();
            var client = (S3Client) java.lang.reflect.Proxy.newProxyInstance(S3Client.class.getClassLoader(), new Class<?>[] {S3Client.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "close" -> null;
                        case "createMultipartUpload" -> CreateMultipartUploadResponse.builder().uploadId("upload").build();
                        case "uploadPart" -> {
                            var request = (UploadPartRequest) args[0];
                            events.add("part-" + request.partNumber());
                            if (mode.equals("fail") && request.partNumber() == 2) throw S3Exception.builder().statusCode(503).message("injected").build();
                            long expected = request.partNumber() == 1 ? 64L << 20 : 1;
                            assertEquals(expected, request.contentLength().longValue());
                            var body = (RequestBody) args[1];
                            for (int attempt = 0; attempt < 2; attempt++)
                                try (var stream = body.contentStreamProvider().newStream()) {
                                    assertEquals(expected, stream.transferTo(java.io.OutputStream.nullOutputStream()));
                                }
                            yield UploadPartResponse.builder().eTag("part-" + request.partNumber()).build();
                        }
                        case "completeMultipartUpload" -> {
                            var parts = ((CompleteMultipartUploadRequest) args[0]).multipartUpload().parts();
                            assertEquals(java.util.List.of(1, 2), parts.stream().map(CompletedPart::partNumber).toList());
                            events.add("complete");yield CompleteMultipartUploadResponse.builder().build();
                        }
                        case "abortMultipartUpload" -> { events.add("abort");yield AbortMultipartUploadResponse.builder().build(); }
                        default -> throw new AssertionError(method);
                    });
            try (var store = new InputStore(client, "test", "inputs/")) {
                switch (mode) {
                    case "fail" -> assertThrows(S3Exception.class, () -> store.upload(file, "inputs/object", size, sha256));
                    // Bytes differing from the content key never complete an object under that key.
                    case "changed" -> assertThrows(java.io.IOException.class, () -> store.upload(file, "inputs/object", size, "0".repeat(64)));
                    default -> store.upload(file, "inputs/object", size, sha256);
                }
            }
            assertEquals(switch (mode) {
                case "complete" -> java.util.List.of("part-1", "part-2", "complete");
                case "fail" -> java.util.List.of("part-1", "part-2", "abort");
                default -> java.util.List.of("part-1", "abort");
            }, events);
        }
    }

    @Test public void publishNeverStoresBytesThatChangedAfterValidation() throws Exception {
        var input = CsvInputTest.fixture(temporary.newFolder("source").toPath());
        var file = CsvInput.open(input.output()).manifest().getAsJsonObject("baseline_files").getAsJsonObject("customer.csv");
        Path csv = input.output().resolve(file.get("path").getAsString());
        String key = "inputs/objects/" + file.get("sha256").getAsString();
        // The payload changes, keeping its size, between validation and its upload.
        beforeHead = head -> {
            if (!head.equals(key)) return;
            try {
                byte[] bytes = Files.readAllBytes(csv);
                bytes[0] ^= 1;
                Files.write(csv, bytes);
            } catch (java.io.IOException error) { throw new java.io.UncheckedIOException(error); }
        };
        try (var store = store()) { assertThrows(java.io.IOException.class, () -> store.publish(input.output())); }
        assertFalse("A content key must never hold other bytes", objects.containsKey(key));
    }

    private InputStore store() throws Exception {
        var client = (S3Client) java.lang.reflect.Proxy.newProxyInstance(S3Client.class.getClassLoader(), new Class<?>[] {S3Client.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "close" -> null;
                    case "headObject" -> {
                        beforeHead.accept(((HeadObjectRequest) args[0]).key());
                        byte[] data = objects.get(((HeadObjectRequest) args[0]).key());
                        if (data == null) throw NoSuchKeyException.builder().statusCode(404).message("missing").build();
                        yield HeadObjectResponse.builder().contentLength((long) data.length).build();
                    }
                    case "putObject" -> {
                        try (var stream = ((RequestBody) args[1]).contentStreamProvider().newStream()) {
                            objects.put(((PutObjectRequest) args[0]).key(), stream.readAllBytes());
                        }
                        uploads++;
                        yield PutObjectResponse.builder().eTag("test").build();
                    }
                    case "getObject" -> {
                        byte[] data = objects.get(((GetObjectRequest) args[0]).key());
                        if (data == null) throw NoSuchKeyException.builder().statusCode(404).message("missing").build();
                        yield new ResponseInputStream<>(GetObjectResponse.builder().contentLength((long) data.length).build(),
                                AbortableInputStream.create(new ByteArrayInputStream(data)));
                    }
                    default -> throw new AssertionError("Unexpected object-store call: " + method);
                });
        return new InputStore(client, "test", "inputs/");
    }
}
