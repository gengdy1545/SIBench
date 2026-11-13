package org.sibench.capture;

import static org.sibench.capture.InputFiles.MAX_MANIFEST_BYTES;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Map;
import org.sibench.config.Digests;
import org.sibench.config.ObjectStore;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

/** Immutable input publication. Shared content objects avoid duplicating CSVs in CDC artifacts. */
public final class InputStore implements AutoCloseable {
    private final S3Client client;
    private final String bucket, prefix;

    public InputStore(ObjectStore store) throws IOException {
        this(S3Client.builder().endpointOverride(URI.create(store.endpoint())).region(Region.of(store.region()))
                .credentialsProvider(StaticCredentialsProvider.create(store.sessionToken().isEmpty()
                        ? AwsBasicCredentials.create(store.accessKey(), store.secretKey())
                        : AwsSessionCredentials.create(store.accessKey(), store.secretKey(), store.sessionToken())))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(!store.virtualHosted())
                        .chunkedEncodingEnabled(false).build())
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofMinutes(10)).apiCallAttemptTimeout(Duration.ofMinutes(3)))
                .build(), store.bucket(), store.objectPath());
    }

    InputStore(S3Client client, String bucket, String prefix) throws IOException {
        this.client = client;
        this.bucket = bucket;
        this.prefix = name(prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix) + "/";
    }

    public Map<String, Object> publish(Path input) throws Exception {
        input = input.toRealPath();
        var manifest = check(input);
        var payloads = new java.util.TreeMap<String, JsonObject>();
        for (String field : java.util.List.of("schema", "source_ddl", "parameter_file", "load_inventory"))
            if (manifest.has(field)) add(payloads, manifest.getAsJsonObject(field));
        if (manifest.has("baseline_files")) manifest.getAsJsonObject("baseline_files").entrySet().forEach(e -> add(payloads, e.getValue().getAsJsonObject()));
        if (manifest.has("cdc_files")) manifest.getAsJsonArray("cdc_files").forEach(e -> add(payloads, e.getAsJsonObject()));
        var manifestFile = InputFiles.file(input, input.resolve("manifest.json"));
        require(JsonParser.parseString(Files.readString(input.resolve("manifest.json"))).equals(manifest),
                "Input manifest changed during validation");
        add(payloads, manifestFile);
        var entries = new JsonArray();
        for (var entry : payloads.values()) {
            Path path = InputFiles.resolve(input, name(entry.get("path").getAsString()));
            String key = prefix + "objects/" + digest(entry.get("sha256").getAsString());
            long size = entry.get("bytes").getAsLong();
            try {
                var existing = client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
                require(existing.contentLength() == size, "Existing content object has a different size");
            } catch (S3Exception absent) {
                if (absent.statusCode() != 404) throw absent;
                upload(path, key, size, entry.get("sha256").getAsString());
            }
            entries.add(entry);
        }
        var inventory = new JsonObject();
        inventory.addProperty("kind", "sibench_input_files");
        inventory.addProperty("status", "complete");
        inventory.add("files", entries);
        byte[] bytes = new GsonBuilder().setPrettyPrinting().create().toJson(inventory).getBytes(StandardCharsets.UTF_8);
        require(bytes.length <= MAX_MANIFEST_BYTES, "Input inventory exceeds 16 MiB");
        String identity = hash(bytes);
        // Identical keys always carry identical bytes; no conditional PUT or directory rename is assumed.
        client.putObject(PutObjectRequest.builder().bucket(bucket).key(prefix + "manifests/" + identity + ".json").build(),
                RequestBody.fromBytes(bytes));
        return Map.of("status", "published", "bucket", bucket, "object_path", prefix,
                "manifest_sha256", identity, "files", entries.size());
    }

    public Map<String, Object> fetch(String identity, Path output) throws Exception {
        digest(identity);
        byte[] bytes;
        try (var stream = client.getObject(GetObjectRequest.builder().bucket(bucket).key(prefix + "manifests/" + identity + ".json").build())) {
            bytes = stream.readNBytes(MAX_MANIFEST_BYTES + 1);
        }
        require(bytes.length <= MAX_MANIFEST_BYTES && hash(bytes).equals(identity), "Remote input inventory digest differs");
        JsonObject inventory;
        try { inventory = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject(); }
        catch (RuntimeException malformed) { throw new IOException("Invalid remote input inventory", malformed); }
        require(inventory.has("kind") && inventory.get("kind").getAsString().equals("sibench_input_files")
                && inventory.has("status") && inventory.get("status").getAsString().equals("complete"), "Unpublished remote input");
        var files = inventory.getAsJsonArray("files");
        require(files != null && !files.isEmpty(), "Missing remote input files");
        var paths = new java.util.HashSet<String>();
        var entries = new ArrayList<JsonObject>();
        try {
            for (var item : files) {
                var entry = item.getAsJsonObject();
                String path = name(entry.get("path").getAsString());
                require(paths.add(path), "Duplicate remote input path");
                digest(entry.get("sha256").getAsString());
                InputFiles.number(entry, "bytes");
                entries.add(entry);
            }
        } catch (RuntimeException malformed) { throw new IOException("Invalid remote input file", malformed); }
        require(paths.contains("manifest.json"), "Missing input manifest");
        for (String path : paths) {
            Path parent = Path.of(path).getParent();
            while (parent != null) {
                require(!paths.contains(parent.toString()), "Overlapping remote input paths");
                parent = parent.getParent();
            }
        }
        output = output.toAbsolutePath().normalize();
        Files.createDirectories(output.getParent());
        Files.createDirectory(output);
        // Publish the canonical manifest only after all payloads are present and verified.
        entries.sort(java.util.Comparator.comparing(e -> e.get("path").getAsString().equals("manifest.json")));
        try {
            for (var entry : entries) {
                Path target = output.resolve(entry.get("path").getAsString());
                Files.createDirectories(target.getParent());
                var digest = Digests.digest();
                try (var source = new DigestInputStream(client.getObject(GetObjectRequest.builder().bucket(bucket)
                        .key(prefix + "objects/" + entry.get("sha256").getAsString()).build()), digest);
                     var destination = Files.newOutputStream(target, StandardOpenOption.CREATE_NEW)) {
                    long remaining = entry.get("bytes").getAsLong();
                    byte[] buffer = new byte[1 << 16];
                    while (remaining > 0) {
                        int count = source.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                        require(count > 0, "Truncated remote input");
                        destination.write(buffer, 0, count);
                        remaining -= count;
                    }
                    require(source.read() == -1
                            && HexFormat.of().formatHex(digest.digest()).equals(entry.get("sha256").getAsString()),
                            "Remote input content differs: " + entry.get("path").getAsString());
                }
            }
            check(output);
        } catch (Exception | Error failure) {
            try { Files.deleteIfExists(output.resolve("manifest.json")); }
            catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
        return Map.of("status", "passed", "manifest_sha256", identity, "directory", output.toString());
    }

    /** Uploads exactly the hashed bytes, so a content key never holds bytes other than its digest. */
    void upload(Path file, String key, long size, String sha256) throws IOException {
        var digest = Digests.digest();
        try (var input = Files.newInputStream(file)) {
            if (size <= 64L << 20) {
                byte[] bytes = read(input, digest, size, file, sha256);
                client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromBytes(bytes));
                return;
            }
            long partBytes = Math.max(64L << 20, (size + 9_999) / 10_000);
            require(partBytes <= 256L << 20, "Input object exceeds multipart capacity");
            String upload = client.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build()).uploadId();
            var completed = new ArrayList<CompletedPart>();
            try {
                for (long offset = 0; offset < size; offset += partBytes) {
                    long length = Math.min(partBytes, size - offset);
                    byte[] part = read(input, digest, length, file, offset + length == size ? sha256 : null);
                    int number = completed.size() + 1;
                    var response = client.uploadPart(UploadPartRequest.builder().bucket(bucket).key(key).uploadId(upload)
                            .partNumber(number).contentLength((long) part.length).build(), RequestBody.fromBytes(part));
                    completed.add(CompletedPart.builder().partNumber(number).eTag(response.eTag()).build());
                }
                client.completeMultipartUpload(CompleteMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(upload)
                        .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build()).build());
            } catch (IOException | RuntimeException | Error failure) {
                try { client.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(upload).build()); }
                catch (RuntimeException abort) { failure.addSuppressed(abort); }
                throw failure;
            }
        }
    }

    /** Reads the next bytes; the last read also requires end of file and the expected digest. */
    private static byte[] read(java.io.InputStream input, java.security.MessageDigest digest, long length, Path file,
            String sha256) throws IOException {
        byte[] bytes = input.readNBytes(Math.toIntExact(length));
        digest.update(bytes);
        require(bytes.length == length && (sha256 == null || input.read() == -1
                && HexFormat.of().formatHex(digest.digest()).equals(sha256)), "Input changed during publication: " + file);
        return bytes;
    }

    private static JsonObject check(Path input) throws IOException {
        try {
            var manifest = InputFiles.manifest(input).document();
            return switch (manifest.get("kind").getAsString()) {
                case "hybench_csv_input" -> CsvInput.open(input).manifest();
                case "pixels_cdc_input" -> CdcInput.open(input).manifest();
                case "pixels_baseline_input" -> PixelsBaseline.open(input).manifest();
                default -> throw new IOException("Unsupported input artifact kind");
            };
        } catch (RuntimeException malformed) { throw new IOException("Invalid input manifest", malformed); }
    }

    private static void add(Map<String, JsonObject> files, JsonObject entry) {
        JsonObject previous = files.putIfAbsent(entry.get("path").getAsString(), entry);
        if (previous != null && !previous.equals(entry)) throw new IllegalArgumentException("Conflicting input file descriptors");
    }

    private static String name(String value) throws IOException {
        require(!value.isBlank() && value.indexOf('\\') < 0 && value.indexOf('\0') < 0, "Invalid object/input path");
        Path path = Path.of(value);
        require(!path.isAbsolute() && path.normalize().toString().equals(value), "Noncanonical object/input path");
        for (Path part : path) require(!part.toString().equals("..") && !part.toString().equals("."), "Object/input path traversal");
        return value;
    }

    private static String digest(String value) throws IOException {
        require(value.matches("[a-f0-9]{64}"), "Invalid content digest");
        return value;
    }

    private static String hash(byte[] bytes) { return HexFormat.of().formatHex(Digests.digest().digest(bytes)); }
    private static void require(boolean value, String message) throws IOException { if (!value) throw new IOException(message); }
    @Override public void close() { client.close(); }
}
