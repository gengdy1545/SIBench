package org.sibench.adapters;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.protobuf.ByteString;
import org.sibench.adapters.proto.*;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.Flow;

/** SIBench's gRPC backend over Relive's loopback HTTP boundary; it never interprets Trace. */
public final class ReliveBackend implements IndexBackend {
    private static final int MAX_WRITE_BYTES = 8 << 20, MAX_ROWS = 10_000;
    private static final long TIMEOUT_SECONDS = 60;
    private final URI endpoint;
    private final ValueKind valueKind;
    private final String location;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Set<Body> active = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;
    private String binding;

    public ReliveBackend(String endpoint, ValueKind value) throws IOException {
        try {
            this.endpoint = URI.create(endpoint);
            if (!"http".equals(this.endpoint.getScheme()) || this.endpoint.getHost() == null
                    || this.endpoint.getUserInfo() != null || this.endpoint.getQuery() != null
                    || this.endpoint.getFragment() != null || !Set.of("", "/").contains(this.endpoint.getPath()))
                throw new IllegalArgumentException("Relive requires an HTTP origin without credentials or a path");
            valueKind = Objects.requireNonNull(value);
            var description = call("/describe", null);
            location = string(description, "location");
            if (location.isBlank()) throw new IOException("Missing Relive storage location");
            if (!"relive".equals(string(description, "name")) || !bool(description, "versionedKeys")
                    || !bool(description, "streamingReads") || bool(description, "transactionLocalReads")
                    || bool(description, "searchGroups") || number(description, "maxWriteRows") != MAX_ROWS
                    || !"shared_nfs_fsync".equals(string(description, "durability"))
                    || !"object_storage".equals(string(description, "levelOneStorage")))
                throw new IOException("Unsupported Relive deployment description");
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid Relive endpoint or description", invalid); }
    }

    @Override public IndexDescription describe() {
        return new IndexDescription("relive", valueKind, Acknowledgement.ACK_WAL, true,
                "shared_nfs_fsync", location);
    }

    @Override public synchronized void bind(String supplied) throws IOException {
        if (supplied == null || supplied.isBlank()) throw new IOException("Missing Trace binding");
        String selected = "sibench " + valueKind.name() + " " + supplied;
        if (selected.equals(binding)) return;
        if (binding != null) throw new IOException("Relive bridge belongs to another input");
        var request = new JsonObject(); request.addProperty("binding", selected);
        var answer = call("/bind", request.toString());
        fields(answer, "binding", "identity", "location");
        if (!selected.equals(string(answer, "binding")) || string(answer, "identity").isBlank()
                || string(answer, "location").isBlank()) throw new IOException("Invalid Relive binding response");
        binding = selected;
    }

    private synchronized void requireBinding() throws IOException {
        if (closed) throw new IOException("Relive bridge is closed");
        if (binding == null) bind("ordinary-callers");
    }

    @Override public void write(Mutation write) throws IOException { writeBatch(List.of(write)); }

    @Override public void writeBatch(List<Mutation> writes) throws IOException {
        // Check every member against the narrower transport's limits before the first HTTP write.
        for (var write : writes) {
            key(write.hasPut() ? write.getPut().getKey() : write.getDelete().getKey());
            if (write.hasPut() && write.getPut().getValue().size() > 65_536)
                throw new IllegalArgumentException("Relive values are limited to 65536 bytes");
        }
        requireBinding();
        var batch = new StringBuilder("{\"rows\":[");
        int count = 0;
        for (var write : writes) {
            var row = new JsonObject();
            row.addProperty("key", key(write.hasPut() ? write.getPut().getKey() : write.getDelete().getKey()));
            row.addProperty("ts", write.hasPut() ? write.getPut().getTs() : write.getDelete().getTs());
            row.addProperty("value", write.hasPut() ? HexFormat.of().formatHex(write.getPut().getValue().toByteArray()) : null);
            String encoded = row.toString();
            if (count == MAX_ROWS || batch.length() + encoded.length() + 3 > MAX_WRITE_BYTES) {
                sendBatch(batch, count); batch = new StringBuilder("{\"rows\":["); count = 0;
            }
            if (count++ > 0) batch.append(',');
            batch.append(encoded);
        }
        sendBatch(batch, count);
    }

    private void sendBatch(StringBuilder batch, int count) throws IOException {
        var response = call("/write", batch.append("]}").toString());
        fields(response, "rows");
        if (number(response, "rows") != count) throw new IOException("Incomplete Relive write acknowledgement");
    }

    @Override public ByteString get(GetRequest get) throws IOException {
        key(get.getKey()); requireBinding();
        if (get.getLocalWritesCount() != 0) {
            var local = get.getLocalWrites(0);
            return local.hasPut() ? local.getPut().getValue() : null;
        }
        ByteString[] found = {null};
        read(get.getReadTs(), key(get.getKey()), true, key(get.getKey()), true, false, 1,
                (key, value) -> found[0] = value);
        return found[0];
    }

    @Override public void scan(ScanRequest scan, ValueSink sink) throws IOException {
        String start = scan.hasStartKey() ? key(scan.getStartKey()) : null;
        String end = scan.hasEndKey() ? key(scan.getEndKey()) : null;
        requireBinding();
        if (start != null && end != null && start.compareTo(end) >= 0) return;
        var merge = new PrivateScan(scan, sink, ByteString::toByteArray);
        read(scan.getReadTs(), start, true, end, false, scan.getReverse(),
                scan.getLocalWritesCount() == 0 ? scan.getLimit() : 0, merge::accept);
        merge.finish();
    }

    @FunctionalInterface private interface Entries { void accept(byte[] key, ByteString value) throws IOException; }

    private void read(long ts, String lower, boolean lowerInclusive, String upper, boolean upperInclusive,
                      boolean reverse, long limit, Entries sink) throws IOException {
        var scan = new JsonObject();
        scan.addProperty("lower", lower); scan.addProperty("lowerInclusive", lowerInclusive);
        scan.addProperty("upper", upper); scan.addProperty("upperInclusive", upperInclusive);
        scan.addProperty("descending", reverse); scan.addProperty("limit", limit);
        var scans = new JsonArray(); scans.add(scan);
        var request = new JsonObject(); request.addProperty("ts", ts); request.add("scans", scans);
        try (var body = open("/stream", request.toString())) {
            var header = json(body.line()); fields(header, "ts", "scans");
            if (number(header, "ts") != ts || number(header, "scans") != 1)
                throw new IOException("Relive stream header differs from request");
            long count = 0;
            String previous = null;
            for (;;) {
                var frame = json(body.line());
                if (frame.has("done")) {
                    fields(frame, "done", "counts");
                    var counts = frame.getAsJsonArray("counts");
                    if (!bool(frame, "done") || counts.size() != 1 || number(counts.get(0)) != count)
                        throw new IOException("Invalid Relive terminal count");
                    if (body.line() != null) throw new IOException("Trailing Relive stream data");
                    return;
                }
                fields(frame, "scan", "entry");
                if (number(frame, "scan") != 0) throw new IOException("Unexpected Relive scan index");
                var entry = frame.getAsJsonObject("entry"); fields(entry, "key", "ts", "value");
                String key = string(entry, "key"), value = string(entry, "value");
                byte[] decoded = decodeKey(key);
                if (number(entry, "ts") > ts || lower != null && (key.compareTo(lower) < 0 || !lowerInclusive && key.equals(lower))
                        || upper != null && (key.compareTo(upper) > 0 || !upperInclusive && key.equals(upper))
                        || previous != null && (reverse ? key.compareTo(previous) >= 0 : key.compareTo(previous) <= 0)
                        || limit != 0 && count >= limit)
                    throw new IOException("Relive returned an invalid version, range, order or limit");
                sink.accept(decoded, ByteString.copyFrom(hex(value, 131072)));
                previous = key; count++;
            }
        } catch (RuntimeException invalid) { throw new IOException("Malformed Relive stream", invalid); }
    }

    // A common leading byte preserves byte order and lets Relive represent the empty SIBench key.
    private static String key(ByteString key) {
        if (key.size() > 65_535) throw new IllegalArgumentException("Relive keys are limited to 65535 bytes");
        return "00" + HexFormat.of().formatHex(key.toByteArray());
    }
    private static byte[] decodeKey(String key) throws IOException {
        if (!key.startsWith("00")) throw new IOException("Relive key lacks the SIBench namespace");
        byte[] decoded = hex(key, 131072);
        return Arrays.copyOfRange(decoded, 1, decoded.length);
    }
    private static byte[] hex(String value, int maximum) throws IOException {
        if (value.length() > maximum || (value.length() & 1) != 0 || !value.matches("[0-9a-f]*"))
            throw new IOException("Invalid Relive hexadecimal bytes");
        return HexFormat.of().parseHex(value);
    }

    @Override public void flush() throws IOException { if (closed) throw new IOException("Relive bridge is closed"); }
    @Override public void close() { closed = true; active.forEach(Body::close); }

    private JsonObject call(String path, String request) throws IOException {
        try (var body = open(path, request)) { return json(body.all()); }
        catch (RuntimeException invalid) { throw new IOException("Malformed Relive response", invalid); }
    }
    private Body open(String path, String request) throws IOException {
        if (closed) throw new IOException("Relive bridge is closed");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        var builder = HttpRequest.newBuilder(endpoint.resolve(path)).timeout(Duration.ofSeconds(TIMEOUT_SECONDS));
        if (request == null) builder.GET();
        else builder.header("Content-Type", "application/json").POST(requestBody(request));
        try {
            var response = client.send(builder.build(), HttpResponse.BodyHandlers.ofPublisher());
            var body = new Body(deadline);
            active.add(body); response.body().subscribe(body);
            if (closed) { body.close(); throw new IOException("Relive bridge is closed"); }
            if (response.statusCode() != 200) {
                try (body) { throw new IOException("Relive HTTP " + response.statusCode() + ": " + body.all()); }
            }
            return body;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new IOException("Relive HTTP interrupted; write outcome may be unknown", interrupted);
        }
    }

    static HttpRequest.BodyPublisher requestBody(String request) {
        var delegate = HttpRequest.BodyPublishers.ofString(request);
        var subscribed = new java.util.concurrent.atomic.AtomicBoolean();
        return new HttpRequest.BodyPublisher() {
            public long contentLength() { return delegate.contentLength(); }
            public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
                // Refuse payload replay even when JVM-wide HTTP retry settings enable POST retries.
                if (subscribed.compareAndSet(false, true)) { delegate.subscribe(subscriber); return; }
                subscriber.onSubscribe(new Flow.Subscription() {
                    public void request(long count) {}
                    public void cancel() {}
                });
                subscriber.onError(new IOException("Relive request body cannot be retransmitted; write outcome may be unknown"));
            }
        };
    }

    /** Demand and a fixed deadline bound streamed body buffering and stalls after HTTP headers. */
    private final class Body implements Flow.Subscriber<List<ByteBuffer>>, AutoCloseable {
        private final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(2);
        private final ArrayDeque<ByteBuffer> buffers = new ArrayDeque<>();
        private final long deadline;
        private volatile Flow.Subscription subscription;
        private boolean end, requested = true;
        private volatile boolean cancelled;
        Body(long deadline) { this.deadline = deadline; }
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            if (cancelled) subscription.cancel(); else subscription.request(1);
        }
        public void onNext(List<ByteBuffer> bytes) { queue.offer(bytes); }
        public void onError(Throwable error) { queue.offer(error); }
        public void onComplete() { queue.offer(Boolean.TRUE); }
        private int read() throws IOException {
            for (;;) {
                while (!buffers.isEmpty()) {
                    var buffer = buffers.peekFirst();
                    if (buffer.hasRemaining()) return Byte.toUnsignedInt(buffer.get());
                    buffers.removeFirst();
                }
                if (end) return -1;
                if (System.nanoTime() >= deadline) throw new IOException("Relive HTTP body deadline exceeded");
                if (!requested && subscription != null) { subscription.request(1); requested = true; }
                Object next;
                try { next = queue.poll(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Relive body interrupted", interrupted); }
                if (next == null) throw new IOException("Relive HTTP body deadline exceeded");
                if (next instanceof Throwable error) throw new IOException("Relive HTTP body failed", error);
                if (next == Boolean.TRUE) { end = true; continue; }
                @SuppressWarnings("unchecked") var bytes = (List<ByteBuffer>) next;
                buffers.addAll(bytes); requested = false;
            }
        }
        String line() throws IOException { return text(true, 512 << 10); }
        String all() throws IOException { return text(false, 64 << 10); }
        private String text(boolean line, int limit) throws IOException {
            var bytes = new ByteArrayOutputStream();
            for (;;) {
                int value = read();
                if (value == -1) {
                    if (line && bytes.size() != 0) throw new IOException("Unterminated Relive frame");
                    if (bytes.size() == 0 && line) return null;
                    break;
                }
                if (line && value == '\n') break;
                if (bytes.size() == limit) throw new IOException("Relive response exceeds byte limit");
                bytes.write(value);
            }
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes.toByteArray())).toString();
        }
        @Override public void close() {
            cancelled = true;
            if (subscription != null) subscription.cancel();
            queue.clear(); queue.offer(new IOException("Relive response closed")); active.remove(this);
        }
    }

    private static JsonObject json(String text) throws IOException {
        if (text == null) throw new IOException("Missing Relive terminal frame");
        try (var reader = new JsonReader(new StringReader(text))) {
            reader.setStrictness(Strictness.STRICT);
            var value = JsonParser.parseReader(reader);
            if (!value.isJsonObject() || reader.peek() != JsonToken.END_DOCUMENT) throw new IOException("Invalid Relive JSON object");
            return value.getAsJsonObject();
        } catch (RuntimeException invalid) { throw new IOException("Malformed Relive JSON", invalid); }
    }
    private static void fields(JsonObject object, String... names) throws IOException {
        if (!object.keySet().equals(Set.of(names))) throw new IOException("Unexpected Relive response fields");
    }
    private static String string(JsonObject object, String name) throws IOException {
        var value = object.get(name);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString())
            throw new IOException("Expected Relive string: " + name);
        return value.getAsString();
    }
    private static boolean bool(JsonObject object, String name) throws IOException {
        var value = object.get(name);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isBoolean())
            throw new IOException("Expected Relive boolean: " + name);
        return value.getAsBoolean();
    }
    private static long number(JsonObject object, String name) throws IOException { return number(object.get(name)); }
    private static long number(JsonElement value) throws IOException {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()
                || !value.getAsString().matches("[0-9]+")) throw new IOException("Expected nonnegative Relive integer");
        try { return value.getAsBigInteger().longValueExact(); }
        catch (ArithmeticException invalid) { throw new IOException("Relive number exceeds int64", invalid); }
    }
}
