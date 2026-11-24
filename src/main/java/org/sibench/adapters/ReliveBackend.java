package org.sibench.adapters;

import com.google.protobuf.ByteString;
import org.relive.Protocol;
import org.relive.ReliveClient;
import org.relive.ReliveClientConfig;
import org.sibench.adapters.proto.GetRequest;
import org.sibench.adapters.proto.Mutation;
import org.sibench.adapters.proto.ScanRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SIBench's backend over embedded Relive clients in this process. Each of the {@code clients}
 * instances owns its metadata connection and manifest cache, as independent callers would; requests
 * rotate among them. The index uses one shard; every write is durable when its future completes.
 */
public final class ReliveBackend implements IndexBackend {
    private static final int MAX_ROWS = 10_000;
    // Leave room for the request envelope below Relive's 8 MiB payload limit.
    private static final int MAX_WRITE_BYTES = 7 << 20;
    private static final String DATA_PREFIX = "00";
    // Outside the "00" data namespace: the Trace binding, written once at timestamp zero.
    private static final String BINDING_KEY = "01";
    private static final Protocol.Scan BINDING_SCAN = point(BINDING_KEY);

    private final List<ReliveClient> clients;
    private final String shard;
    private final String location;
    private final ValueKind valueKind;
    private final AtomicInteger next = new AtomicInteger();
    private volatile String binding;

    public ReliveBackend(Path clientConfig, int count, ValueKind value) throws IOException {
        if (count < 1) throw new IllegalArgumentException("Relive needs at least one client");
        valueKind = Objects.requireNonNull(value);
        var config = ReliveClientConfig.load(clientConfig);
        if (config.shards().size() != 1) throw new IOException("The Relive backend serves a single-shard index");
        shard = config.shards().get(0);
        location = config.cfsRoot() + " " + config.metastore().endpoint() + "/" + config.metastore().table();
        ReliveClient.initialize(config);
        var opened = new ArrayList<ReliveClient>();
        try {
            for (int i = 0; i < count; i++) opened.add(ReliveClient.open(config));
        } catch (IOException | RuntimeException error) {
            opened.forEach(ReliveClient::close);
            throw error;
        }
        clients = List.copyOf(opened);
    }

    private ReliveClient client() { return clients.get(Math.floorMod(next.getAndIncrement(), clients.size())); }

    @Override public IndexDescription describe() {
        return new IndexDescription("relive", valueKind, Acknowledgement.ACK_WAL, true, "cfs_fsync_metadata_commit", location);
    }

    @Override public void bind(String supplied) throws IOException {
        if (supplied == null || supplied.isBlank()) throw new IOException("Missing Trace binding");
        String selected = "sibench " + valueKind.name() + " " + supplied;
        if (selected.equals(binding)) return;
        synchronized (this) {
            if (selected.equals(binding)) return;
            if (binding != null) throw new IOException("Relive index belongs to another input");
            byte[] identity = selected.getBytes(StandardCharsets.UTF_8);
            String value = HexFormat.of().formatHex(identity);
            var client = client();
            try {
                // A deterministic request ID makes concurrent binders of the same input one commit.
                client.write(shard, UUID.nameUUIDFromBytes(identity),
                        new Protocol.Write(List.of(new Protocol.Mutation(BINDING_KEY, 0, value))));
            } catch (ReliveClient.Conflict other) {
                throw new IOException("Relive index holds another input than " + selected, other);
            }
            String[] stored = {null};
            client.stream(shard, new Protocol.Read(0, List.of(BINDING_SCAN)), sink((key, found) -> stored[0] = found));
            if (!value.equals(stored[0])) throw new IOException("Relive index holds another input than " + selected);
            binding = selected;
        }
    }

    private void requireBinding() throws IOException {
        if (binding == null) bind("ordinary-callers");
    }

    /** The common backend interface permits synchronous work before returning a completed future. */
    @Override public CompletableFuture<Void> writeBatchAsync(List<Mutation> writes) throws IOException {
        requireBinding();
        var rows = new ArrayList<Protocol.Mutation>(Math.min(writes.size(), MAX_ROWS));
        long bytes = 0;
        var client = client();
        for (var write : writes) {
            var row = encodeMutation(write);
            long size = row.key().length() + (row.value() == null ? 0 : row.value().length()) + 64L;
            if (rows.size() == MAX_ROWS || bytes + size > MAX_WRITE_BYTES) {
                commit(client, rows);
                rows.clear();
                bytes = 0;
            }
            rows.add(row);
            bytes += size;
        }
        if (!rows.isEmpty()) commit(client, rows);
        return CompletableFuture.completedFuture(null);
    }

    private void commit(ReliveClient client, List<Protocol.Mutation> rows) throws IOException {
        var ack = client.write(shard, UUID.randomUUID(), new Protocol.Write(rows));
        if (ack.rows() != rows.size()) throw new IOException("Incomplete Relive write acknowledgement");
    }

    private static Protocol.Mutation encodeMutation(Mutation mutation) {
        if (mutation.hasPut()) {
            var put = mutation.getPut();
            return new Protocol.Mutation(encodeKey(put.getKey()), put.getTs(),
                    HexFormat.of().formatHex(put.getValue().toByteArray()));
        }
        var delete = mutation.getDelete();
        return new Protocol.Mutation(encodeKey(delete.getKey()), delete.getTs(), null);
    }

    @Override public ByteString get(GetRequest get) throws IOException {
        requireBinding();
        if (get.getLocalWritesCount() != 0) {
            var local = get.getLocalWrites(0);
            return local.hasPut() ? local.getPut().getValue() : null;
        }
        ByteString[] found = {null};
        client().stream(shard, new Protocol.Read(get.getReadTs(), List.of(point(encodeKey(get.getKey())))),
                sink((key, value) -> found[0] = ByteString.copyFrom(HexFormat.of().parseHex(value))));
        return found[0];
    }

    @Override public void scan(ScanRequest scan, ValueSink sink) throws IOException {
        requireBinding();
        String start = scan.hasStartKey() ? encodeKey(scan.getStartKey()) : DATA_PREFIX;
        String end = scan.hasEndKey() ? encodeKey(scan.getEndKey()) : BINDING_KEY;
        if (start.compareTo(end) >= 0) return;
        var merge = new PrivateScan(scan, sink, ByteString::toByteArray);
        long limit = scan.getLocalWritesCount() == 0 ? scan.getLimit() : 0;
        var request = new Protocol.Read(scan.getReadTs(),
                List.of(new Protocol.Scan(start, true, end, false, scan.getReverse(), limit)));
        IOException[] failure = {null};
        client().stream(shard, request, sink((key, value) -> {
            if (failure[0] != null) return;
            try { merge.accept(decodeKey(key), ByteString.copyFrom(HexFormat.of().parseHex(value))); }
            catch (IOException error) { failure[0] = error; }
        }));
        if (failure[0] != null) throw failure[0];
        merge.finish();
    }

    @FunctionalInterface private interface Entries { void accept(String key, String value); }

    private static ReliveClient.ReadSink sink(Entries entries) {
        return new ReliveClient.ReadSink() {
            @Override public void begin(long ts, int scans) {}
            @Override public void entry(int scan, Protocol.Entry entry) { entries.accept(entry.key(), entry.value()); }
        };
    }

    private static Protocol.Scan point(String key) { return new Protocol.Scan(key, true, key, true, false, 1); }

    // A common leading byte preserves byte order and lets Relive represent the empty SIBench key.
    private static String encodeKey(ByteString key) {
        if (key.size() > 65_535) throw new IllegalArgumentException("Relive keys are limited to 65535 bytes");
        return DATA_PREFIX + HexFormat.of().formatHex(key.toByteArray());
    }

    private static byte[] decodeKey(String key) throws IOException {
        if (!key.startsWith(DATA_PREFIX)) throw new IOException("Relive key lacks the SIBench namespace");
        return HexFormat.of().parseHex(key, DATA_PREFIX.length(), key.length());
    }

    @Override public Map<String, Object> statistics() {
        var totals = new LinkedHashMap<String, Object>();
        long requests = 0, retries = 0, metadataNanos = 0, casNanos = 0, stageNanos = 0, readNanos = 0;
        for (var client : clients) {
            var stats = client.stats();
            requests += stats.databaseRequests();
            retries += stats.casRetries();
            metadataNanos += stats.metadataNanos();
            casNanos += stats.casNanos();
            stageNanos += stats.stageNanos();
            readNanos += stats.readNanos();
        }
        totals.put("clients", clients.size());
        totals.put("metadata_requests", requests);
        totals.put("cas_retries", retries);
        totals.put("metadata_nanos", metadataNanos);
        totals.put("cas_nanos", casNanos);
        totals.put("stage_nanos", stageNanos);
        totals.put("read_nanos", readNanos);
        return totals;
    }

    // Each successful write already forced its files and committed its metadata.
    @Override public void flush() {}

    @Override public void close() { clients.forEach(ReliveClient::close); }
}
