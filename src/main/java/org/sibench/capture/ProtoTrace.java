package org.sibench.capture;

import com.github.luben.zstd.Zstd;
import com.google.protobuf.ByteString;

import org.sibench.adapters.TraceRequests;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.trace.IndexKey;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.util.*;
import java.util.zip.CRC32C;

/** Independently compressed blocks of delimited service requests, with source positions. */
public final class ProtoTrace {
    public static final int MAX_TRANSACTION_OPERATIONS = CommittedOperations.MAX_TRANSACTION_OPERATIONS;
    private static final int MAGIC = 0x53495042;
    private static final int MAX_BYTES = 32 << 20;
    private static final int BLOCK_BYTES = 1 << 20;

    static final int ZSTD_LEVEL = 3;

    private ProtoTrace() {}

    public static IndexOperationTrace.Source source(Path directory, com.google.gson.JsonObject manifest,
            String name) throws IOException {
        if (!manifest.has("proto_files")) throw new IOException("Missing proto Trace file inventory");
        if (!manifest.get("proto_files").isJsonObject()) throw new IOException("Invalid proto Trace file inventory");
        var inventory = manifest.getAsJsonObject("proto_files");
        if (!inventory.has(name) || !inventory.get(name).isJsonArray())
            throw new IOException("Invalid proto Trace file inventory: " + name);
        var files = inventory.getAsJsonArray(name);
        if (files.size() != 1) throw new IOException("Expected one canonical proto Trace file: " + name);
        for (var item : files) {
            if (!item.isJsonObject()) throw new IOException("Invalid proto Trace inventory entry");
            var file = item.getAsJsonObject();
            for (String field : List.of("sha256", "logical_sha256"))
                if (!file.has(field) || !file.get(field).getAsString().matches("[0-9a-f]{64}"))
                    throw new IOException("Invalid proto Trace digest: " + field);
            if (!file.has("operations") || file.get("operations").getAsLong() < 0 || !file.has("path"))
                throw new IOException("Invalid proto Trace count or path");
        }
        String identity = identity(files);
        if (!identity.equals(manifest.get(name + "_trace_sha256").getAsString()))
            throw new IOException("Proto Trace inventory identity mismatch: " + name);
        var paths = new HashSet<Path>();
        for (var item : files) {
            var file = item.getAsJsonObject();
            Path relative = Path.of(file.get("path").getAsString());
            if (relative.isAbsolute() || !relative.normalize().equals(relative) || relative.startsWith("..")
                    || !relative.toString().equals(name + ".pb.zst")
                    || !paths.add(relative))
                throw new IOException("Invalid proto Trace file path: " + relative);
        }
        return consumer -> {
            long count = 0;
            for (var item : files) {
                var file = item.getAsJsonObject();
                long actual = read(directory.resolve(file.get("path").getAsString()),
                        file.get("sha256").getAsString(), file.get("logical_sha256").getAsString(), consumer);
                if (actual != file.get("operations").getAsLong())
                    throw new IOException("Proto Trace file operation count mismatch");
                count = Math.addExact(count, actual);
            }
            return count;
        };
    }

    /** Binds shard order and semantic counts without decoding and re-encoding requests. */
    public static String identity(com.google.gson.JsonArray files) {
        var digest = org.sibench.config.Digests.digest();
        for (var item : files) {
            var file = item.getAsJsonObject();
            digest.update(HexFormat.of().parseHex(file.get("logical_sha256").getAsString()));
            digest.update(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(file.get("operations").getAsLong()).array());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    public static Map<String, Object> file(String path, Writer writer) {
        return Map.of("path", path, "sha256", writer.sha256(),
                "logical_sha256", writer.logicalSha256(), "operations", writer.count(),
                "bytes", writer.bytesWritten(), "statistics", writer.statistics());
    }

    public static IndexOperationTrace.Source source(Path file, String physical, String logical) {
        return consumer -> read(file, physical, logical, consumer);
    }

    public static final class Writer implements Closeable, IndexOperationTrace.Consumer {
        private final java.security.MessageDigest physical = org.sibench.config.Digests.digest();
        private final DataOutputStream output;
        private final ByteArrayOutputStream block = new ByteArrayOutputStream();
        private final java.security.MessageDigest logical = org.sibench.config.Digests.digest();
        private final OutputStream messages = new DigestOutputStream(block, logical);
        private long count;
        private String logicalSha256;
        private final TraceRequests requests = new TraceRequests(ValueKind.ROW_ID);
        private final CommittedOperations projection = new CommittedOperations(this::appendExported);
        private final List<Operation> group = new ArrayList<>();
        private int records;
        private long transaction;
        private long transactionOperations, transactionWrites;
        private String sha256;
        private boolean closed;
        private volatile long bytesWritten = Integer.BYTES;
        private final boolean secondaryIndexes;
        private final RequestBudget budget;
        private long indexEntries, primaryEntries, effectiveRequests, uncompressedBytes;
        private final Map<String, Long> requestTypes = new LinkedHashMap<>();
        private final Map<String, Long> controls = new LinkedHashMap<>();
        private final Map<String, Long> writeOrigins = new LinkedHashMap<>();

        public Writer(Path file) throws IOException {
            this(file, true);
        }

        /** Export filtering leaves source execution unchanged and keeps query groups indivisible. */
        public Writer(Path file, boolean secondaryIndexes) throws IOException {
            this(file, secondaryIndexes, null);
        }

        Writer(Path file, boolean secondaryIndexes, RequestBudget budget) throws IOException {
            this.budget = budget;
            this.secondaryIndexes = secondaryIndexes;
            output = new DataOutputStream(new BufferedOutputStream(new DigestOutputStream(
                    Files.newOutputStream(file, StandardOpenOption.CREATE_NEW), physical)));
            output.writeInt(MAGIC);
        }

        @Override
        public void accept(Operation operation) throws IOException {
            append(operation);
        }

        public void append(Operation operation) throws IOException {
            if (closed) throw new IOException("Proto Trace writer is closed");
            projection.accept(operation);
        }

        private void appendExported(Operation operation) throws IOException {
            if (operation.grouped()) {
                if (operation.expansionPosition() != group.size()
                        || !group.isEmpty() && (operation.expansionSize() != group.get(0).expansionSize()
                                || operation.sourceEvent() != group.get(0).sourceEvent()))
                    throw new IOException("Invalid proto search group order");
                group.add(operation);
                if (group.size() == operation.expansionSize()) {
                    if (secondaryIndexes || group.stream().allMatch(member -> member.index() == 0)) {
                        var record = TraceRecord.newBuilder().setRequest(requests.group(0, group));
                        for (Operation member : group) record.addPositions(position(member));
                        write(record.build());
                    }
                    group.clear();
                }
                return;
            }
            if (!group.isEmpty()) throw new IOException("Interrupted proto search group");
            if (!secondaryIndexes && operation.index() != 0) return;
            if (!secondaryIndexes && !IndexOperationTrace.isControl(operation)
                    && operation.kind() != IndexOperationTrace.SEARCH)
                operation = operation.withExpansion(0, 1);
            var record = TraceRecord.newBuilder().addPositions(position(operation));
            if (operation.kind() == IndexOperationTrace.TRANSACTION_BEGIN) {
                if (transaction != 0) throw new IOException("Nested proto transaction");
                transaction = operation.transactionId();
                transactionOperations = transactionWrites = 0;
                record.setBegin(TraceBegin.newBuilder().setTransactionId(transaction).setVersion(operation.version()));
            } else if (operation.kind() == IndexOperationTrace.TRANSACTION_COMMIT
                    || operation.kind() == IndexOperationTrace.TRANSACTION_ABORT) {
                if (transaction != operation.transactionId()) throw new IOException("Unmatched proto commit");
                record.setEnd(TraceEnd.newBuilder().setTransactionId(transaction).setVersion(operation.version())
                        .setAborted(operation.kind() == IndexOperationTrace.TRANSACTION_ABORT)
                        .setOperations(transactionOperations).setWrites(transactionWrites));
                transaction = 0;
            } else {
                var request = requests.request(0, operation);
                if (request.hasWrite() && operation.kind() != IndexOperationTrace.LOGICAL_DELETE) {
                    // Persist both value representations for replay from the same physical data layout.
                    request = request.toBuilder().setWrite(request.getWrite().toBuilder().setValue(
                            request.getWrite().getValue().toBuilder()
                                    .setFile(operation.file()).setPosition(operation.position()))).build();
                }
                record.setRequest(request);
            }
            write(record.build());
        }

        private void write(TraceRecord record) throws IOException {
            if (record.getSerializedSize() > MAX_BYTES - 5) throw new IOException("Proto Trace request exceeds block limit");
            if (records != 0 && (records == IndexOperationTrace.BLOCK_RECORDS
                    || block.size() + record.getSerializedSize() + 5 > BLOCK_BYTES)) flush();
            record.writeDelimitedTo(messages);
            records++;
            count += record.getPositionsCount();
            var request = record.getRequest();
            if (record.hasBegin() || record.hasEnd()) {
                controls.merge(record.hasBegin() ? "begin" : record.getEnd().getAborted() ? "abort" : "commit", 1L, Long::sum);
                return;
            }
            int operations = record.getPositionsCount();
            if (transaction != 0) {
                transactionOperations += operations;
                if (request.hasWrite()) transactionWrites++;
            }
            effectiveRequests = Math.addExact(effectiveRequests, operations);
            if (budget != null) budget.record(operations);
            indexEntries += record.getPositionsCount();
            String type;
            if (request.hasWrite()) {
                var write = request.getWrite();
                if (write.getKey().getIndex() == 0) primaryEntries++;
                type = write.getKind().name().toLowerCase(Locale.ROOT);
                int flags = record.getPositions(0).getFlags();
                if ((flags & IndexOperationTrace.BUSINESS_INSERT) != 0) writeOrigins.merge("business_insert", 1L, Long::sum);
                if ((flags & IndexOperationTrace.BUSINESS_UPDATE) != 0) writeOrigins.merge("business_update", 1L, Long::sum);
            } else if (request.hasGroup()) {
                for (Search member : request.getGroup().getMembersList()) if (member.getIndex() == 0) primaryEntries++;
                type = request.getGroup().getMode().name().toLowerCase(Locale.ROOT) + "_search";
            } else {
                var search = request.getSearch();
                if (search.getIndex() == 0) primaryEntries++;
                type = search.hasLower() && search.hasUpper() && search.getLower().getInclusive()
                        && search.getUpper().getInclusive() && search.getLower().getKey().equals(search.getUpper().getKey())
                        ? "point_search" : "range_search";
            }
            requestTypes.merge(type, 1L, Long::sum);
        }

        private void flush() throws IOException {
            if (records == 0) return;
            byte[] plain = block.toByteArray(), compressed = Zstd.compress(plain, ZSTD_LEVEL);
            var crc = new CRC32C();
            crc.update(plain);
            output.writeInt(records);
            output.writeInt(compressed.length);
            output.writeInt(plain.length);
            output.writeInt((int) crc.getValue());
            output.write(compressed);
            bytesWritten += 4L * Integer.BYTES + compressed.length;
            uncompressedBytes += plain.length;
            block.reset();
            records = 0;
        }

        public long count() { return count; }
        long bytesWritten() { return bytesWritten; }
        public Map<String, Object> statistics() {
            return Map.of("index_entries", indexEntries, "effective_requests", effectiveRequests,
                    "primary_entries", primaryEntries, "secondary_entries", indexEntries - primaryEntries,
                    "request_types", Map.copyOf(requestTypes), "transaction_markers", Map.copyOf(controls),
                    "write_origins", Map.copyOf(writeOrigins), "uncompressed_message_bytes", uncompressedBytes,
                    "source_projection", projection.statistics());
        }
        public String logicalSha256() {
            if (logicalSha256 == null) throw new IllegalStateException("Proto Trace not closed successfully");
            return logicalSha256;
        }
        public String sha256() {
            if (sha256 == null) throw new IllegalStateException("Proto Trace not closed successfully");
            return sha256;
        }

        @Override
        public void close() throws IOException {
            if (closed) return;
            closed = true;
            try {
                projection.finish();
                if (!group.isEmpty() || transaction != 0) throw new IOException("Incomplete proto transaction or search group");
                flush();
                for (int i = 0; i < 4; i++) output.writeInt(0);
                bytesWritten += 4L * Integer.BYTES;
            } finally {
                output.close();
            }
            sha256 = HexFormat.of().formatHex(physical.digest());
            logicalSha256 = HexFormat.of().formatHex(logical.digest());
        }
    }

    private static TracePosition position(Operation operation) {
        var position = TracePosition.newBuilder()
                .setSourceEvent(operation.sourceEvent()).setWorkUnitId(operation.workUnitId())
                .setMutationOrdinal(operation.mutationOrdinal()).setFlags(operation.flags())
                .setExpectedCount(operation.expectedCount()).setExpansionPosition(operation.expansionPosition())
                .setExpansionSize(operation.expansionSize());
        if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE) position.setDeletedRowId(operation.rowId());
        return position.build();
    }

    public static long read(Path file, String expectedPhysical, String expectedLogical,
            IndexOperationTrace.Consumer consumer) throws IOException {
        var physical = org.sibench.config.Digests.digest();
        var decoder = new Decoder();
        var logical = org.sibench.config.Digests.digest();
        long count = 0;
        try (var input = new DataInputStream(new BufferedInputStream(new DigestInputStream(
                    Files.newInputStream(file), physical)))) {
            if (input.readInt() != MAGIC) throw new IOException("Invalid proto Trace header");
            while (true) {
                int records = input.readInt(), compressed = input.readInt(), length = input.readInt(), checksum = input.readInt();
                if (records == 0 && compressed == 0 && length == 0 && checksum == 0) {
                    if (input.read() != -1) throw new IOException("Trailing proto Trace bytes");
                    break;
                }
                if (records < 1 || records > IndexOperationTrace.BLOCK_RECORDS
                        || length < 1 || length > MAX_BYTES || compressed < 1 || compressed > MAX_BYTES + (1 << 20))
                    throw new IOException("Invalid proto Trace block lengths");
                byte[] encoded = input.readNBytes(compressed);
                if (encoded.length != compressed) throw new EOFException("Truncated proto Trace block");
                byte[] plain;
                try { plain = Zstd.decompress(encoded, length); }
                catch (RuntimeException error) { throw new IOException("Invalid compressed proto Trace block", error); }
                var crc = new CRC32C();
                crc.update(plain);
                if (plain.length != length || (int) crc.getValue() != checksum)
                    throw new IOException("Proto Trace block checksum mismatch");
                logical.update(plain);
                var messages = com.google.protobuf.CodedInputStream.newInstance(plain);
                for (int i = 0; i < records; i++) {
                    if (messages.isAtEnd()) throw new EOFException("Missing proto Trace request");
                    var record = messages.readMessage(TraceRecord.parser(),
                            com.google.protobuf.ExtensionRegistryLite.getEmptyRegistry());
                    var views = new ArrayList<Operation>(record.getPositionsCount());
                    decoder.accept(record, views::add);
                    for (Operation operation : views) IndexOperationTrace.validate(operation);
                    consumer.accept(record, List.copyOf(views));
                    count = Math.addExact(count, views.size());
                }
                if (!messages.isAtEnd()) throw new IOException("Extra proto Trace requests");
            }
            if (decoder.transaction != 0) throw new IOException("Unclosed proto Trace transaction");
        }
        if (expectedPhysical != null && !HexFormat.of().formatHex(physical.digest()).equals(expectedPhysical))
            throw new IOException("Proto Trace file digest mismatch");
        if (expectedLogical != null && !HexFormat.of().formatHex(logical.digest()).equals(expectedLogical))
            throw new IOException("Proto Trace logical digest mismatch");
        return count;
    }

    private static final class Decoder {
        long transaction, version, operations, writes, buffered;

        void accept(TraceRecord record, IndexOperationTrace.Consumer consumer) throws IOException {
            if (!record.getUnknownFields().asMap().isEmpty() || record.getPositionsCount() == 0)
                throw new IOException("Unsupported or missing proto Trace metadata");
            var first = record.getPositions(0);
            if (record.hasBegin()) {
                var begin = record.getBegin();
                if (transaction != 0 || begin.getTransactionId() <= 0 || begin.getVersion() <= 0
                        || record.getPositionsCount() != 1) throw new IOException("Invalid proto transaction begin");
                transaction = begin.getTransactionId();
                version = begin.getVersion();
                operations = writes = buffered = 0;
                consumer.accept(IndexOperationTrace.transaction(first.getSourceEvent(),
                        IndexOperationTrace.TRANSACTION_BEGIN, transaction, version, first.getExpectedCount()));
                return;
            }
            if (record.hasEnd()) {
                var end = record.getEnd();
                if (transaction == 0 || end.getTransactionId() != transaction || end.getVersion() != version
                        || end.getOperations() != operations || end.getWrites() != writes
                        || end.getAborted() && writes != 0
                        || record.getPositionsCount() != 1) throw new IOException("Invalid proto terminal counts or identity");
                consumer.accept(IndexOperationTrace.transaction(first.getSourceEvent(),
                        end.getAborted() ? IndexOperationTrace.TRANSACTION_ABORT : IndexOperationTrace.TRANSACTION_COMMIT,
                        transaction, version, first.getExpectedCount()));
                transaction = 0;
                return;
            }
            if (!record.hasRequest()) throw new IOException("Missing proto service request");
            var request = record.getRequest();
            if (request.getId() != 0) throw new IOException("Proto request IDs are assigned at replay");
            if (!request.getUnknownFields().asMap().isEmpty()) throw new IOException("Unsupported proto service fields");
            int members = request.hasGroup() ? request.getGroup().getMembersCount() : 1;
            if (transaction != 0) {
                if (request.hasWrite()) writes++;
                operations += members;
                // Generation buffers a source group only from its first write; earlier searches stream.
                if (writes > 0) buffered += members;
                if (buffered > MAX_TRANSACTION_OPERATIONS) throw new IOException("Proto source group exceeds operation buffer limit");
            }
            if (record.getPositionsCount() != members) throw new IOException("Proto source-member count mismatch");
            if (request.hasGroup() && (members < 2 || request.getGroup().getMode() == SearchGroup.Mode.MODE_UNSPECIFIED
                    || request.getGroup().getMode() == SearchGroup.Mode.UNRECOGNIZED))
                throw new IOException("Invalid proto search group");
            for (int i = 0; i < members; i++) {
                var metadata = record.getPositions(i);
                Operation operation;
                if (request.hasWrite()) operation = write(request.getWrite(), transaction, metadata);
                else if (request.hasSearch() || request.hasGroup()) {
                    var search = request.hasSearch() ? request.getSearch() : request.getGroup().getMembers(i);
                    if (request.hasGroup()) {
                        int flag = request.getGroup().getMode() == SearchGroup.Mode.INTERSECTION
                                ? IndexOperationTrace.INTERSECT_GROUP : IndexOperationTrace.UNION_GROUP;
                        if ((metadata.getFlags() & flag) == 0 || metadata.getExpansionPosition() != i
                                || metadata.getExpansionSize() != members)
                            throw new IOException("Proto search group metadata mismatch");
                    }
                    operation = search(search, transaction, metadata);
                } else throw new IOException("Unsupported proto service request");
                consumer.accept(operation);
            }
        }
    }

    private static Operation write(Write write, long transaction, TracePosition p) throws IOException {
        int kind = switch (write.getKind()) {
            case INSERT -> IndexOperationTrace.INSERT;
            case UPSERT -> IndexOperationTrace.UPSERT;
            case LOGICAL_DELETE -> IndexOperationTrace.LOGICAL_DELETE;
            case RELOCATE -> IndexOperationTrace.RELOCATE;
            default -> throw new IOException("Unsupported proto write kind");
        };
        var key = write.getKey();
        return new Operation(p.getSourceEvent(), kind, key.getTable(), key.getIndex(), p.getFlags(),
                key(key.getPrimaryKey()), key.getIndexedValue().isEmpty() ? null : key(key.getIndexedValue()),
                write.getVersion(), write.getValue().getFile(), write.getValue().getPosition(),
                kind == IndexOperationTrace.LOGICAL_DELETE ? p.getDeletedRowId() : write.getValue().getRowId(),
                p.getExpectedCount(), transaction, p.getMutationOrdinal(), -1, p.getWorkUnitId(),
                p.getExpansionPosition(), p.getExpansionSize(), null, null, 0);
    }

    private static Operation search(Search search, long transaction, TracePosition p) throws IOException {
        int bounds = (search.hasLower() && search.getLower().getInclusive() ? IndexOperationTrace.LOWER_INCLUSIVE : 0)
                | (search.hasUpper() && search.getUpper().getInclusive() ? IndexOperationTrace.UPPER_INCLUSIVE : 0)
                | (search.getDescending() ? IndexOperationTrace.DESCENDING : 0);
        if ((p.getFlags() & (IndexOperationTrace.LOWER_INCLUSIVE | IndexOperationTrace.UPPER_INCLUSIVE
                | IndexOperationTrace.DESCENDING)) != bounds
                || (p.getFlags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0
                || !search.getUnknownFields().asMap().isEmpty())
            throw new IOException("Proto search metadata mismatch");
        return new Operation(p.getSourceEvent(), IndexOperationTrace.SEARCH, search.getTable(), search.getIndex(),
                p.getFlags(), search.getPinnedPrimaryKey().isEmpty() ? null : key(search.getPinnedPrimaryKey()), null,
                0, 0, 0, 0, p.getExpectedCount(), transaction, p.getMutationOrdinal(),
                search.getReadVersion(), p.getWorkUnitId(), p.getExpansionPosition(), p.getExpansionSize(),
                search.hasLower() ? key(search.getLower().getKey()) : null,
                search.hasUpper() ? key(search.getUpper().getKey()) : null, search.getLimit());
    }

    private static IndexKey key(ByteString bytes) throws IOException {
        return IndexKey.decode(bytes.toByteArray());
    }
}
