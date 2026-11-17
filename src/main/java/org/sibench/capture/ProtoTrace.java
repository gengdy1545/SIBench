package org.sibench.capture;

import com.github.luben.zstd.Zstd;
import com.google.protobuf.ByteString;

import org.sibench.adapters.TraceRequests;
import org.sibench.adapters.ValueKind;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.trace.IndexKey;
import org.sibench.trace.IndexEntry;
import org.sibench.adapters.IndexExecution;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.util.*;
import java.util.zip.CRC32C;

/** Independently compressed blocks of delimited service requests, with source metadata. */
public final class ProtoTrace {
    public static final int MAX_TRANSACTION_OPERATIONS = CommittedOperations.MAX_TRANSACTION_OPERATIONS;
    private static final int MAGIC = 0x53495042;
    private static final int MAX_BYTES = 32 << 20;
    private static final int BLOCK_BYTES = 1 << 20;

    static final int ZSTD_LEVEL = 3;

    private ProtoTrace() {}

    /** The stream's descriptor in {@code proto_files}; its payload is always {@code NAME.pb.zst}. */
    public static IndexOperationTrace.Source source(Path directory, com.google.gson.JsonObject manifest,
            String name) throws IOException {
        if (!manifest.has("proto_files") || !manifest.get("proto_files").isJsonObject())
            throw new IOException("Missing proto Trace file inventory");
        var inventory = manifest.getAsJsonObject("proto_files");
        if (!inventory.has(name) || !inventory.get(name).isJsonObject())
            throw new IOException("Invalid proto Trace file inventory: " + name);
        var file = inventory.getAsJsonObject(name);
        for (String field : List.of("sha256", "logical_sha256"))
            if (!file.has(field) || !file.get(field).getAsString().matches("[0-9a-f]{64}"))
                throw new IOException("Invalid proto Trace digest: " + field);
        if (!file.has("operations") || file.get("operations").getAsLong() < 0)
            throw new IOException("Invalid proto Trace count");
        long operations = file.get("operations").getAsLong();
        String logical = file.get("logical_sha256").getAsString();
        if (!identity(logical, operations).equals(manifest.get(name + "_trace_sha256").getAsString()))
            throw new IOException("Proto Trace inventory identity mismatch: " + name);
        String physical = file.get("sha256").getAsString();
        return consumer -> {
            long actual = read(directory.resolve(name + ".pb.zst"), physical, logical, consumer);
            if (actual != operations) throw new IOException("Proto Trace file operation count mismatch");
            return actual;
        };
    }

    /** Binds the decoded messages and their semantic count without re-encoding requests. */
    public static String identity(String logicalSha256, long operations) {
        var digest = org.sibench.config.Digests.digest();
        digest.update(HexFormat.of().parseHex(logicalSha256));
        digest.update(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(operations).array());
        return HexFormat.of().formatHex(digest.digest());
    }

    public static Map<String, Object> file(Writer writer) {
        return Map.of("sha256", writer.sha256(), "logical_sha256", writer.logicalSha256(),
                "operations", writer.count(), "bytes", writer.bytesWritten(), "statistics", writer.statistics());
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
        private final CommittedOperations projection;
        private final List<Operation> group = new ArrayList<>();
        private int records;
        private long transaction;
        private String sha256;
        private boolean closed;
        private volatile long bytesWritten = Integer.BYTES;
        private final boolean secondaryIndexes;
        private final RequestBudget budget;
        private long primaryEntries, effectiveRequests, uncompressedBytes;
        private final Map<String, Long> requestTypes = new LinkedHashMap<>();
        private final Map<String, Long> controls = new LinkedHashMap<>();
        private final Map<String, Long> writeOrigins = new LinkedHashMap<>();
        private final Map<String,Long> primaryRequests=new LinkedHashMap<>(),secondaryRequests=new LinkedHashMap<>();
        private final java.util.function.Consumer<TraceRecord> observer;

        /** A baseline stream without a workload request budget. */
        public Writer(Path file, boolean secondaryIndexes) throws IOException {
            this(file, secondaryIndexes, null);
        }

        Writer(Path file, boolean secondaryIndexes, RequestBudget budget) throws IOException {
            this(file,secondaryIndexes,budget,null);
        }
        Writer(Path file, boolean secondaryIndexes, RequestBudget budget,
                java.util.function.Consumer<TraceRecord> observer) throws IOException {
            this.observer=observer;
            this.budget = budget;
            this.secondaryIndexes = secondaryIndexes;
            projection = new CommittedOperations(this::appendExported);
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

        void expectAbort() throws IOException { projection.expectAbort(); }

        private void appendExported(Operation operation) throws IOException {
            if (operation.grouped()) {
                if (operation.expansionPosition() != group.size()
                        || !group.isEmpty() && (operation.expansionSize() != group.get(0).expansionSize()
                                || operation.sourceEvent() != group.get(0).sourceEvent()))
                    throw new IOException("Invalid proto search group order");
                group.add(operation);
                if (group.size() == operation.expansionSize()) {
                    if (secondaryIndexes || group.stream().allMatch(member -> member.index() == 0)) {
                        var data = TraceOperation.newBuilder().setReadGroup(requests.group(group))
                                .setMetadata(metadata(group.get(0)));
                        write(TraceRecord.newBuilder().setOperation(data).build());
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
            var record = TraceRecord.newBuilder();
            if (operation.kind() == IndexOperationTrace.TRANSACTION_BEGIN) {
                if (transaction != 0) throw new IOException("Nested proto transaction");
                if (operation.transactionId() != operation.version()) throw new IOException("Source identity must equal its timestamp");
                transaction = operation.version();
                record.setTransactionBegin(operation.version());
            } else if (operation.kind() == IndexOperationTrace.TRANSACTION_COMMIT) {
                if (transaction != operation.transactionId()) throw new IOException("Unmatched proto commit");
                record.setTransactionCommit(operation.version());
                transaction = 0;
            } else {
                var request = requests.request(operation);
                var data = TraceOperation.newBuilder().setMetadata(metadata(operation))
                        .addAllLocalLocators(TraceRequests.localLocators(operation));
                if (request instanceof GetRequest get) data.setGet(get);
                else if (request instanceof ScanRequest scan) data.setScan(scan);
                else if (request instanceof PutRequest put) data.setPut(put);
                else if (request instanceof DeleteRequest delete) data.setDelete(delete);
                else throw new IOException("Unsupported persisted operation");
                record.setOperation(data);
            }
            write(record.build());
        }

        private void write(TraceRecord record) throws IOException {
            if (record.getSerializedSize() > MAX_BYTES - 5) throw new IOException("Proto Trace request exceeds block limit");
            if (records != 0 && (records == IndexOperationTrace.BLOCK_RECORDS
                    || block.size() + record.getSerializedSize() + 5 > BLOCK_BYTES)) flush();
            record.writeDelimitedTo(messages);
            records++;
            count += operationCount(record);
            if (record.hasTransactionBegin() || record.hasTransactionCommit()) {
                controls.merge(record.hasTransactionBegin() ? "begin" : "commit", 1L, Long::sum);
                return;
            }
            var request = record.getOperation();
            int operations = operationCount(record);
            effectiveRequests = Math.addExact(effectiveRequests, operations);
            if (budget != null) budget.record(operations);
            String type;
            if (request.hasPut() || request.hasDelete()) {
                ByteString key = request.hasPut() ? request.getPut().getKey() : request.getDelete().getKey();
                if (IndexEntry.decode(key).index() == 0) primaryEntries++;
                type = request.hasPut() ? "put" : "delete";
                int flags = request.getMetadata().getWriteOrigin();
                if ((flags & IndexOperationTrace.BUSINESS_INSERT) != 0) writeOrigins.merge("business_insert", 1L, Long::sum);
                if ((flags & IndexOperationTrace.BUSINESS_UPDATE) != 0) writeOrigins.merge("business_update", 1L, Long::sum);
            } else if (request.hasReadGroup()) {
                for (ReadRequest member : request.getReadGroup().getRequestsList())
                    if (queryKey(member).asReadOnlyByteBuffer().getInt(4) == 0) primaryEntries++;
                type = request.getReadGroup().getCombineMode().name().toLowerCase(Locale.ROOT) + "_query";
            } else {
                ByteString key = request.hasGet() ? request.getGet().getKey() : request.getScan().getStartKey();
                if (key.asReadOnlyByteBuffer().getInt(4) == 0) primaryEntries++;
                type = request.hasGet() ? "get" : "scan";
            }
            requestTypes.merge(type, 1L, Long::sum);
            if(request.hasReadGroup()) {
                for(var member:request.getReadGroup().getRequestsList())
                    countRequest(queryKey(member),member.hasGet()?"get":"scan");
            } else {
                ByteString key=request.hasGet()?request.getGet().getKey():request.hasScan()?request.getScan().getStartKey():
                        request.hasPut()?request.getPut().getKey():request.getDelete().getKey();
                countRequest(key,type);
            }
            if(observer!=null)observer.accept(record);
        }

        private void countRequest(ByteString key,String type) {
            (key.asReadOnlyByteBuffer().getInt(4)==0?primaryRequests:secondaryRequests).merge(type,1L,Long::sum);
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
        public long lastCommittedVersion() { return projection.lastCommittedVersion(); }
        long bytesWritten() { return bytesWritten; }
        public Map<String, Object> statistics() {
            return Map.of("effective_requests", effectiveRequests,
                    "primary_entries", primaryEntries, "secondary_entries", effectiveRequests - primaryEntries,
                    "request_types", Map.copyOf(requestTypes), "transaction_markers", Map.copyOf(controls),
                    "index_operations",Map.of("primary",Map.copyOf(primaryRequests),"secondary",Map.copyOf(secondaryRequests)),
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
            try (output) {
                projection.finish();
                if (!group.isEmpty() || transaction != 0) throw new IOException("Incomplete proto transaction or search group");
                flush();
                for (int i = 0; i < 4; i++) output.writeInt(0);
                bytesWritten += 4L * Integer.BYTES;
            }
            sha256 = HexFormat.of().formatHex(physical.digest());
            logicalSha256 = HexFormat.of().formatHex(logical.digest());
        }
    }

    private static TraceMetadata metadata(Operation operation) {
        var metadata = TraceMetadata.newBuilder().setSourceEvent(operation.sourceEvent())
                .setWorkUnitId(operation.workUnitId()).setMutationOrdinal(operation.mutationOrdinal());
        if (operation.kind() == IndexOperationTrace.SEARCH) metadata.setExpectedCount(operation.expectedCount());
        else {
            metadata.setWriteOrigin(operation.flags());
            if (operation.kind() == IndexOperationTrace.INSERT)
                metadata.setBaselinePosition(operation.expansionPosition()).setBaselineSize(operation.expansionSize());
            if (operation.kind() != IndexOperationTrace.LOGICAL_DELETE && (operation.file() != 0 || operation.position() != 0))
                metadata.setLocator(IndexEntry.locator(operation.file(), operation.position()));
        }
        return metadata.build();
    }

    private static int operationCount(TraceRecord record) {
        var operation = record.getOperation();
        return operation.hasReadGroup() ? operation.getReadGroup().getRequestsCount() : 1;
    }

    public static long read(Path file, String expectedPhysical, String expectedLogical,
            IndexOperationTrace.Consumer consumer) throws IOException {
        var physical = org.sibench.config.Digests.digest();
        var decoder = new Decoder();
        var logical = org.sibench.config.Digests.digest();
        long count = 0;
        // Amortize NFS read/attribute checks while retaining bounded, streaming validation.
        try (var input = new DataInputStream(new BufferedInputStream(new DigestInputStream(
                    Files.newInputStream(file), physical), BLOCK_BYTES))) {
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
                    var views = new ArrayList<Operation>(operationCount(record));
                    decoder.accept(record, views::add);
                    for (Operation operation : views) IndexOperationTrace.validateShape(operation);
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

    private static ByteString queryKey(ReadRequest read) {
        return read.hasGet() ? read.getGet().getKey() : read.getScan().getStartKey();
    }

    private static final class Decoder {
        long transaction, operations;

        void accept(TraceRecord record, IndexOperationTrace.Consumer consumer) throws IOException {
            known(record);
            if (record.hasTransactionBegin() || record.hasTransactionCommit()) {
                if (record.hasTransactionBegin()) {
                    long begin = record.getTransactionBegin();
                    if (transaction != 0 || begin <= 0) throw new IOException("Invalid source transaction begin");
                    transaction = begin; operations = 0;
                    consumer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, transaction, transaction, 0));
                } else {
                    if (transaction == 0 || record.getTransactionCommit() != transaction)
                        throw new IOException("Invalid source transaction commit version");
                    consumer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_COMMIT,
                            transaction, transaction, 0));
                    transaction = 0;
                }
                return;
            }
            if (!record.hasOperation()) throw new IOException("Missing Trace event");
            var request = record.getOperation(); known(request);
            if (request.getRequestCase() == TraceOperation.RequestCase.REQUEST_NOT_SET || !request.hasMetadata())
                throw new IOException("Missing operation request or source metadata");
            var p = request.getMetadata(); known(p);
            if (p.getSourceEvent() < 0 || p.getWorkUnitId() < 0 || p.getMutationOrdinal() < 0 || p.getExpectedCount() < 0
                    || (p.getWriteOrigin() & ~3) != 0) throw new IOException("Invalid source metadata");
            int count = operationCount(record);
            if (transaction != 0) {
                operations += count;
                if (operations > MAX_TRANSACTION_OPERATIONS) throw new IOException("Source group exceeds operation buffer limit");
            }
            if (request.hasReadGroup()) {
                if (request.getLocalLocatorsCount() != 0) throw new IOException("Group locators belong to individual members");
                var group = request.getReadGroup(); known(group);
                if (count < 2 || group.getCombineMode() != ReadGroup.CombineMode.INTERSECTION && group.getCombineMode() != ReadGroup.CombineMode.UNION)
                    throw new IOException("Invalid query group");
                int flag = group.getCombineMode() == ReadGroup.CombineMode.INTERSECTION ? IndexOperationTrace.INTERSECT_GROUP : IndexOperationTrace.UNION_GROUP;
                int table = 0; long readTs = -1;
                for (int i = 0; i < count; i++) {
                    var member = group.getRequests(i); known(member);
                    var operation = query(TraceRequests.read(member), transaction, p, flag, i, count, member.getLocalLocatorsList());
                    if (i > 0 && (table != operation.table() || readTs != operation.readTimestamp()))
                        throw new IOException("Query group spans tables or read points");
                    table = operation.table(); readTs = operation.readTimestamp();
                    consumer.accept(operation);
                }
            } else if (request.hasGet() || request.hasScan()) {
                consumer.accept(query(request.hasGet() ? request.getGet() : request.getScan(), transaction, p, 0, 0, 0, request.getLocalLocatorsList()));
            } else if (request.hasPut() || request.hasDelete()) {
                if (request.getLocalLocatorsCount() != 0) throw new IOException("Write carries private read locators");
                consumer.accept(write(request, transaction, p));
            }
            else throw new IOException("Unsupported persisted operation");
        }
    }

    private static Operation write(TraceOperation request, long transaction, TraceMetadata p) throws IOException {
        boolean put = request.hasPut();
        var mutation = put ? Mutation.newBuilder().setPut(request.getPut()).build()
                : Mutation.newBuilder().setDelete(request.getDelete()).build();
        try { IndexExecution.validateMutation(mutation); }
        catch (IllegalArgumentException error) { throw new IOException(error); }
        long ts = put ? request.getPut().getTs() : request.getDelete().getTs();
        boolean baseline = transaction == 0 && ts == 0 && put;
        if (p.getExpectedCount() != 0
                || !baseline && (p.getBaselinePosition() != 0 || p.getBaselineSize() != 0))
            throw new IOException("Invalid write metadata");
        return entry(mutation, p.getLocator(), p.getSourceEvent(),
                baseline ? IndexOperationTrace.INSERT : put ? IndexOperationTrace.UPSERT : IndexOperationTrace.LOGICAL_DELETE,
                p.getWriteOrigin(), transaction, p.getMutationOrdinal(), p.getWorkUnitId(),
                baseline ? p.getBaselinePosition() : 0, baseline ? p.getBaselineSize() : 1);
    }

    /** The index entry a validated Put or Delete names, with its RowID and optional source locator. */
    private static Operation entry(Mutation mutation, ByteString locator, long sourceEvent, int kind, int flags,
            long transaction, long ordinal, long workUnit, int position, int size) throws IOException {
        boolean put = mutation.hasPut();
        if (!put && !locator.isEmpty()) throw new IOException("Invalid write metadata");
        var key = IndexEntry.decode(put ? mutation.getPut().getKey() : mutation.getDelete().getKey());
        long ts = put ? mutation.getPut().getTs() : mutation.getDelete().getTs();
        long rowId = 0, file = 0, offset = 0;
        if (put) {
            if (mutation.getPut().getValue().size() != 8) throw new IOException("Trace Put must carry an encoded RowID");
            rowId = mutation.getPut().getValue().asReadOnlyByteBuffer().getLong();
            if (rowId < 0) throw new IOException("Negative source RowID");
            if (!locator.isEmpty()) {
                if (locator.size() != 16) throw new IOException("Invalid source locator");
                var bytes = locator.asReadOnlyByteBuffer(); file = bytes.getLong(); offset = bytes.getLong();
            }
        }
        return new Operation(sourceEvent, kind, key.table(), key.index(), flags, key.primaryKey(),
                key.indexedValue(), ts, file, offset, rowId, 0, transaction, ordinal, -1, workUnit,
                position, size, null, null, 0, java.util.List.of());
    }

    private static Operation query(com.google.protobuf.Message read, long transaction, TraceMetadata p, int group, int member, int count,
            List<ByteString> locators) throws IOException {
        if (p.getWriteOrigin() != 0 || p.getBaselinePosition() != 0 || p.getBaselineSize() != 0 || !p.getLocator().isEmpty())
            throw new IOException("Query carries write metadata");
        int table, index, flags = IndexOperationTrace.SNAPSHOT_SEARCH | group;
        long ts, limit = 0;
        IndexKey lower, upper, pinned = null;
        try {
            if (read instanceof GetRequest get) {
                IndexExecution.validateGet(get);
                var key = IndexEntry.decode(get.getKey()); table = key.table(); index = key.index(); ts = get.getReadTs();
                lower = upper = index == 0 ? key.primaryKey() : key.indexedValue();
                if (index != 0) pinned = key.primaryKey();
                flags |= IndexOperationTrace.LOWER_INCLUSIVE | IndexOperationTrace.UPPER_INCLUSIVE;
            } else if (read instanceof ScanRequest scan) {
                IndexExecution.validateScan(scan);
                if (!scan.hasStartKey() || !scan.hasEndKey() || scan.getStartKey().size() < 8)
                    throw new IOException("Trace Scan must be bounded by its index namespace");
                var bytes = scan.getStartKey().asReadOnlyByteBuffer(); table = bytes.getInt(); index = bytes.getInt();
                var start = IndexEntry.boundary(scan.getStartKey(), table, index, false);
                var end = IndexEntry.boundary(scan.getEndKey(), table, index, true);
                lower = start.key(); upper = end.key(); ts = scan.getReadTs(); limit = scan.getLimit();
                if (lower != null && !start.after()) flags |= IndexOperationTrace.LOWER_INCLUSIVE;
                if (upper != null && end.after()) flags |= IndexOperationTrace.UPPER_INCLUSIVE;
                if (scan.getReverse()) flags |= IndexOperationTrace.DESCENDING;
            } else throw new IOException("Missing read operation");
        } catch (IllegalArgumentException error) { throw new IOException(error); }
        var context = read instanceof GetRequest get ? get.getLocalWritesList() : ((ScanRequest) read).getLocalWritesList();
        if (locators.size() != context.size()) throw new IOException("Private locator count mismatch");
        // validateGet/validateScan already bound each private write to the encoded read key or range,
        // with timestamp zero and unique ordered keys.
        var local = new ArrayList<Operation>(context.size());
        for (int i = 0; i < context.size(); i++) {
            var mutation = context.get(i);
            var decoded = entry(mutation, locators.get(i), 0,
                    mutation.hasPut() ? IndexOperationTrace.UPSERT : IndexOperationTrace.LOGICAL_DELETE, 0, 0, 0, 0, 0, 1);
            if (decoded.file() < 0 || decoded.position() < 0) throw new IOException("Invalid private write locator");
            local.add(decoded);
        }
        return new Operation(p.getSourceEvent(), IndexOperationTrace.SEARCH, table, index, flags, pinned, null,
                0, 0, 0, 0, p.getExpectedCount(), transaction, p.getMutationOrdinal(), ts, p.getWorkUnitId(), member, count,
                lower, upper, limit, local);
    }

    private static void known(com.google.protobuf.Message message) throws IOException {
        if (!message.getUnknownFields().asMap().isEmpty()) throw new IOException("Unsupported proto fields: " + message.getDescriptorForType().getName());
    }
}
