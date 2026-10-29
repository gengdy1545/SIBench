package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.adapters.proto.Result;
import org.sibench.trace.IndexKey;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * A strong per-query result identity: query inputs, result order and every result field. The
 * digest is computed independently from the reference model and from a service response, so
 * swapping two queries, dropping a row, or returning the same count with another key/version
 * cannot pass as equal.
 */
public final class QueryDigest {
    private QueryDigest() {}

    /** The reference replay's per-query sink. */
    @FunctionalInterface
    public interface Sink {
        void accept(List<Operation> members, List<LookupResult> results) throws IOException;
    }

    public static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }

    public static String of(List<Operation> members, List<LookupResult> results, boolean rowIds) {
        MessageDigest digest = digest();
        query(digest, members);
        putInt(digest, results.size());
        for (LookupResult result : results) {
            putBytes(digest, result.key().bytes());
            putLong(digest, result.commitVersion());
            if (rowIds) putLong(digest, result.value());
            else {
                putLong(digest, result.value());
                putLong(digest, result.position());
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** Result-only identity, for comparing the visible outcome of two read points. */
    public static String results(List<LookupResult> results, boolean rowIds) {
        MessageDigest digest = digest();
        putInt(digest, results.size());
        for (LookupResult result : results) {
            putBytes(digest, result.key().bytes());
            putLong(digest, result.commitVersion());
            if (rowIds) putLong(digest, result.value());
            else {
                putLong(digest, result.value());
                putLong(digest, result.position());
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    public static String ofResponse(
            List<Operation> members, List<Result> results, boolean rowIds) {
        var digest = new Incremental(members, results.size(), rowIds);
        for (Result result : results) digest.add(result);
        return digest.hex();
    }

    /**
     * A service response's digest computed as its results arrive. The count is the one the
     * digest covers; it equals {@link #ofResponse} exactly when that many results were added.
     */
    public static final class Incremental {
        private final MessageDigest digest = digest();
        private final boolean rowIds;

        public Incremental(List<Operation> members, long count, boolean rowIds) {
            this.rowIds = rowIds;
            query(digest, members);
            putInt(digest, Math.toIntExact(count));
        }

        public void add(Result result) {
            putBytes(digest, result.getPrimaryKey().toByteArray());
            putLong(digest, result.getVersion());
            if (rowIds) putLong(digest, result.getValue().getRowId());
            else {
                putLong(digest, result.getValue().getFile());
                putLong(digest, result.getValue().getPosition());
            }
        }

        public String hex() {
            return HexFormat.of().formatHex(digest.digest());
        }
    }

    private static void query(MessageDigest digest, List<Operation> members) {
        putInt(digest, members.size());
        for (Operation member : members) {
            putInt(digest, member.table());
            putInt(digest, member.index());
            putInt(digest, member.flags());
            putLong(digest, member.version());
            putLong(digest, member.transactionId());
            putLong(digest, member.mutationOrdinal());
            putLong(digest, member.readTimestamp());
            putLong(digest, member.expectedCount());
            putLong(digest, member.sourceEvent());
            putLong(digest, member.workUnitId());
            putInt(digest, member.expansionPosition());
            putInt(digest, member.expansionSize());
            putLong(digest, member.limit());
            putKey(digest, member.key());
            putKey(digest, member.lower());
            putKey(digest, member.upper());
        }
    }

    private static void putKey(MessageDigest digest, IndexKey key) {
        if (key == null) putInt(digest, -1);
        else putBytes(digest, key.bytes());
    }

    private static void putBytes(MessageDigest digest, byte[] bytes) {
        putInt(digest, bytes.length);
        digest.update(bytes);
    }

    private static void putInt(MessageDigest digest, int value) {
        digest.update((byte) (value >>> 24));
        digest.update((byte) (value >>> 16));
        digest.update((byte) (value >>> 8));
        digest.update((byte) value);
    }

    private static void putLong(MessageDigest digest, long value) {
        putInt(digest, (int) (value >>> 32));
        putInt(digest, (int) value);
    }

    /**
     * Writes one {@code sourceEvent, workUnitId, digest} line per query in trace order. Reading
     * it back in the same order binds each expected digest to the request the driver is about to
     * send, without retaining all queries in memory.
     */
    public static final class File implements Sink, Closeable {
        private final Path path, partial;
        private final BufferedWriter writer;
        private final boolean rowIds;
        private long count;
        private boolean closed;

        public File(Path path, boolean rowIds) throws IOException {
            this(path, rowIds, null);
        }

        /** {@code binding}: trace/baseline/version/mode/coverage identity written as a header. */
        public File(Path path, boolean rowIds, String binding) throws IOException {
            this.path = path;
            this.rowIds = rowIds;
            this.partial = path.resolveSibling(path.getFileName() + ".partial");
            if (Files.exists(path) || Files.exists(partial))
                throw new IOException("Query digest file already exists: " + path);
            writer =
                    Files.newBufferedWriter(
                            partial,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE);
            if (binding != null) {
                writer.write("# " + binding);
                writer.newLine();
            }
        }

        @Override
        public void accept(List<Operation> members, List<LookupResult> results)
                throws IOException {
            Operation first = members.get(0);
            writer.write(
                    first.sourceEvent()
                            + "\t"
                            + first.workUnitId()
                            + "\t"
                            + QueryDigest.of(members, results, rowIds));
            writer.newLine();
            count++;
        }

        public long count() {
            return count;
        }

        public Path path() {
            return path;
        }

        @Override
        public void close() throws IOException {
            if (closed) return;
            closed = true;
            writer.close();
            Files.move(
                    partial,
                    path,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        }
    }
}