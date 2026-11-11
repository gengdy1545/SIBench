package org.sibench.trace;

import com.google.protobuf.ByteString;
import org.sibench.state.ByteKeys;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** SIBench's key/value encoding; the index service treats these bytes as opaque. */
public record IndexEntry(int table, int index, IndexKey indexedValue, IndexKey primaryKey) {
    public static byte[] prefix(int table, int index) {
        if (table < 1 || table > 65535 || index < 0 || index > 1024)
            throw new IllegalArgumentException("Invalid index namespace");
        return ByteBuffer.allocate(8).putInt(table).putInt(index).array();
    }

    public ByteString bytes() {
        if (primaryKey == null || primaryKey.isNull() || (index == 0) != (indexedValue == null))
            throw new IllegalArgumentException("Invalid index entry");
        byte[] key = prefix(table, index);
        if (indexedValue != null) key = ByteKeys.concat(key, ByteKeys.encode(indexedValue.bytes()));
        return ByteString.copyFrom(ByteKeys.concat(key, ByteKeys.encode(primaryKey.bytes())));
    }

    public static IndexEntry decode(ByteString bytes) throws IOException {
        ByteBuffer in = bytes.asReadOnlyByteBuffer();
        if (in.remaining() < 8) throw new IOException("Missing index namespace");
        int table = in.getInt(), index = in.getInt();
        try { prefix(table, index); }
        catch (IllegalArgumentException error) { throw new IOException(error); }
        IndexKey value = index == 0 ? null : IndexKey.decode(ByteKeys.decode(in));
        IndexKey pk = IndexKey.decode(ByteKeys.decode(in));
        if (in.hasRemaining() || pk.isNull()) throw new IOException("Invalid complete index key");
        return new IndexEntry(table, index, value, pk);
    }

    public static ByteString bound(int table, int index, IndexKey component, boolean after) {
        byte[] prefix = prefix(table, index);
        if (component == null) return ByteString.copyFrom(after ? ByteKeys.successor(prefix) : prefix);
        byte[] value = ByteKeys.concat(prefix, ByteKeys.encode(component.bytes()));
        return ByteString.copyFrom(after ? ByteKeys.successor(value) : value);
    }

    public record Boundary(IndexKey key, boolean after) {}

    /** Reconstructs source bounds from the canonical half-open namespace range. */
    public static Boundary boundary(ByteString bytes, int table, int index, boolean upper) throws IOException {
        byte[] raw = bytes.toByteArray(), prefix = prefix(table, index);
        if (Arrays.equals(raw, upper ? ByteKeys.successor(prefix) : prefix)) return new Boundary(null, false);
        if (raw.length <= 8 || !Arrays.equals(raw, 0, 8, prefix, 0, 8))
            throw new IOException("Scan crosses its index namespace");
        boolean after = raw[raw.length - 1] == 1;
        if (after) raw[raw.length - 1] = 0;
        ByteBuffer in = ByteBuffer.wrap(raw, 8, raw.length - 8);
        IndexKey key = IndexKey.decode(ByteKeys.decode(in));
        if (in.hasRemaining()) throw new IOException("Invalid index boundary");
        return new Boundary(key, after);
    }

    public static ByteString rowId(long rowId) {
        return ByteString.copyFrom(ByteBuffer.allocate(8).putLong(rowId).array());
    }
    public static ByteString locator(long file, long position) {
        return ByteString.copyFrom(ByteBuffer.allocate(16).putLong(file).putLong(position).array());
    }
}
