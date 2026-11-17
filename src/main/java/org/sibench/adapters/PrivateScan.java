package org.sibench.adapters;

import com.google.protobuf.ByteString;
import org.sibench.adapters.proto.Mutation;
import org.sibench.adapters.proto.ScanRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/** Merges validated private writes with ordered visible entries before applying the result limit. */
final class PrivateScan {
    private final ScanRequest scan;
    private final IndexBackend.ValueSink sink;
    private final Function<ByteString, byte[]> encode;
    private final List<Mutation> writes;
    private int position;
    private long count;
    private byte[] key;
    private ByteString value;

    PrivateScan(ScanRequest scan, IndexBackend.ValueSink sink, Function<ByteString, byte[]> encode) {
        this.scan = scan; this.sink = sink; this.encode = encode;
        writes = scan.getLocalWritesList();
        position = scan.getReverse() ? writes.size() - 1 : 0;
        select();
    }

    private void select() {
        if (position < 0 || position >= writes.size()) { key = null; return; }
        var write = writes.get(position);
        key = encode.apply(write.hasPut() ? write.getPut().getKey() : write.getDelete().getKey());
        value = write.hasPut() ? write.getPut().getValue() : null;
    }
    private void advance() { position += scan.getReverse() ? -1 : 1; select(); }
    private boolean full() { return scan.getLimit() != 0 && count >= scan.getLimit(); }
    private void emit(ByteString value) throws IOException {
        if (value != null && !full()) { sink.accept(value); count++; }
    }
    boolean accept(byte[] baseKey, ByteString baseValue) throws IOException {
        while (key != null && !full()) {
            int order = Arrays.compareUnsigned(key, baseKey) * (scan.getReverse() ? -1 : 1);
            if (order > 0) break;
            emit(value); advance();
            if (order == 0) return !full();
        }
        emit(baseValue);
        return !full();
    }
    void finish() throws IOException {
        while (key != null && !full()) { emit(value); advance(); }
    }
}
