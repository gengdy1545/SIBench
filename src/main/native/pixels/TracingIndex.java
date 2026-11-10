package org.sibench.pixels;

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import java.io.IOException;
import java.util.List;

/**
 * Records every logical primary-index call Pixels makes, after the delegate returns and before the
 * call returns. Only the calls of Retina ingestion and LOAD are modelled; any other mutation is
 * reported as unsupported so generation stops instead of exporting an incomplete Trace.
 */
final class TracingIndex implements SinglePointIndex {
    private final SinglePointIndex delegate;
    private final IndexTraceChannel channel;

    TracingIndex(SinglePointIndex delegate, IndexTraceChannel channel) {
        this.delegate = delegate;
        this.channel = channel;
    }

    @Override public long getTableId() { return delegate.getTableId(); }
    @Override public long getIndexId() { return delegate.getIndexId(); }
    @Override public boolean isUnique() { return delegate.isUnique(); }

    @Override
    public long getUniqueRowId(IndexProto.IndexKey key) throws SinglePointIndexException {
        long rowId = delegate.getUniqueRowId(key);
        record(IndexTraceChannel.LOOKUP, key, rowId, null);
        return rowId;
    }

    @Override
    public boolean putPrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException {
        boolean success = delegate.putPrimaryEntries(entries);
        if (!success) return false;
        for (var entry : entries) record(IndexTraceChannel.PUT, entry.getIndexKey(), entry.getRowId(), entry);
        return true;
    }

    @Override
    public List<Long> updatePrimaryEntries(List<IndexProto.PrimaryIndexEntry> entries) throws SinglePointIndexException {
        List<Long> previous = delegate.updatePrimaryEntries(entries);
        if (previous.size() != entries.size()) unsupported("updatePrimaryEntries with absent keys");
        for (int i = 0; i < entries.size(); i++)
            record(IndexTraceChannel.UPDATE, entries.get(i).getIndexKey(), entries.get(i).getRowId(), entries.get(i));
        return previous;
    }

    @Override
    public List<Long> deleteEntries(List<IndexProto.IndexKey> keys) throws SinglePointIndexException {
        List<Long> deleted = delegate.deleteEntries(keys);
        // Retina deletes only keys it has just resolved; a size mismatch would lose the key/row pairing.
        if (deleted.size() != keys.size()) unsupported("deleteEntries with absent keys");
        for (int i = 0; i < keys.size(); i++) record(IndexTraceChannel.DELETE, keys.get(i), deleted.get(i), null);
        return deleted;
    }

    @Override
    public List<Long> getRowIds(IndexProto.IndexKey key) throws SinglePointIndexException {
        unsupported("getRowIds");
        return delegate.getRowIds(key);
    }

    @Override
    public boolean putEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException {
        unsupported("putEntry");
        return delegate.putEntry(key, rowId);
    }

    @Override
    public boolean putSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException {
        unsupported("putSecondaryEntries");
        return delegate.putSecondaryEntries(entries);
    }

    @Override
    public long updatePrimaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException {
        unsupported("updatePrimaryEntry");
        return delegate.updatePrimaryEntry(key, rowId);
    }

    @Override
    public List<Long> updateSecondaryEntry(IndexProto.IndexKey key, long rowId) throws SinglePointIndexException {
        unsupported("updateSecondaryEntry");
        return delegate.updateSecondaryEntry(key, rowId);
    }

    @Override
    public List<Long> updateSecondaryEntries(List<IndexProto.SecondaryIndexEntry> entries) throws SinglePointIndexException {
        unsupported("updateSecondaryEntries");
        return delegate.updateSecondaryEntries(entries);
    }

    @Override
    public long deleteUniqueEntry(IndexProto.IndexKey key) throws SinglePointIndexException {
        unsupported("deleteUniqueEntry");
        return delegate.deleteUniqueEntry(key);
    }

    @Override
    public List<Long> deleteEntry(IndexProto.IndexKey key) throws SinglePointIndexException {
        unsupported("deleteEntry");
        return delegate.deleteEntry(key);
    }

    @Override
    public List<Long> purgeEntries(List<IndexProto.IndexKey> keys) throws SinglePointIndexException {
        unsupported("purgeEntries");
        return delegate.purgeEntries(keys);
    }

    @Override
    @Deprecated
    public void close() throws IOException { delegate.close(); }

    @Override
    public boolean closeAndRemove() throws SinglePointIndexException { return delegate.closeAndRemove(); }

    private void record(int kind, IndexProto.IndexKey key, long rowId, IndexProto.PrimaryIndexEntry entry)
            throws SinglePointIndexException {
        var location = entry == null ? null : entry.getRowLocation();
        append(IndexTraceChannel.fact(kind, key.getTableId(), key.getIndexId(), key.getTimestamp(), key.getKey().toByteArray(),
                rowId, location == null ? 0 : location.getFileId(), location == null ? 0 : location.getRgId(),
                location == null ? 0 : location.getRgRowOffset()));
    }

    private void unsupported(String method) throws SinglePointIndexException {
        append(IndexTraceChannel.unsupported(method));
    }

    private void append(byte[] fact) throws SinglePointIndexException {
        try {
            channel.append(fact);
        } catch (IOException error) {
            throw new SinglePointIndexException("SIBench index trace unavailable", error);
        }
    }
}
