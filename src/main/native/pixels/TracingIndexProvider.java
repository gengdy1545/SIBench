package org.sibench.pixels;

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndexProvider;
import io.pixelsdb.pixels.index.rocksdb.RocksDBIndexProvider;
import java.io.IOException;
import javax.annotation.Nonnull;

/** Listed before the upstream RocksDB provider so ServiceLoader selects it for the rocksdb scheme. */
public final class TracingIndexProvider implements SinglePointIndexProvider {
    private final RocksDBIndexProvider rocksdb = new RocksDBIndexProvider();

    @Override
    public SinglePointIndex createInstance(long tableId, long indexId, @Nonnull SinglePointIndex.Scheme scheme,
            boolean unique, IndexOption indexOption) throws SinglePointIndexException {
        try {
            return new TracingIndex(rocksdb.createInstance(tableId, indexId, scheme, unique, indexOption), IndexTraceChannel.get());
        } catch (IOException error) {
            throw new SinglePointIndexException("SIBench index trace unavailable", error);
        }
    }

    @Override
    public boolean compatibleWith(@Nonnull SinglePointIndex.Scheme scheme) {
        return System.getProperty("sibench.trace.endpoint") != null && rocksdb.compatibleWith(scheme);
    }
}
