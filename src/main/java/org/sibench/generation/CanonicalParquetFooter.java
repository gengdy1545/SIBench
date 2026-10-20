package org.sibench.generation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.Util;

/** Canonicalizes unordered footer collections before a new file is published. */
public final class CanonicalParquetFooter {
    private CanonicalParquetFooter() {}

    public static void normalize(Path path) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw")) {
            long size = file.length();
            if (size < 12) throw new IOException("Truncated Parquet file");
            file.seek(size - 8);
            int length = Integer.reverseBytes(file.readInt());
            if (file.readInt() != 0x50415231 || length < 0 || length > size - 12) {
                throw new IOException("Invalid Parquet footer");
            }
            long offset = size - 8 - length;
            byte[] bytes = new byte[length];
            file.seek(offset); file.readFully(bytes);
            var metadata = Util.readFileMetaData(new ByteArrayInputStream(bytes));
            if (metadata.isSetKey_value_metadata()) metadata.getKey_value_metadata().sort(Comparator.comparing(KeyValue::getKey));
            for (var group : metadata.getRow_groups()) {
                for (var column : group.getColumns()) {
                    if (!column.isSetMeta_data()) throw new IOException("Encrypted columns are not supported");
                    var meta = column.getMeta_data();
                    meta.getEncodings().sort(Comparator.comparingInt(e -> e.getValue()));
                    if (meta.isSetEncoding_stats()) meta.getEncoding_stats().sort(
                            Comparator.comparingInt((PageEncodingStats s) -> s.getPage_type().getValue())
                                    .thenComparingInt(s -> s.getEncoding().getValue()));
                    if (meta.isSetKey_value_metadata()) meta.getKey_value_metadata().sort(Comparator.comparing(KeyValue::getKey));
                }
            }
            ByteArrayOutputStream encoded = new ByteArrayOutputStream();
            Util.writeFileMetaData(metadata, encoded);
            file.seek(offset); file.write(encoded.toByteArray());
            file.writeInt(Integer.reverseBytes(encoded.size())); file.writeInt(0x50415231);
            file.setLength(file.getFilePointer());
        }
    }
}
