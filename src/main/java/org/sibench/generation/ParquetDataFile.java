package org.sibench.generation;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.parquet.Version;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.internal.column.columnindex.*;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.*;

/** Explicit group boundaries over parquet-java encoders; no reflective writer internals. */
public final class ParquetDataFile implements AutoCloseable {
    private final GenerationConfig config;
    private final MessageType schema;
    private final ParquetProperties properties;
    private final CodecFactory codecs;
    private final ParquetFileWriter file;
    private final Map<String, String> metadata;
    private final Path path;
    private boolean closed;
    private ColumnChunkPageWriteStore pages;
    private ColumnWriteStore columns;
    private RecordConsumer consumer;
    private GroupWriter writer;
    private long groupRows;
    private int ordinal;
    private boolean full;
    private long sizeBytes = -1;
    private long completeBytes;
    private final List<BlockMetaData> blocks = new ArrayList<>();
    private final List<List<ColumnIndex>> columnIndexes = new ArrayList<>();
    private final List<List<OffsetIndex>> offsetIndexes = new ArrayList<>();

    public ParquetDataFile(
            Path path, TableData table, GenerationConfig config, Map<String, String> metadata)
            throws IOException {
        this.config = config;
        this.path = path;
        this.schema = table.schema();
        this.metadata = new java.util.TreeMap<>(metadata);
        Configuration hadoop = new Configuration(false);
        hadoop.setInt("parquet.compression.codec.zstd.level", config.codecLevel());
        hadoop.setInt("parquet.compression.codec.zstd.workers", 0);
        properties =
                ParquetProperties.builder()
                        .withPageSize(config.pageBytes())
                        .withDictionaryPageSize(config.dictionaryBytes())
                        .withDictionaryEncoding(config.dictionary())
                        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                        .withPageWriteChecksumEnabled(true)
                        .build();
        codecs = new CodecFactory(hadoop, config.pageBytes());
        file =
                new ParquetFileWriter(
                        new LocalOutputFile(path),
                        schema,
                        ParquetFileWriter.Mode.CREATE,
                        config.groupBytes(),
                        0,
                        null,
                        properties);
        file.start();
        completeBytes = completeBytes();
    }

    private void openGroup() {
        pages =
                new ColumnChunkPageWriteStore(
                        codecs.getCompressor(config.codec()),
                        schema,
                        properties.getAllocator(),
                        properties.getColumnIndexTruncateLength(),
                        true,
                        null,
                        ordinal);
        columns = properties.newColumnWriteStore(schema, pages, pages);
        consumer = new ColumnIOFactory(true).getColumnIO(schema).getRecordWriter(columns);
        writer = new GroupWriter(consumer, schema);
    }

    public boolean write(Group row) throws IOException {
        if (closed || full) throw new IOException("Parquet file is closed or has reached its target");
        if (columns == null) openGroup();
        writer.write(row);
        groupRows++;
        if (shouldFlush(
                groupRows, columns.getBufferedSize(), config.maxRows(), groupBudget())) {
            flushGroup();
            full = completeBytes >= config.targetBytes();
            if (!full && ordinal >= Short.MAX_VALUE)
                throw new IOException("Parquet row-group limit reached before the file target; increase row_group_max_rows");
            return full;
        }
        return false;
    }

    private long groupBudget() throws IOException {
        // Keep normal groups large; reduce the final groups using actual compressed progress.
        long remaining = Math.max(0, config.targetBytes() - completeBytes);
        long margin = config.maxFileBytes() - config.targetBytes();
        return Math.max(1, Math.min(config.groupBytes(), Math.max(margin / 4, remaining / 2)));
    }

    /** Only available after finish verified the complete file, including indexes and footer. */
    public Map<String, Object> sizing() {
        if (sizeBytes < 0) throw new IllegalStateException("Parquet file has not been verified");
        return Map.of("policy", GenerationConfig.FILE_SIZE_POLICY,
                "target_bytes", config.targetBytes(), "max_bytes", config.maxFileBytes(),
                "actual_bytes", sizeBytes, "tail", !full);
    }

    static boolean shouldFlush(long rows, long bufferedBytes, long maxRows, long maxBytes) {
        return rows >= maxRows || bufferedBytes >= maxBytes;
    }

    private void flushGroup() throws IOException {
        if (columns == null) return;
        // Finalize one encoded group in bounded memory. The public append API preserves pages
        // without recompressing them, and exposes the metadata needed to size a complete file.
        var encoded = new MemoryFile();
        try {
            consumer.flush();
            try (var group = new ParquetFileWriter(encoded, schema, ParquetFileWriter.Mode.CREATE,
                    config.groupBytes(), 0, null, properties)) {
                group.start();
                group.startBlock(groupRows);
                columns.flush();
                pages.flushToFileWriter(group);
                group.endBlock();
                group.end(Map.of());
            }
            var memory = encoded.input();
            try (var input = ParquetFileReader.open(memory); var data = memory.newStream()) {
                var source = input.getFooter().getBlocks().get(0);
                var block = new BlockMetaData();
                block.setRowCount(groupRows);
                block.setOrdinal(ordinal);
                var indexes = new ArrayList<ColumnIndex>();
                var offsets = new ArrayList<OffsetIndex>();
                file.startBlock(groupRows);
                var descriptors = schema.getColumns();
                for (int i = 0; i < source.getColumns().size(); i++) {
                    var column = source.getColumns().get(i);
                    long delta = file.getPos() - column.getStartingPos();
                    var index = input.readColumnIndex(column);
                    var offset = input.readOffsetIndex(column);
                    file.appendColumnChunk(descriptors.get(i), data, column, null, index, offset);
                    indexes.add(index);
                    offsets.add(offset == null ? null : OffsetIndexBuilder.getBuilder().fromOffsetIndex(offset).build(delta));
                    block.addColumn(ColumnChunkMetaData.get(column.getPath(), column.getPrimitiveType(),
                            column.getCodec(), column.getEncodingStats(), column.getEncodings(), column.getStatistics(),
                            column.getFirstDataPageOffset() + delta,
                            column.hasDictionaryPage() ? column.getDictionaryPageOffset() + delta : 0,
                            column.getValueCount(), column.getTotalSize(), column.getTotalUncompressedSize()));
                    block.setTotalByteSize(block.getTotalByteSize() + column.getTotalUncompressedSize());
                }
                file.endBlock();
                blocks.add(block);
                columnIndexes.add(indexes);
                offsetIndexes.add(offsets);
                completeBytes = completeBytes();
            }
        } finally {
            columns.close();
            pages.close();
            columns = null;
            pages = null;
        }
        groupRows = 0;
        ordinal++;
    }

    /** Use parquet-java's serializers, including page indexes and real offsets, not a footer estimate. */
    private long completeBytes() throws IOException {
        long position = file.getPos();
        var buffer = new ByteArrayOutputStream();
        for (int b = 0; b < blocks.size(); b++) {
            for (int c = 0; c < blocks.get(b).getColumns().size(); c++) {
                var column = blocks.get(b).getColumns().get(c);
                var index = ParquetMetadataConverter.toParquetColumnIndex(column.getPrimitiveType(), columnIndexes.get(b).get(c));
                if (index == null) continue;
                buffer.reset();
                Util.writeColumnIndex(index, buffer);
                column.setColumnIndexReference(new IndexReference(position, buffer.size()));
                position += buffer.size();
            }
        }
        for (int b = 0; b < blocks.size(); b++) {
            for (int c = 0; c < blocks.get(b).getColumns().size(); c++) {
                var offset = offsetIndexes.get(b).get(c);
                if (offset == null) continue;
                buffer.reset();
                Util.writeOffsetIndex(ParquetMetadataConverter.toParquetOffsetIndex(offset), buffer);
                blocks.get(b).getColumns().get(c).setOffsetIndexReference(new IndexReference(position, buffer.size()));
                position += buffer.size();
            }
        }
        buffer.reset();
        var footer = new ParquetMetadata(new FileMetaData(schema, metadata, Version.FULL_VERSION), blocks);
        Util.writeFileMetaData(new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer), buffer);
        return position + buffer.size() + 8;
    }

    private static final class MemoryFile implements OutputFile {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        public PositionOutputStream create(long hint) {
            return new PositionOutputStream() {
                public long getPos() { return bytes.size(); }
                public void write(int value) { bytes.write(value); }
                public void write(byte[] value, int offset, int length) { bytes.write(value, offset, length); }
            };
        }
        public PositionOutputStream createOrOverwrite(long hint) { throw new UnsupportedOperationException(); }
        public boolean supportsBlockSize() { return false; }
        public long defaultBlockSize() { return 0; }
        InputFile input() {
            byte[] data = bytes.toByteArray();
            return new InputFile() {
                public long getLength() { return data.length; }
                public SeekableInputStream newStream() {
                    var stream = new ByteArrayInputStream(data);
                    return new DelegatingSeekableInputStream(stream) {
                        public long getPos() { return data.length - stream.available(); }
                        public void seek(long position) throws IOException {
                            if (position < 0 || position > data.length) throw new IOException("Invalid Parquet memory offset");
                            stream.reset();
                            stream.skip(position);
                        }
                    };
                }
            };
        }
    }

    public ParquetMetadata finish() throws IOException {
        flushGroup();
        file.end(metadata);
        ParquetMetadata footer = file.getFooter();
        close();
        CanonicalParquetFooter.normalize(path);
        long actual = java.nio.file.Files.size(path);
        if (actual != completeBytes)
            throw new IOException("Parquet complete-size accounting mismatch: expected=" + completeBytes + ", actual=" + actual);
        if (actual > config.maxFileBytes())
            throw new IOException("Parquet file exceeds its configured byte limit: " + path
                    + "; actual=" + actual + ", target=" + config.targetBytes()
                    + ", maximum=" + config.maxFileBytes()
                    + "; reduce row-group/page budgets or increase the target/overshoot allowance");
        sizeBytes = actual;
        return footer;
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            if (columns != null) {
                columns.close();
                pages.close();
                columns = null;
            }
        } finally {
            try {
                file.close();
            } finally {
                codecs.release();
            }
        }
    }
}
