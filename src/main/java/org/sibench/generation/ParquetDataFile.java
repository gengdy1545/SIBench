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
import java.util.Map;

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
        if (columns == null) openGroup();
        writer.write(row);
        groupRows++;
        if (shouldFlush(
                groupRows, columns.getBufferedSize(), config.maxRows(), config.groupBytes())) {
            flushGroup();
            return file.getPos() >= config.targetBytes() || ordinal >= Short.MAX_VALUE;
        }
        return false;
    }

    static boolean shouldFlush(long rows, long bufferedBytes, long maxRows, long maxBytes) {
        return rows >= maxRows || bufferedBytes >= maxBytes;
    }

    private void flushGroup() throws IOException {
        if (columns == null) return;
        try {
            consumer.flush();
            file.startBlock(groupRows);
            columns.flush();
            pages.flushToFileWriter(file);
            file.endBlock();
        } finally {
            columns.close();
            pages.close();
            columns = null;
            pages = null;
        }
        groupRows = 0;
        ordinal++;
    }

    public ParquetMetadata finish() throws IOException {
        flushGroup();
        file.end(metadata);
        ParquetMetadata footer = file.getFooter();
        close();
        CanonicalParquetFooter.normalize(path);
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
