package org.sibench.generation;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class LocalOutputFile implements OutputFile {
    private final Path path;

    LocalOutputFile(Path path) {
        this.path = path;
    }

    @Override
    public PositionOutputStream create(long hint) throws IOException {
        return stream(
                Files.newOutputStream(
                        path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
    }

    @Override
    public PositionOutputStream createOrOverwrite(long hint) throws IOException {
        // Published files are immutable; even this API refuses to overwrite an existing file.
        return create(hint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    private PositionOutputStream stream(OutputStream raw) {
        return new PositionOutputStream() {
            private final OutputStream out = new BufferedOutputStream(raw, 65536);
            private long position;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int value) throws IOException {
                out.write(value);
                position++;
            }

            @Override
            public void write(byte[] value, int offset, int length) throws IOException {
                out.write(value, offset, length);
                position += length;
            }

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public void close() throws IOException {
                out.close();
            }
        };
    }
}
