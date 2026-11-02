package org.sibench.capture;

import com.github.luben.zstd.ZstdInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The backend trace files of a capture: {@code pg-<pid>.jsonl.zst}. A compressed file whose
 * backend did not end its frame fails to read. Client traces ({@code jdbc-*.jsonl.zst}) open the
 * same way.
 */
public final class ServerTrace {
    private static final String PREFIX = "pg-", ZSTD = ".jsonl.zst";

    static final String COMMIT = "{\"event\":\"transaction_commit\"";
    static final String ABORT = "{\"event\":\"transaction_abort\"";

    private ServerTrace() {}

    static String detail(com.google.gson.JsonObject header) throws IOException {
        if (header == null || !header.has("event")
                || !header.get("event").getAsString().equals("capture_start"))
            throw new IOException("Trace does not start with capture_start");
        if (!header.has("pid") || header.get("pid").getAsInt() < 1)
            throw new IOException("Capture header lacks its backend identity");
        if (!header.has("detail") || !java.util.Set.of("rows", "summary")
                .contains(header.get("detail").getAsString()))
            throw new IOException("Unknown capture detail");
        return header.get("detail").getAsString();
    }

    public static boolean isServerTrace(String name) {
        return name.startsWith(PREFIX) && name.endsWith(ZSTD);
    }

    /** The backend pid a trace file is named after. */
    public static int pid(String name) {
        if (!isServerTrace(name)) throw new IllegalArgumentException("Not a backend trace: " + name);
        return Integer.parseInt(name.substring(PREFIX.length(), name.length() - ZSTD.length()));
    }

    /** The name a backend's trace has, whether or not it exists yet. */
    static Path path(Path directory, int pid) {
        return directory.resolve(PREFIX + pid + ZSTD);
    }

    /** The trace file of one backend in a raw directory. */
    public static Path locate(Path directory, int pid) throws IOException {
        Path file = path(directory, pid);
        if (!Files.isRegularFile(file)) throw new IOException("Backend trace not found: " + file);
        return file;
    }

    public static BufferedReader open(Path file) throws IOException {
        return new BufferedReader(new InputStreamReader(input(file), StandardCharsets.UTF_8), 1 << 16);
    }

    private static InputStream input(Path file) throws IOException {
        if (!file.getFileName().toString().endsWith(ZSTD))
            throw new IOException("Not a zstd backend trace: " + file);
        return new ZstdInputStream(new BufferedInputStream(Files.newInputStream(file), 1 << 20));
    }

    /**
     * Selected records of one trace, found without decoding the lines between them: the
     * bytes are searched for line ends exactly as {@link java.io.BufferedReader#readLine} splits
     * lines (LF, CR or CRLF; a last line without one still counts), and only a line that starts
     * with a selected record is decoded. Row auditing also retains noncanonical lines for its
     * parser, preserving validation and source positions.
     */
    static final class Records implements AutoCloseable {
        private static final byte[] COMMIT_BYTES = COMMIT.getBytes(StandardCharsets.UTF_8);
        private static final byte[] ABORT_BYTES = ABORT.getBytes(StandardCharsets.UTF_8);
        private static final int HEAD = Math.max(COMMIT_BYTES.length, ABORT_BYTES.length);

        private static final byte[] EVENT_BYTES = "{\"event\":\"".getBytes(StandardCharsets.UTF_8);
        private static final byte[] ROW_BYTES = "{\"event\":\"row_change\"".getBytes(StandardCharsets.UTF_8);

        private final boolean rowChanges;
        private final InputStream input;
        private final byte[] buffer = new byte[1 << 20];
        private int position, limit;
        private boolean eof;
        /** The previous byte ended a line with CR, so an LF right after it ends nothing. */
        private boolean afterCr;
        private long lines;
        /** The current line's first bytes, and whether it has any byte at all. */
        private final byte[] head = new byte[HEAD];
        private int headLength;
        private boolean inLine;
        /** -1 undecided, 0 skipped, 1 kept whole. */
        private int record = -1;
        private final java.io.ByteArrayOutputStream kept = new java.io.ByteArrayOutputStream();

        Records(Path file) throws IOException {
            this(file, false);
        }

        static Records rowChanges(Path file) throws IOException {
            return new Records(file, true);
        }

        private Records(Path file, boolean rowChanges) throws IOException {
            this.rowChanges = rowChanges;
            input = ServerTrace.input(file);
        }

        /** Lines the last {@link #next} read, the record it returned included. */
        long lines() {
            return lines;
        }

        /** The next selected line, or null at the end of the trace. */
        String next() throws IOException {
            lines = 0;
            while (true) {
                if (position == limit) {
                    if (eof || !fill()) {
                        eof = true;
                        if (!inLine) return null;
                        return endLine();
                    }
                }
                byte b = buffer[position++];
                if (afterCr) {
                    afterCr = false;
                    if (b == '\n') continue;
                }
                if (b == '\n' || b == '\r') {
                    afterCr = b == '\r';
                    String line = endLine();
                    if (line != null) return line;
                    continue;
                }
                inLine = true;
                if (record == 1) {
                    kept.write(b);
                    scanLine(true);
                    continue;
                }
                if (record == 0) {
                    scanLine(false);
                    continue;
                }
                head[headLength++] = b;
                decide();
            }
        }

        /** Classifies the line once its head can tell. */
        private void decide() {
            boolean commit = startsWith(COMMIT_BYTES), abort = startsWith(ABORT_BYTES);
            boolean row = rowChanges && startsWith(ROW_BYTES);
            if (!commit && !abort && !row) {
                // Preserve noncanonical lines for the row audit parser, including malformed input.
                if (rowChanges && !startsWith(EVENT_BYTES)) keep();
                else record = 0;
                return;
            }
            if (headLength >= COMMIT_BYTES.length && commit
                    || headLength >= ABORT_BYTES.length && abort
                    || headLength >= ROW_BYTES.length && row) keep();
        }

        private void keep() {
            record = 1;
            kept.reset();
            kept.write(head, 0, headLength);
        }

        /** Whether the head read so far is consistent with the prefix. */
        private boolean startsWith(byte[] prefix) {
            int n = Math.min(headLength, prefix.length);
            for (int i = 0; i < n; i++) if (head[i] != prefix[i]) return false;
            return true;
        }

        /** Copies or skips the rest of the current buffered line in one block. */
        private void scanLine(boolean retain) {
            int i = position;
            while (i < limit && buffer[i] != '\n' && buffer[i] != '\r') i++;
            if (retain) kept.write(buffer, position, i - position);
            position = i;
        }

        /** Ends the current line; its text when selected. */
        private String endLine() {
            lines++;
            if (rowChanges && record == -1
                    && !(headLength >= EVENT_BYTES.length && startsWith(EVENT_BYTES))) keep();
            String line = record == 1 ? kept.toString(StandardCharsets.UTF_8) : null;
            inLine = false;
            headLength = 0;
            record = -1;
            return line;
        }

        private boolean fill() throws IOException {
            int n = input.read(buffer, 0, buffer.length);
            if (n <= 0) return false;
            position = 0;
            limit = n;
            return true;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }
}
