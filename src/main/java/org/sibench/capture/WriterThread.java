package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/** Bounded, ordered encoding queue; full output adds a detailed stream beside proto. */
final class WriterThread implements IndexOperationTrace.Consumer, Closeable {
    /** Batches waiting for the writer; bounds the operations held. */
    static final int AHEAD = 8;

    private static final List<Operation> END = List.of();

    private final IndexOperationTrace.Writer writer;
    private final ProtoTrace.Writer protocol;
    private final BlockingQueue<List<Operation>> queue = new ArrayBlockingQueue<>(AHEAD);
    private final Thread thread;
    private volatile Throwable failure;
    private List<Operation> batch = new ArrayList<>(IndexOperationTrace.BLOCK_RECORDS);
    private long count;
    private boolean closed;

    WriterThread(IndexOperationTrace.Writer writer, ProtoTrace.Writer protocol, String name) {
        this.writer = writer;
        this.protocol = protocol;
        thread = new Thread(this::run, name);
        thread.setDaemon(true);
        thread.start();
    }

    private void run() {
        try {
            while (true) {
                List<Operation> next = queue.take();
                if (next == END) break;
                for (Operation operation : next) {
                    if (writer != null) writer.append(operation);
                    if (protocol != null) protocol.append(operation);
                }
            }
            closeWriters();
        } catch (Throwable error) {
            failure = error;
            // Unblock an appender waiting for room; nothing more is written.
            queue.clear();
            try {
                closeWriters();
            } catch (IOException | RuntimeException ignored) {
                // The first failure is the one reported.
            }
        }
    }

    private void closeWriters() throws IOException {
        try { if (writer != null) writer.close(); } finally { if (protocol != null) protocol.close(); }
    }

    @Override
    public void accept(Operation operation) throws IOException {
        append(operation);
    }

    void append(Operation operation) throws IOException {
        check();
        batch.add(operation);
        count++;
        if (batch.size() == IndexOperationTrace.BLOCK_RECORDS) {
            put(batch);
            batch = new ArrayList<>(IndexOperationTrace.BLOCK_RECORDS);
        }
    }

    long count() {
        return count;
    }

    private void put(List<Operation> operations) throws IOException {
        try {
            while (!queue.offer(operations, 100, java.util.concurrent.TimeUnit.MILLISECONDS)) check();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing a trace", interrupted);
        }
    }

    private void check() throws IOException {
        Throwable error = failure;
        if (error == null) return;
        if (error instanceof IOException io) throw io;
        if (error instanceof RuntimeException runtime) throw runtime;
        if (error instanceof Error fatal) throw fatal;
        throw new IOException(error);
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            if (failure == null) {
                if (!batch.isEmpty()) put(batch);
                put(END);
            }
            thread.join();
        } catch (InterruptedException interrupted) {
            thread.interrupt();
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing a trace", interrupted);
        }
        check();
    }
}
