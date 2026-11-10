package org.sibench.pixels;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * One connection from the Retina JVM to the SIBench generator. Index calls append raw facts before
 * they return, so a fact precedes every acknowledgement that depends on it. A barrier request from
 * the generator is answered after all facts appended before it. Any transport failure fails the
 * index call; facts are never dropped.
 */
final class IndexTraceChannel {
    static final int MAGIC = 0x53495854, LOOKUP = 1, PUT = 2, UPDATE = 3, DELETE = 4, UNSUPPORTED = 9, BARRIER = 10;
    private static IndexTraceChannel instance;
    private final Socket socket;
    private final DataOutputStream output;
    private volatile IOException failure;

    private IndexTraceChannel(String endpoint, String token) throws IOException {
        int colon = endpoint.lastIndexOf(':');
        if (colon < 1) throw new IOException("Invalid sibench.trace.endpoint: " + endpoint);
        socket = new Socket();
        socket.connect(new InetSocketAddress(endpoint.substring(0, colon), Integer.parseInt(endpoint.substring(colon + 1))), 30_000);
        socket.setTcpNoDelay(true);
        output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 1 << 20));
        output.writeInt(MAGIC);
        output.writeUTF(token);
        output.flush();
        var reader = new Thread(this::serveBarriers, "sibench-trace-barriers");
        reader.setDaemon(true);
        reader.start();
        var flusher = new Thread(this::flushPeriodically, "sibench-trace-flush");
        flusher.setDaemon(true);
        flusher.start();
    }

    static synchronized IndexTraceChannel get() throws IOException {
        if (instance == null) {
            String endpoint = System.getProperty("sibench.trace.endpoint"), token = System.getProperty("sibench.trace.token");
            if (endpoint == null || token == null) throw new IOException("Missing sibench.trace.endpoint/token");
            instance = new IndexTraceChannel(endpoint, token);
        }
        return instance;
    }

    /** Appends one encoded fact; the caller's index operation fails if the generator cannot receive it. */
    void append(byte[] fact) throws IOException {
        if (failure != null) throw new IOException("Index trace channel failed", failure);
        synchronized (output) {
            try {
                output.write(fact);
            } catch (IOException error) {
                failure = error;
                throw error;
            }
        }
    }

    static byte[] fact(int kind, long table, long index, long timestamp, byte[] key, long rowId, long fileId, int rgId,
            int rgRowOffset) {
        var bytes = new ByteArrayOutputStream(64 + key.length);
        var data = new DataOutputStream(bytes);
        try {
            data.writeByte(kind);
            data.writeLong(table);
            data.writeLong(index);
            data.writeLong(timestamp);
            data.writeInt(key.length);
            data.write(key);
            data.writeLong(rowId);
            data.writeLong(fileId);
            data.writeInt(rgId);
            data.writeInt(rgRowOffset);
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
        return bytes.toByteArray();
    }

    static byte[] unsupported(String method) {
        byte[] name = method.getBytes(StandardCharsets.UTF_8);
        return fact(UNSUPPORTED, 0, 0, 0, name, -1, 0, 0, 0);
    }

    private void serveBarriers() {
        try (var input = new DataInputStream(socket.getInputStream())) {
            while (true) {
                long barrier = input.readLong();
                synchronized (output) {
                    output.write(fact(BARRIER, 0, 0, 0, new byte[0], barrier, 0, 0, 0));
                    output.flush();
                }
            }
        } catch (IOException error) {
            if (failure == null) failure = error;
        }
    }

    private void flushPeriodically() {
        try {
            while (failure == null) {
                Thread.sleep(20);
                synchronized (output) {
                    output.flush();
                }
            }
        } catch (InterruptedException | IOException error) {
            if (failure == null) failure = error instanceof IOException io ? io : new IOException(error);
        }
    }
}
