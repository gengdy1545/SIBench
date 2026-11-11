package org.sibench.adapters;

import org.sibench.adapters.proto.*;
import java.io.IOException;

/** Validates and executes opaque versioned KV requests for local and remote services. */
public final class IndexExecution {
    @FunctionalInterface
    public interface Executor {
        java.util.concurrent.CompletableFuture<Response> submit(Request request,
                java.util.function.Consumer<Response> partial);
    }
    static final int CHUNK_RESULTS = 16_384;
    static final int MAX_MESSAGE_BYTES = 32 * 1024 * 1024;
    private IndexExecution() {}

    public static Response execute(IndexBackend backend, Request request) {
        return execute(backend, request, chunk -> { throw new IllegalStateException("Unexpected partial response"); });
    }
    public static Response execute(IndexBackend backend, Request request,
            java.util.function.Consumer<Response> partial) {
        var response = Response.newBuilder();
        try {
            known(request);
            switch (request.getOperationCase()) {
                case PUT -> { var mutation = Mutation.newBuilder().setPut(request.getPut()).build();
                    validateMutation(mutation); backend.write(mutation); }
                case DELETE -> { var mutation = Mutation.newBuilder().setDelete(request.getDelete()).build();
                    validateMutation(mutation); backend.write(mutation); }
                case BATCH -> {
                    var batch = request.getBatch(); known(batch);
                    if (batch.getMutationsCount() == 0) throw new IllegalArgumentException("Empty write batch");
                    for (var mutation : batch.getMutationsList()) validateMutation(mutation);
                    backend.writeBatch(batch.getMutationsList());
                }
                case GET -> {
                    validateGet(request.getGet());
                    var result = backend.get(request.getGet());
                    if (result != null) response.addResults(result);
                }
                case SCAN -> { validateScan(request.getScan()); response = scan(backend, request.getScan(), partial); }
                default -> throw new IllegalArgumentException("Expected an individual data operation");
            }
            return response.setStatus(Status.OK).build();
        } catch (IllegalArgumentException invalid) {
            return response.setStatus(Status.INVALID).setMessage(String.valueOf(invalid.getMessage())).build();
        } catch (IOException | RuntimeException error) {
            return response.setStatus(Status.UNAVAILABLE).setMessage(String.valueOf(error)).build();
        }
    }
    public static void known(com.google.protobuf.GeneratedMessageV3 message) {
        if (!message.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("Unsupported fields");
    }
    public static void validateMutation(Mutation mutation) {
        known(mutation);
        if (mutation.hasPut()) { known(mutation.getPut()); ts(mutation.getPut().getTs()); }
        else if (mutation.hasDelete()) { known(mutation.getDelete()); ts(mutation.getDelete().getTs()); }
        else throw new IllegalArgumentException("Missing mutation");
    }
    public static void validateGet(GetRequest get) { known(get); ts(get.getReadTs()); }
    public static void validateScan(ScanRequest scan) {
        known(scan); ts(scan.getReadTs());
        if (scan.getLimit() < 0) throw new IllegalArgumentException("Limit exceeds int64");
    }
    private static void ts(long ts) {
        if (ts < 0) throw new IllegalArgumentException("Timestamp exceeds int64");
    }
    private static Response.Builder scan(IndexBackend backend, ScanRequest scan,
            java.util.function.Consumer<Response> partial) throws IOException {
        var chunk = new Response.Builder[] {Response.newBuilder()};
        int[] bytes = {0};
        backend.scan(scan, result -> {
            int size = result.getSerializedSize() + 10;
            if (size > MAX_MESSAGE_BYTES - 64) throw new IOException("One index result exceeds the RPC message limit");
            if (chunk[0].getResultsCount() > 0 && bytes[0] + size > 1024 * 1024) {
                partial.accept(chunk[0].setStatus(Status.OK).setMore(true).build());
                chunk[0] = Response.newBuilder(); bytes[0] = 0;
            }
            chunk[0].addResults(result); bytes[0] += size;
            if (chunk[0].getResultsCount() == CHUNK_RESULTS) {
                partial.accept(chunk[0].setStatus(Status.OK).setMore(true).build());
                chunk[0] = Response.newBuilder(); bytes[0] = 0;
            }
        });
        return chunk[0];
    }
}
