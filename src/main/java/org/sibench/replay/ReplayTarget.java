package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** A client separates complete local transmission from the eventual service response. */
@FunctionalInterface
public interface ReplayTarget extends AutoCloseable {
    /**
     * sent completes only after local transmission, not enqueueing. response completes after
     * matching and validating the response against this operation; mismatches complete
     * exceptionally. The adapter owns transport cancellation on close and must not reorder local
     * sends.
     */
    SendHandle send(long dispatchSequence, Operation operation) throws IOException;

    /** Logical reference targets consume views; service clients forward the stored payload. */
    default SendHandle send(long dispatchSequence, Operation operation,
            org.sibench.adapters.proto.Request stored) throws IOException {
        return send(dispatchSequence, operation);
    }

    /**
     * Sends a complete-commit marker after the transaction's operations. Service targets must
     * implement it; logical reference targets consume operation views without publication markers.
     */
    default SendHandle sendCommit(long dispatchSequence, long transactionId, long version)
            throws IOException {
        throw new IOException("Replay target does not support complete-commit markers");
    }

    default SendHandle sendAbort(long dispatchSequence, long transactionId, long version)
            throws IOException {
        throw new IOException("Replay target does not support Abort");
    }

    /** Sends whatever the client still holds back, such as a partial batch, before a drain. */
    default void flush() throws IOException {}

    record SendHandle(CompletionStage<Void> sent, CompletionStage<Void> response) {
        public SendHandle {
            Objects.requireNonNull(sent);
            Objects.requireNonNull(response);
        }

        public static SendHandle completed() {
            return new SendHandle(
                    CompletableFuture.completedFuture(null),
                    CompletableFuture.completedFuture(null));
        }
    }

    @FunctionalInterface
    interface Factory {
        ReplayTarget open(int clientId) throws IOException;
    }

    /** In-process references execute immediately; they do not claim network send semantics. */
    static Factory immediate(IndexOperationTrace.Consumer consumer) {
        return clientId -> new ReplayTarget() {
                @Override
                public SendHandle send(long sequence, Operation operation) throws IOException {
                    consumer.accept(operation);
                    return SendHandle.completed();
                }

                @Override
                public SendHandle sendAbort(long sequence, long transactionId, long version) throws IOException {
                    consumer.accept(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_ABORT,
                            transactionId, version, 0));
                    return SendHandle.completed();
                }
            };
    }

    @Override
    default void close() throws IOException {}
}
