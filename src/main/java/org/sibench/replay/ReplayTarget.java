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

    /**
     * Sends a complete-commit marker after the transaction's operations. Services that only use
     * sequence ordering do not need it; the default fails rather than silently dropping it.
     */
    default SendHandle sendCommit(long dispatchSequence, long transactionId, long version)
            throws IOException {
        throw new IOException("Replay target does not support complete-commit markers");
    }

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
        return clientId ->
                (sequence, operation) -> {
                    consumer.accept(operation);
                    return SendHandle.completed();
                };
    }

    @Override
    default void close() throws IOException {}
}
