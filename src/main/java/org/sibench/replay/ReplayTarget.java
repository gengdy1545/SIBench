package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** A client separates local transport completion from the eventual service response. */
@FunctionalInterface
public interface ReplayTarget extends AutoCloseable {
    /**
     * sent completes at the adapter transport callback (gRPC outboundMessageSent), not at the
     * send API return. This is not a peer acknowledgement. response completes after
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

    /** Sends whatever the client still holds back, such as a partial batch, before a drain. */
    default void flush() throws IOException {}

    /** response includes source-group validation; businessResponse ends this operation's latency. */
    record SendHandle(CompletionStage<Void> sent, CompletionStage<Void> response,
                      CompletionStage<Void> businessResponse) {
        public SendHandle(CompletionStage<Void> sent, CompletionStage<Void> response) {
            this(sent, response, response);
        }
        public SendHandle {
            Objects.requireNonNull(sent);
            Objects.requireNonNull(response);
            Objects.requireNonNull(businessResponse);
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

            };
    }

    @Override
    default void close() throws IOException {}
}
