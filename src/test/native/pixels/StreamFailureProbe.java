package org.sibench.pixels;

import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** A Retina update stream that the server fails must fail its pending and later requests, not leave them waiting. */
public final class StreamFailureProbe {
    public static void main(String[] args) throws Exception {
        var server = ServerBuilder.forPort(0).addService(new RetinaWorkerServiceGrpc.RetinaWorkerServiceImplBase() {
            @Override public StreamObserver<RetinaProto.UpdateRecordRequest> streamUpdateRecord(
                    StreamObserver<RetinaProto.UpdateRecordResponse> response) {
                return new StreamObserver<>() {
                    @Override public void onNext(RetinaProto.UpdateRecordRequest request) {
                        response.onError(Status.UNAVAILABLE.withDescription("injected stream failure").asRuntimeException());
                    }
                    @Override public void onError(Throwable error) {}
                    @Override public void onCompleted() {}
                };
            }
        }).build().start();
        var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
        int status = 1;
        try {
            var service = RetinaService.Instance();
            var asyncStub = RetinaService.class.getDeclaredField("asyncStub");
            asyncStub.setAccessible(true);
            asyncStub.set(service, RetinaWorkerServiceGrpc.newStub(channel));
            var stream = service.startUpdateStream();
            var pending = stream.updateRecord("postgres", 0, List.of());
            try {
                pending.get(30, TimeUnit.SECONDS);
                throw new AssertionError("A failed Retina stream completed a request");
            } catch (ExecutionException expected) {
                System.out.println("Pending request failed: " + expected.getCause());
            }
            try {
                stream.updateRecord("postgres", 0, List.of());
                throw new AssertionError("A failed Retina stream accepted another request");
            } catch (RetinaException expected) {
                System.out.println("Later request rejected: " + expected.getMessage());
            }
            status = 0;
        } catch (Throwable failure) {
            failure.printStackTrace();
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
        // gRPC leaves non-daemon threads; the exit status is the probe result.
        System.exit(status);
    }
}
