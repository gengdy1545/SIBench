package org.sibench.pixels;

import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.cli.executor.LoadExecutor;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import net.sourceforge.argparse4j.inf.Namespace;

/** Real LOAD with only the visibility response replaced by an explicit rejection. */
public final class LoadFailureProbe {
    public static void main(String[] args) throws Exception {
        var rejected = new AtomicBoolean();
        var server = ServerBuilder.forPort(0).addService(new RetinaWorkerServiceGrpc.RetinaWorkerServiceImplBase() {
            @Override public void addVisibility(RetinaProto.AddVisibilityRequest request,
                    StreamObserver<RetinaProto.AddVisibilityResponse> response) {
                rejected.set(true);
                response.onNext(RetinaProto.AddVisibilityResponse.newBuilder().setHeader(RetinaProto.ResponseHeader.newBuilder()
                        .setToken(request.getHeader().getToken()).setErrorCode(1).setErrorMsg("injected visibility rejection")).build());
                response.onCompleted();
            }
        }).build().start();
        var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
        var config = ConfigFactory.Instance();
        var stub = RetinaService.class.getDeclaredField("stub");
        stub.setAccessible(true);
        for (var service : new RetinaService[] {RetinaService.Instance(), RetinaService.CreateInstance(
                config.getProperty("retina.server.host"), Integer.parseInt(config.getProperty("retina.server.port")))})
            stub.set(service, RetinaWorkerServiceGrpc.newBlockingStub(channel));
        try {
            new LoadExecutor().execute(new Namespace(Map.of("schema", "postgres", "table", "savingaccount", "origin", args[0],
                    "row_num", "1000", "row_regex", "|", "consumer_thread_num", "1", "encoding_level", "2", "nulls_padding", "false")), "LOAD");
            throw new AssertionError("LOAD accepted a rejected visibility publication");
        } catch (IllegalStateException expected) {
            if (!rejected.get() || !expected.getMessage().contains("Cannot publish LOAD visibility")) throw expected;
            System.out.println("Rejected visibility publication fails LOAD");
        } finally { channel.shutdownNow(); server.shutdownNow(); }
        System.exit(0);
    }
}
