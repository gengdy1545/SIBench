package org.sibench.pixels;

import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.cli.executor.LoadExecutor;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Real renewal RPCs with controlled replies; no coordinator or storage is needed. */
public final class LoadLeaseProbe {
    public static void main(String[] args) throws Exception {
        var entered = new CountDownLatch(1);
        var reply = new CountDownLatch(1);
        var rejected = new CountDownLatch(1);
        var requests = new AtomicInteger();
        var server = ServerBuilder.forPort(0).addService(new TransServiceGrpc.TransServiceImplBase() {
            @Override public void extendTransLease(TransProto.ExtendTransLeaseRequest request,
                    StreamObserver<TransProto.ExtendTransLeaseResponse> response) {
                requests.incrementAndGet();
                if (request.getTransId() == 1) {
                    entered.countDown();
                    try {
                        if (!reply.await(10, TimeUnit.SECONDS)) throw new AssertionError("Reply not released");
                    } catch (InterruptedException error) { throw new AssertionError(error); }
                }
                response.onNext(TransProto.ExtendTransLeaseResponse.newBuilder()
                        .setErrorCode(request.getTransId() == 1 ? 0 : 1)
                        .setNewLeaseStartMs(System.currentTimeMillis()).build());
                response.onCompleted();
                if (request.getTransId() == 2) rejected.countDown();
            }
        }).build().start();
        var config = ConfigFactory.Instance();
        config.addProperty("trans.server.host", "127.0.0.1");
        config.addProperty("trans.server.port", Integer.toString(server.getPort()));
        var stopper = Executors.newSingleThreadExecutor();
        ScheduledExecutorService renewal = null;
        try {
            var failure = new AtomicReference<Throwable>();
            renewal = renew(1, failure);
            if (!entered.await(5, TimeUnit.SECONDS)) throw new AssertionError("Renewal did not reach the server");
            var active = renewal;
            var stopping = stopper.submit(() -> { invoke("stopRenewal", ScheduledExecutorService.class, active); return null; });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!active.isShutdown()) {
                if (System.nanoTime() >= deadline) throw new AssertionError("Renewal stop did not start");
                Thread.sleep(1);
            }
            try {
                stopping.get(200, TimeUnit.MILLISECONDS);
                throw new AssertionError("Stopping renewal interrupted an in-flight RPC");
            } catch (TimeoutException expected) {
                // Commit must wait for the successful extension, not cancel it.
            } finally { reply.countDown(); }
            stopping.get(5, TimeUnit.SECONDS);
            invoke("requireLease", AtomicReference.class, failure);
            if (!renewal.isTerminated() || requests.get() != 1) throw new AssertionError("Renewal still active");

            failure = new AtomicReference<>();
            renewal = renew(2, failure);
            if (!rejected.await(5, TimeUnit.SECONDS)) throw new AssertionError("Rejection was not delivered");
            invoke("stopRenewal", ScheduledExecutorService.class, renewal);
            try {
                invoke("requireLease", AtomicReference.class, failure);
                throw new AssertionError("Rejected renewal allowed commit");
            } catch (TransException expected) {
                if (!(expected.getCause() instanceof TransException)) throw new AssertionError("Lost renewal cause", expected);
            }
            if (!renewal.isTerminated() || requests.get() != 2) throw new AssertionError("Failed renewal still active");
            System.out.println("LOAD renewal: in-flight completion and rejection checks passed");
        } finally {
            reply.countDown();
            if (renewal != null) renewal.shutdownNow();
            stopper.shutdownNow();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static ScheduledExecutorService renew(long id, AtomicReference<Throwable> failure) throws Exception {
        var context = new TransContext(id, id, System.currentTimeMillis() - 5000, 10000, false);
        var method = LoadExecutor.class.getDeclaredMethod("renewLease", TransService.class, TransContext.class, AtomicReference.class);
        method.setAccessible(true);
        return (ScheduledExecutorService) method.invoke(null, TransService.Instance(), context, failure);
    }

    private static void invoke(String name, Class<?> type, Object argument) throws Exception {
        var method = LoadExecutor.class.getDeclaredMethod(name, type);
        method.setAccessible(true);
        try { method.invoke(null, argument); }
        catch (InvocationTargetException error) {
            if (error.getCause() instanceof Exception cause) throw cause;
            throw error;
        }
    }
}
