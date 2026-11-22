package org.sibench.adapters;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.protobuf.ByteString;
import org.sibench.adapters.proto.*;

/** Forked by ReliveBackendTest to fix common-pool parallelism before HttpClient initializes. */
public final class ReliveAsyncResponseCheck {
    public static void main(String[] args) throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        var http=Executors.newCachedThreadPool(); server.setExecutor(http);
        var slowHeaders=new CountDownLatch(2); var release=new CountDownLatch(1);
        var fastReplied=new CountDownLatch(1); var calls=new AtomicInteger();
        server.createContext("/", exchange -> {
            try(exchange) {
                String path=exchange.getRequestURI().getPath();
                exchange.getRequestBody().readAllBytes();
                String body;
                if(path.equals("/describe")) body="{\"name\":\"relive\",\"versionedKeys\":true,\"streamingReads\":true,\"transactionLocalReads\":false,\"searchGroups\":false,\"maxWriteRows\":10000,\"durability\":\"shared_nfs_fsync\",\"levelOneStorage\":\"object_storage\",\"location\":\"fixture\",\"readConcurrency\":4}";
                else if(path.equals("/bind")) body="{\"binding\":\"sibench ROW_ID probe\",\"identity\":\"fixture\",\"location\":\"fixture\"}";
                else {
                    body="{\"rows\":1}";
                    if(calls.incrementAndGet()<=2) {
                        byte[] bytes=body.getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200,bytes.length);
                        exchange.getResponseBody().write(bytes,0,1); exchange.getResponseBody().flush(); slowHeaders.countDown();
                        try { if(!release.await(8,TimeUnit.SECONDS)) throw new AssertionError("release timeout"); }
                        catch(InterruptedException e) {throw new RuntimeException(e);}
                        exchange.getResponseBody().write(bytes,1,bytes.length-1); return;
                    }
                }
                byte[] bytes=body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200,bytes.length); exchange.getResponseBody().write(bytes);
                if(path.equals("/write")) fastReplied.countDown();
            }
        });
        server.start();
        try(var backend=new ReliveBackend("http://127.0.0.1:"+server.getAddress().getPort(),ValueKind.ROW_ID)) {
            backend.bind("probe");
            var write=Mutation.newBuilder().setPut(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("k")).setTs(1)).build();
            var a=backend.writeAsync(write); var b=backend.writeAsync(write);
            if(!slowHeaders.await(3,TimeUnit.SECONDS)) throw new AssertionError("headers timeout");
            // Let both stalled bodies reach their handlers before testing an independent ACK.
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (ForkJoinPool.commonPool().getActiveThreadCount() < 2 && System.nanoTime() < until)
                Thread.sleep(10);
            var fast=backend.writeAsync(write);
            if(!fastReplied.await(3,TimeUnit.SECONDS)) throw new AssertionError("fast request not served");
            boolean completed;
            try {fast.get(1,TimeUnit.SECONDS); completed=true;} catch(TimeoutException expected) {completed=false;}
            System.out.println("common_parallelism="+ForkJoinPool.getCommonPoolParallelism());
            System.out.println("fast_server_reply_sent=true");
            System.out.println("fast_client_completed_while_other_bodies_stalled="+completed);
            release.countDown();
            CompletableFuture.allOf(a,b,fast).get(3,TimeUnit.SECONDS);
            System.out.println("all_completed_after_releasing_stalled_bodies=true");
            if (!completed) throw new AssertionError("A complete ACK waited for unrelated response bodies");
        } finally {release.countDown(); server.stop(0); http.shutdownNow();}
    }
}
