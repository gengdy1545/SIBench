package org.sibench.adapters;

import static org.junit.Assert.*;
import static org.sibench.adapters.TraceRequestsTest.send;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.*;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.trace.IndexKey;
import org.sibench.trace.IndexEntry;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/** Independent cases for the data API, completion, target identity and streamed failures. */
public class IndexServerTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    static ByteString key(int pk) { return new IndexEntry(1, 0, null, IndexKey.ofInt(pk)).bytes(); }
    static PutRequest put(int pk,long ts,long row) { return PutRequest.newBuilder().setKey(key(pk)).setTs(ts).setValue(IndexEntry.rowId(row)).build(); }
    static GetRequest get(int pk,long ts) { return GetRequest.newBuilder().setKey(key(pk)).setReadTs(ts).build(); }
    static ManagedChannel channel(int port) { return ManagedChannelBuilder.forAddress("127.0.0.1",port).usePlaintext().build(); }
    static IndexServiceGrpc.IndexServiceBlockingStub stub(ManagedChannel channel) { return IndexServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5,TimeUnit.SECONDS); }

    @Test public void publicSurfaceContainsOnlyFiveDataMethods() {
        assertEquals(Set.of("Get","Put","Delete","Scan","WriteBatch"),
                IndexServiceGrpc.getServiceDescriptor().getMethods().stream().map(MethodDescriptor::getBareMethodName).collect(java.util.stream.Collectors.toSet()));
        for (var method : GetRequest.getDescriptor().getFile().findServiceByName("IndexService").getMethods()) {
            assertEquals(method.getName() + "Request", method.getInputType().getName());
            assertEquals(method.getName() + "Response", method.getOutputType().getName());
        }
    }

    @Test(timeout=10000) public void emptyScanStillReturnsAndValidatesTheDeploymentDescription() throws Exception {
        try (var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("empty-scan"),ValueKind.ROW_ID);
             var server=IndexServer.start(0,backend);
             var grpc=new GrpcService("127.0.0.1:"+server.port(),new TraceRequests(ValueKind.ROW_ID),"empty",2)) {
            var query=IndexOperationTrace.search(1,1,0,new IndexOperationTrace.Range(null,false,null,false,false,0),0,0,0);
            send(grpc.factory().open(0), 1,query).response().toCompletableFuture().get(3,TimeUnit.SECONDS);
            assertEquals(backend.describe(),grpc.description());
        }
    }

    @Test(timeout=10000) public void emptyScanCannotBypassAMissingDeploymentDescription() throws Exception {
        var server=ServerBuilder.forPort(0).addService(new IndexServiceGrpc.IndexServiceImplBase() {
            @Override public void scan(ScanRequest request,StreamObserver<ScanResponse> out) { out.onCompleted(); }
        }).build().start();
        try (var grpc=new GrpcService("127.0.0.1:"+server.getPort(),new TraceRequests(ValueKind.ROW_ID),"empty",2)) {
            var query=IndexOperationTrace.search(1,1,0,new IndexOperationTrace.Range(null,false,null,false,false,0),0,0,0);
            var failure=assertThrows(ExecutionException.class,()->send(grpc.factory().open(0), 1,query)
                    .response().toCompletableFuture().get(3,TimeUnit.SECONDS));
            assertTrue(failure.getCause().getMessage().contains("deployment description"));
        } finally { server.shutdownNow(); }
    }

    @Test(timeout=15000) public void acknowledgementWaitsForApplicationAndOtherClientsReadItsHistoricalVersions() throws Exception {
        var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        try(var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("visible"),ValueKind.ROW_ID)) {
            var blocked=(IndexBackend)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{IndexBackend.class},(proxy,method,args)->{
                if(method.getName().equals("write") && ((Mutation)args[0]).getPut().getTs()==1) {
                    entered.countDown(); if(!release.await(5,TimeUnit.SECONDS)) throw new IOException("release timeout");
                }
                try { return method.invoke(backend,args); } catch(java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
            });
            try(var server=IndexServer.start(0,blocked); var client=new GrpcService("127.0.0.1:"+server.port(),new TraceRequests(ValueKind.ROW_ID),"visible",5)) {
                var target=client.factory().open(0);
                send(target, 1,IndexOperationTrace.write(1,IndexOperationTrace.INSERT,1,0,IndexKey.ofInt(7),0,0,0,70)).response().toCompletableFuture().get(3,TimeUnit.SECONDS);
                var ack=send(target, 2,IndexOperationTrace.write(2,IndexOperationTrace.UPSERT,1,0,IndexKey.ofInt(7),1,0,0,71)).response().toCompletableFuture();
                assertTrue(entered.await(2,TimeUnit.SECONDS));
                assertThrows(TimeoutException.class,()->ack.get(50,TimeUnit.MILLISECONDS));
                release.countDown(); ack.get(3,TimeUnit.SECONDS);
                var reader=channel(server.port());
                try {
                    var api=stub(reader);
                    assertEquals(71,api.get(get(7,1)).getValue().asReadOnlyByteBuffer().getLong());
                    api.delete(DeleteRequest.newBuilder().setKey(key(7)).setTs(2).build());
                    assertFalse(api.get(get(7,2)).hasValue());
                    assertEquals(71,api.get(get(7,1)).getValue().asReadOnlyByteBuffer().getLong());
                    api.put(put(7,3,73));
                    assertEquals(73,api.get(get(7,3)).getValue().asReadOnlyByteBuffer().getLong());
                    assertEquals(70,api.get(get(7,0)).getValue().asReadOnlyByteBuffer().getLong());
                    assertFalse(api.get(get(7,2)).hasValue());
                } finally { reader.shutdownNow(); }
            }
        } finally { release.countDown(); }
    }

    @Test(timeout=10000) public void wrongTraceAndWrongValueModeAreRejectedBeforeWriting() throws Exception {
        try(var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("binding"),ValueKind.ROW_ID);
                var server=IndexServer.start(0,backend)) {
            backend.bind("original");
            for(var value:List.of(ValueKind.ROW_ID,ValueKind.LOCATOR)) {
                try(var client=new GrpcService("127.0.0.1:"+server.port(),new TraceRequests(value),value==ValueKind.ROW_ID?"foreign":"original",2)) {
                    var response=send(client.factory().open(0), 1,IndexOperationTrace.write(1,IndexOperationTrace.INSERT,1,0,IndexKey.ofInt(7),0,1,0,70)).response().toCompletableFuture();
                    assertThrows(ExecutionException.class,()->response.get(3,TimeUnit.SECONDS));
                }
            }
            assertTrue(backend.scan(ScanRequest.getDefaultInstance()).isEmpty());
        }
    }

    @Test(timeout=10000) public void transportHandoffDoesNotWaitForResponseAndEveryCallHasADeadline() throws Exception {
        var service=new IndexServiceGrpc.IndexServiceImplBase() {
            @Override public void put(PutRequest request,StreamObserver<PutResponse> out) { assertNotNull(Context.current().getDeadline()); }
        };
        var server=ServerBuilder.forPort(0).addService(service).build().start();
        try(var grpc=new GrpcService("127.0.0.1:"+server.getPort(),new TraceRequests(ValueKind.ROW_ID),"deadline",1)) {
            var target=grpc.factory().open(0);
            var handle=send(target, 1,IndexOperationTrace.write(1,IndexOperationTrace.UPSERT,1,0,IndexKey.ofInt(7),1,0,0,70));
            handle.sent().toCompletableFuture().get(2,TimeUnit.SECONDS);
            assertFalse(handle.response().toCompletableFuture().isDone());
            var error=assertThrows(ExecutionException.class,()->handle.response().toCompletableFuture().get(3,TimeUnit.SECONDS));
            assertEquals(io.grpc.Status.Code.DEADLINE_EXCEEDED,io.grpc.Status.fromThrowable(error.getCause()).getCode());
        } finally { server.shutdownNow(); }
    }

    @Test(timeout=10000) public void missingDescriptionFailsBeforeALaterTerminalCallbackCanReplaceTheError() throws Exception {
        var server=ServerBuilder.forPort(0).addService(new IndexServiceGrpc.IndexServiceImplBase() {
            @Override public void put(PutRequest request,StreamObserver<PutResponse> out) { out.onNext(PutResponse.getDefaultInstance()); out.onCompleted(); }
        }).build().start();
        try(var grpc=new GrpcService("127.0.0.1:"+server.getPort(),new TraceRequests(ValueKind.ROW_ID),"identity",2)) {
            var target=grpc.factory().open(0);
            var row=IndexOperationTrace.write(1,IndexOperationTrace.UPSERT,1,0,IndexKey.ofInt(7),1,0,0,70);
            var failed=send(target, 1,row).response().toCompletableFuture();
            var first=assertThrows(ExecutionException.class,()->failed.get(3,TimeUnit.SECONDS));
            assertTrue(first.getCause().getMessage().contains("deployment description"));
            var next=assertThrows(IOException.class,()->send(target, 2,row));
            assertSame(first.getCause(),next.getCause());
        } finally { server.shutdownNow(); }
    }

    @Test(timeout=10000) public void closeCancelsAnUnresponsiveCall() throws Exception {
        var server=ServerBuilder.forPort(0).addService(new IndexServiceGrpc.IndexServiceImplBase() {
            @Override public void put(PutRequest request,StreamObserver<PutResponse> out) {}
        }).build().start();
        try(var grpc=new GrpcService("127.0.0.1:"+server.getPort(),new TraceRequests(ValueKind.ROW_ID),"close",60)) {
            var handle=send(grpc.factory().open(0), 1,IndexOperationTrace.write(1,IndexOperationTrace.UPSERT,1,0,IndexKey.ofInt(7),1,0,0,70));
            handle.sent().toCompletableFuture().get(2,TimeUnit.SECONDS);
            grpc.close();
            assertThrows(ExecutionException.class,()->handle.response().toCompletableFuture().get(2,TimeUnit.SECONDS));
        } finally { server.shutdownNow(); }
    }

    @Test(timeout=10000) public void resolverRetryPolicyCannotRepeatAWriteWithAnUnknownOutcome() throws Exception {
        var calls=new java.util.concurrent.atomic.AtomicInteger();
        var server=ServerBuilder.forPort(0).addService(new IndexServiceGrpc.IndexServiceImplBase() {
            @Override public void put(PutRequest request,StreamObserver<PutResponse> out) {
                calls.incrementAndGet();
                out.onError(io.grpc.Status.UNAVAILABLE.withDescription("Applied, but acknowledgement lost").asRuntimeException());
            }
        }).build().start();
        var provider=new NameResolverProvider() {
            protected boolean isAvailable() { return true; }
            protected int priority() { return 5; }
            public String getDefaultScheme() { return "audit-retry"; }
            public NameResolver newNameResolver(java.net.URI uri,NameResolver.Args args) {
                if (!getDefaultScheme().equals(uri.getScheme())) return null;
                return new NameResolver() {
                    public String getServiceAuthority() { return "localhost"; }
                    public void shutdown() { }
                    public void start(Listener2 listener) {
                        var retry=Map.of("maxAttempts",2.0,"initialBackoff","0.001s","maxBackoff","0.001s",
                                "backoffMultiplier",1.0,"retryableStatusCodes",List.of("UNAVAILABLE"));
                        var config=Map.<String,Object>of("methodConfig",List.of(Map.of("name",List.of(
                                Map.of("service","sibench.index.IndexService","method","Put")),"retryPolicy",retry)));
                        listener.onResult(ResolutionResult.newBuilder().setAddresses(List.of(new EquivalentAddressGroup(
                                new java.net.InetSocketAddress("127.0.0.1",server.getPort()))))
                                .setServiceConfig(args.getServiceConfigParser().parseServiceConfig(config)).build());
                    }
                };
            }
        };
        NameResolverRegistry.getDefaultRegistry().register(provider);
        try (var grpc=new GrpcService("audit-retry:///service",new TraceRequests(ValueKind.ROW_ID),"retry",2)) {
            var operation=IndexOperationTrace.write(1,IndexOperationTrace.UPSERT,1,0,IndexKey.ofInt(7),1,0,0,70);
            assertThrows(ExecutionException.class,()->send(grpc.factory().open(0), 1,operation).response().toCompletableFuture().get(3,TimeUnit.SECONDS));
            assertEquals("An unknown-outcome write must execute only once",1,calls.get());
        } finally { NameResolverRegistry.getDefaultRegistry().deregister(provider); server.shutdownNow(); }
    }

    @Test(timeout=30000) public void scansStreamCompleteOrderedResultsAndRejectTruncation() throws Exception {
        int rows=IndexExecution.CHUNK_RESULTS+4464;
        try(var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("scan"),ValueKind.ROW_ID);
                var server=IndexServer.start(0,backend)) {
            var channel=channel(server.port());
            try {
                var api=stub(channel); var batch=WriteBatchRequest.newBuilder();
                for(int k=0;k<rows;k++) {
                    batch.addMutations(Mutation.newBuilder().setPut(put(k,0,k)));
                    if(batch.getMutationsCount()==256) { api.writeBatch(batch.build()); batch.clear(); }
                }
                if(batch.getMutationsCount()!=0) api.writeBatch(batch.build());
                var chunks=api.scan(ScanRequest.newBuilder().build());
                int next=0,count=0;
                var operation=IndexOperationTrace.search(1,1,0,new IndexOperationTrace.Range(null,false,null,false,false,0),0,rows,0);
                var received=new TraceRequests(ValueKind.ROW_ID).receive(operation,1);
                var truncated=new TraceRequests(ValueKind.ROW_ID).receive(operation,1);
                while(chunks.hasNext()) {
                    var chunk=chunks.next(); count++;
                    for(var result:chunk.getValuesList()) { assertEquals(next++,result.asReadOnlyByteBuffer().getLong()); }
                    received.add(new IndexResponse(IndexStatus.OK, "", chunk.getValuesList(), true));
                }
                assertEquals(rows,next); assertTrue(count>=2);
                var end=IndexResponse.ok(java.util.List.of());
                received.finish(end);
                assertThrows(IOException.class,()->truncated.finish(end));
                var scan=api.scan(ScanRequest.newBuilder().setEndKey(key(10)).setReverse(true).setLimit(3).build());
                var values=new ArrayList<Long>(); while(scan.hasNext()) for(var row:scan.next().getValuesList()) values.add(row.asReadOnlyByteBuffer().getLong());
                assertEquals(List.of(9L,8L,7L),values);
            } finally { channel.shutdownNow(); }
        }
    }

    @Test(timeout=10000) public void genericBatchAcceptsVersionsAndDeletesAndPrevalidatesAllMembers() throws Exception {
        try(var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("batch"),ValueKind.ROW_ID);
                var server=IndexServer.start(0,backend)) {
            var channel=channel(server.port());
            try {
                var api=stub(channel);
                api.writeBatch(WriteBatchRequest.newBuilder().addMutations(Mutation.newBuilder().setPut(put(1,0,11)))
                        .addMutations(Mutation.newBuilder().setPut(put(1,3,13)))
                        .addMutations(Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(key(1)).setTs(4))).build());
                assertEquals(11,api.get(get(1,0)).getValue().asReadOnlyByteBuffer().getLong());
                assertEquals(13,api.get(get(1,3)).getValue().asReadOnlyByteBuffer().getLong());
                assertFalse(api.get(get(1,4)).hasValue());
                for(var invalid:List.of(put(3,-1,3),put(3,1,3).toBuilder().setUnknownFields(unknown()).build())) {
                    var bad=WriteBatchRequest.newBuilder().addMutations(Mutation.newBuilder().setPut(put(9,0,9)))
                            .addMutations(Mutation.newBuilder().setPut(invalid)).build();
                    var error=assertThrows(StatusRuntimeException.class,()->api.writeBatch(bad));
                    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT,error.getStatus().getCode());
                    assertFalse(api.get(get(9,0)).hasValue());
                }
            } finally { channel.shutdownNow(); }
        }
    }

    private static com.google.protobuf.UnknownFieldSet unknown() {
        return com.google.protobuf.UnknownFieldSet.newBuilder().addField(99,
                com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    @Test(timeout=10000) public void opaqueKeysAndEmptyValuesRoundTripAndUnknownFieldsAreRejected() throws Exception {
        try (var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("opaque"),ValueKind.ROW_ID);
             var server=IndexServer.start(0,backend)) {
            var channel=channel(server.port());
            try {
                var api=stub(channel);
                for (var key:List.of(ByteString.EMPTY, ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("ab"),
                        ByteString.copyFrom(new byte[]{0, (byte)255}))) {
                    api.put(PutRequest.newBuilder().setKey(key).setTs(3).build());
                    var result=api.get(GetRequest.newBuilder().setKey(key).setReadTs(3).build());
                    assertTrue(result.hasValue());
                    assertEquals(ByteString.EMPTY,result.getValue());
                    assertFalse(api.get(GetRequest.newBuilder().setKey(key).setReadTs(2).build()).hasValue());
                }
                assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT,assertThrows(StatusRuntimeException.class,
                        ()->api.get(get(1,0).toBuilder().setUnknownFields(unknown()).build())).getStatus().getCode());
                for (var scan:List.of(ScanRequest.newBuilder().setUnknownFields(unknown()).build(),
                        ScanRequest.newBuilder().setLimit(-1).build()))
                    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT,assertThrows(StatusRuntimeException.class,
                            ()->api.scan(scan).hasNext()).getStatus().getCode());
            } finally { channel.shutdownNow(); }
        }
    }

    @Test public void oneBatchReachesTheBackendInOneCall() throws Exception {
        var sizes=new ArrayList<Integer>();
        try(var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("one-call"),ValueKind.ROW_ID)) {
            var wrapped=(IndexBackend)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{IndexBackend.class},(proxy,method,args)->{
                if(method.getName().equals("writeBatch")) sizes.add(((List<?>)args[0]).size());
                try { return method.invoke(backend,args); } catch(java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
            });
            var batch=WriteBatchRequest.newBuilder(); for(int k=1;k<=3;k++) batch.addMutations(Mutation.newBuilder().setPut(put(k,0,k)));
            assertEquals(IndexStatus.OK,IndexExecution.execute(wrapped,batch.build()).status());
            assertEquals(List.of(3),sizes);
        }
    }

    @Test(timeout=15000) public void partialScanFailureNeverBecomesACompleteSuccessfulQuery() throws Exception {
        var fail=new java.util.concurrent.atomic.AtomicBoolean();
        int rows=IndexExecution.CHUNK_RESULTS+5;
        try(var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("partial"),ValueKind.ROW_ID)) {
            var synthetic=(IndexBackend)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{IndexBackend.class},(proxy,method,args)->{
                if(method.getName().equals("scan") && args.length==2) {
                    var sink=(IndexBackend.ValueSink)args[1];
                    for(int k=0;k<rows;k++) {
                        if(fail.get() && k==IndexExecution.CHUNK_RESULTS+1) throw new IOException("failed after a chunk");
                        sink.accept(IndexEntry.rowId(k));
                    }
                    return null;
                }
                try { return method.invoke(backend,args); } catch(java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
            });
            var requests=new TraceRequests(ValueKind.ROW_ID);
            try(var server=IndexServer.start(0,synthetic);var grpc=new GrpcService("127.0.0.1:"+server.port(),requests,"partial",5)) {
                var target=grpc.factory().open(0);
                var query=IndexOperationTrace.search(1,1,0,new IndexOperationTrace.Range(null,false,null,false,false,0),0,rows,0);
                send(target, 1,query).response().toCompletableFuture().get(5,TimeUnit.SECONDS);
                assertEquals(1,requests.queries());
                fail.set(true);
                assertThrows(ExecutionException.class,()->send(target, 2,query).response().toCompletableFuture().get(5,TimeUnit.SECONDS));
                assertEquals(1,requests.queries());
                assertEquals(Map.of("Scan",2L),requests.rpcKinds());
            }
        }
    }

    @Test(timeout=10000) public void scanChunksAreAlsoBoundedByBytes() throws Exception {
        var large=ByteString.copyFrom(new byte[600_000]);
        try(var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("bytes"),ValueKind.ROW_ID)) {
            var synthetic=(IndexBackend)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{IndexBackend.class},(proxy,method,args)->{
                if(method.getName().equals("scan") && args.length==2) {
                    for(int k=0;k<3;k++) ((IndexBackend.ValueSink)args[1]).accept(large);
                    return null;
                }
                try { return method.invoke(backend,args); } catch(java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
            });
            try(var server=IndexServer.start(0,synthetic)) {
                var channel=channel(server.port());
                try {
                    var chunks=stub(channel).scan(ScanRequest.newBuilder().build());
                    int count=0; while(chunks.hasNext()) { assertEquals(1,chunks.next().getValuesCount()); count++; }
                    assertEquals(3,count);
                } finally { channel.shutdownNow(); }
            }
        }
    }
}
