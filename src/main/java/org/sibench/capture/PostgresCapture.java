package org.sibench.capture;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Live PG facts; only transactions awaiting ordered consumption can spill to disk. */
final class PostgresCapture implements AutoCloseable {
    private final Path endpoint;
    private final ServerSocketChannel listener;
    private final ExecutorService readers=Executors.newCachedThreadPool(task->{
        var thread=new Thread(task,"postgres-capture");thread.setDaemon(true);return thread;
    });
    private final Map<Integer,Session> sessions=new HashMap<>();
    private final java.util.Set<SocketChannel> sockets=new java.util.HashSet<>();
    final PostgresTransactions transactions;
    /** Last hold state sent to the sources; only the ordered consumer thread changes it. */
    private boolean held;
    private volatile boolean stopping;
    private Throwable failure;
    private boolean finished;

    private static final class Session {
        final int pid;
        final CompletableFuture<Void> ended=new CompletableFuture<>();
        final PostgresCounts counts=new PostgresCounts();
        long completedSequence,completedCsn,acknowledgedSequence,acknowledgedConsumed,events,bytes;
        String sha256;
        Session(int pid){this.pid=pid;}
    }

    PostgresCapture(Path endpoint,Path scratch,long pendingBytes,int pendingTransactions)throws IOException {
        this.endpoint=endpoint;
        transactions=new PostgresTransactions(scratch,pendingBytes,pendingTransactions);
        listener=ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        try{listener.bind(UnixDomainSocketAddress.of(endpoint));}
        catch(IOException failure){listener.close();transactions.close();throw failure;}
        readers.submit(this::accept);
    }

    private void accept() {
        try {
            while(!stopping) {
                var socket=listener.accept();
                synchronized(this) {
                    if(sockets.size()>=512){socket.close();throw new IOException("Too many live PG capture sessions");}
                    sockets.add(socket);
                }
                readers.submit(()->receive(socket));
            }
        }catch(Throwable error){if(!stopping)fail(error);}
    }

    private void receive(SocketChannel socket) {
        Session session=null;
        try(socket;var input=new BufferedReader(new InputStreamReader(Channels.newInputStream(socket),StandardCharsets.UTF_8),1<<16)) {
            String name=input.readLine();
            if(name==null||!name.matches("pg-[0-9]+\\.jsonl\\.zst"))throw new IOException("Invalid PG source stream name");
            int pid=Integer.parseInt(name.substring(3,name.indexOf('.')));
            session=new Session(pid);
            synchronized(this) {
                if(sessions.putIfAbsent(pid,session)!=null)throw new IOException("Repeated PG source process");
            }
            var stream=new PostgresFacts.Stream(name);
            var digest=org.sibench.config.Digests.digest();
            PostgresTransactions.Chunk chunk=null;
            for(String line;(line=input.readLine())!=null;) {
                var event=PostgresFacts.parse(line);stream.accept(event);session.counts.accept(event);
                byte[] encoded=(line+"\n").getBytes(StandardCharsets.UTF_8);digest.update(encoded);
                session.events++;session.bytes=Math.addExact(session.bytes,encoded.length);
                String type=event.get("event").getAsString();
                if(type.equals("capture_end")) {
                    if(chunk!=null)throw new IOException("PG source ended inside a transaction");
                    stream.finish();transactions.endStream(pid);
                    session.sha256=HexFormat.of().formatHex(digest.digest());
                    // The backend may leave pg_stat_activity as soon as it reads the ACK; publish completion first.
                    session.ended.complete(null);
                    var ack=ByteBuffer.wrap(new byte[]{'E'});while(ack.hasRemaining())socket.write(ack);
                    return;
                }
                if(chunk==null)chunk=transactions.begin(pid);
                chunk.append(line,event,encoded.length);
                if(type.equals("transaction_commit")||type.equals("transaction_abort")) {
                    long sequence=event.get("capture_sequence").getAsLong(),csn=event.get("csn").getAsLong();
                    synchronized(session) {
                        if(sequence!=session.completedSequence+1||csn<session.completedCsn)
                            throw new IOException("PG transaction completion sequence is not continuous");
                        chunk.captureSequence=sequence;
                        transactions.complete(chunk,csn,event.get("xid").getAsLong()!=0,type.equals("transaction_commit"));
                        chunk=null;session.completedSequence=sequence;session.completedCsn=csn;
                    }
                }
            }
            throw new IOException("PG source lost its completion marker: "+name);
        }catch(Throwable error) {
            if(session!=null)session.ended.completeExceptionally(error);
            fail(error);
        }finally{synchronized(this){sockets.remove(socket);notifyAll();}}
    }

    static void install(Connection database)throws SQLException {
        try(var query=database.createStatement()) {
            query.execute("CREATE FUNCTION sibench_history.capture_watermark(integer,bigint,bigint,bigint) RETURNS BIGINT "
                    +"AS '$libdir/row_capture','sibench_capture_watermark' LANGUAGE C VOLATILE");
            query.execute("CREATE FUNCTION sibench_history.capture_hold(boolean) RETURNS void "
                    +"AS '$libdir/row_capture','sibench_capture_hold' LANGUAGE C VOLATILE");
        }
    }

    /** Called by the ordered consumer: tells sources to hold read-only statements while its queue is full. */
    void updateHold(Connection database)throws SQLException {
        boolean full=transactions.full();
        if(full==held)return;
        try(var query=database.prepareStatement("SELECT sibench_history.capture_hold(?)")) {
            query.setBoolean(1,full);query.execute();
        }
        held=full;
    }

    /** Only the ordered consumer uses this connection; readers acknowledge queued facts, never guessed time. */
    long watermark(Connection database)throws SQLException,IOException {
        check();updateHold(database);List<Session> snapshot;
        synchronized(this){snapshot=List.copyOf(sessions.values());}
        long safe=-1;
        try(var query=database.prepareStatement("SELECT sibench_history.capture_watermark(?,?,?,?)")) {
            for(var session:snapshot) {
                long sequence,csn;
                synchronized(session){sequence=session.completedSequence;csn=session.completedCsn;}
                long consumed=transactions.consumed(session.pid);
                if(sequence<=session.acknowledgedSequence&&consumed<=session.acknowledgedConsumed)continue;
                query.setInt(1,session.pid);query.setLong(2,sequence);query.setLong(3,csn);query.setLong(4,consumed);
                try(var rows=query.executeQuery()){if(!rows.next())throw new SQLException("Missing capture watermark");safe=rows.getLong(1);}
                session.acknowledgedSequence=sequence;
                session.acknowledgedConsumed=consumed;
            }
            if(safe<0) {
                query.setInt(1,0);query.setLong(2,0);query.setLong(3,0);query.setLong(4,0);
                try(var rows=query.executeQuery()){if(!rows.next())throw new SQLException("Missing capture watermark");safe=rows.getLong(1);}
            }
        }
        return safe;
    }

    /** Called after all source clients exit. PG's closing ACK makes backend exit a received-facts boundary. */
    void finishSources(Connection control)throws Exception {
        awaitSources(control);
        finishSources();
    }

    void awaitSources(Connection control)throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        try(var query=control.createStatement()) {
            query.setQueryTimeout(5);
            while(true) {
                check();
                try(var rows=query.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        +"AND pid<>pg_backend_pid() AND application_name LIKE 'sibench:%'")) {
                    if(!rows.next())throw new SQLException("Missing source session count");
                    if(rows.getLong(1)==0)break;
                }
                if(System.nanoTime()>=deadline)throw new IOException("PG source close exceeded 30 seconds");
                Thread.sleep(20);
            }
        }
        check();
    }

    void finishSources()throws Exception {
        List<Session> snapshot;
        synchronized(this){check();snapshot=List.copyOf(sessions.values());}
        for(var session:snapshot)session.ended.get(5,TimeUnit.SECONDS);
        finished=true;transactions.finish();
    }

    synchronized List<Map<String,Object>> reports()throws IOException {
        check();if(!finished)throw new IOException("PG sources are not drained");
        return completedSessions();
    }

    synchronized List<Map<String,Object>> completedSessions()throws IOException {
        check();
        var reports=new ArrayList<Map<String,Object>>();
        for(var session:sessions.values()) {
            if(!session.ended.isDone())throw new IOException("PG source has not closed: "+session.pid);
            reports.add(Map.of("process",session.pid,"events",session.events,
                    "bytes",session.bytes,"sha256",session.sha256,"counts",session.counts.report()));
        }
        return reports;
    }

    synchronized void check()throws IOException {if(failure!=null)throw new IOException("Direct PostgreSQL capture failed",failure);}
    synchronized void fail(Throwable error) {
        if(failure!=null)return;
        failure=error;transactions.fail(error);
        for(var socket:sockets)try{socket.close();}catch(IOException close){error.addSuppressed(close);}
    }

    @Override public void close()throws Exception {
        stopping=true;listener.close();
        if(!finished)fail(new IOException("PG capture closed before source completion"));
        synchronized(this){for(var socket:sockets)socket.close();}
        readers.shutdownNow();
        if(!readers.awaitTermination(5,TimeUnit.SECONDS))fail(new IOException("PG capture readers did not stop"));
        if(failure==null)transactions.close();else transactions.retainFailure();
        Files.deleteIfExists(endpoint);
    }
}
