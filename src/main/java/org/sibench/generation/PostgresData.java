package org.sibench.generation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import org.sibench.config.Digests;

/** Inventory of a normally stopped native PostgreSQL cluster, without exporting its rows. */
public final class PostgresData {
    private PostgresData() {}
    public record File(long bytes,String sha256) {
        public File {
            if(bytes<0||sha256==null||!sha256.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("Invalid PG file identity");
        }
    }
    public record Inventory(Map<String,String> runtime,Map<String,File> files,long lastResolvedVersion) {
        public Inventory {
            if(runtime==null||files==null||lastResolvedVersion<0||files.isEmpty())throw new IllegalArgumentException("Invalid PG inventory");
            runtime=Map.copyOf(runtime);files=Map.copyOf(files);
            if(!runtime.keySet().equals(java.util.Set.of("bin/postgres","lib/row_capture.so"))
                    ||runtime.values().stream().anyMatch(hash->!hash.matches("[0-9a-f]{64}")))
                throw new IllegalArgumentException("Invalid PG runtime identity");
            for(String name:files.keySet()) {
                Path path=Path.of(name);
                if(path.isAbsolute()||!path.normalize().equals(path)||path.startsWith(".."))
                    throw new IllegalArgumentException("Invalid PG inventory path");
            }
        }
    }
    public static Inventory inventory(Path data,Path runtime,long lastResolvedVersion)throws IOException {
        if(Files.exists(data.resolve("postmaster.pid"))||!Files.isRegularFile(data.resolve("global/pg_control"))
                ||!Files.readString(data.resolve("PG_VERSION")).strip().equals("14"))
            throw new IOException("PG cluster is not a stopped PostgreSQL 14 data directory");
        var files=new TreeMap<String,File>();
        try(var paths=Files.walk(data)) {
            for(Path path:paths.sorted().toList()) {
                if(Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))continue;
                if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("External or special PG data dependency: "+path);
                files.put(data.relativize(path).toString(),new File(Files.size(path),Digests.sha256(path)));
            }
        }
        return new Inventory(runtimeIdentity(runtime),files,lastResolvedVersion);
    }
    static Map<String,String> runtimeIdentity(Path runtime)throws IOException {
        var identity=new TreeMap<String,String>();
        for(String name:List.of("bin/postgres","lib/row_capture.so"))identity.put(name,Digests.sha256(runtime.resolve(name)));
        return identity;
    }

    /** PG needs writable control/WAL files: read an isolated copy and leave the published cluster untouched. */
    public static NativeData open(Path directory,Inventory expected,Path runtime,Path working)throws IOException,SQLException {
        directory=directory.toRealPath();working=working.toAbsolutePath().normalize();
        Path ancestor=working;
        while(!Files.exists(ancestor))ancestor=ancestor.getParent();
        working=ancestor.toRealPath().resolve(ancestor.relativize(working)).normalize();
        if(working.startsWith(directory)||directory.startsWith(working))throw new IOException("PG reader working directory overlaps published data");
        if(Files.exists(working))throw new IOException("PG reader working directory already exists");
        if(expected==null||!runtimeIdentity(runtime).equals(expected.runtime()))throw new IOException("PG reader runtime differs from inventory");
        if(expected==null||!inventory(directory,runtime,expected.lastResolvedVersion()).equals(expected))
            throw new IOException("PG data/runtime differs from its published inventory");
        PostgresDatabase cluster;
        try{cluster=PostgresDatabase.copy(runtime,working,directory);}
        catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}
        Connection database=null;
        try {
            database=cluster.connect("data-reader");
            try(var query=database.createStatement();var rows=query.executeQuery("SELECT complete FROM sibench_history.state")) {
                if(!rows.next()||!rows.getBoolean(1)||rows.next())throw new SQLException("PG history is not sealed");
            }
            try(var query=database.createStatement();var rows=query.executeQuery("SELECT coalesce(max(version),0) FROM sibench_history.transactions")) {
                if(!rows.next()||rows.getLong(1)!=expected.lastResolvedVersion())throw new SQLException("PG resolution watermark differs from inventory");
            }
            return new Reader(database,cluster,working,expected.lastResolvedVersion());
        }catch(SQLException|RuntimeException failure) {
            if(database!=null)try{database.close();}catch(SQLException close){failure.addSuppressed(close);}
            try{cluster.close();}catch(Exception close){if(close instanceof InterruptedException)Thread.currentThread().interrupt();failure.addSuppressed(close);}
            throw failure;
        }
    }

    private static final class Reader implements NativeData {
        private final Connection database;
        private final PostgresDatabase cluster;
        private final Path working;
        private final long lastVersion;
        private boolean closed;
        Reader(Connection database,PostgresDatabase cluster,Path working,long lastVersion) {
            this.database=database;this.cluster=cluster;this.working=working;this.lastVersion=lastVersion;
        }
        @Override public Optional<Map<String,Object>> snapshotRow(int table,long rowId,long readVersion)throws SQLException {
            requireRow(table,rowId);
            if(readVersion<0||readVersion>lastVersion)throw new IllegalArgumentException("Snapshot outside published resolutions");
            return NativeHistory.readRow(database,table,rowId,readVersion,0,0);
        }
        @Override public Optional<Map<String,Object>> rowBeforeStatement(int table,long rowId,long connection,long query)throws SQLException {
            requireRow(table,rowId);
            if(connection<=0||query<=0)throw new IllegalArgumentException("Invalid native statement identity");
            long transaction,readVersion;boolean local;String outcome;
            try(var statement=database.prepareStatement("SELECT t.source_transaction,p.read_version,t.outcome,p.local_writes "
                    +"FROM sibench_history.read_points p JOIN sibench_history.transactions t USING(version) "
                    +"WHERE p.connection_id=? AND p.query_id=?")) {
                statement.setLong(1,connection);statement.setLong(2,query);
                try(var row=statement.executeQuery()) {
                    if(!row.next())throw new SQLException("Unknown native statement read point");
                    transaction=row.getLong(1);readVersion=row.getLong(2);outcome=row.getString(3);local=row.getBoolean(4);
                }
            }
            if(outcome.equals("rollback")&&local)
                throw new SQLException("Rolled-back local row contents were not retained; index-only replay remains supported");
            return NativeHistory.readRow(database,table,rowId,readVersion,local?transaction:0,local?query-1:0);
        }
        @Override public void close()throws SQLException {
            if(closed)return;SQLException failure=null;
            try{database.close();}catch(SQLException close){failure=close;}
            try{cluster.close();}
            catch(Exception close) {
                if(close instanceof InterruptedException)Thread.currentThread().interrupt();
                if(failure==null)failure=new SQLException("Cannot close native PG reader",close);else failure.addSuppressed(close);
            }
            if(failure!=null)throw failure;
            closed=true;
            try(var paths=Files.walk(working)) {
                for(Path path:paths.sorted(java.util.Comparator.reverseOrder()).toList())Files.delete(path);
            }catch(IOException close){throw new SQLException("Cannot remove closed PG reader copy",close);}
        }
    }
    private static void requireRow(int table,long rowId) {
        if(table<1||table>65535||rowId<=0)throw new IllegalArgumentException("Invalid native row identity");
    }
}
