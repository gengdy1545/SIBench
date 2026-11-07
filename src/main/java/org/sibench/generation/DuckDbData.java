package org.sibench.generation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import org.sibench.config.Digests;

/** The sealed native file's inventory and independent, read-only row consumer. */
public final class DuckDbData {
    private DuckDbData() {}

    /** Stored in the enclosing Trace manifest; paths remain relative to its native data directory. */
    public record Inventory(DuckDbDatabase.Identity engine, long bytes, String sha256, long lastResolvedVersion) {
        public Inventory {
            if(engine==null || engine.version()==null || engine.version().isBlank()
                    || engine.sourceId()==null || !engine.sourceId().matches("[0-9a-f]{7,40}")
                    || bytes<=0 || sha256==null || !sha256.matches("[0-9a-f]{64}") || lastResolvedVersion<0)
                throw new IllegalArgumentException("Invalid native data inventory");
        }
    }

    /** Call after the writer process has closed normally; a WAL or spill directory is not a sealed file. */
    public static Inventory inventory(Path directory) throws IOException,SQLException {
        Path file=standaloneFile(directory);
        try(var database=DuckDbDatabase.openReadOnly(directory)) {
            long version=lastVersion(database);
            return new Inventory(DuckDbDatabase.identity(database),Files.size(file),Digests.sha256(file),version);
        }
    }

    public static Reader open(Path directory,Inventory expected) throws IOException,SQLException {
        if(expected==null)throw new IllegalArgumentException("Native data inventory is required");
        Path file=standaloneFile(directory);
        if(Files.size(file)!=expected.bytes())throw new IOException("Native data size differs from inventory");
        if(!Digests.sha256(file).equals(expected.sha256()))throw new IOException("Native data checksum mismatch");
        Connection database=DuckDbDatabase.openReadOnly(directory);
        try {
            if(!DuckDbDatabase.identity(database).equals(expected.engine()))throw new IOException("Native reader build differs from inventory");
            if(lastVersion(database)!=expected.lastResolvedVersion())throw new IOException("Native resolution watermark differs from inventory");
            return new Reader(database,expected.lastResolvedVersion());
        }catch(IOException|SQLException|RuntimeException failure) {
            try{database.close();}catch(SQLException close){failure.addSuppressed(close);}throw failure;
        }
    }

    public static final class Reader implements NativeData {
        private final Connection database;
        private final long lastVersion;
        private Reader(Connection database,long lastVersion){this.database=database;this.lastVersion=lastVersion;}

        public Optional<Map<String,Object>> snapshotRow(int table,long rowId,long readVersion)throws SQLException {
            requireRow(table,rowId);
            if(readVersion<0||readVersion>lastVersion)throw new IllegalArgumentException("Snapshot outside published resolutions");
            return NativeHistory.readRow(database,table,rowId,readVersion,0,0);
        }

        /** Exact statement-start state, including preceding writes of a committed source transaction. */
        public Optional<Map<String,Object>> rowBeforeStatement(int table,long rowId,long connection,long query)throws SQLException {
            requireRow(table,rowId);
            if(connection<0||query<=0)throw new IllegalArgumentException("Invalid native statement identity");
            long transaction,readVersion;boolean localWrites;String outcome;
            try(var statement=database.prepareStatement("SELECT t.transaction_id,t.read_version,t.outcome,p.local_writes "
                    +"FROM sibench_history.read_points p JOIN sibench_history.transactions t USING(transaction_id) "
                    +"WHERE p.connection_id=? AND p.query_id=?")) {
                statement.setLong(1,connection);statement.setLong(2,query);
                try(var row=statement.executeQuery()) {
                    if(!row.next())throw new SQLException("Unknown native statement read point");
                    transaction=row.getLong(1);readVersion=row.getLong(2);outcome=row.getString(3);localWrites=row.getBoolean(4);
                }
            }
            if(outcome.equals("rollback")&&localWrites)
                throw new SQLException("Rolled-back local row contents were not retained; index-only replay remains supported");
            return NativeHistory.readRow(database,table,rowId,readVersion,
                    localWrites?transaction:0,localWrites?query-1:0);
        }

        @Override public void close()throws SQLException {database.close();}
    }

    private static void requireRow(int table,long rowId) {
        if(table<1||table>65535||rowId<=0)throw new IllegalArgumentException("Invalid native row identity");
    }

    private static long lastVersion(Connection database)throws SQLException {
        try(var query=database.createStatement();var rows=query.executeQuery("SELECT complete FROM sibench_history.state")) {
            if(!rows.next()||!rows.getBoolean(1)||rows.next())throw new SQLException("Native history is not sealed");
        }
        try(var query=database.createStatement();var rows=query.executeQuery("SELECT coalesce(max(version),0) FROM sibench_history.transactions")) {
            if(!rows.next())throw new SQLException("Missing native resolution metadata");return rows.getLong(1);
        }
    }

    private static Path standaloneFile(Path directory)throws IOException {
        Path file=DuckDbDatabase.file(directory.toRealPath());
        if(!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))throw new IOException("Missing standalone native data file");
        try(var files=Files.list(file.getParent())) {
            if(files.anyMatch(path->!path.equals(file)))
                throw new IOException("Native data directory still contains a WAL, spill file or unexpected dependency");
        }
        return file;
    }
}
