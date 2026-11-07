package org.sibench.capture;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TreeMap;

/** Native completion counters map to Trace versions; statement snapshots remain independent under RC. */
final class PostgresVersions implements AutoCloseable {
    private final Connection database;
    private final TreeMap<Long,Long> recent=new TreeMap<>();
    private final java.util.Map<Long,Long> pendingCommits=new java.util.HashMap<>();
    private final PreparedStatement commit,transaction,readPoint,older;
    private long version,previousCsn;
    private int pendingStatements,pendingTransactions;

    PostgresVersions(Connection database)throws SQLException {
        this.database=database;
        // Empty-table statistics must not cache full scans as the private mapping tables grow.
        sql("SET LOCAL enable_seqscan=off");
        sql("ALTER TABLE sibench_history.commits ADD COLUMN source_csn BIGINT");
        sql("CREATE UNIQUE INDEX sibench_commit_csn ON sibench_history.commits(source_csn)");
        sql("CREATE UNLOGGED TABLE sibench_history.transactions(version BIGINT PRIMARY KEY,source_transaction BIGINT NOT NULL,"
                +"source_csn BIGINT NOT NULL,outcome VARCHAR NOT NULL)");
        sql("CREATE UNLOGGED TABLE sibench_history.read_points(connection_id BIGINT NOT NULL,query_id BIGINT NOT NULL,"
                +"version BIGINT NOT NULL,read_version BIGINT NOT NULL,local_writes BOOLEAN NOT NULL,PRIMARY KEY(connection_id,query_id))");
        commit=database.prepareStatement("INSERT INTO sibench_history.commits VALUES(?,?,?)");
        transaction=database.prepareStatement("INSERT INTO sibench_history.transactions VALUES(?,?,?,?)");
        readPoint=database.prepareStatement("INSERT INTO sibench_history.read_points VALUES(?,?,?,?,?)");
        older=database.prepareStatement("SELECT commit_version FROM sibench_history.commits WHERE source_csn<=? ORDER BY source_csn DESC LIMIT 1");
    }

    long begin(PostgresTransactions.Chunk chunk)throws SQLException {
        if(chunk.csn<previousCsn)throw new SQLException("PG completion order moved backwards");
        previousCsn=chunk.csn;return ++version;
    }
    long readVersion(long csn)throws SQLException {
        if(csn<=0)throw new SQLException("PG statement has no native snapshot counter");
        var floor=recent.floorEntry(csn);
        if(floor!=null)return floor.getValue();
        older.setLong(1,csn);
        try(var rows=older.executeQuery()){return rows.next()?rows.getLong(1):0;}
    }
    long commitVersion(long transaction,long persisted){return pendingCommits.getOrDefault(transaction,persisted);}
    void statement(int pid,long query,long readVersion,boolean localWrites)throws SQLException {
        if(readVersion>=version)throw new SQLException("PG statement sees its own or a future Trace resolution");
        readPoint.setLong(1,pid);readPoint.setLong(2,query);readPoint.setLong(3,version);
        readPoint.setLong(4,readVersion);readPoint.setBoolean(5,localWrites);readPoint.addBatch();
        if(++pendingStatements==1024){readPoint.executeBatch();pendingStatements=0;}
    }
    void finish(PostgresTransactions.Chunk chunk)throws SQLException {
        transaction.setLong(1,version);transaction.setLong(2,chunk.transaction);transaction.setLong(3,chunk.csn);
        transaction.setString(4,chunk.committed?"commit":"rollback");transaction.addBatch();
        if(++pendingTransactions==1024){transaction.executeBatch();pendingTransactions=0;}
        if(chunk.committed&&chunk.transaction>0) {
            if(pendingCommits.put(chunk.transaction,version)!=null)throw new SQLException("Duplicate PG writer transaction");
            commit.setLong(1,chunk.transaction);commit.setLong(2,version);commit.setLong(3,chunk.csn);commit.addBatch();
            if(pendingCommits.size()==1024){commit.executeBatch();pendingCommits.clear();}
            if(recent.put(chunk.csn,version)!=null)throw new SQLException("Duplicate PG writer completion counter");
            if(recent.size()>65536)recent.pollFirstEntry();
        }
    }
    long version(){return version;}
    private void sql(String text)throws SQLException {try(var query=database.createStatement()){query.execute(text);}}
    @Override public void close()throws SQLException {
        if(!pendingCommits.isEmpty())commit.executeBatch();
        if(pendingStatements>0)readPoint.executeBatch();
        if(pendingTransactions>0)transaction.executeBatch();
        commit.close();transaction.close();readPoint.close();older.close();
    }
}
