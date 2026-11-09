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
    private final PreparedStatement commit,older;
    private long version,previousCsn;

    PostgresVersions(Connection database)throws SQLException {
        this.database=database;
        // Empty-table statistics must not cache full scans as the private mapping tables grow.
        sql("SET LOCAL enable_seqscan=off");
        sql("ALTER TABLE sibench_history.commits ADD COLUMN source_csn BIGINT");
        sql("CREATE UNIQUE INDEX sibench_commit_csn ON sibench_history.commits(source_csn)");
        commit=database.prepareStatement("INSERT INTO sibench_history.commits VALUES(?,?,?)");
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
    void finish(PostgresTransactions.Chunk chunk)throws SQLException {
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
        commit.close();older.close();
    }
}
