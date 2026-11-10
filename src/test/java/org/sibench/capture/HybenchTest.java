package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HybenchTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    static Path source(){return Path.of("benchmark/HyBench-2024");}
    static void fixture(Path data)throws Exception {
        Map<String,String> rows=Map.of(
                "customer.csv","1,2,female,Alice,30,123,Province,City,1000,1,1,1,0,2023-01-01,2023-01-02 00:00:00",
                "company.csv","2,Acme,software_IT,10,100000,123,Province,City,1,1,1,0,2023-01-01,2023-01-02 00:00:00",
                "savingAccount.csv","1,1,2000,0,2023-01-01 00:00:00,",
                "checkingAccount.csv","1,1,2000,0,2023-01-01 00:00:00",
                "transfer.csv","10,2,1,50,salary ,2023-01-01 00:00:00,",
                "checking.csv","20,2,1,50,transfer,2023-01-01 00:00:00",
                "loanApps.csv","30,1,100,30,under_review,2023-01-01 00:00:00",
                "loanTrans.csv","40,1,30,100,accept,2023-01-01 00:00:00,30,,0");
        for(var row:rows.entrySet())Files.writeString(data.resolve(row.getKey()),row.getValue()+"\n");
    }
    private static long number(Connection db,String sql)throws SQLException {
        try(var query=db.createStatement();var result=query.executeQuery(sql)) {
            assertTrue(result.next());long value=result.getLong(1);assertFalse(result.next());return value;
        }
    }
    @Test public void loadsEightTablesPreservesNullsAndStartsSequencesAfterLoadedKeys()throws Exception {
        Path data=temporary.newFolder().toPath();fixture(data);
        try(var db=DriverManager.getConnection("jdbc:duckdb:")) {
            var counts=Hybench.load(db,source(),data);
            assertEquals(8,counts.size());assertTrue(counts.values().stream().allMatch(n->n==1));
            for(var count:counts.entrySet())
                assertEquals(count.getValue().longValue(),number(db,"SELECT count(*) FROM "+count.getKey()));
            assertEquals(1,number(db,"SELECT count(*) FROM transfer WHERE fresh_ts IS NULL"));
            assertEquals(0,number(db,"SELECT count(*) FROM transfer WHERE type='salary'"));
            assertEquals(1,number(db,"SELECT count(*) FROM transfer WHERE type='salary '"));
            for(String table:new String[]{"transfer","checking","loanapps","loantrans"}) {
                long max=number(db,"SELECT max(id) FROM "+table);
                try(var q=db.createStatement()){q.executeUpdate("INSERT INTO "+table+"(id) VALUES(DEFAULT)");}
                assertEquals(max+1,number(db,"SELECT max(id) FROM "+table));
            }
            assertEquals(1,number(db,"SELECT count(*) FROM transfer WHERE id=11 AND fresh_ts IS NOT NULL"));
            assertEquals(13,number(db,"SELECT count(*) FROM duckdb_indexes()"));
            var catalog=Hybench.catalog(source());assertEquals(21,catalog.size());
            var type=catalog.stream().filter(i->i.name().equals("idx_transfer_3")).findFirst().orElseThrow();
            assertEquals("varchar",type.type());assertFalse(type.encode("salary").equals(type.encode("salary ")));
            assertTrue(db.getAutoCommit());
        }
    }
    @Test public void malformedCsvRollsBackTheLoadWithoutDroppingInput()throws Exception {
        Path data=temporary.newFolder().toPath();fixture(data);
        Files.writeString(data.resolve("loanTrans.csv"),"40,1,30,100,accept,2023-01-01 00:00:00,30,,0,extra\n");
        try(var db=DriverManager.getConnection("jdbc:duckdb:")) {
            assertThrows(SQLException.class,()->Hybench.load(db,source(),data));
            assertEquals(0,number(db,"SELECT count(*) FROM duckdb_tables() WHERE NOT internal"));
            assertTrue(db.getAutoCommit());assertTrue(Files.exists(data.resolve("customer.csv")));
        }
    }

}
