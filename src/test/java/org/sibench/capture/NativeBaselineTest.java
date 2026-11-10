package org.sibench.capture;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.generation.DuckDbDatabase;
import org.sibench.generation.NativeHistory;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayProjection;

public class NativeBaselineTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static void sql(Connection db,String text)throws Exception {try(var query=db.createStatement()){query.execute(text);}}
    @Test public void keepsEachRowsIndexesTogetherAcrossTablesAndClientBlocks()throws Exception {
        try(var db=DuckDbDatabase.create(temporary.getRoot().toPath().resolve("data"))) {
            sql(db,"CREATE TABLE first(id BIGINT PRIMARY KEY,k BIGINT)");
            sql(db,"INSERT INTO first SELECT range,range%3 FROM range(1,4097)");
            sql(db,"CREATE INDEX first_k ON first(k)");
            sql(db,"CREATE TABLE second(id BIGINT PRIMARY KEY)");sql(db,"INSERT INTO second VALUES(99)");
            db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main","first","id");NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,2,"main","second","id");
            db.commit();
            var catalog=List.of(new IndexCatalog.Definition(1,"first",0,"first_primary","id","int"),
                    new IndexCatalog.Definition(1,"first",1,"first_k","k","int"),
                    new IndexCatalog.Definition(2,"second",0,"second_primary","id","int"));
            long[] counts=new long[2];
            var config=new ReplayConfig(true,0,0,0,0,"operation",2,16,2);
            try(var replay=new ReplayProjection(config,false,operation->{
                assertEquals(operation.table()==1?1:2,operation.workUnitId());
                counts[operation.table()-1]++;
            })) {
                assertEquals(java.util.Map.of("first",4096L,"second",1L),NativeBaseline.write(db,org.sibench.config.Engine.DUCKDB,catalog,replay));
                replay.finish();replay.requireClientCoverage();
            }
            assertArrayEquals(new long[]{8192,1},counts);
        }
    }
}
