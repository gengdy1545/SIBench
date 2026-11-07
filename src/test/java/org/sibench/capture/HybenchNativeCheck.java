package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.After;
import org.sibench.adapters.IndexExecutor;
import org.sibench.adapters.VersionedIndexBackend;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.TraceRecord;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.generation.DuckDbDatabase;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.DuckDbHostDriver;
import org.sibench.generation.DuckDbHostFixture;

/** Explicit native 8,706-row source test; requires sibench.hybench.build and sibench.duckdb.host. */
public class HybenchNativeCheck extends DuckDbHostFixture {
    public HybenchNativeCheck() { timeout=org.junit.rules.Timeout.seconds(60); }
    private Path retainedOutput;
    @After public void retainNativeDatabase()throws Exception {
        if(retainedOutput==null||host==null)return;
        stop();host=null;
        Files.copy(errors,retainedOutput.resolve("host.log"));
        Path data=Files.createDirectory(retainedOutput.resolve("data"));
        Files.move(DuckDbDatabase.file(root.resolve("data")),DuckDbDatabase.file(data));
    }
    private static void sql(Connection database,String text)throws Exception {
        try(var query=database.createStatement()){query.execute(text);}
    }
    private static long number(Connection database,String text)throws Exception {
        try(var query=database.createStatement();var row=query.executeQuery(text)) {
            assertTrue(row.next());long value=row.getLong(1);assertFalse(row.next());return value;
        }
    }
    private static String classpath(Path repository,Path build,Path parameters)throws Exception {
        return String.join(java.io.File.pathSeparator,List.of(parameters.toString(),
                repository.resolve("build/sibench/test-classes").toString(),repository.resolve("build/sibench/classes").toString(),
                repository.resolve("build/sibench/sibench.jar").toString(),
                Path.of(org.junit.Assert.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString(),
                Path.of(org.hamcrest.SelfDescribing.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString(),
                build.resolve("target/classes").toString(),build.resolve("lib/*").toString()));
    }
    @Test public void rejectsFailedNativePhaseAndClosesItsTransaction()throws Exception {
        Path repository=Path.of("").toRealPath(),build=Path.of(System.getProperty("sibench.hybench.build")).toRealPath();
        String retained=System.getProperty("sibench.hybench.output");
        Path output=Files.createDirectory(retained==null?root.resolve("failure"):Path.of(retained+"-failure"));
        retainedOutput=output;
        Path inputs=Files.createDirectory(output.resolve("Data_1x"));
        for(String name:List.of("Related_transfer_bids","Related_checking_bids"))
            try(var stream=new java.io.ObjectOutputStream(Files.newOutputStream(inputs.resolve(name)))) {
                stream.writeObject(new ArrayList<Integer>());
            }
        Files.createDirectory(output.resolve("clients"));
        try(var observer=connect()) {
            Path raw=output.resolve("native.csv");
            sql(observer,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    +"'SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path='"+raw+"')");
            var command=List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx256m","-cp",
                    classpath(repository,build,output),getClass().getName(),"failure",repository.toString(),build.toString(),socket.toString());
            var result=NativeProcess.run("failure",command,Map.of("SIBENCH_BUDGET_STOP",output.resolve("stop").toString()),
                    output,output,Duration.ofSeconds(10));
            assertEquals(Files.readString(result.err())+result.stdout(),0,result.exit());
            sql(observer,"CALL disable_logging()");
            List<Path> receipts;try(var files=Files.list(output.resolve("clients"))){receipts=files.sorted().toList();}
            var sessions=DuckDbReceipts.load(observer,receipts,output.resolve("receipts"));
            DuckDbFacts.load(observer,raw);DuckDbStatements.bind(observer,sessions);DuckDbReceipts.bind(observer);
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_statements WHERE outcome='failed'"));
            assertEquals(1,number(observer,"SELECT count(*) FROM "+DuckDbFacts.table("transaction")+" WHERE event='rollback'"));
        }
    }
    @Test public void preservesInputKeyGroupingAndPaysEveryEmployee()throws Exception {
        Path repository=Path.of("").toRealPath(),build=Path.of(System.getProperty("sibench.hybench.build")).toRealPath();
        String retained=System.getProperty("sibench.hybench.output");
        Path output=Files.createDirectory(retained==null?root.resolve("dialect"):Path.of(retained+"-dialect"));
        retainedOutput=output;
        var command=List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx256m","-cp",
                classpath(repository,build,output),getClass().getName(),"dialect",repository.toString(),build.toString(),socket.toString());
        var result=NativeProcess.run("dialect",command,Map.of(),output,output,Duration.ofSeconds(10));
        assertEquals(Files.readString(result.err())+result.stdout(),0,result.exit());
    }
    @Test public void rollsBackWriteConflictAndContinuesWithoutLosingPriorOperations()throws Exception {
        Path repository=Path.of("").toRealPath(),build=Path.of(System.getProperty("sibench.hybench.build")).toRealPath();
        Path output=Files.createDirectory(root.resolve("conflict"));
        var command=List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx256m","-cp",
                classpath(repository,build,output),getClass().getName(),"conflict",repository.toString(),build.toString(),socket.toString());
        try(var observer=connect()) {
            Path raw=output.resolve("native.csv");
            sql(observer,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    +"'SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path='"+raw+"')");
            var result=NativeProcess.run("conflict",command,Map.of(),output,output,Duration.ofSeconds(10));
            assertEquals(Files.readString(result.err())+result.stdout(),0,result.exit());
            sql(observer,"CALL disable_logging()");
            DuckDbFacts.load(observer,raw);
            assertEquals(1,number(observer,"SELECT count(*) FROM "+DuckDbFacts.table("transaction")+" WHERE event='rollback'"));
            assertTrue(number(observer,"SELECT count(*) FROM "+DuckDbFacts.table("mutation")+" m JOIN "
                    +DuckDbFacts.table("transaction")+" t USING(connection_id,transaction_id) WHERE t.event='rollback'")>0);
        }
    }
    private ArrayList<String> prepareInputs(Path repository,Path build,Path source,Path output)throws Exception {
        Path parameters=Files.createDirectory(output.resolve("parameters"));
        var settings=org.sibench.workloads.HybenchParameters.read(source,Map.of("sf","1x","parameters",
                Map.of("customer_number",300,"company_number",2,"transfer_number",6000,
                        "checking_number",600,"loanapps_number",600,"loantrans_number",600)));
        Files.writeString(parameters.resolve("parameters.toml"),new com.moandjiezana.toml.TomlWriter().write(Map.of("1x",settings)));
        var base=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx256m","-cp",
                classpath(repository,build,parameters),getClass().getName()));
        String reused=System.getProperty("sibench.hybench.inputs");
        if(reused==null) {
            var generate=new ArrayList<>(base);generate.addAll(List.of("generate",repository.toString(),build.toString(),socket.toString()));
            var generated=NativeProcess.run("generate",generate,Map.of(),output,output,Duration.ofSeconds(10));
            assertEquals(Files.readString(generated.err())+generated.stdout(),0,generated.exit());
        }else {
            Path inputs=Path.of(reused).toRealPath(),destination=Files.createDirectory(output.resolve("Data_1x"));
            try(var files=Files.list(inputs)){for(Path file:files.toList())Files.copy(file,destination.resolve(file.getFileName()));}
            Files.writeString(output.resolve("reused-inputs.txt"),inputs+"\n");
        }
        return base;
    }
    @Test public void runsNativeApTpAndConcurrentXpPhases()throws Exception {
        Path repository=Path.of("").toRealPath(),source=repository.resolve("benchmark/HyBench-2024");
        Path build=Path.of(System.getProperty("sibench.hybench.build")).toRealPath();
        String retained=System.getProperty("sibench.hybench.output");
        Path output=Files.createDirectory(retained==null?root.resolve("phases"):Path.of(retained+"-phases"));
        retainedOutput=output;
        var base=prepareInputs(repository,build,source,output);
        try(var observer=connect()) {
            assertEquals(8706,HybenchDuckDb.load(observer,source,output.resolve("Data_1x")).values().stream().mapToLong(Long::longValue).sum());
            observer.setAutoCommit(false);NativeHistory.initialize(observer, org.sibench.config.Engine.DUCKDB);
            for(var index:HybenchDuckDb.catalog(source))if(index.indexId()==0)
                NativeHistory.install(observer, org.sibench.config.Engine.DUCKDB,index.tableId(),"main",index.table(),index.column());
            observer.commit();observer.setAutoCommit(true);
            Path raw=output.resolve("native.csv");
            sql(observer,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    +"'SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path='"+raw+"')");
            base.addAll(List.of("phases",repository.toString(),build.toString(),socket.toString()));
            var result=NativeProcess.run("phases",base,Map.of("SIBENCH_BUDGET_STOP",output.resolve("stop").toString()),
                    output,output,Duration.ofSeconds(50));
            assertEquals(Files.readString(result.err())+result.stdout(),0,result.exit());
            sql(observer,"CALL disable_logging()");
            List<Path> receipts;try(var files=Files.walk(output.resolve("clients"))){receipts=files.filter(Files::isRegularFile).sorted().toList();}
            assertEquals(4,receipts.size());
            for(Path receipt:receipts)DuckDbReceipts.read(receipt,(connection,call)->assertEquals("Failed SQL submission "+call,0,call.errorCode()));
            var sessions=DuckDbReceipts.load(observer,receipts,output.resolve("receipts"));
            DuckDbFacts.load(observer,raw);DuckDbStatements.bind(observer,sessions);DuckDbReceipts.bind(observer);
            DuckDbTransactions.bind(observer,"data");DuckDbIdentities.bind(observer);
            assertEquals(0,number(observer,"SELECT count(*) FROM sibench_statements WHERE outcome<>'completed'"));
            assertTrue(number(observer,"SELECT count(*) FROM "+DuckDbFacts.table("transaction")+" WHERE event='commit'")>0);
            assertTrue(number(observer,"SELECT count(*) FROM "+DuckDbFacts.table("transaction")+" WHERE event='rollback'")>0);
        }
    }
    @Test public void generatesLoadsAndRunsAllBusinessMethodsThroughCaptureAndReplay()throws Exception {
        Path repository=Path.of("").toRealPath();
        String configured=System.getProperty("sibench.hybench.build");assertNotNull("Set sibench.hybench.build",configured);
        Path build=Path.of(configured).toRealPath(),source=repository.resolve("benchmark/HyBench-2024");
        String retained=System.getProperty("sibench.hybench.output");
        Path output=Files.createDirectory(retained==null?root.resolve("hybench"):Path.of(retained));
        retainedOutput=output;
        var base=prepareInputs(repository,build,source,output);
        try(var observer=connect()) {
            var loaded=HybenchDuckDb.load(observer,source,output.resolve("Data_1x"));
            assertEquals(8706,loaded.values().stream().mapToLong(Long::longValue).sum());
            Files.writeString(output.resolve("loaded.json"),new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(loaded));
            var catalog=HybenchDuckDb.catalog(source);
            observer.setAutoCommit(false);NativeHistory.initialize(observer, org.sibench.config.Engine.DUCKDB);
            for(var index:catalog)if(index.indexId()==0)NativeHistory.install(observer, org.sibench.config.Engine.DUCKDB,index.tableId(),"main",index.table(),index.column());
            observer.commit();observer.setAutoCommit(true);
            var baseline=output.resolve("baseline.pb.zst");var baseWriter=new ProtoTrace.Writer(baseline);
            observer.setAutoCommit(false);
            try(baseWriter){assertEquals(loaded,NativeBaseline.write(observer,org.sibench.config.Engine.DUCKDB,catalog,baseWriter));}
            observer.commit();observer.setAutoCommit(true);
            Path raw=output.resolve("native.csv");
            sql(observer,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    +"'SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path='"+raw+"')");
            Files.createDirectory(output.resolve("clients"));
            var workload=new ArrayList<>(base);workload.addAll(List.of("workload",repository.toString(),build.toString(),socket.toString()));
            var result=NativeProcess.run("workload",workload,Map.of(),output,output,Duration.ofSeconds(20));
            assertEquals(Files.readString(result.err())+result.stdout(),0,result.exit());
            sql(observer,"CALL disable_logging()");
            List<Path> receipts;try(var files=Files.list(output.resolve("clients"))){receipts=files.sorted().toList();}
            for(Path receipt:receipts)DuckDbReceipts.read(receipt,(connection,call)->assertEquals("Failed SQL submission "+call,0,call.errorCode()));
            var sessions=DuckDbReceipts.load(observer,receipts,output.resolve("receipts"));
            DuckDbFacts.load(observer,raw);DuckDbStatements.bind(observer,sessions);DuckDbReceipts.bind(observer);
            DuckDbTransactions.bind(observer,"data");DuckDbIdentities.bind(observer);DuckDbAccesses.bind(observer,catalog);DuckDbMutations.bind(observer);
            assertEquals(0,number(observer,"SELECT count(*) FROM sibench_statements WHERE outcome<>'completed'"));
            assertTrue(number(observer,"SELECT count(*) FROM sibench_accesses")>0);
            assertTrue(number(observer,"SELECT count(*) FROM sibench_changes")>0);
            var expected=new HashMap<Long,HashSet<Long>>();
            record Observed(long record,int table,long read,long local,long query,String key) {}
            var observed=new ArrayList<Observed>();
            try(var q=observer.createStatement();var rows=q.executeQuery("SELECT a.raw_record,a.table_id,a.read_version,a.version,a.query_id,r.key_sql FROM sibench_accesses a "
                    +"LEFT JOIN sibench_access_keys r USING(access_id) WHERE NOT EXISTS (SELECT 1 FROM sibench_changes c "
                    +"WHERE c.transaction_id=a.transaction_id AND c.query_id<a.query_id) ORDER BY a.raw_record")) {
                while(rows.next()) {
                    observed.add(new Observed(rows.getLong(1),rows.getInt(2),rows.getLong(3),
                            rows.getLong(4),rows.getLong(5),rows.getString(6)));
                }
            }
            for(var row:observed) {
                var ids=expected.computeIfAbsent(row.record(),ignored->new HashSet<>());
                if(row.key()!=null)assertTrue(ids.add(DuckDbIdentities.observedRow(observer,row.table(),row.key(),
                        row.read(),row.local(),row.query())));
            }
            var trace=output.resolve("workload.pb.zst");var writer=new ProtoTrace.Writer(trace);
            try(writer){DuckDbProjection.write(observer,writer);}
            long[] ids={0},searches={0},matched={0};
            try(var backend=VersionedIndexBackend.create(output.resolve("index"),ValueKind.ROW_ID);var index=new IndexExecutor(backend)) {
                var consumer=new IndexOperationTrace.Consumer(){
                    @Override public void accept(IndexOperationTrace.Operation operation){fail("Missing stored request");}
                    @Override public void accept(TraceRecord record,List<IndexOperationTrace.Operation> operations)throws java.io.IOException {
                        if(record.hasBegin())return;
                        if(record.hasEnd())return;
                        try {
                            var request=record.getRequest().toBuilder().setId(++ids[0]).build();
                            var response=index.submit(request).get(5,TimeUnit.SECONDS);assertEquals(response.getMessage(),Status.OK,response.getStatus());
                            if(request.hasSearch()||request.hasGroup()) {
                                var wanted=expected.get(operations.get(0).sourceEvent());assertNotNull(wanted);
                                var actual=new HashSet<Long>();for(var row:response.getResultsList())assertTrue(actual.add(row.getValue().getRowId()));
                                assertEquals(wanted,actual);searches[0]++;matched[0]+=actual.size();
                            }
                        }catch(Exception failure){throw new java.io.IOException(failure);}
                    }
                };
                ProtoTrace.read(baseline,baseWriter.sha256(),baseWriter.logicalSha256(),consumer);
                ProtoTrace.read(trace,writer.sha256(),writer.logicalSha256(),consumer);
            }
            assertEquals(expected.size(),searches[0]);
            Files.writeString(output.resolve("summary.json"),new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(
                    Map.of("rows",8706,"business_methods",43,"searches",searches[0],"verified_row_ids",matched[0],"requests",ids[0],
                            "baseline_sha256",baseWriter.sha256(),"workload_sha256",writer.sha256())));
        }
    }

    /** Separate JVM keeps upstream global configuration and its relative data paths isolated. */
    public static void main(String[] args)throws Exception {
        Path repository=Path.of(args[1]),build=Path.of(args[2]),socket=Path.of(args[3]);
        String commit=org.sibench.config.ConfigValues.string(org.sibench.config.ConfigValues.object(
                org.sibench.config.ConfigValues.parse(Files.readString(repository.resolve("config/benchmark/hybench.yaml"))),"source"),"commit");
        NativeBuildCheck.verify(repository,"hybench",repository.resolve("benchmark/HyBench-2024"),build,commit);
        var properties=new Properties();properties.setProperty("sf","1x");properties.setProperty("db","duckdb");
        properties.setProperty("contention_num","4");properties.setProperty("risk_rate","0.1");
        properties.setProperty("tpclient","0");properties.setProperty("apclient","0");
        properties.setProperty("xtpclient","0");properties.setProperty("xapclient","0");
        properties.setProperty("url",DuckDbHostDriver.PREFIX+socket);properties.setProperty("url_ap",DuckDbHostDriver.PREFIX+socket);
        properties.setProperty("sibench.receipts",Path.of("clients").toAbsolutePath().toString());
        Class.forName("com.hybench.ConfigLoader").getField("prop").set(null,properties);
        if(args[0].equals("generate")) {
            Object generator=Class.forName("com.hybench.load.DataGenerator_RiskControlling").getConstructor(String.class).newInstance("1x");
            generator.getClass().getMethod("dataGenerator").invoke(generator);return;
        }
        Object reader=Class.forName("com.hybench.workload.SqlReader").getConstructor(String.class).newInstance(build.resolve("conf/stmt_postgres.toml").toString());
        Object sqls=reader.getClass().getMethod("loader").invoke(reader);
        Class<?> client=Class.forName("com.hybench.workload.Client");
        if(args[0].equals("phases")) {
            for(String name:List.of("apclient","tpclient","xapclient","xtpclient","apRunMins","tpRunMins","xpRunMins"))
                properties.setProperty(name,"1");
            Object benchmark=Class.forName("com.hybench.HyBench").getConstructor().newInstance();
            var setSqls=benchmark.getClass().getDeclaredMethod("setSqls",sqls.getClass());setSqls.setAccessible(true);setSqls.invoke(benchmark,sqls);
            var verbose=benchmark.getClass().getDeclaredField("verbose");verbose.setAccessible(true);verbose.set(benchmark,false);
            Object result=benchmark.getClass().getMethod("getRes").invoke(benchmark);
            Object hist=result.getClass().getMethod("getHist").invoke(result);
            Path stop=Path.of(System.getenv("SIBENCH_BUDGET_STOP"));
            for(String phase:List.of("ap","tp","xp")) {
                Path receipts=Files.createDirectories(Path.of("clients",phase).toAbsolutePath());
                properties.setProperty("sibench.receipts",receipts.toString());
                var watchFailure=new java.util.concurrent.atomic.AtomicReference<Throwable>();
                Thread watch=new Thread(()-> {
                    try {
                        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
                        while(!phaseProgress(hist,phase)) {
                            client.getMethod("checkFailure").invoke(null);
                            if(System.nanoTime()>deadline)throw new AssertionError("No native phase progress: "+phase);
                            Thread.sleep(5);
                        }
                    }catch(Throwable failure){watchFailure.set(failure);}
                    finally {
                        try{Files.writeString(stop,"bounded-native-phase\n");}
                        catch(Exception failure){watchFailure.compareAndSet(null,failure);}
                    }
                });
                watch.start();
                try {
                    if(phase.equals("xp"))benchmark.getClass().getMethod("runXP",int.class).invoke(benchmark,0);
                    else benchmark.getClass().getMethod(phase.equals("ap")?"runAP":"runTP").invoke(benchmark);
                }finally {watch.join(16000);assertFalse(watch.isAlive());}
                if(watchFailure.get()!=null)throw new AssertionError("Phase watcher failed",watchFailure.get());
                assertTrue(phaseProgress(hist,phase));
                System.out.println("PASS native phase "+phase);
                Files.delete(stop);
            }
            return;
        }
        if(args[0].equals("failure")) {
            properties.setProperty("apclient","1");properties.setProperty("apRunMins","1");
            Object benchmark=Class.forName("com.hybench.HyBench").getConstructor().newInstance();
            var setSqls=benchmark.getClass().getDeclaredMethod("setSqls",sqls.getClass());
            setSqls.setAccessible(true);setSqls.invoke(benchmark,sqls);
            // Bound the old implementation too: it swallowed every failed SQL and returned success.
            Thread stop=new Thread(()-> {
                try {Thread.sleep(1000);Files.writeString(Path.of(System.getenv("SIBENCH_BUDGET_STOP")),"test-stop\n");}
                catch(Exception error){throw new RuntimeException(error);}
            });
            stop.start();
            try {
                var failure=assertThrows(java.lang.reflect.InvocationTargetException.class,
                        ()->benchmark.getClass().getMethod("runAP").invoke(benchmark));
                Throwable cause=failure;
                while(cause.getCause()!=null)cause=cause.getCause();
                assertTrue(cause.toString(),cause instanceof java.sql.SQLException);
                assertTrue(cause.getMessage(),cause.getMessage().contains("does not exist"));
            }finally {stop.join(2000);assertFalse(stop.isAlive());}
            return;
        }
        if(args[0].equals("conflict")) {
            try(var connection=DuckDbHostDriver.open(socket);var blocker=DuckDbHostDriver.open(socket)) {
                sql(connection,"CREATE TABLE savingaccount(accountid INTEGER PRIMARY KEY,balance DOUBLE)");
                sql(connection,"INSERT INTO savingaccount VALUES(1,1000)");
                sql(connection,"CREATE TABLE loantrans(id INTEGER PRIMARY KEY,applicantid INTEGER,duration INTEGER,"
                        +"amount DOUBLE,timestamp TIMESTAMP,status VARCHAR,contract_timestamp TIMESTAMP)");
                sql(connection,"INSERT INTO loantrans VALUES(1,1,30,100,'2023-01-01','lent',NULL)");
                sql(connection,"CREATE TABLE prior_write(id INTEGER PRIMARY KEY)");
                Object worker=Class.forName("com.hybench.workload.TPClient").getConstructor().newInstance();
                client.getMethod("setSqls",sqls.getClass()).invoke(worker,sqls);
                var config=client.getDeclaredField("CR");config.setAccessible(true);
                config.set(worker,Class.forName("com.hybench.load.ConfigReader").getConstructor(String.class).newInstance("1x"));
                var hist=client.getDeclaredField("hist");hist.setAccessible(true);
                hist.set(worker,Class.forName("com.hybench.stats.Histogram").getConstructor().newInstance());
                blocker.setAutoCommit(false);sql(blocker,"UPDATE savingaccount SET balance=balance+1 WHERE accountid=1");
                connection.setAutoCommit(false);sql(connection,"INSERT INTO prior_write VALUES(1)");
                Object result=worker.getClass().getMethod("execTxn16",Connection.class).invoke(worker,connection);
                assertEquals(false,result.getClass().getMethod("isSucceed").invoke(result));
                var error=result.getClass().getDeclaredField("errorMsg");error.setAccessible(true);
                assertTrue(error.get(result).toString().contains("Conflict on"));
                client.getMethod("checkFailure").invoke(null);
                assertEquals(0,number(connection,"SELECT count(*) FROM prior_write"));
                connection.commit();blocker.commit();
                result=worker.getClass().getMethod("execTxn16",Connection.class).invoke(worker,connection);
                assertEquals(true,result.getClass().getMethod("isSucceed").invoke(result));
                assertEquals(901,number(connection,"SELECT balance FROM savingaccount WHERE accountid=1"));
                connection.commit();client.getMethod("checkFailure").invoke(null);
            }
            return;
        }
        if(args[0].equals("dialect")) {
            try(var connection=DuckDbHostDriver.open(socket)) {
                sql(connection,"CREATE TABLE customer(custid INTEGER PRIMARY KEY,companyid INTEGER,name VARCHAR)");
                sql(connection,"INSERT INTO customer VALUES(1,3,'Alice'),(2,3,'Bob')");
                sql(connection,"CREATE SEQUENCE transfer_id_seq START 4");
                sql(connection,"CREATE TABLE transfer(id INTEGER PRIMARY KEY DEFAULT nextval('transfer_id_seq'),"
                        +"sourceid INTEGER,targetid INTEGER,amount DOUBLE,type VARCHAR,timestamp TIMESTAMP,fresh_ts TIMESTAMP DEFAULT current_timestamp)");
                sql(connection,"INSERT INTO transfer(id,sourceid,targetid,amount,timestamp) VALUES"
                        +"(1,2,1,10,'2023-11-01'),(2,2,1,20,'2023-11-01'),(3,1,2,5,'2023-11-01')");
                String text=(String)sqls.getClass().getMethod("ap_q10").invoke(sqls);
                var revenue=new HashMap<String,Double>();
                try(var query=connection.createStatement();var rows=query.executeQuery(text)) {
                    while(rows.next())revenue.put(rows.getString(2),rows.getDouble(3));
                }
                // Grouping by the output target ID instead would incorrectly return Alice=25.
                assertEquals(Map.of("Alice",10.0,"Bob",-25.0),revenue);
                sql(connection,"CREATE TABLE savingaccount(accountid INTEGER PRIMARY KEY,balance DOUBLE)");
                sql(connection,"INSERT INTO savingaccount VALUES(1,1000),(2,1000),(3,1000)");
                Object worker=Class.forName("com.hybench.workload.TPClient").getConstructor().newInstance();
                client.getMethod("setSqls",sqls.getClass()).invoke(worker,sqls);
                for(String name:List.of("customer_no","company_no")) {
                    var field=worker.getClass().getDeclaredField(name);field.setAccessible(true);field.set(worker,name.equals("customer_no")?3:1);
                }
                var config=client.getDeclaredField("CR");config.setAccessible(true);
                config.set(worker,Class.forName("com.hybench.load.ConfigReader").getConstructor(String.class).newInstance("1x"));
                var hist=client.getDeclaredField("hist");hist.setAccessible(true);
                hist.set(worker,Class.forName("com.hybench.stats.Histogram").getConstructor().newInstance());
                Object result=worker.getClass().getMethod("execTxn10",Connection.class).invoke(worker,connection);
                assertEquals(true,result.getClass().getMethod("isSucceed").invoke(result));
                assertEquals(2,number(connection,"SELECT count(DISTINCT targetid) FROM transfer WHERE sourceid=3 AND type='salary' AND fresh_ts IS NOT NULL"));
                assertEquals(3000,number(connection,"SELECT round(sum(balance)) FROM savingaccount"));
                sql(connection,"ALTER TABLE savingaccount ADD COLUMN isblocked INTEGER DEFAULT 0");
                sql(connection,"UPDATE savingaccount SET balance=balance+0.5");
                sql(connection,"CREATE TABLE checkingaccount(accountid INTEGER PRIMARY KEY,balance DOUBLE,isblocked INTEGER)");
                sql(connection,"INSERT INTO checkingaccount VALUES(1,1000.5,0),(2,1000.5,0),(3,1000.5,0)");
                for(String method:List.of("execTxn17","execTxn18")) {
                    Object converted=worker.getClass().getMethod(method,Connection.class).invoke(worker,connection);
                    assertEquals(method,true,converted.getClass().getMethod("isSucceed").invoke(converted));
                }
                assertEquals(6003,number(connection,"SELECT round((SELECT sum(balance) FROM savingaccount)+(SELECT sum(balance) FROM checkingaccount))"));
                var integer=client.getDeclaredMethod("integer",java.sql.ResultSet.class,int.class);integer.setAccessible(true);
                try(var query=connection.createStatement();var rows=query.executeQuery("SELECT 1.9::DOUBLE,-1.9::DOUBLE,NULL::DOUBLE,1e20::DOUBLE")) {
                    assertTrue(rows.next());assertEquals(1,integer.invoke(null,rows,1));assertEquals(-1,integer.invoke(null,rows,2));
                    assertEquals(0,integer.invoke(null,rows,3));
                    var failure=assertThrows(java.lang.reflect.InvocationTargetException.class,()->integer.invoke(null,rows,4));
                    assertTrue(failure.getCause() instanceof java.sql.SQLDataException);
                    assertFalse(rows.next());
                }
            }
            return;
        }
        Object ap=client.getMethod("initTask",Properties.class,String.class,int.class).invoke(null,properties,"APClient",0);
        Object tp=client.getMethod("initTask",Properties.class,String.class,int.class).invoke(null,properties,"TPClient",1);
        for(Object worker:List.of(ap,tp)) {
            client.getMethod("setSqls",sqls.getClass()).invoke(worker,sqls);
            var hist=client.getDeclaredField("hist");hist.setAccessible(true);
            hist.set(worker,Class.forName("com.hybench.stats.Histogram").getConstructor().newInstance());
        }
        for(String phase:List.of("Q","Txn","IQ","AT")) {
            Object worker=phase.equals("Q")||phase.equals("IQ")?ap:tp;
            int count=phase.equals("Q")?13:phase.equals("Txn")?18:6;
            try(var connection=(Connection)Class.forName("com.hybench.dbconn.ConnectionMgr").getMethod("getConnection").invoke(null)) {
                for(int i=1;i<=count;i++) {
                    String method="exec"+phase+i;System.out.println("START "+method);
                    Object result=worker.getClass().getMethod(method,Connection.class).invoke(worker,connection);
                    assertEquals("Failed HyBench business method "+method,true,result.getClass().getMethod("isSucceed").invoke(result));
                    System.out.println("PASS "+method);
                }
            }
        }
    }
    private static boolean phaseProgress(Object hist,String phase)throws Exception {
        List<String> groups=phase.equals("ap")?List.of("getAPItem"):phase.equals("tp")?List.of("getTPItem"):List.of("getXPIQItem","getXPATItem");
        for(String group:groups) {
            long total=0;int size=phase.equals("ap")?13:phase.equals("tp")?18:6;
            for(int i=0;i<size;i++) {
                Object item=hist.getClass().getMethod(group,int.class).invoke(hist,i);
                total+=((Number)item.getClass().getMethod("getN").invoke(item)).longValue();
            }
            if(total<(phase.equals("ap")?13:phase.equals("tp")?20:10))return false;
        }
        return true;
    }
}
