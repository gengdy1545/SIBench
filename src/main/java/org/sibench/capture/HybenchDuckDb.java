package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.schema.SourceSchema;

/** Loads upstream HyBench CSVs into native tables; sequences start after the actual loaded keys. */
public final class HybenchDuckDb {
    private HybenchDuckDb() {}

    /** Separate JVM owns upstream globals and relative Data_<sf> paths. */
    public static void main(String[] args)throws Exception {
        if(args.length!=4||!List.of("generate","run").contains(args[0]))
            throw new IllegalArgumentException("HybenchDuckDb generate|run PROPERTIES STATEMENTS SUMMARY");
        Path resolved=Path.of(args[1]),statements=Path.of(args[2]);
        NativeBuildCheck.requireLoadedHybench(statements.getParent().getParent());
        var properties=new java.util.Properties();try(var file=Files.newInputStream(resolved)){properties.load(file);}
        Class.forName("com.hybench.ConfigLoader").getField("prop").set(null,properties);
        if(args[0].equals("generate")) {
            Object generator=Class.forName("com.hybench.load.DataGenerator_RiskControlling")
                    .getConstructor(String.class).newInstance(properties.getProperty("sf"));
            invoke(generator,"dataGenerator");return;
        }
        Object benchmark=Class.forName("com.hybench.HyBench").getConstructor().newInstance();
        Object reader=Class.forName("com.hybench.workload.SqlReader").getConstructor(String.class).newInstance(statements.toString());
        Object sql=invoke(reader,"loader");
        var setter=benchmark.getClass().getDeclaredMethod("setSqls",sql.getClass());setter.setAccessible(true);setter.invoke(benchmark,sql);
        var verbose=benchmark.getClass().getDeclaredField("verbose");verbose.setAccessible(true);verbose.set(benchmark,false);
        Object hist=invoke(invoke(benchmark,"getRes"),"getHist");var report=new LinkedHashMap<String,Object>();
        for(String phase:List.of("ap","tp","xp")) {
            if(phase.equals("xp")) {
                try{benchmark.getClass().getMethod("runXP",int.class).invoke(benchmark,0);}
                catch(java.lang.reflect.InvocationTargetException error){throw cause(error);}
            }else invoke(benchmark,phase.equals("ap")?"runAP":"runTP");
            Class.forName("com.hybench.workload.Client").getMethod("checkFailure").invoke(null);
            var counts=new LinkedHashMap<String,Long>();
            for(String group:phase.equals("xp")?List.of("iq","at"):List.of(phase)) {
                String method=switch(group){case "ap"->"getAPItem";case "tp"->"getTPItem";case "iq"->"getXPIQItem";default->"getXPATItem";};
                for(int i=0;i<(group.equals("ap")?13:group.equals("tp")?18:6);i++) {
                    Object item=hist.getClass().getMethod(method,int.class).invoke(hist,i);
                    counts.put(group+(i+1),((Number)invoke(item,"getN")).longValue());
                }
            }
            if(counts.values().stream().mapToLong(Long::longValue).sum()==0)throw new IOException("HyBench phase made no progress: "+phase);
            report.put(phase,counts);
        }
        Files.writeString(Path.of(args[3]),new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(report),java.nio.file.StandardOpenOption.CREATE_NEW);
    }
    private static Object invoke(Object object,String method)throws Exception {
        try{return object.getClass().getMethod(method).invoke(object);}
        catch(java.lang.reflect.InvocationTargetException error){throw cause(error);}
    }
    private static Exception cause(java.lang.reflect.InvocationTargetException error) {
        if(error.getCause() instanceof Exception cause)return cause;
        if(error.getCause() instanceof Error cause)throw cause;return error;
    }

    public static Map<String,Long> load(Connection database,Path source,Path data)throws Exception {
        if(!database.getAutoCommit())throw new SQLException("HyBench load requires an idle connection");
        var schema=SourceSchema.hybench(source);
        var inputs=new LinkedHashMap<String,Path>();
        for(String line:Files.readAllLines(source.resolve("conf/load_data_pg.sql"))) {
            if(line.isBlank())continue;
            String[] pair=HybenchInputs.copyStatement(line);
            if(pair==null)throw new IOException("Unsupported upstream load statement: "+line);
            String table=pair[0].toLowerCase(java.util.Locale.ROOT);
            Path file=data.resolve(pair[1]).toRealPath();
            if(inputs.put(table,file)!=null)throw new IOException("Duplicate HyBench load table: "+table);
        }
        if(!inputs.keySet().equals(schema.tables().stream().map(SourceSchema.Table::name)
                .collect(java.util.stream.Collectors.toSet())))throw new IOException("HyBench load tables differ from schema");
        var counts=new LinkedHashMap<String,Long>();
        database.setAutoCommit(false);
        try {
            for(var table:schema.tables()) {
                String primary=table.indexes().stream().filter(SourceSchema.Index::primary).findFirst().orElseThrow().column();
                String columns=table.columns().stream().map(c->quote(c.name())+" "+c.type()
                        +(c.name().equals(primary)?" PRIMARY KEY":c.nullable()?"":" NOT NULL")
                        +(c.defaultValue().kind().equals("current_timestamp")?" DEFAULT CURRENT_TIMESTAMP":""))
                        .collect(java.util.stream.Collectors.joining(","));
                execute(database,"CREATE TABLE "+quote(table.name())+"("+columns+")");
            }
            for(var table:schema.tables()) {
                String columns=table.columns().stream()
                        .map(c->quote(c.name())).collect(java.util.stream.Collectors.joining(","));
                String sql="COPY "+quote(table.name())+"("+columns+") FROM "+literal(inputs.get(table.name()).toString())
                        +" (FORMAT CSV, HEADER false, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '')";
                try(var query=database.createStatement()) {
                    long count=query.executeLargeUpdate(sql);
                    if(count<0)throw new SQLException("Invalid native COPY row count");
                    counts.put(table.name(),count);
                }
            }
            for(var table:schema.tables()) {
                for(var index:table.indexes())if(!index.primary())
                    execute(database,"CREATE INDEX "+quote(index.name())+" ON "+quote(table.name())+"("+quote(index.column())+")");
                for(var column:table.columns())if(column.defaultValue().kind().equals("sequence")) {
                    long next;
                    try(var query=database.createStatement();var result=query.executeQuery("SELECT coalesce(max("
                            +quote(column.name())+"),0)::BIGINT+1 FROM "+quote(table.name()))) {
                        if(!result.next())throw new SQLException("Missing native sequence start");next=result.getLong(1);
                        if(next<=0||result.next())throw new SQLException("Invalid native sequence start");
                    }
                    String name=column.defaultValue().value();
                    execute(database,"CREATE SEQUENCE "+quote(name)+" START "+next);
                    execute(database,"ALTER TABLE "+quote(table.name())+" ALTER COLUMN "+quote(column.name())
                            +" SET DEFAULT nextval("+literal(name)+")");
                }
            }
            execute(database,"ANALYZE");database.commit();
        }catch(Exception|Error failure) {
            try{database.rollback();}catch(SQLException rollback){failure.addSuppressed(rollback);}throw failure;
        }finally {database.setAutoCommit(true);}
        return java.util.Collections.unmodifiableMap(counts);
    }

    /** DuckDB CHAR aliases VARCHAR; preserve its exact native string comparison semantics. */
    public static List<IndexCatalog.Definition> catalog(Path source)throws IOException {
        return IndexCatalog.duckDbDefinitions(SourceSchema.hybench(source));
    }
    private static void execute(Connection database,String sql)throws SQLException {
        try(var query=database.createStatement()){query.execute(sql);}
    }
    private static String quote(String value){return "\""+value.replace("\"","\"\"")+"\"";}
    private static String literal(String value){return "'"+value.replace("'","''")+"'";}
}
