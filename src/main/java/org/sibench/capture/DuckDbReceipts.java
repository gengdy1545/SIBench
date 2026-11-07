package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** SQL client submission receipts; returned SQL row counts are not index result counts. */
public final class DuckDbReceipts {
    private static final String HEADER="event,operation,kind,connection_id,after_query,error_code,rows";
    public record Call(long operation,String kind,int errorCode,long rows) {}
    @FunctionalInterface public interface Consumer { void accept(long connection,Call call) throws IOException; }
    private DuckDbReceipts() {}

    public static DuckDbStatements.Session read(Path file,Consumer consumer) throws IOException {
        try(var input=Files.newBufferedReader(file)) {
            if(!HEADER.equals(input.readLine())) throw new IOException("Invalid SQL receipt header");
            String[] open=record(input.readLine());
            if(!open[0].equals("open")||!open[1].equals("0")||!open[2].equals("session")
                    ||!open[5].equals("0")||!open[6].equals("0")) throw new IOException("Invalid SQL session open");
            long connection=number(open[3]),after=number(open[4]),count=0;
            while(true) {
                String[] begin=record(input.readLine());
                if(begin[0].equals("close")) {
                    if(number(begin[1])!=count||!begin[2].equals("session")||!begin[3].isEmpty()||!begin[4].isEmpty()
                            ||!begin[5].equals("0")||!begin[6].equals("0")||input.readLine()!=null)
                        throw new IOException("SQL close count or trailing records disagree");
                    return new DuckDbStatements.Session(connection,after,count);
                }
                if(!begin[0].equals("begin")||number(begin[1])!=count+1||!List.of("query","rollback").contains(begin[2])
                        ||!begin[3].isEmpty()||!begin[4].isEmpty()||!begin[5].equals("0")||!begin[6].equals("0"))
                    throw new IOException("Missing or invalid SQL submission begin");
                String[] end=record(input.readLine());
                if(!end[0].equals("end")||!end[1].equals(begin[1])||!end[2].equals(begin[2])
                        ||!end[3].isEmpty()||!end[4].isEmpty()) throw new IOException("Mismatched SQL submission end");
                long error=number(end[5]);
                if(error>Integer.MAX_VALUE) throw new IOException("SQL error code exceeds int32");
                long rows;
                if(error==0) rows=number(end[6]);
                else {
                    if(!end[6].equals("-1")) throw new IOException("Failed SQL submission requires unknown row count");
                    rows=-1;
                }
                consumer.accept(connection,new Call(++count,begin[2],(int)error,rows));
            }
        }
    }

    /** Import validated calls once, retaining normalized metadata for diagnostics in a fresh directory. */
    public static List<DuckDbStatements.Session> load(Connection database,List<Path> files,Path staging)
            throws IOException,SQLException {
        if(!database.getAutoCommit()) throw new SQLException("Receipt import requires an idle connection");
        if(files.isEmpty()) throw new IOException("SQL client receipts are required");
        Files.createDirectory(staging);Path calls=staging.resolve("calls.csv");
        var sessions=new ArrayList<DuckDbStatements.Session>();var connections=new HashSet<Long>();
        try(var output=Files.newBufferedWriter(calls)) {
            output.write("connection_id,operation,kind,error_code,returned_rows\n");
            for(Path file:files) {
                var session=read(file,(connection,call)->output.write(connection+","+call.operation()+","+call.kind()+","
                        +call.errorCode()+","+call.rows()+"\n"));
                if(!connections.add(session.connection())) throw new IOException("Repeated SQL source connection");
                sessions.add(session);
            }
        }
        try(var query=database.prepareStatement("CREATE TEMP TABLE sibench_sql_calls AS SELECT * FROM read_csv(?,"
                +"header=true,auto_detect=false,parallel=true,delim=',',strict_mode=true,null_padding=false,"
                +"columns={'connection_id':'UBIGINT','operation':'UBIGINT','kind':'VARCHAR','error_code':'INTEGER','returned_rows':'BIGINT'})")) {
            query.setString(1,calls.toRealPath().toString());query.execute();
        }
        return List.copyOf(sessions);
    }

    /** Bind statements first. A submitted SQL string may contain several native statements. */
    public static void bind(Connection database) throws SQLException {
        if(!database.getAutoCommit()) throw new SQLException("SQL receipt binding requires an idle connection");
        database.setAutoCommit(false);
        try {
            execute(database,"CREATE TEMP TABLE sibench_sql_submissions AS SELECT c.*,q.root_query,q.outcome,q.sql_text "
                    +"FROM temp.main.sibench_sql_calls c FULL OUTER JOIN (SELECT *,row_number() OVER(PARTITION BY connection_id "
                    +"ORDER BY root_query) AS operation FROM temp.main.sibench_statements WHERE submission_end) q "
                    +"USING(connection_id,operation)");
            reject(database,"SELECT 1 FROM temp.main.sibench_sql_submissions WHERE root_query IS NULL OR kind IS NULL "
                    +"OR (error_code=0 AND outcome NOT IN ('completed','abandoned')) OR (error_code<>0 AND outcome<>'failed') "
                    +"OR (kind='rollback' AND NOT regexp_full_match(trim(sql_text),'(?i)ROLLBACK;?')) LIMIT 1",
                    "SQL receipts disagree with native submissions");
            reject(database,"SELECT 1 FROM temp.main.sibench_sql_submissions GROUP BY connection_id,operation "
                    +"HAVING count(*)<>1 LIMIT 1","Duplicate SQL submission receipt");
            execute(database,"CREATE TEMP VIEW sibench_sql_statements AS SELECT s.*,r.operation,r.kind AS submission_kind,"
                    +"r.error_code,r.returned_rows FROM temp.main.sibench_statements s JOIN temp.main.sibench_sql_submissions r "
                    +"USING(connection_id,root_query)");
            database.commit();
        } catch(SQLException|RuntimeException failure) {
            try{database.rollback();}catch(SQLException rollback){failure.addSuppressed(rollback);}throw failure;
        } finally {database.setAutoCommit(true);}
    }

    private static String[] record(String line) throws IOException {
        if(line==null) throw new IOException("Truncated SQL receipt");
        String[] fields=line.split(",",-1);
        if(fields.length!=7) throw new IOException("Unexpected SQL receipt fields");
        return fields;
    }
    private static long number(String value) throws IOException {
        if(!value.matches("0|[1-9][0-9]*")) throw new IOException("Invalid SQL receipt integer");
        try{return Long.parseLong(value);}catch(NumberFormatException failure){throw new IOException("SQL receipt integer exceeds int64",failure);}
    }
    private static void execute(Connection database,String sql) throws SQLException {
        try(var query=database.createStatement()){query.execute(sql);}
    }
    private static void reject(Connection database,String sql,String message) throws SQLException {
        try(var query=database.createStatement();var rows=query.executeQuery(sql)){if(rows.next())throw new SQLException(message);}
    }
}
