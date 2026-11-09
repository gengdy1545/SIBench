package org.sibench.generation;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** One fresh native cluster; failures retain data and diagnostics, and shutdown never deletes it. */
public final class PostgresDatabase implements AutoCloseable {
    private final Path runtime, root, data, socket;
    private final int port;
    private boolean stopped;

    public static PostgresDatabase create(Path runtime,Path root)throws IOException,InterruptedException {
        return create(runtime,root,null);
    }

    public static PostgresDatabase create(Path runtime,Path root,Path captureSocket)throws IOException,InterruptedException {
        return new PostgresDatabase(runtime.toRealPath(),root.toAbsolutePath().normalize(),captureSocket);
    }

    private PostgresDatabase(Path runtime,Path root,Path captureSocket)throws IOException,InterruptedException {
        this.runtime=runtime;this.root=root;this.data=root.resolve("data");
        if(Files.exists(root))throw new IOException("PostgreSQL output already exists: "+root);
        Files.createDirectories(root.getParent());Files.createDirectory(root);
        socket=Files.createTempDirectory("sibench-pg-");
        try(var reserved=new ServerSocket(0,1,InetAddress.getLoopbackAddress())) {port=reserved.getLocalPort();}
        try {
            command("initdb",List.of("-D",data.toString(),"--no-locale","--encoding=UTF8","--auth-local=trust",
                    "--auth-host=reject","--username=sibench"),Map.of(),60);
            // Only the private loopback port accepts the dedicated local benchmark role.
            Files.writeString(data.resolve("pg_hba.conf"),"local all sibench trust\nhost all sibench 127.0.0.1/32 trust\n");
            Files.createDirectory(root.resolve("capture"));
            Files.writeString(data.resolve("postgresql.auto.conf"),"listen_addresses = '127.0.0.1'\nport = "+port
                    +"\nunix_socket_directories = '"+socket.toString().replace("'","''")+"'\n"
                    +"shared_buffers = '256MB'\nwork_mem = '32MB'\nmax_parallel_workers_per_gather = 0\n"
                    +"max_parallel_maintenance_workers = 0\n"
                    +"wal_level = minimal\nmax_wal_senders = 0\n"
                    +"fsync = off\nsynchronous_commit = off\nfull_page_writes = off\n"
                    +"compute_query_id = on\nautovacuum = off\n");
            command("pg_ctl",List.of("-D",data.toString(),"-l",root.resolve("server.log").toString(),"-w","-t","30","start"),
                    captureSocket==null?Map.of("SIBENCH_CAPTURE_DIR",root.resolve("capture").toString())
                            :Map.of("SIBENCH_CAPTURE_SOCKET",captureSocket.toAbsolutePath().toString()),35);
        }catch(IOException|InterruptedException|RuntimeException failure) {
            // pg_ctl may time out after the server starts; preserve the first setup error.
            if(Files.exists(data.resolve("postmaster.pid")))try {
                command("pg_ctl",List.of("-D",data.toString(),"-m","fast","-w","-t","10","stop"),Map.of(),15);
            }catch(IOException|InterruptedException cleanup){failure.addSuppressed(cleanup);}
            if(!Files.exists(data.resolve("postmaster.pid")))try{Files.deleteIfExists(socket);}
            catch(IOException cleanup){failure.addSuppressed(cleanup);}
            if(failure instanceof InterruptedException)Thread.currentThread().interrupt();
            throw failure;
        }
    }

    public Connection connect(String application)throws SQLException {
        if(stopped)throw new SQLException("PostgreSQL cluster is stopped");
        var properties=new Properties();properties.setProperty("user","sibench");
        properties.setProperty("ApplicationName",application);properties.setProperty("connectTimeout","10");
        properties.setProperty("reWriteBatchedInserts","true");
        return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:"+port+"/postgres",properties);
    }

    /** Upstream loaders create their own tables; select persistence while those tables are empty. */
    public static void unloggedTables(Connection database)throws SQLException {
        try(var statement=database.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION public.sibench_unlogged_tables() RETURNS event_trigger
                    LANGUAGE plpgsql AS $$
                    DECLARE command record;
                    BEGIN
                      FOR command IN
                        SELECT d.object_identity FROM pg_event_trigger_ddl_commands() d
                        JOIN pg_class c ON d.classid = 'pg_class'::regclass AND d.objid = c.oid
                        WHERE d.schema_name = 'public' AND c.relkind = 'r' AND c.relpersistence = 'p'
                      LOOP
                        EXECUTE 'ALTER TABLE ' || command.object_identity || ' SET UNLOGGED';
                      END LOOP;
                    END $$
                    """);
            statement.execute("CREATE EVENT TRIGGER sibench_unlogged_tables ON ddl_command_end"
                    +" WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION public.sibench_unlogged_tables()");
        }
    }

    public static void finishLoad(Connection database)throws SQLException {
        try(var statement=database.createStatement()) {
            statement.execute("DROP EVENT TRIGGER sibench_unlogged_tables");
            statement.execute("DROP FUNCTION public.sibench_unlogged_tables()");
        }
    }

    public Path data(){return data;}
    public Path capture(){return root.resolve("capture");}
    public int port(){return port;}

    private void command(String executable,List<String> arguments,Map<String,String> environment,int seconds)
            throws IOException,InterruptedException {
        var command=new ArrayList<String>();command.add(runtime.resolve("bin/"+executable).toString());command.addAll(arguments);
        var builder=new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(root.resolve(executable+".log").toFile()));
        builder.environment().remove("LD_PRELOAD");builder.environment().remove("LD_LIBRARY_PATH");
        builder.environment().remove("SIBENCH_CAPTURE_DIR");builder.environment().remove("SIBENCH_CAPTURE_DETAIL");
        builder.environment().remove("SIBENCH_CAPTURE_SOCKET");
        builder.environment().putAll(environment);
        var process=builder.start();
        try {
            if(!process.waitFor(seconds,TimeUnit.SECONDS))throw new IOException(executable+" timed out; see "+root);
            if(process.exitValue()!=0)throw new IOException(executable+" exited with "+process.exitValue()+"; see "+root);
        }finally {if(process.isAlive()){process.destroyForcibly();process.waitFor(5,TimeUnit.SECONDS);}}
    }

    @Override public void close()throws IOException,InterruptedException {
        if(stopped)return;
        command("pg_ctl",List.of("-D",data.toString(),"-m","fast","-w","-t","30","stop"),Map.of(),35);
        stopped=true;Files.deleteIfExists(socket);
    }
}
