package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.sibench.generation.DuckDbDatabase;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.DuckDbHostFixture;

/** Explicit native integration: sibench.duckdb.host and sibench.sysbench.build are required. */
public class SysbenchNativeCheck extends DuckDbHostFixture {
    private Path build() throws Exception {
        String configured = System.getProperty("sibench.sysbench.build");
        assertNotNull("Set sibench.sysbench.build", configured);
        Path repository = Path.of("").toRealPath(), build = Path.of(configured).toRealPath();
        var source = object(parse(Files.readString(repository.resolve("config/benchmark/sysbench.yaml"))), "source");
        NativeBuildCheck.verify(repository, "sysbench", repository.resolve(string(source, "path")), build, string(source, "commit"));
        return build;
    }

    private Path output(String name) throws Exception {
        String retained = System.getProperty("sibench.sysbench.output");
        return Files.createDirectory(retained == null ? root.resolve(name) : Path.of(retained + "-" + name));
    }

    private static long number(Connection database, String sql) throws Exception {
        try (var statement = database.createStatement(); var rows = statement.executeQuery(sql)) {
            assertTrue(rows.next()); long result = rows.getLong(1); assertFalse(rows.next()); return result;
        }
    }

    private static void execute(Connection database, String sql) throws Exception {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }

    private void enable(Connection database, Path output) throws Exception {
        execute(database, "CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                + "'SibenchIndex','SibenchIndexRows','SibenchConsumption'], storage_path='"
                + output.resolve("native.csv").toString().replace("'", "''") + "')");
    }

    private NativeProcess.Result run(Path build, Path script, Path stage, int threads, int events, String action)
            throws Exception {
        Path receipts = Files.createDirectory(stage.resolve("clients"));
        var command = new ArrayList<>(List.of(build.resolve("src/sysbench").toString(), script.toString(),
                "--db-driver=duckdb", "--duckdb-socket=" + socket, "--duckdb-ledger-dir=" + receipts,
                "--threads=" + threads, "--events=" + events, "--time=0", "--rand-seed=20261019"));
        if (script.startsWith(build)) command.addAll(List.of("--tables=2", "--table-size=512", "--rand-type=uniform",
                "--auto-inc=on", "--secondary=off", "--create-secondary=on", "--skip-trx=off"));
        command.add(action);
        return NativeProcess.run(action, command, Map.of("LUA_PATH", build.resolve("src/lua") + "/?.lua;;"),
                Path.of("").toRealPath(), stage, Duration.ofSeconds(15));
    }

    /** Return ordinary calls and errors; recovery ROLLBACK is separately receipted. */
    private long[] receipts(Connection observer, Path stage) throws Exception {
        long calls = 0, failures = 0, recovery = 0;
        try (var files = Files.list(stage.resolve("clients"))) {
            var sessions = files.sorted().toList(); assertFalse(sessions.isEmpty());
            for (Path file : sessions) {
                long[] counts={0,0,0};
                var session=DuckDbReceipts.read(file,(connection,call)->{
                    if(call.kind().equals("query")) { counts[0]++; if(call.errorCode()!=0)counts[1]++; }
                    else counts[2]++;
                });
                calls+=counts[0];failures+=counts[1];recovery+=counts[2];
                assertEquals(session.submissions(), number(observer, "SELECT count(DISTINCT root_query) FROM duckdb_logs_parsed('SibenchQuery') "
                        + "WHERE connection_id=" + session.connection() + " AND query_id>" + session.afterQuery()
                        + " AND event='end' AND submission_end"));
            }
        }
        return new long[] {calls, failures, recovery};
    }

    @Test public void loadsAndExecutesAllSixPinnedWorkloads() throws Exception {
        Path build = build(), output = output("workloads");
        try (var observer = connect()) {
            enable(observer, output);
            Path prepare = Files.createDirectory(output.resolve("prepare"));
            var loaded = run(build, build.resolve("src/lua/oltp_read_write.lua"), prepare, 2, 0, "prepare");
            assertEquals(Files.readString(loaded.err()) + loaded.stdout(), 0, loaded.exit());
            receipts(observer, prepare);
            for (int i = 1; i <= 2; i++) {
                assertEquals(512, number(observer, "SELECT count(*) FROM sbtest" + i));
                assertEquals(1, number(observer, "SELECT min(id) FROM sbtest" + i));
                assertEquals(512, number(observer, "SELECT max(id) FROM sbtest" + i));
            }
            observer.setAutoCommit(false); NativeHistory.initialize(observer, org.sibench.config.Engine.DUCKDB);
            for (int i = 1; i <= 2; i++) NativeHistory.install(observer, org.sibench.config.Engine.DUCKDB, i, "main", "sbtest" + i, "id");
            observer.commit(); observer.setAutoCommit(true);
            for (String script : List.of("oltp_read_write", "oltp_update_index", "oltp_update_non_index",
                    "oltp_delete", "oltp_insert", "oltp_point_select")) {
                Path stage = Files.createDirectory(output.resolve(script));
                var result = run(build, build.resolve("src/lua/" + script + ".lua"), stage, 2, 20, "run");
                assertEquals(Files.readString(result.err()) + result.stdout(), 0, result.exit());
                var stats = SysbenchSummary.parse(result.stdout());
                assertEquals(20L, (long) stats.get("transactions"));
                var actual = receipts(observer, stage);
                assertEquals((long) stats.get("total"), actual[0] - actual[1]);
                assertEquals((long) stats.get("ignored errors"), actual[1]);
            }
            assertEquals(20, number(observer, "SELECT count(*) FROM sbtest1 WHERE id>512")
                    + number(observer, "SELECT count(*) FROM sbtest2 WHERE id>512"));
            assertEquals(1024, number(observer, "SELECT (SELECT count(*) FROM sibench_history.identities_1 WHERE _sib_tx=0)"
                    + "+(SELECT count(*) FROM sibench_history.identities_2 WHERE _sib_tx=0)"));
        }
        stop(); host = null;
        assertFalse(Files.exists(root.resolve("data/data.duckdb.wal")));
    }

    @Test public void nativeConflictRollsBackBeforeTheUpstreamEventRetry() throws Exception {
        conflict(true);
    }

    @Test public void autoCommitConflictRetriesWithoutInventingRollback() throws Exception {
        conflict(false);
    }

    private void conflict(boolean explicit) throws Exception {
        Path build = build(), output = output(explicit ? "conflict" : "autocommit-conflict");
        Path stage = Files.createDirectory(output.resolve("run"));
        Path marker = output.resolve("conflict"), release = output.resolve("release");
        Path script = output.resolve("conflict.lua");
        Files.writeString(script, """
                local ffi = require('ffi')
                ffi.cdef[[int usleep(unsigned int);]]
                local con
                function thread_init() con = sysbench.sql.driver():connect() end
                function thread_done() con:disconnect() end
                function event()
                  %s
                  con:query('UPDATE items SET k=k+1 WHERE id=1')
                  %s
                end
                function sysbench.hooks.before_restart_event(e)
                  assert(e.sql_errno == 10)
                  local file = assert(io.open('%s','w')); file:write('conflict'); file:close()
                  for i=1,500 do
                    file = io.open('%s','r')
                    if file then file:close(); return end
                    ffi.C.usleep(10000)
                  end
                  error('parent did not release conflict')
                end
                """.formatted(explicit ? "con:query('BEGIN')" : "", explicit ? "con:query('COMMIT')" : "", marker, release));
        try (var writer = connect(); var observer = connect()) {
            execute(writer, "CREATE TABLE items(id BIGINT PRIMARY KEY,k BIGINT)");
            execute(writer, "INSERT INTO items VALUES(1,10)"); enable(observer, output);
            writer.setAutoCommit(false); execute(writer, "UPDATE items SET k=20 WHERE id=1");
            var running = CompletableFuture.supplyAsync(() -> {
                try { return run(build, script, stage, 1, 1, "run"); }
                catch (Exception e) { throw new RuntimeException(e); }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(marker) && !running.isDone() && System.nanoTime() < deadline) Thread.sleep(10);
            if (!Files.exists(marker)) {
                writer.rollback();
                var result = running.get(10, TimeUnit.SECONDS);
                fail(Files.readString(result.err()) + result.stdout());
            }
            writer.commit(); Files.writeString(release, "released");
            var result = running.get(10, TimeUnit.SECONDS);
            assertEquals(Files.readString(result.err()) + result.stdout(), 0, result.exit());
            assertEquals(21, number(observer, "SELECT k FROM items WHERE id=1"));
            var actual = receipts(observer, stage);
            assertEquals(1, actual[1]); assertEquals(explicit ? 1 : 0, actual[2]);
            assertEquals(1, number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchTransaction') WHERE event='rollback'"));
        }
    }

    @Test public void typedParametersAndFetchedRowsPreserveExactValuesAcrossBatches() throws Exception {
        Path build = build(), output = output("values"), stage = Files.createDirectory(output.resolve("run"));
        Path script = output.resolve("values.lua");
        Files.writeString(script, """
                local con
                function thread_init() con = sysbench.sql.driver():connect() end
                function thread_done() con:disconnect() end
                function event()
                  local insert = con:prepare('INSERT INTO items VALUES(?,?,?)')
                  local id = insert:bind_create(sysbench.sql.type.BIGINT)
                  local value = insert:bind_create(sysbench.sql.type.VARCHAR,128)
                  local absent = insert:bind_create(sysbench.sql.type.VARCHAR,128)
                  insert:bind_param(id,value,absent)
                  id:set(9007199254740993LL)
                  value:set("quoted'雪\\0tail")
                  absent:set(nil)
                  insert:execute(); insert:close()
                  local rows = con:query('SELECT id,value,absent FROM items')
                  local row = rows:fetch_row()
                  assert(row[1] == '9007199254740993')
                  assert(row[2] == "quoted'雪\\0tail")
                  assert(row[3] == nil)
                  assert(rows:fetch_row() == nil)
                  rows = con:query('SELECT range FROM range(6000)')
                  for i=0,5999 do assert(rows:fetch_row()[1] == tostring(i)) end
                  assert(rows:fetch_row() == nil)
                end
                """);
        try (var observer = connect()) {
            execute(observer, "CREATE TABLE items(id BIGINT PRIMARY KEY,value VARCHAR,absent VARCHAR)");
            enable(observer, output);
            var result = run(build, script, stage, 1, 1, "run");
            assertEquals(Files.readString(result.err()) + result.stdout(), 0, result.exit());
            assertEquals(3, receipts(observer, stage)[0]);
            assertEquals(9_007_199_254_740_993L, number(observer, "SELECT id FROM items"));
        }
    }

    @Test public void onlyDuplicateConstraintsAreRetried() throws Exception {
        Path build = build(), output = output("constraints");
        try (var observer = connect()) {
            execute(observer, "CREATE TABLE items(id BIGINT PRIMARY KEY,value VARCHAR NOT NULL)");
            execute(observer, "INSERT INTO items VALUES(1,'original')"); enable(observer, output);
            Path duplicate = Files.createDirectory(output.resolve("duplicate")), script = output.resolve("duplicate.lua");
            Files.writeString(script, """
                    local con
                    local key = 1
                    function thread_init() con = sysbench.sql.driver():connect() end
                    function thread_done() con:disconnect() end
                    function event()
                      con:query('BEGIN')
                      con:query("INSERT INTO items VALUES(" .. key .. ",'inserted')")
                      con:query('COMMIT')
                    end
                    function sysbench.hooks.before_restart_event(e)
                      assert(e.sql_errno == 18 and key == 1)
                      key = 2
                    end
                    """);
            var retried = run(build, script, duplicate, 1, 1, "run");
            assertEquals(Files.readString(retried.err()) + retried.stdout(), 0, retried.exit());
            assertEquals(2, number(observer, "SELECT count(*) FROM items"));
            var actual = receipts(observer, duplicate); assertEquals(1, actual[1]); assertEquals(1, actual[2]);

            Path invalid = Files.createDirectory(output.resolve("not-null")); script = output.resolve("not-null.lua");
            Files.writeString(script, """
                    local con
                    function thread_init() con = sysbench.sql.driver():connect() end
                    function thread_done() con:disconnect() end
                    function event() con:query('INSERT INTO items VALUES(3,NULL)') end
                    function sysbench.hooks.before_restart_event(e) error('incorrect constraint retry') end
                    """);
            var rejected = run(build, script, invalid, 1, 1, "run");
            assertNotEquals(0, rejected.exit());
            String diagnostics = Files.readString(rejected.err()) + rejected.stdout();
            assertTrue(diagnostics, diagnostics.contains("NOT NULL"));
            assertFalse(diagnostics, diagnostics.contains("incorrect constraint retry"));
            assertEquals(2, number(observer, "SELECT count(*) FROM items"));
        }
    }
}
