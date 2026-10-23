package org.sibench.capture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class HybenchNativePrepareTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void theUpstreamIndexScriptIsSplitIntoStatements() throws Exception {
        Path script = temporary.newFile("create_index_pg.sql").toPath();
        Files.writeString(
                script,
                """
                -- upstream secondary indexes

                create index idx_loanapps_1 on loanapps ( applicantid );

                create index idx_loanapps_2 on loanapps ( timestamp );
                """);
        List<String> statements = HybenchNativePrepare.statements(script);
        assertEquals(
                List.of(
                        "create index idx_loanapps_1 on loanapps ( applicantid )",
                        "create index idx_loanapps_2 on loanapps ( timestamp )"),
                statements);
        // The pinned script carries the 13 upstream secondary indexes, the four identity
        // sequences and their setvals; all of them must reach the cloned database.
        var upstream = HybenchNativePrepare.statements(upstream());
        assertEquals(13, upstream.stream().filter(sql -> sql.startsWith("create index ")).count());
        assertEquals(
                4, upstream.stream().filter(sql -> sql.startsWith("CREATE SEQUENCE IF NOT EXISTS")).count());
        assertEquals(4, upstream.stream().filter(sql -> sql.startsWith("SELECT SETVAL")).count());
    }

    @Test
    public void noStatementKeepsItsTerminatorOrRunsEmpty() throws Exception {
        for (String sql : HybenchNativePrepare.statements(upstream())) {
            assertFalse(sql, sql.isBlank());
            assertFalse(sql, sql.contains(";"));
        }
    }

    /** The pinned upstream script, so a change to it is visible instead of silent. */
    private static Path upstream() {
        return Path.of("benchmark/HyBench-2024/conf/create_index_pg.sql");
    }
}
