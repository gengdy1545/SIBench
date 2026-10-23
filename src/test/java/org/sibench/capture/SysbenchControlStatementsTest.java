package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class SysbenchControlStatementsTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    /** The pinned oltp_read_write.lua shape: prepared at connect, one begin() and one commit() per event. */
    private static final String EXPLICIT =
            """
            require("oltp_common")

            function prepare_statements()
               if not sysbench.opt.skip_trx then
                  prepare_begin()
                  prepare_commit()
               end
               prepare_point_selects()
            end

            function event()
               if not sysbench.opt.skip_trx then
                  begin()
               end
               execute_point_selects()
               if not sysbench.opt.skip_trx then
                  commit()
               end
               check_reconnect()
            end
            """;

    /** oltp_delete.lua / oltp_update_index.lua: no control calls at all, autocommit. */
    private static final String AUTOCOMMIT =
            """
            require("oltp_common")

            function prepare_statements()
               prepare_for_each_table("deletes")
            end

            function event()
               param[tnum].deletes[1]:set(id)
               stmt[tnum].deletes:execute()
               check_reconnect()
            end
            """;

    @Test
    public void anExplicitTransactionScriptIssuesTwoControlStatementsPerEvent() {
        var derived = SysbenchControlStatements.derive(EXPLICIT, false, 8000);
        assertFalse(derived.skipTrx());
        assertEquals(1, derived.beginCallsPerEvent());
        assertEquals(1, derived.commitCallsPerEvent());
        assertEquals(16000L, derived.controlStatements());
        assertTrue(derived.explicitTransactions());
        // The prepare-time prepare_begin()/prepare_commit() are not counted; neither is the
        // definition of begin()/commit() in oltp_common.lua.
        var definitions =
                SysbenchControlStatements.derive(
                        "function begin()\n  stmt.begin:execute()\nend\nfunction commit()\nend\n",
                        false,
                        10);
        assertEquals(0, definitions.beginCallsPerEvent());
        assertEquals(0, definitions.commitCallsPerEvent());
    }

    @Test
    public void skipTrxOnRemovesEveryControlStatementOfTheSameScript() {
        var derived = SysbenchControlStatements.derive(EXPLICIT, true, 8000);
        assertTrue(derived.skipTrx());
        assertEquals(1, derived.beginCallsPerEvent());
        assertEquals(1, derived.commitCallsPerEvent());
        assertEquals(0L, derived.controlStatements());
        assertFalse(derived.explicitTransactions());
        assertEquals(0L, derived.controlStatements() / 8000);
    }

    @Test
    public void anAutocommitScriptHasNoControlStatementsEitherWay() {
        for (boolean skipTrx : new boolean[] {false, true}) {
            var derived = SysbenchControlStatements.derive(AUTOCOMMIT, skipTrx, 5000);
            assertEquals(0, derived.beginCallsPerEvent());
            assertEquals(0, derived.commitCallsPerEvent());
            assertEquals(0L, derived.controlStatements());
            assertFalse(derived.explicitTransactions());
        }
    }

    @Test
    public void theScriptIsReadFromItsOwnFileAndCommentsAreNotCalls() throws Exception {
        Path lua = folder.newFolder("lua").toPath().resolve("script.lua");
        Files.writeString(
                lua,
                "-- begin() in a comment\n"
                        + "--[[ begin() in a block comment\n commit() ]]\n"
                        + "function event()\n  begin()  -- the real one\n  commit()\nend\n");
        var derived = SysbenchControlStatements.derive(lua, false, 3);
        assertEquals(1, derived.beginCallsPerEvent());
        assertEquals(1, derived.commitCallsPerEvent());
        assertEquals(6L, derived.controlStatements());
    }

    @Test
    public void skipTrxComesFromTheRunsOwnArgumentsNeverFromTheSource() throws Exception {
        assertEquals(
                false,
                SysbenchControlStatements.skipTrx(
                        List.of("src/sysbench", "oltp_read_write.lua", "--skip-trx=off", "run")));
        assertEquals(
                true,
                SysbenchControlStatements.skipTrx(
                        List.of("src/sysbench", "oltp_read_write.lua", "--skip-trx=on", "run")));
        assertEquals(
                true,
                SysbenchControlStatements.skipTrx(List.of("--skip-trx=1", "run")));
        assertEquals(
                false,
                SysbenchControlStatements.skipTrx(List.of("--skip-trx=false", "run")));
        assertTrue(
                assertThrows(
                                IOException.class,
                                () -> SysbenchControlStatements.skipTrx(List.of("--events=5", "run")))
                        .getMessage()
                        .contains("does not state --skip-trx"));
        assertTrue(
                assertThrows(
                                IOException.class,
                                () -> SysbenchControlStatements.skipTrx(List.of("--skip-trx=maybe")))
                        .getMessage()
                        .contains("Invalid sysbench --skip-trx value"));
    }

    @Test
    public void theResidualCrossCheckFailsWhenTheServerObservedAnotherCount() throws Exception {
        var explicit = SysbenchControlStatements.derive(EXPLICIT, false, 100);
        // 100 events x (10 point selects + 1 index update + 1 non-index update + delete + insert)
        // plus 200 control statements; the server sees only the executed statements.
        long clientStatements = 1400L;
        long serverExecutions = 1200L;
        assertEquals(
                200L,
                SysbenchControlStatements.requireAgreement(
                        "oltp_read_write", clientStatements, serverExecutions, explicit));

        var autocommit = SysbenchControlStatements.derive(AUTOCOMMIT, false, 50);
        assertEquals(
                0L,
                SysbenchControlStatements.requireAgreement(
                        "oltp_delete", 50L, 50L, autocommit));
        var failure =
                assertThrows(
                        IOException.class,
                        () ->
                                SysbenchControlStatements.requireAgreement(
                                        "oltp_delete", 50L, 49L, autocommit));
        assertTrue(failure.getMessage().startsWith("oltp_delete:"));
        assertTrue(failure.getMessage().contains("derive 0 control statements"));
        assertTrue(failure.getMessage().contains("residual") || failure.getMessage().contains("1"));

        // The old substring rule would have said 2 x events for this autocommit script.
        assertTrue(
                assertThrows(
                                IOException.class,
                                () ->
                                        SysbenchControlStatements.requireAgreement(
                                                "oltp_read_write",
                                                1200L,
                                                1000L,
                                                SysbenchControlStatements.derive(EXPLICIT, true, 100)))
                        .getMessage()
                        .contains("skip_trx=true"));
    }

    @Test
    public void thePinnedScriptsDeriveTheCountsTheUpstreamSourceDefines() throws Exception {
        Path lua = Path.of("benchmark/sysbench/src/lua");
        assertEquals(
                200L,
                SysbenchControlStatements.derive(lua.resolve("oltp_read_write.lua"), false, 100)
                        .controlStatements());
        assertEquals(
                0L,
                SysbenchControlStatements.derive(lua.resolve("oltp_read_write.lua"), true, 100)
                        .controlStatements());
        for (String script :
                List.of("oltp_delete", "oltp_update_index", "oltp_update_non_index", "oltp_insert"))
            assertEquals(
                    script,
                    0L,
                    SysbenchControlStatements.derive(lua.resolve(script + ".lua"), false, 100)
                            .controlStatements());
        assertEquals(
                200L,
                SysbenchControlStatements.derive(lua.resolve("oltp_write_only.lua"), false, 100)
                        .controlStatements());
        assertEquals(
                200L,
                SysbenchControlStatements.derive(lua.resolve("oltp_read_only.lua"), false, 100)
                        .controlStatements());
    }
}
