package org.sibench.capture;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The capture server's effective non-default parameters. Every one is recorded with the run; the
 * subset that can change a query plan, and with it the index accesses a workload performs, is part
 * of the build identity, so a certification never vouches for a capture planned differently.
 * Parameters that only change speed (memory, WAL, checkpoint, durability) are recorded alone.
 */
final class PgSettings {
    /** Plan-relevant parameters outside the planner categories of {@code pg_settings}. */
    private static final Set<String> PLAN_NAMES =
            Set.of(
                    "work_mem",
                    "hash_mem_multiplier",
                    "max_parallel_workers",
                    "max_parallel_workers_per_gather");
    /** Where a value came from an operator rather than from the server or the client session. */
    private static final Set<String> CONFIGURED =
            Set.of("configuration file", "command line", "environment variable");

    private PgSettings() {}

    /** Whether a parameter in {@code category} named {@code name} can change a query plan. */
    static boolean affectsPlans(String name, String category) {
        return PLAN_NAMES.contains(name) || (category != null && category.startsWith("Query Tuning"));
    }

    /** {@code all} holds every configured parameter, {@code plan} the plan-relevant ones. */
    record Snapshot(Map<String, String> all, Map<String, String> plan) {}

    static Snapshot read(String jdbc, String user) throws SQLException {
        var all = new TreeMap<String, String>();
        var plan = new TreeMap<String, String>();
        try (var connection =
                        DriverManager.getConnection(jdbc, user, System.getenv().getOrDefault("PGPASSWORD", ""));
                var statement = connection.createStatement();
                var rows =
                        statement.executeQuery(
                                "SELECT name, setting, coalesce(unit, ''), category, source"
                                        + " FROM pg_settings")) {
            while (rows.next()) {
                if (!CONFIGURED.contains(rows.getString(5))) continue;
                String name = rows.getString(1), unit = rows.getString(3);
                // A block-sized unit such as 8kB would otherwise run into the number.
                String value = unit.isEmpty() ? rows.getString(2) : rows.getString(2) + " " + unit;
                all.put(name, value);
                if (affectsPlans(name, rows.getString(4))) plan.put(name, value);
            }
        }
        return new Snapshot(all, plan);
    }

    /** The build identity of the code a capture runs plus the plan-relevant server parameters. */
    static Map<String, Object> identity(Map<String, Object> build, Snapshot settings) {
        var result = new LinkedHashMap<>(build);
        result.put("postgres_plan_settings", new TreeMap<>(settings.plan()));
        return result;
    }
}
