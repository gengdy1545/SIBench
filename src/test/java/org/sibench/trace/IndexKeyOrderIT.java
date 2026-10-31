package org.sibench.trace;

import static org.junit.Assert.*;

import org.junit.Test;

import java.sql.*;
import java.util.*;

/**
 * Opt-in differential check against the capture PostgreSQL: rows sorted by the encoded keys of
 * PG's own text output must come back in PG's btree order. Uses a temporary table only.
 */
public class IndexKeyOrderIT {
    @Test
    public void encodedOrderMatchesPostgresOrder() throws Exception {
        String jdbc = System.getenv("SIBENCH_KEY_TEST_JDBC");
        assertNotNull("Set SIBENCH_KEY_TEST_JDBC to a PostgreSQL database URL", jdbc);
        var random = new Random(20261022);
        try (Connection connection =
                        DriverManager.getConnection(
                                jdbc,
                                System.getenv().getOrDefault("SIBENCH_KEY_TEST_USER", "postgres"),
                                System.getenv().getOrDefault("PGPASSWORD", ""));
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TEMP TABLE sibench_key_order (id int, i int8, t timestamp, v varchar"
                            + " COLLATE \"C\", c char(6) COLLATE \"C\")");
            try (var insert =
                    connection.prepareStatement(
                            "INSERT INTO sibench_key_order VALUES (?, ?, ?::timestamp, ?, ?)")) {
                String alphabet = "aAzé中 \u0001~";
                for (int id = 0; id < 4000; id++) {
                    insert.setInt(1, id);
                    if (id % 17 == 0) insert.setNull(2, Types.BIGINT);
                    else insert.setLong(2, id % 5 == 0 ? random.nextInt(9) - 4 : random.nextLong());
                    insert.setString(
                            3,
                            id % 19 == 0
                                    ? null
                                    : id % 101 == 0
                                            ? (id % 2 == 0 ? "infinity" : "-infinity")
                                            : String.format(
                                                    "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                                                    1 + random.nextInt(9998),
                                                    1 + random.nextInt(12),
                                                    1 + random.nextInt(28),
                                                    random.nextInt(24),
                                                    random.nextInt(60),
                                                    random.nextInt(60),
                                                    random.nextInt(3) == 0 ? 0 : random.nextInt(1_000_000)));
                    var text = new StringBuilder();
                    for (int k = random.nextInt(6); k > 0; k--)
                        text.append(alphabet.charAt(random.nextInt(alphabet.length())));
                    if (random.nextInt(4) == 0) text.append("  ");
                    insert.setString(4, id % 23 == 0 ? null : text.toString());
                    insert.setString(5, id % 29 == 0 ? null : text.toString());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            for (String[] column :
                    new String[][] {
                        {"i", "int"}, {"t", "timestamp"}, {"v", "varchar"}, {"c", "char(6)"}
                    }) {
                var expected = new ArrayList<Integer>();
                var encoded = new ArrayList<Map.Entry<IndexKey, Integer>>();
                try (ResultSet rows =
                        statement.executeQuery(
                                "SELECT id, "
                                        + column[0]
                                        + "::text AS output FROM sibench_key_order ORDER BY "
                                        + column[0]
                                        + ", id")) {
                    while (rows.next()) {
                        expected.add(rows.getInt(1));
                        encoded.add(
                                Map.entry(
                                        IndexKey.parse(column[1], rows.getString(2)),
                                        rows.getInt(1)));
                    }
                }
                encoded.sort(
                        Map.Entry.<IndexKey, Integer>comparingByKey()
                                .thenComparing(Map.Entry.comparingByValue()));
                assertEquals(
                        column[0],
                        expected,
                        encoded.stream().map(Map.Entry::getValue).toList());
            }
        }
    }
}
