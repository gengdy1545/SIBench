package org.sibench.capture;

import com.google.gson.GsonBuilder;

import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Separately labelled, rolled-back controls; not part of native transaction proportions. */
public final class PgBoundaryRun {
    private PgBoundaryRun() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IllegalArgumentException(
                    "PgBoundaryRun CAPTURE_JDBC_URL RAW_DIRECTORY REPORT");
        System.setProperty(
                "sibench.capture.directory", Path.of(args[1]).toAbsolutePath().toString());
        Class.forName(CaptureDriver.class.getName());
        Map<String, Object> report = new LinkedHashMap<>();
        try (Connection c = DriverManager.getConnection(args[0], "postgres", "")) {
            c.setAutoCommit(false);
            try (PreparedStatement query =
                    c.prepareStatement("SELECT balance FROM savingaccount WHERE accountid=?")) {
                query.setInt(1, -1);
                try (ResultSet r = query.executeQuery()) {
                    if (r.next()) throw new IllegalStateException("Expected miss");
                }
                query.setInt(1, 1);
                float before;
                try (ResultSet r = query.executeQuery()) {
                    if (!r.next()) throw new IllegalStateException("Missing account");
                    before = r.getFloat(1);
                }
                try (PreparedStatement update =
                        c.prepareStatement(
                                "UPDATE savingaccount SET balance=balance+1 WHERE accountid=?")) {
                    update.setInt(1, 1);
                    if (update.executeUpdate() != 1)
                        throw new IllegalStateException("Update count");
                }
                try (ResultSet r = query.executeQuery()) {
                    r.next();
                    if (r.getFloat(1) != before + 1)
                        throw new IllegalStateException("Read own write");
                }
                c.rollback();
                try (ResultSet r = query.executeQuery()) {
                    r.next();
                    if (r.getFloat(1) != before)
                        throw new IllegalStateException("Rollback changed row");
                }
                c.commit();
                report.put("miss_read_own_write_rollback", true);
            }
            List<Double> randomResults = new ArrayList<>();
            try (PreparedStatement query =
                    c.prepareStatement(
                            "SELECT accountid, random() FROM savingaccount WHERE accountid=?")) {
                query.setInt(1, 1);
                for (int i = 0; i < 2; i++) {
                    try (ResultSet r = query.executeQuery()) {
                        r.next();
                        randomResults.add(r.getDouble(2));
                    }
                    c.commit();
                }
            }
            report.put("actual_random_results", randomResults);
            try (PreparedStatement bad =
                    c.prepareStatement("INSERT INTO savingaccount(accountid) VALUES(?)")) {
                bad.setInt(1, 1);
                try {
                    bad.executeUpdate();
                    throw new IllegalStateException("Expected duplicate key");
                } catch (SQLException expected) {
                    if (!"23505".equals(expected.getSQLState())) throw expected;
                }
                c.rollback();
            }
            report.put("duplicate_key_rollback", true);
        }
        report.put("scope", "controlled boundary workload, separate from native distribution");
        Files.writeString(
                Path.of(args[2]),
                new GsonBuilder().setPrettyPrinting().create().toJson(report),
                StandardOpenOption.CREATE_NEW);
    }
}
