package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class IndexOperationTraceTest {

    @Test
    public void encodedKeysAndRangesRoundTrip() throws Exception {
        Path directory = Files.createTempDirectory("sibench-variable-values-");
        Path file = directory.resolve("trace.pb.zst");
        try {
            var written = new ArrayList<Operation>();
            written.add(
                    IndexOperationTrace.write(
                                    1,
                                    IndexOperationTrace.INSERT,
                                    5,
                                    0,
                                    IndexKey.ofText("user1"),
                                    0,
                                    1,
                                    0,
                                    1)
                            .withIndex(3, IndexKey.NULL, IndexOperationTrace.INSERT));
            for (String value : new String[] {"", "ordinary", "中文"})
                written.add(
                        IndexOperationTrace.search(
                                2, 5, 3, Range.equal(IndexKey.ofText(value)), 0, 0, 0));
            written.add(
                    IndexOperationTrace.search(
                            3,
                            5,
                            0,
                            new Range(IndexKey.ofInt(-5), false, null, false, true, 2),
                            4,
                            2,
                            0));
            written.add(
                    IndexOperationTrace.search(
                            4, 5, 0, new Range(null, false, null, false, false, 0), 4, 9, 0));
            try (var writer = new ProtoTrace.Writer(file)) {
                for (Operation operation : written) writer.append(operation);
            }
            var decoded = new ArrayList<Operation>();
            ProtoTrace.read(file, null, null, decoded::add);
            assertEquals(written, decoded);
            assertTrue(decoded.get(0).indexedValue().isNull());
            assertTrue(decoded.get(1).range().point());
            Range range = decoded.get(4).range();
            assertEquals(IndexKey.ofInt(-5), range.lower());
            assertFalse(range.lowerInclusive());
            assertTrue(range.descending());
            assertEquals(2, range.limit());
            assertNull(decoded.get(5).range().lower());
        } finally {
            Files.deleteIfExists(file);
            Files.delete(directory);
        }
    }


    @Test
    public void rejectsInvalidIdentitiesAndAmbiguousFields() throws Exception {
        Path file = Files.createTempFile("sibench-unit-", ".pb.zst");
        Files.delete(file);
        Range point = Range.equal(IndexKey.ofInt(1));
        try (var writer = new ProtoTrace.Writer(file)) {
            for (Operation invalid :
                    List.of(
                            IndexOperationTrace.search(1, 3, 0, point, 0, 0, 0).withWorkUnitId(-1),
                            IndexOperationTrace.transaction(1, 6, 1, 1, 0).withWorkUnitId(1),
                            // A limited search cannot return more than its limit.
                            IndexOperationTrace.search(
                                    1, 3, 0, new Range(null, false, null, false, false, 3), 0, 4, 0),
                            // Inclusive flags need a bound.
                            IndexOperationTrace.search(
                                    1, 3, 0, new Range(null, true, null, false, false, 0), 0, 0, 0),
                            // A secondary write carries its indexed value; a primary one does not.
                            IndexOperationTrace.write(
                                            1, IndexOperationTrace.INSERT, 3, 0, IndexKey.ofInt(1), 0,
                                            1, 0, 1)
                                    .withIndex(1, null, IndexOperationTrace.INSERT),
                            IndexOperationTrace.write(
                                    1, IndexOperationTrace.INSERT, 3, 0, null, 0, 1, 0, 1),
                            IndexOperationTrace.write(
                                    1, IndexOperationTrace.INSERT, 70000, 0, IndexKey.ofInt(1), 0,
                                    1, 0, 1)))
                assertThrows(IOException.class, () -> writer.append(invalid));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void searchesCarryAReadTimestampNotAVersion() throws Exception {
        Path file = Files.createTempFile("sibench-read-time-", ".pb.zst");
        Files.delete(file);
        try {
            Operation search =
                    IndexOperationTrace.search(
                            1,
                            3,
                            0,
                            Range.equal(IndexKey.ofInt(1)),
                            105,
                            1,
                            IndexOperationTrace.SNAPSHOT_SEARCH);
            try (var writer = new ProtoTrace.Writer(file)) {
                writer.append(search);
                // A search that also carries a version is ambiguous and must not be written.
                assertThrows(
                        IOException.class,
                        () ->
                                writer.append(
                                        new Operation(
                                                1, 2, 3, 0, 0, null, null, 105, 0, 0, 0, 1, 0, 0,
                                                105, 0, 0, 0, IndexKey.ofInt(1), IndexKey.ofInt(1),
                                                0)));
            }
            List<Operation> actual = new ArrayList<>();
            ProtoTrace.read(file, null, null, actual::add);
            assertEquals(List.of(search), actual);
            assertEquals(0, actual.get(0).version());
            assertEquals(105, actual.get(0).readTimestamp());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /** Pinned searches and group members round-trip; malformed combinations never get written. */
    @Test
    public void pinnedSearchesAndGroupsRoundTripAndMalformedOnesFail() throws Exception {
        Path file = Files.createTempFile("sibench-groups-", ".pb.zst");
        Files.delete(file);
        Range point = Range.equal(IndexKey.ofInt(6));
        Range interval = new Range(IndexKey.ofInt(1), true, IndexKey.ofInt(4), false, false, 0);
        int intersect = IndexOperationTrace.INTERSECT_GROUP, union = IndexOperationTrace.UNION_GROUP;
        try {
            var written =
                    List.of(
                            IndexOperationTrace.search(1, 5, 1, point, IndexKey.ofInt(9), 3, 1, 0, 0, 0),
                            IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, intersect, 0, 2),
                            IndexOperationTrace.search(2, 5, 2, point, null, 3, 4, intersect, 1, 2),
                            IndexOperationTrace.search(3, 5, 1, point, null, 3, 2, union, 0, 2),
                            IndexOperationTrace.search(3, 5, 1, Range.equal(IndexKey.ofInt(8)), null, 3, 2, union, 1, 2));
            try (var writer = new ProtoTrace.Writer(file)) {
                for (Operation operation : written) writer.append(operation);
            }
            var decoded = new ArrayList<Operation>();
            ProtoTrace.read(file, null, null, decoded::add);
            assertEquals(written, decoded);
            assertTrue(decoded.get(1).grouped());
            assertFalse(decoded.get(0).grouped());
        } finally {
            Files.deleteIfExists(file);
        }
        for (Operation invalid :
                List.of(
                        // A pinned search names one secondary entry: a point, count 1, no PK index.
                        IndexOperationTrace.search(1, 5, 1, interval, IndexKey.ofInt(9), 3, 1, 0, 0, 0),
                        IndexOperationTrace.search(1, 5, 1, point, IndexKey.ofInt(9), 3, 2, 0, 0, 0),
                        IndexOperationTrace.search(1, 5, 0, point, IndexKey.ofInt(9), 3, 1, 0, 0, 0),
                        // A group has at least two members, a valid position, no limit or pin.
                        IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, intersect, 0, 1),
                        IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, intersect, 2, 2),
                        IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, intersect | union, 0, 2),
                        IndexOperationTrace.search(2, 5, 1, point, IndexKey.ofInt(9), 3, 1, union, 0, 2),
                        IndexOperationTrace.search(
                                2, 5, 1, new Range(null, false, null, false, false, 4), null, 3, 4, union, 0, 2),
                        // Only group members carry positions.
                        IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, 0, 0, 2))) {
            Path target = Files.createTempFile("sibench-invalid-", ".pb.zst");
            Files.delete(target);
            try (var writer = new ProtoTrace.Writer(target)) {
                assertThrows(IOException.class, () -> writer.append(invalid));
            } finally {
                Files.deleteIfExists(target);
            }
        }
    }



}
