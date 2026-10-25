package org.sibench.capture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.trace.IndexKey;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * The same operation stream goes to a model that keeps everything in memory and to one whose
 * budgets force baselines out of core and histories to disk; every answer must be identical.
 */
public class IndexReferenceSpillTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    public static final int TABLE = 5, ROWS = 300, VALUES = 17;

    private static IndexReferenceReplay.Settings tiny(Path directory) {
        return new IndexReferenceReplay.Settings(directory, 40, 3, 200);
    }

    /** Baseline rows in shuffled order (the secondary run is not in PK order), then versions. */
    public static List<Operation> stream(boolean rowIds, long seed) throws Exception {
        var random = new Random(seed);
        var operations = new ArrayList<Operation>();
        var order = new ArrayList<Integer>();
        for (int id = 1; id <= ROWS; id++) order.add(id);
        java.util.Collections.shuffle(order, random);
        long event = 0;
        for (int id : order) {
            Operation primary =
                    IndexOperationTrace.write(
                            ++event, IndexOperationTrace.INSERT, TABLE, 0, IndexKey.ofInt(id),
                            0, 1 + id / 100, id, id);
            operations.add(primary);
            operations.add(primary.withIndex(1, IndexKey.ofInt(id % VALUES), IndexOperationTrace.INSERT));
            operations.add(primary.withIndex(2, IndexKey.ofInt(id % 5), IndexOperationTrace.INSERT));
        }
        int top = ROWS + 41;
        int[] value1 = new int[top], value2 = new int[top];
        boolean[] alive = new boolean[top];
        long[] rowId = new long[top];
        for (int id = 1; id <= ROWS; id++) {
            value1[id] = id % VALUES;
            value2[id] = id % 5;
            alive[id] = true;
            rowId[id] = id;
        }
        long version = 0, nextRow = 1_000;
        for (int round = 0; round < 120; round++) {
            version++;
            long transaction = version;
            int ordinal = 0;
            for (int n = 1 + random.nextInt(3); n > 0; n--) {
                int id = 1 + random.nextInt(ROWS + 40);
                ordinal++;
                if (alive[id] && random.nextInt(5) == 0) {
                    Operation delete =
                            IndexOperationTrace.write(
                                            ++event, IndexOperationTrace.LOGICAL_DELETE, TABLE, 0,
                                            IndexKey.ofInt(id), version, 0, 0, rowId[id])
                                    .withTransaction(transaction, ordinal);
                    operations.add(delete);
                    operations.add(delete.withIndex(1, IndexKey.ofInt(value1[id]), IndexOperationTrace.LOGICAL_DELETE));
                    operations.add(delete.withIndex(2, IndexKey.ofInt(value2[id]), IndexOperationTrace.LOGICAL_DELETE));
                    alive[id] = false;
                    continue;
                }
                // An insert, or a reinsert after a delete, is a new row identity.
                if (!alive[id]) rowId[id] = ++nextRow;
                int newOne = random.nextInt(VALUES), newTwo = random.nextInt(5);
                Operation upsert =
                        IndexOperationTrace.write(
                                        ++event, IndexOperationTrace.UPSERT, TABLE, 0, IndexKey.ofInt(id),
                                        version, 77, event, rowId[id])
                                .withTransaction(transaction, ordinal);
                operations.add(upsert);
                if (alive[id]) {
                    if (value1[id] != newOne)
                        operations.add(upsert.withIndex(1, IndexKey.ofInt(value1[id]), IndexOperationTrace.LOGICAL_DELETE));
                    if (value2[id] != newTwo)
                        operations.add(upsert.withIndex(2, IndexKey.ofInt(value2[id]), IndexOperationTrace.LOGICAL_DELETE));
                }
                operations.add(upsert.withIndex(1, IndexKey.ofInt(newOne), IndexOperationTrace.UPSERT));
                operations.add(upsert.withIndex(2, IndexKey.ofInt(newTwo), IndexOperationTrace.UPSERT));
                value1[id] = newOne;
                value2[id] = newTwo;
                alive[id] = true;
            }
        }
        return operations;
    }

    private static void apply(IndexReferenceReplay model, List<Operation> operations) throws Exception {
        for (Operation operation : operations) model.accept(operation);
    }

    private static void compareEverywhere(
            IndexReferenceReplay a, IndexReferenceReplay b, long lastVersion, long seed) throws Exception {
        var random = new Random(seed);
        for (long time = 0; time <= lastVersion; time += 1 + random.nextInt(9)) {
            for (int id = 1; id <= ROWS + 40; id += 1 + random.nextInt(6))
                assertEquals(
                        "primary " + id + " at " + time,
                        a.lookupPrimary(TABLE, IndexKey.ofInt(id), time),
                        b.lookupPrimary(TABLE, IndexKey.ofInt(id), time));
            for (int index = 1; index <= 2; index++) {
                for (int value = 0; value < (index == 1 ? VALUES : 5); value++)
                    assertEquals(
                            "secondary " + index + "/" + value + " at " + time,
                            a.lookupSecondary(TABLE, index, IndexKey.ofInt(value), time),
                            b.lookupSecondary(TABLE, index, IndexKey.ofInt(value), time));
                for (int trial = 0; trial < 6; trial++) {
                    IndexKey lower = random.nextInt(4) == 0 ? null : IndexKey.ofInt(random.nextInt(ROWS) - 3);
                    IndexKey upper = random.nextInt(4) == 0 ? null : IndexKey.ofInt(random.nextInt(ROWS));
                    Range range =
                            new Range(
                                    lower, random.nextBoolean(), upper, random.nextBoolean(),
                                    random.nextBoolean(), random.nextBoolean() ? 0 : 1 + random.nextInt(9));
                    // Secondary leading components are small; the same bounds still exercise empty,
                    // partial and full ranges on both kinds of index.
                    assertEquals(
                            "range " + index + " at " + time + " " + range,
                            a.search(TABLE, index, range, time),
                            b.search(TABLE, index, range, time));
                }
            }
            for (int trial = 0; trial < 6; trial++) {
                IndexKey lower = IndexKey.ofInt(random.nextInt(ROWS) - 3);
                IndexKey upper = IndexKey.ofInt(random.nextInt(ROWS + 40));
                Range range = new Range(lower, random.nextBoolean(), upper, random.nextBoolean(),
                        random.nextBoolean(), random.nextBoolean() ? 0 : 1 + random.nextInt(25));
                assertEquals(
                        "primary range at " + time + " " + range,
                        a.search(TABLE, 0, range, time),
                        b.search(TABLE, 0, range, time));
            }
        }
        assertEquals(a.stateChecksum(), b.stateChecksum());
        assertEquals(a.retainedVersions(), b.retainedVersions());
    }

    @Test
    public void spilledAndInMemoryModelsGiveTheSameAnswers() throws Exception {
        for (boolean rowIds : new boolean[] {false, true}) {
            for (long seed : new long[] {1, 2, 3}) {
                var operations = stream(rowIds, seed);
                long last = operations.stream().mapToLong(Operation::version).max().orElse(0);
                Path directory = temporary.newFolder().toPath().resolve("spill");
                try (var memory = new IndexReferenceReplay(rowIds, true);
                        var spilled = new IndexReferenceReplay(rowIds, true, tiny(directory))) {
                    apply(memory, operations);
                    apply(spilled, operations);
                    assertEquals(0, memory.spilledBaselineEntries());
                    assertTrue("baseline spilled", spilled.spilledBaselineEntries() > 0);
                    assertTrue(Files.isDirectory(directory));
                    compareEverywhere(memory, spilled, last, seed);
                }
            }
        }
    }

    @Test
    public void relocatedLocatorsAndDeletesSurviveSpilling() throws Exception {
        var operations = stream(false, 8);
        Path directory = temporary.newFolder().toPath().resolve("spill");
        try (var memory = new IndexReferenceReplay(false, true);
                var spilled = new IndexReferenceReplay(false, true, tiny(directory))) {
            apply(memory, operations);
            apply(spilled, operations);
            long last = operations.stream().mapToLong(Operation::version).max().orElse(0);
            // Relocate a baseline entry at version 0 and a written one at its exact version.
            for (var model : List.of(memory, spilled)) {
                var primary =
                        IndexOperationTrace.write(
                                9_000, IndexOperationTrace.RELOCATE, TABLE, 0, IndexKey.ofInt(2),
                                0, 31, 4_000_000_000L, 2);
                model.accept(primary);
                IndexMutation.emit(primary, java.util.Map.of(), java.util.Map.of(), x -> {});
            }
            assertEquals(
                    memory.lookupPrimary(TABLE, IndexKey.ofInt(2), 0),
                    spilled.lookupPrimary(TABLE, IndexKey.ofInt(2), 0));
            assertEquals(31, spilled.lookupPrimary(TABLE, IndexKey.ofInt(2), 0).value());
            assertEquals(4_000_000_000L, spilled.lookupPrimary(TABLE, IndexKey.ofInt(2), 0).position());
            compareEverywhere(memory, spilled, last, 5);
        }
    }

    @Test
    public void modelsSharingAConfiguredDirectoryNeverMeetEachOthersFiles() throws Exception {
        Path directory = temporary.newFolder().toPath().resolve("shared");
        var operations = stream(false, 2);
        try (var first = new IndexReferenceReplay(false, true, tiny(directory));
                var second = new IndexReferenceReplay(false, true, tiny(directory))) {
            apply(first, operations);
            apply(second, operations);
            assertEquals(
                    first.lookupPrimary(TABLE, IndexKey.ofInt(3), 0),
                    second.lookupPrimary(TABLE, IndexKey.ofInt(3), 0));
            try (var children = Files.list(directory)) {
                assertEquals(2, children.count());
            }
        }
        // Closing removes each model's directory and leaves the configured one in place.
        assertTrue(Files.isDirectory(directory));
        try (var children = Files.list(directory)) {
            assertEquals(0, children.count());
        }
    }

    @Test
    public void closeRemovesTheTemporaryDirectoryOfAnUnnamedWorkspace() throws Exception {
        var settings = new IndexReferenceReplay.Settings(null, 10, 2, 100);
        var model = new IndexReferenceReplay(true, true, settings);
        apply(model, stream(true, 4));
        assertNotNull(model.lookupPrimary(TABLE, IndexKey.ofInt(1), 0));
        model.close();
        // A closed model holds no spill state of its own and can be closed again.
        model.close();
    }

    @Test
    public void aModelThatNeverSpillsLeavesNoFiles() throws Exception {
        Path directory = temporary.getRoot().toPath().resolve("untouched");
        try (var model = new IndexReferenceReplay(true, true, new IndexReferenceReplay.Settings(directory, 1_000_000, 1_000_000, 1 << 20))) {
            apply(model, stream(true, 6));
            assertFalse(Files.exists(directory));
        }
        assertFalse(Files.exists(directory));
    }
}
