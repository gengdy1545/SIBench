package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.Response;
import org.sibench.adapters.proto.Result;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.Value;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.replay.QueryDigest;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class TraceRequestsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static Operation query(long sourceEvent, long key, long count) {
        return IndexOperationTrace.search(
                        sourceEvent,
                        1,
                        0,
                        Range.equal(IndexKey.ofInt(key)),
                        10,
                        count,
                        0)
                .withWorkUnitId(7);
    }

    private static LookupResult result(long key, long version, long rowId) {
        return new LookupResult(IndexKey.ofInt(key), version, rowId, 0);
    }

    private static Result responseResult(long key, long version, long rowId) {
        return Result.newBuilder()
                .setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(key).bytes()))
                .setVersion(version)
                .setValue(Value.newBuilder().setRowId(rowId))
                .build();
    }

    private static Response response(long id, Result... results) {
        return Response.newBuilder()
                .setId(id)
                .setStatus(Status.OK)
                .addAllResults(List.of(results))
                .build();
    }

    private Path expected(List<Operation> operations, List<List<LookupResult>> results)
            throws IOException {
        Path file = temporary.newFile().toPath();
        Files.delete(file);
        try (var writer = new QueryDigest.File(file, true)) {
            for (int i = 0; i < operations.size(); i++)
                writer.accept(List.of(operations.get(i)), results.get(i));
        }
        return file;
    }

    @Test
    public void sameCountWrongKeyVersionRowIdDuplicateAndOrderFail() throws Exception {
        for (String fault :
                List.of("version", "key", "duplicate", "reversed", "xor")) {
            Operation operation = query(3, 1, 2);
            var expectedResults = List.of(result(1, 1, 1), result(2, 3, 2));
            Path stream = expected(List.of(operation), List.of(expectedResults));
            var requests = new TraceRequests(ValueKind.ROW_ID, stream);
            var request = requests.request(11, operation);
            var actual =
                    switch (fault) {
                        case "version" ->
                                List.of(responseResult(1, 1, 1), responseResult(2, 4, 2));
                        case "key" ->
                                List.of(responseResult(1, 1, 1), responseResult(9, 3, 2));
                        case "duplicate" ->
                                List.of(responseResult(1, 1, 1), responseResult(1, 1, 1));
                        case "reversed" ->
                                List.of(responseResult(2, 3, 2), responseResult(1, 1, 1));
                        // The old cross-query value XOR: 1 ^ 2 == 4 ^ 7 == 3.
                        default ->
                                List.of(responseResult(1, 1, 4), responseResult(2, 3, 7));
                    };
            var failure =
                    assertThrows(
                            IOException.class,
                            () -> requests.check(operation, response(request.getId(), actual.toArray(Result[]::new))));
            assertTrue(fault + ": " + failure.getMessage(), failure.getMessage().contains("differ"));
        }
    }

    @Test
    public void correctResponsesMayCompleteOutOfOrderAndCrossQuerySwapFails() throws Exception {
        Operation first = query(3, 1, 1), second = query(4, 10, 1);
        Path stream =
                expected(
                        List.of(first, second),
                        List.of(List.of(result(1, 1, 1)), List.of(result(10, 2, 10))));
        var requests = new TraceRequests(ValueKind.ROW_ID, stream);
        var a = requests.request(21, first);
        var b = requests.request(22, second);
        // Responses complete in the opposite order and still verify against their own queries.
        requests.check(second, response(b.getId(), responseResult(10, 2, 10)));
        requests.check(first, response(a.getId(), responseResult(1, 1, 1)));
        requests.finish();
        assertEquals(2, requests.digestChecks());

        Path swappedStream =
                expected(
                        List.of(first, second),
                        List.of(List.of(result(1, 1, 1)), List.of(result(10, 2, 10))));
        var swapped = new TraceRequests(ValueKind.ROW_ID, swappedStream);
        var c = swapped.request(31, first);
        var d = swapped.request(32, second);
        assertThrows(
                IOException.class,
                () -> swapped.check(first, response(c.getId(), responseResult(10, 2, 10))));
        assertThrows(
                IOException.class,
                () -> swapped.check(second, response(d.getId(), responseResult(1, 1, 1))));
    }

    @Test
    public void groupResultDigestCoversTheCombinedMembersInOrder() throws Exception {
        Operation first =
                IndexOperationTrace.search(
                                7,
                                1,
                                0,
                                Range.equal(IndexKey.ofInt(1)),
                                null,
                                10,
                                2,
                                IndexOperationTrace.INTERSECT_GROUP,
                                0,
                                2)
                        .withWorkUnitId(7);
        Operation second =
                IndexOperationTrace.search(
                                7,
                                1,
                                1,
                                Range.equal(IndexKey.ofInt(1)),
                                null,
                                10,
                                2,
                                IndexOperationTrace.INTERSECT_GROUP,
                                1,
                                2)
                        .withWorkUnitId(7);
        var expected = List.of(result(1, 1, 1), result(2, 1, 2));
        Path stream = temporary.newFile().toPath();
        Files.delete(stream);
        try (var writer = new QueryDigest.File(stream, true)) {
            writer.accept(List.of(first, second), expected);
        }
        var requests = new TraceRequests(ValueKind.ROW_ID, stream);
        var request = requests.group(71, List.of(first, second));
        var wrong =
                response(
                        request.getId(),
                        responseResult(1, 1, 1),
                        responseResult(3, 1, 3));
        assertThrows(
                IOException.class, () -> requests.check(second, wrong));

        Path correctStream = temporary.newFile().toPath();
        Files.delete(correctStream);
        try (var writer = new QueryDigest.File(correctStream, true)) {
            writer.accept(List.of(first, second), expected);
        }
        var accepted = new TraceRequests(ValueKind.ROW_ID, correctStream);
        var correctRequest = accepted.group(72, List.of(first, second));
        accepted.check(
                second,
                response(
                        correctRequest.getId(),
                        responseResult(1, 1, 1),
                        responseResult(2, 1, 2)));
        accepted.finish();
    }

    @Test
    public void identicalResultsFromADifferentSourceQueryCannotReuseTheExpectedRecord() throws Exception {
        Operation first = query(3, 1, 1), otherSource = query(4, 1, 1);
        var results = List.of(result(1, 1, 1));
        Path stream = expected(List.of(first), List.of(results));
        var requests = new TraceRequests(ValueKind.ROW_ID, stream);
        assertThrows(IOException.class, () -> requests.request(1, otherSource));
        assertNotEquals(QueryDigest.of(List.of(first), results, true),
                QueryDigest.of(List.of(first.withWorkUnitId(8)), results, true));
    }

    @Test
    public void resultDigestDetectsEachFieldAndOrderWithoutDependingOnQueryIdentity() {
        var expected = List.of(new LookupResult(IndexKey.ofInt(1), 2, 3, 4),
                new LookupResult(IndexKey.ofInt(5), 6, 7, 8));
        String digest = QueryDigest.results(expected, false);
        for (var changed : List.of(
                new LookupResult(IndexKey.ofInt(9), 2, 3, 4),
                new LookupResult(IndexKey.ofInt(1), 9, 3, 4),
                new LookupResult(IndexKey.ofInt(1), 2, 3 + (1L << 32), 4),
                new LookupResult(IndexKey.ofInt(1), 2, 3, 4 + (1L << 32)))) {
            assertNotEquals(digest, QueryDigest.results(List.of(changed, expected.get(1)), false));
        }
        assertNotEquals(digest, QueryDigest.results(List.of(expected.get(1), expected.get(0)), false));
        assertNotEquals(digest, QueryDigest.results(List.of(expected.get(0)), false));
        // RowID mode intentionally excludes the unrelated physical position.
        assertEquals(QueryDigest.results(List.of(expected.get(0)), true),
                QueryDigest.results(List.of(new LookupResult(IndexKey.ofInt(1), 2, 3, 999)), true));
    }

    @Test
    public void bothLocatorInt64WordsArePartOfTheDigest() throws Exception {
        Operation operation = query(3, 1, 1);
        var expected = List.of(new LookupResult(IndexKey.ofInt(1), 1, 5, 11));
        Path stream = temporary.newFile().toPath();
        Files.delete(stream);
        try (var writer = new QueryDigest.File(stream, false)) {
            writer.accept(List.of(operation), expected);
        }
        var requests = new TraceRequests(ValueKind.LOCATOR, stream);
        var request = requests.request(61, operation);
        var wrong =
                Result.newBuilder()
                        .setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(1).bytes()))
                        .setVersion(1)
                        .setValue(Value.newBuilder().setFile(5).setPosition(12))
                        .build();
        assertThrows(
                IOException.class,
                () -> requests.check(operation, response(request.getId(), wrong)));

        Path correctStream = temporary.newFile().toPath();
        Files.delete(correctStream);
        try (var writer = new QueryDigest.File(correctStream, false)) {
            writer.accept(List.of(operation), expected);
        }
        var accepted = new TraceRequests(ValueKind.LOCATOR, correctStream);
        var correctRequest = accepted.request(62, operation);
        var correct =
                Result.newBuilder()
                        .setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(1).bytes()))
                        .setVersion(1)
                        .setValue(Value.newBuilder().setFile(5).setPosition(11))
                        .build();
        accepted.check(operation, response(correctRequest.getId(), correct));
        accepted.finish();
    }

    @Test
    public void digestStreamMustMatchTheTraceAndFilterBinding() throws Exception {
        Operation operation = query(3, 1, 1);
        Path file = temporary.newFile().toPath();
        Files.delete(file);
        try (var writer = new QueryDigest.File(file, true, "workload=a baseline=b version=9 secondary=true mode=rowid")) {
            writer.accept(List.of(operation), List.of(result(1, 1, 1)));
        }
        assertThrows(
                IOException.class,
                () ->
                        new TraceRequests(
                                ValueKind.ROW_ID,
                                file,
                                "workload=a baseline=b version=9 secondary=false mode=rowid"));
        var accepted =
                new TraceRequests(
                        ValueKind.ROW_ID,
                        file,
                        "workload=a baseline=b version=9 secondary=true mode=rowid");
        accepted.request(51, operation);
    }

    @Test
    public void missingExpectedRecordsAndUncheckedResponsesFailAtFinish() throws Exception {
        Operation operation = query(3, 1, 1);
        Path stream = expected(List.of(operation), List.of(List.of(result(1, 1, 1))));
        var requests = new TraceRequests(ValueKind.ROW_ID, stream);
        requests.request(41, operation);
        assertThrows(IOException.class, requests::finish);

        Path shortStream = temporary.newFile().toPath();
        var empty = new TraceRequests(ValueKind.ROW_ID, shortStream);
        assertThrows(IOException.class, () -> empty.request(42, operation));
    }
}