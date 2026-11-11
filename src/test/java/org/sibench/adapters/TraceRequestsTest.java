package org.sibench.adapters;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.adapters.proto.Response;
import org.sibench.adapters.proto.Result;
import org.sibench.adapters.proto.Status;
import org.sibench.trace.IndexEntry;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.List;

public class TraceRequestsTest {

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

    private static Result responseResult(long key, long version, long rowId) {
        return Result.newBuilder()
                .setKey(new IndexEntry(1,0,null,IndexKey.ofInt(key)).bytes())
                .setTs(version)
                .setValue(IndexEntry.rowId(rowId))
                .build();
    }

    private static Response response(Result... results) {
        return Response.newBuilder()
                .setStatus(Status.OK)
                .addAllResults(List.of(results))
                .build();
    }

    @Test
    public void exactKeysUseGetAndNonuniqueSecondaryValuesUseScan() throws Exception {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var primary = requests.request(query(1, 7, 1));
        assertTrue(primary.hasGet());
        assertEquals(10, primary.getGet().getReadTs());
        var point = Range.equal(IndexKey.ofInt(7));
        var secondary = requests.request(IndexOperationTrace.search(2, 1, 1, point, 10, 2, 0));
        assertTrue(secondary.hasScan());
        assertEquals(10, secondary.getScan().getReadTs());
        assertEquals(0, secondary.getScan().getLimit());
        var pinned = requests.request(IndexOperationTrace.search(3, 1, 1, point,
                IndexKey.ofInt(9), 10, 1, 0, 0, 0));
        assertTrue(pinned.hasGet());
        assertEquals(new IndexEntry(1, 1, IndexKey.ofInt(7), IndexKey.ofInt(9)).bytes(), pinned.getGet().getKey());
    }

    @Test
    public void finalResponsesRequireSuccessfulCompletion() throws Exception {
        var requests = new TraceRequests(ValueKind.LOCATOR);
        var search = query(1, 1, 0);
        assertThrows(IOException.class, () -> requests.receive(search, 7)
                .finish(response().toBuilder().setMore(true).build()));
        requests.receive(search, 7).finish(response());
        for (Response invalid : List.of(response().toBuilder().setStatus(Status.INVALID).build(), response().toBuilder().setMore(true).build()))
            assertThrows(IOException.class, () -> requests.checkBatch(invalid));
        requests.checkBatch(response());
    }

    @Test
    public void storedFullWritesRespectTheSelectedValueMode() throws Exception {
        var operation = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(7), 0, 3, 9, 17);
        var stored = new TraceRequests(ValueKind.ROW_ID).request(operation);
        for (var kind : List.of(ValueKind.ROW_ID, ValueKind.LOCATOR)) {
            var requests = new TraceRequests(kind);
            var expected = kind == ValueKind.ROW_ID ? IndexEntry.rowId(17)
                    : IndexEntry.locator(3,9);
            assertEquals(expected, requests.forward(stored, List.of(operation)).getPut().getValue());
            assertEquals(expected, requests.batch(List.of(operation), List.of(stored.getPut()))
                    .getBatch().getMutations(0).getPut().getValue());
        }
    }

    @Test
    public void streamedResponsesCheckNativeCountsWithoutAnswerFiles() throws Exception {
        var operation = IndexOperationTrace.search(3, 1, 0,
                new Range(null, false, null, false, false, 0), 10, 2, 0);
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var stored = requests.request(operation);
        assertTrue(requests.forward(stored, List.of(operation)).hasScan());
        var received = requests.receive(operation, 12);
        received.add(response(responseResult(8, 4, 9)).toBuilder().setMore(true).build());
        received.finish(response(responseResult(7, 2, 3)));
        assertEquals(1, requests.queries());
        assertEquals(2, requests.checkedResults());
        assertThrows(IOException.class, () -> requests.check(operation, response(responseResult(1, 1, 1))));
        assertThrows(IOException.class, () -> requests.check(operation, response(
                responseResult(1, 1, 1), responseResult(2, 1, 2), responseResult(3, 1, 3))));
        assertThrows(IOException.class, () -> requests.check(operation,
                response().toBuilder().setStatus(Status.INVALID).build()));
    }

    @Test
    public void baselineBatchMapsToOneGenericBatchWrite() throws Exception {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var first = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(1), 0, 0, 0, 1);
        var second = IndexOperationTrace.write(2, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(2), 0, 0, 0, 2);
        var message = requests.batch(List.of(first, second), List.of());
        var batch = message.getBatch();
        assertEquals(2, batch.getMutationsCount());
        assertEquals(0, batch.getMutations(0).getPut().getTs());
        assertEquals(new IndexEntry(1, 0, null, IndexKey.ofInt(2)).bytes(), batch.getMutations(1).getPut().getKey());
    }
}
