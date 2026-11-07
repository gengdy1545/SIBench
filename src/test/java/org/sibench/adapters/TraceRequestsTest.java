package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;

import org.junit.Test;
import org.sibench.adapters.proto.Response;
import org.sibench.adapters.proto.Result;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.Value;
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

    @Test
    public void finalResponsesRequireMatchingIdentityAndCompletion() throws Exception {
        var requests = new TraceRequests(ValueKind.LOCATOR);
        var search = query(1, 1, 0);
        assertThrows(IOException.class, () -> requests.receive(search, 7).finish(response(8)));
        assertThrows(IOException.class, () -> requests.receive(search, 7)
                .finish(response(7).toBuilder().setMore(true).build()));
        requests.receive(search, 7).finish(response(7));
        for (Response invalid : List.of(response(8), response(7).toBuilder().setMore(true).build()))
            assertThrows(IOException.class, () -> requests.checkBatch(invalid, 7));
        requests.checkBatch(response(7), 7);
    }

    @Test
    public void storedFullWritesRespectTheSelectedValueMode() throws Exception {
        var operation = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(7), 0, 3, 9, 17);
        var stored = new TraceRequests(ValueKind.ROW_ID).request(0, operation);
        stored = stored.toBuilder().setWrite(stored.getWrite().toBuilder().setValue(
                stored.getWrite().getValue().toBuilder().setFile(3).setPosition(9))).build();
        for (var kind : List.of(ValueKind.ROW_ID, ValueKind.LOCATOR)) {
            var requests = new TraceRequests(kind);
            var expected = kind == ValueKind.ROW_ID ? Value.newBuilder().setRowId(17).build()
                    : Value.newBuilder().setFile(3).setPosition(9).build();
            assertEquals(expected, requests.forward(1, stored, List.of(operation)).getWrite().getValue());
            assertEquals(expected, requests.batch(2, List.of(operation), List.of(stored.getWrite()))
                    .getBatch().getWrites(0).getValue());
        }
    }

    @Test
    public void streamedResponsesCheckNativeCountsWithoutAnswerFiles() throws Exception {
        var operation = query(3, 1, 2);
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var stored = requests.request(11, operation);
        assertTrue(requests.forward(12, stored, List.of(operation)).hasSearch());
        var received = requests.receive(operation, 12);
        received.add(response(12, responseResult(8, 4, 9)).toBuilder().setMore(true).build());
        received.finish(response(12, responseResult(7, 2, 3)));
        assertEquals(1, requests.queries());
        assertEquals(2, requests.checkedResults());
        assertThrows(IOException.class, () -> requests.check(operation, response(13, responseResult(1, 1, 1))));
        assertThrows(IOException.class, () -> requests.check(operation, response(14,
                responseResult(1, 1, 1), responseResult(2, 1, 2), responseResult(3, 1, 3))));
        assertThrows(IOException.class, () -> requests.check(operation,
                response(15).toBuilder().setStatus(Status.INVALID).build()));
    }

    @Test
    public void baselineBatchIsOneExecuteMessageForMultipleOperations() throws Exception {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var first = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(1), 0, 0, 0, 1);
        var second = IndexOperationTrace.write(2, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(2), 0, 0, 0, 2);
        var message = requests.batch(1, List.of(first, second), List.of());
        assertEquals(2, message.getBatch().getWritesCount());
        assertEquals(1, requests.executeRequests());
        assertEquals(message.getSerializedSize(), requests.requestBytes());
    }
}
