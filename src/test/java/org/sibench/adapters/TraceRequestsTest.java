package org.sibench.adapters;

import static org.junit.Assert.*;

import org.junit.Test;
import com.google.protobuf.ByteString;
import org.sibench.trace.IndexEntry;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.List;

public class TraceRequestsTest {

    /** Supplies the current Trace payload to transport tests using hand-specified operations. */
    static TraceOperation stored(Operation operation) throws IOException {
        var request = new TraceRequests(ValueKind.ROW_ID).request(operation);
        var stored = TraceOperation.newBuilder();
        if (request instanceof GetRequest get) stored.setGet(get);
        else if (request instanceof ScanRequest scan) stored.setScan(scan);
        else if (request instanceof PutRequest put) stored.setPut(put);
        else if (request instanceof DeleteRequest delete) stored.setDelete(delete);
        else throw new AssertionError("Unexpected data request");
        return stored.build();
    }

    static org.sibench.replay.ReplayTarget.SendHandle send(
            org.sibench.replay.ReplayTarget client, long sequence, Operation operation) throws IOException {
        return client.send(sequence, operation, stored(operation));
    }

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

    private static IndexResponse response(ByteString... results) {
        return IndexResponse.ok(List.of(results));
    }

    @Test
    public void exactKeysUseGetAndNonuniqueSecondaryValuesUseScan() throws Exception {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var primary = requests.request(query(1, 7, 1));
        assertTrue(primary instanceof GetRequest);
        assertEquals(10, ((GetRequest) primary).getReadTs());
        var point = Range.equal(IndexKey.ofInt(7));
        var secondary = requests.request(IndexOperationTrace.search(2, 1, 1, point, 10, 2, 0));
        assertTrue(secondary instanceof ScanRequest);
        assertEquals(10, ((ScanRequest) secondary).getReadTs());
        assertEquals(0, ((ScanRequest) secondary).getLimit());
        var pinned = requests.request(IndexOperationTrace.search(3, 1, 1, point,
                IndexKey.ofInt(9), 10, 1, 0, 0, 0));
        assertTrue(pinned instanceof GetRequest);
        assertEquals(new IndexEntry(1, 1, IndexKey.ofInt(7), IndexKey.ofInt(9)).bytes(), ((GetRequest) pinned).getKey());
    }

    @Test
    public void finalResponsesRequireSuccessfulCompletion() throws Exception {
        var requests = new TraceRequests(ValueKind.LOCATOR);
        var search = query(1, 1, 0);
        assertThrows(IOException.class, () -> requests.receive(search, 7)
                .finish(new IndexResponse(IndexStatus.OK, "", List.of(), true)));
        requests.receive(search, 7).finish(response());
        for (IndexResponse invalid : List.of(IndexResponse.error(IndexStatus.INVALID, ""), new IndexResponse(IndexStatus.OK, "", List.of(), true)))
            assertThrows(IOException.class, () -> requests.checkBatch(invalid));
        requests.checkBatch(response());
    }

    @Test
    public void storedFullWritesRespectTheSelectedValueMode() throws Exception {
        var operation = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(7), 0, 3, 9, 17);
        var stored = TraceOperation.newBuilder().setPut((PutRequest) new TraceRequests(ValueKind.ROW_ID).request(operation)).build();
        for (var kind : List.of(ValueKind.ROW_ID, ValueKind.LOCATOR)) {
            var requests = new TraceRequests(kind);
            var expected = kind == ValueKind.ROW_ID ? IndexEntry.rowId(17)
                    : IndexEntry.locator(3,9);
            assertEquals(expected, ((PutRequest) requests.forward(stored, operation)).getValue());
        }
    }

    @Test
    public void forwardingRequiresTheMatchingDataRequest() throws Exception {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var search = query(1, 7, 1);
        var write = IndexOperationTrace.write(1, IndexOperationTrace.UPSERT, 1, 0,
                IndexKey.ofInt(7), 1, 0, 0, 17);
        assertThrows(IOException.class, () -> requests.forward(stored(write), search));
        assertThrows(IOException.class, () -> requests.forward(stored(search), write));
        assertThrows(IOException.class, () -> requests.forward(stored(write), IndexOperationTrace.transaction(
                0, IndexOperationTrace.TRANSACTION_COMMIT, 1, 1, 0)));
    }

    @Test
    public void streamedResponsesCheckNativeCountsWithoutAnswerFiles() throws Exception {
        var operation = IndexOperationTrace.search(3, 1, 0,
                new Range(null, false, null, false, false, 0), 10, 2, 0);
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var stored = TraceOperation.newBuilder().setScan((ScanRequest) requests.request(operation)).build();
        assertTrue(requests.forward(stored, operation) instanceof ScanRequest);
        var received = requests.receive(operation, 12);
        received.add(new IndexResponse(IndexStatus.OK, "", List.of(IndexEntry.rowId(9)), true));
        received.finish(response(IndexEntry.rowId(3)));
        assertEquals(1, requests.queries());
        assertEquals(2, requests.checkedResults());
        assertThrows(IOException.class, () -> requests.check(operation, response(IndexEntry.rowId(1))));
        assertThrows(IOException.class, () -> requests.check(operation, response(
                IndexEntry.rowId(1), IndexEntry.rowId(2), IndexEntry.rowId(3))));
        assertThrows(IOException.class, () -> requests.check(operation,
                IndexResponse.error(IndexStatus.INVALID, "")));
    }

}
