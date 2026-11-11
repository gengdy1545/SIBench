package org.sibench.adapters;

import static org.junit.Assert.*;
import com.google.protobuf.ByteString;
import java.io.*;
import java.util.List;
import org.junit.Test;

public class DriverCapacityCheckTest {
    @Test public void fixtureRequestsRetainTheirRpcTypeAndRejectTraceRecords() throws Exception {
        var requests = List.of(org.sibench.adapters.proto.GetRequest.getDefaultInstance(),
                org.sibench.adapters.proto.PutRequest.getDefaultInstance(),
                org.sibench.adapters.proto.DeleteRequest.getDefaultInstance(),
                org.sibench.adapters.proto.ScanRequest.getDefaultInstance(),
                org.sibench.adapters.proto.WriteBatchRequest.getDefaultInstance());
        var bytes = new ByteArrayOutputStream();
        for (var request : requests) com.google.protobuf.Any.pack(request).writeDelimitedTo(bytes);
        var input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        for (var request : requests) assertEquals(request, DriverCapacityCheck.readRequest(input));
        assertThrows(IOException.class, () -> DriverCapacityCheck.readRequest(input));
        bytes.reset();
        com.google.protobuf.Any.pack(org.sibench.adapters.proto.TraceRecord.getDefaultInstance()).writeDelimitedTo(bytes);
        assertThrows(IOException.class, () -> DriverCapacityCheck.readRequest(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))));
    }

    @Test public void fixturePreservesEmptyValuesDuplicatesAndCompletion() throws Exception {
        var values = List.of(ByteString.EMPTY, ByteString.copyFrom(new byte[] {0, (byte) 0xff}), ByteString.EMPTY);
        var chunk = new IndexResponse(IndexStatus.OK, "", values, true);
        var bytes = new ByteArrayOutputStream();
        var output = new DataOutputStream(bytes);
        DriverCapacityCheck.writeResponse(output, chunk);
        DriverCapacityCheck.writeResponse(output, IndexResponse.ok(List.of()));
        assertArrayEquals(new byte[] {1, 8, 10, 0, 10, 2, 0, (byte) 0xff, 10, 0, 0, 0}, bytes.toByteArray());
        var input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        assertEquals(chunk, DriverCapacityCheck.readResponse(input));
        assertEquals(IndexResponse.ok(List.of()), DriverCapacityCheck.readResponse(input));
        assertEquals(-1, input.read());
    }

    @Test public void fixtureRejectsFailuresTruncationAndUnknownFields() throws Exception {
        for (var status : List.of(IndexStatus.INVALID, IndexStatus.UNAVAILABLE))
            assertThrows(IOException.class, () -> DriverCapacityCheck.writeResponse(
                    new DataOutputStream(new ByteArrayOutputStream()), IndexResponse.error(status, "failed")));
        for (var bytes : List.of(new byte[0], new byte[] {1}, new byte[] {1, 3, 10, 2, 1}, new byte[] {0, 2, 16, 1}))
            assertThrows(IOException.class, () -> DriverCapacityCheck.readResponse(
                    new DataInputStream(new ByteArrayInputStream(bytes))));
    }
}
