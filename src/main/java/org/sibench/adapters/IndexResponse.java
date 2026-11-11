package org.sibench.adapters;

import com.google.protobuf.ByteString;
import java.util.List;

/** Internal execution completion; data RPCs use their own response messages. */
public record IndexResponse(IndexStatus status, String message, List<ByteString> values, boolean more) {
    public IndexResponse {
        values = List.copyOf(values);
    }

    public static IndexResponse ok(List<ByteString> values) {
        return new IndexResponse(IndexStatus.OK, "", values, false);
    }

    public static IndexResponse error(IndexStatus status, String message) {
        return new IndexResponse(status, message, List.of(), false);
    }
}
