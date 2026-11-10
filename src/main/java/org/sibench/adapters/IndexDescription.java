package org.sibench.adapters;

import java.io.IOException;
import org.sibench.adapters.proto.Acknowledgement;
import org.sibench.adapters.proto.ValueKind;

/** Harness metadata, outside the index data RPCs. Every data service implements versioned reads. */
public record IndexDescription(String name, ValueKind valueKind, Acknowledgement writeAck,
                               String durability, String location) {
    public void require(ValueKind selected) throws IOException {
        if (selected == null || selected == ValueKind.VALUE_KIND_UNSPECIFIED || selected == ValueKind.UNRECOGNIZED)
            throw new IOException("Unspecified index service value mode");
        if (valueKind != selected) throw new IOException("Service stores " + valueKind + ", replay sends " + selected);
        if (name == null || name.isBlank() || durability == null || durability.isBlank()
                || location == null || location.isBlank() || writeAck == null
                || writeAck == Acknowledgement.ACK_UNSPECIFIED || writeAck == Acknowledgement.UNRECOGNIZED)
            throw new IOException("Incomplete index service deployment description");
    }
}
