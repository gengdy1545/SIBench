package org.sibench.adapters;

import java.io.IOException;

/** Harness metadata, outside the index data RPCs. Every data service implements versioned reads. */
public record IndexDescription(String name, ValueKind valueKind, Acknowledgement writeAck,
                               String durability, String location) {
    public void require(ValueKind selected) throws IOException {
        if (selected == null)
            throw new IOException("Unspecified index service value mode");
        if (valueKind != selected) throw new IOException("Service stores " + valueKind + ", replay sends " + selected);
        if (name == null || name.isBlank() || durability == null || durability.isBlank()
                || location == null || location.isBlank() || writeAck == null)
            throw new IOException("Incomplete index service deployment description");
    }
}
