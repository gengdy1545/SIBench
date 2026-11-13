package org.sibench.adapters;

/** The deployment's write completion boundary, independent of remote durability. */
public enum Acknowledgement {
    ACK_MEMORY, // Before durable state.
    ACK_WAL // Recorded in the WAL, not necessarily fsynced.
}
