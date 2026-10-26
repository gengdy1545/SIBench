package org.sibench.trace;

/**
 * A hash of encoded key bytes for hash maps. {@code Arrays.hashCode} (a base-31 polynomial) maps
 * the big-endian integer keys of the order-preserving encoding onto few values (two million
 * consecutive keys onto about 37 thousand), so maps keyed by them degrade into trees.
 */
public final class KeyHash {
    private KeyHash() {}

    /** 64-bit FNV-1a over the bytes, folded to 32 bits. */
    public static int of(byte[] bytes) {
        long hash = 0xcbf29ce484222325L;
        for (byte value : bytes) {
            hash ^= value & 0xff;
            hash *= 0x100000001b3L;
        }
        return (int) (hash ^ (hash >>> 32));
    }
}
