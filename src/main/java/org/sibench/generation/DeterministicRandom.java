package org.sibench.generation;

/** SplitMix64 with a row-addressed state, independent of scheduling and traversal. */
public final class DeterministicRandom {
    private long state;

    public DeterministicRandom(long seed, String domain, long ordinal) {
        long hash = 0xcbf29ce484222325L;
        for (byte b : domain.getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
            hash = (hash ^ (b & 255)) * 0x100000001b3L;
        }
        state = mix(seed ^ hash ^ mix(ordinal));
    }

    public static long mix(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    public long next() {
        state += 0x9e3779b97f4a7c15L;
        return mix(state);
    }

    public long between(long lower, long upper) {
        long bound = Math.subtractExact(upper, lower);
        if (bound <= 0) throw new IllegalArgumentException("Empty random range");
        long bits, value;
        do {
            bits = next() >>> 1;
            value = bits % bound;
        } while (bits - value + (bound - 1) < 0);
        return lower + value;
    }

    public double fraction() {
        return (next() >>> 11) * 0x1.0p-53;
    }

    public String pick(String[] values) {
        return values[(int) between(0, values.length)];
    }
}
