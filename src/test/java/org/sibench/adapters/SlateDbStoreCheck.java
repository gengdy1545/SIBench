package org.sibench.adapters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Explicit S3-compatible integration check; requires a fresh object prefix and built JNI. */
public class SlateDbStoreCheck {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static String required(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Required property: " + name);
        return value;
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    @Test
    public void sameInstanceAckRangesSnapshotFlushAndReopen() throws Exception {
        ObjectStore objects = ObjectStore.of(Path.of(required("sibench.slatedb.storage")),
                required("sibench.slatedb.profile"), required("sibench.slatedb.object_path"));
        Path local = temporary.getRoot().toPath().resolve("index");
        try (var store = SlateDbStore.create(local, objects)) {
            store.put(bytes("a"), bytes("A"), true);
            store.put(bytes("b"), bytes("B"), true);
            store.put(bytes("d"), bytes("D"), true);
            assertArrayEquals(bytes("A"), store.get(bytes("a")));
            assertNull(store.get(bytes("absent")));
            try (var cursor = store.cursor(bytes("b"), bytes("e"))) {
                store.put(bytes("b"), bytes("replacement"), true);
                cursor.seek(bytes("a"));
                assertTrue(cursor.valid());
                assertArrayEquals(bytes("b"), cursor.key());
                assertArrayEquals(bytes("B"), cursor.value());
                cursor.next(); assertTrue(cursor.valid()); assertArrayEquals(bytes("d"), cursor.key());
                cursor.next(); assertFalse(cursor.valid());
                cursor.seekToLast(); assertTrue(cursor.valid()); assertArrayEquals(bytes("d"), cursor.key());
                cursor.prev(); assertTrue(cursor.valid()); assertArrayEquals(bytes("b"), cursor.key());
                cursor.prev(); assertFalse(cursor.valid());
                cursor.seekForPrev(bytes("c")); assertTrue(cursor.valid()); assertArrayEquals(bytes("b"), cursor.key());
                cursor.seek(bytes("e")); assertFalse(cursor.valid());
            }
            assertArrayEquals(bytes("replacement"), store.get(bytes("b")));
            store.flush();
        }
        assertThrows(IOException.class, () -> SlateDbStore.create(local, objects));
        try (var reopened = SlateDbStore.open(local, objects)) {
            assertArrayEquals(bytes("replacement"), reopened.get(bytes("b")));
        }
        // Refuse a second creator even when it uses a different local diagnostic directory.
        assertThrows(IOException.class, () -> SlateDbStore.create(temporary.getRoot().toPath().resolve("other"), objects));
    }
}
