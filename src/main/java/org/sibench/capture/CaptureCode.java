package org.sibench.capture;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.zip.ZipFile;

/**
 * The part of the SIBench jar a certification vouches for: the code that runs while the native
 * workload is captured, not the compiler that turns a capture into a trace afterwards.
 *
 * <p>The row-state audit proves that a capture's raw events match the baseline and PostgreSQL's
 * final state, which depends on the JDBC capture driver, the run and prepare stages (triggers,
 * clones, client launch) and their configuration, never on how the trace is compiled. Compiler
 * changes are guarded by byte-identical recompilation of retained captures and by reference replay,
 * so they must not void a certification; a capture-side change must.
 *
 * <p>The capture code is the static class closure of {@link #ROOTS} inside the jar: every
 * {@code org.sibench} class whose name appears in a reachable class's constant pool, in binary or
 * source form, so reflective and {@code jdbc.drivers} names count too. Closure over names over-
 * approximates what runs, never under-approximates. It stops at {@link #CHECKED}: checks every
 * capture runs itself, certified or not, which therefore need no certificate. Third-party classes
 * are one digest, since they change only when a dependency does.
 */
final class CaptureCode {
    /** Bumped whenever the roots, the stops or the digest change, which voids older certificates. */
    static final int SCHEME = 1;

    private static final String PACKAGE = "org/sibench/";

    /** Code that runs during the prepare and run stages, in the capture JVM or the clients'. */
    static final List<Class<?>> ROOTS =
            List.of(
                    NativeCapture.class,
                    CaptureDriver.class,
                    CaptureRun.class,
                    HybenchNativePrepare.class,
                    HybenchNativeRun.class,
                    YcsbNativeRun.class,
                    SysbenchNativeRun.class);

    /**
     * Checks every capture runs itself, and the still database it starts from, which the exported
     * baseline it is audited against describes whatever it is.
     */
    static final List<Class<?>> CHECKED =
            List.of(
                    PgBaselineExport.class,
                    PgBaselineVerify.class,
                    org.sibench.schema.PgSchemaCheck.class,
                    RequestCaptureAudit.class,
                    ServerCaptureAudit.class,
                    HybenchNativeBase.class);

    private static final Pattern NAME = Pattern.compile("org[./]sibench[./][A-Za-z0-9_$./]*");

    private CaptureCode() {}

    /** {@code scheme}, the digest of every capture class by name, and the third-party digest. */
    static Map<String, Object> identity(Path jar) throws IOException {
        var classes = new TreeMap<String, String>();
        MessageDigest libraries = sha256();
        try (ZipFile zip = new ZipFile(jar.toFile())) {
            var stops = new TreeSet<String>();
            for (Class<?> type : CHECKED) stops.add(entry(type.getName()));
            var pending = new ArrayDeque<String>();
            for (Class<?> type : ROOTS) pending.add(entry(type.getName()));
            while (!pending.isEmpty()) {
                String name = pending.poll();
                if (classes.containsKey(name) || stops.contains(name)) continue;
                var item = zip.getEntry(name);
                if (item == null) continue;
                byte[] bytes;
                try (InputStream input = zip.getInputStream(item)) {
                    bytes = input.readAllBytes();
                }
                classes.put(name, HexFormat.of().formatHex(sha256().digest(bytes)));
                for (String referenced : references(bytes)) {
                    String candidate = referenced.replace('.', '/');
                    pending.add(candidate.endsWith(".class") ? candidate : candidate + ".class");
                }
            }
            var others = new TreeMap<String, java.util.zip.ZipEntry>();
            for (var entries = zip.entries(); entries.hasMoreElements(); ) {
                var item = entries.nextElement();
                String name = item.getName();
                if (item.isDirectory() || name.startsWith(PACKAGE)) continue;
                // Only the service registrations of META-INF change behavior; the rest is build data.
                if (name.startsWith("META-INF/") && !name.startsWith("META-INF/services/")) continue;
                others.put(name, item);
            }
            for (var item : others.entrySet()) {
                libraries.update(item.getKey().getBytes(StandardCharsets.UTF_8));
                libraries.update((byte) 0);
                try (InputStream input = zip.getInputStream(item.getValue())) {
                    libraries.update(sha256().digest(input.readAllBytes()));
                }
            }
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("scheme", SCHEME);
        result.put("classes", classes);
        result.put("libraries_sha256", HexFormat.of().formatHex(libraries.digest()));
        return result;
    }

    private static String entry(String className) {
        return className.replace('.', '/') + ".class";
    }

    /** Every {@code org.sibench} name in the class file's constant pool strings. */
    static Set<String> references(byte[] bytes) throws IOException {
        var names = new TreeSet<String>();
        var input = new DataInputStream(new java.io.ByteArrayInputStream(bytes));
        if (input.readInt() != 0xCAFEBABE) throw new IOException("Not a class file");
        input.readUnsignedShort();
        input.readUnsignedShort();
        int count = input.readUnsignedShort();
        for (int index = 1; index < count; index++) {
            int tag = input.readUnsignedByte();
            switch (tag) {
                case 1 -> {
                    var matcher = NAME.matcher(utf8(input));
                    while (matcher.find()) {
                        // A descriptor or nested name may trail a separator; keep the class part.
                        String name = matcher.group();
                        while (name.endsWith(".") || name.endsWith("/")) name = name.substring(0, name.length() - 1);
                        names.add(name);
                    }
                }
                case 7, 8, 16, 19, 20 -> input.readUnsignedShort();
                case 15 -> {
                    input.readUnsignedByte();
                    input.readUnsignedShort();
                }
                case 3, 4, 9, 10, 11, 12, 17, 18 -> input.readInt();
                case 5, 6 -> {
                    input.readLong();
                    index++;
                }
                default -> throw new IOException("Unknown constant pool tag " + tag);
            }
        }
        return names;
    }

    /** A constant pool string in the class file's modified UTF-8, read as plain bytes. */
    private static String utf8(DataInputStream input) throws IOException {
        int length = input.readUnsignedShort();
        var bytes = new ByteArrayOutputStream(length);
        bytes.write(input.readNBytes(length));
        return bytes.toString(StandardCharsets.ISO_8859_1);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
