package org.sibench.capture;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The code a capture ran: the SIBench jar (the run's immutable copy, which also compiles it), the
 * instrumented PostgreSQL server and the row-capture trigger library. Unpackaged test classes
 * have no JAR identity; production captures record their immutable run JAR.
 */
final class CaptureBuild {
    static final String POSTGRES = "build/services/postgresql-capture/runtime/bin/postgres";
    static final String ROW_CAPTURE = "build/services/postgresql-capture/row_capture.so";

    private CaptureBuild() {}

    /** The build of this process under {@code root}; a missing part is absent from the map. */
    static Map<String, Object> identity(Path root, Map<String, Object> client) throws IOException {
        var result = new LinkedHashMap<String, Object>();
        Path jar = codeSource();
        if (jar != null) result.put("sibench_jar_sha256", FileDigests.sha256(jar));
        Path postgres = root.resolve(POSTGRES), library = root.resolve(ROW_CAPTURE);
        if (Files.isRegularFile(postgres)) result.put("postgres_sha256", FileDigests.sha256(postgres));
        if (Files.isRegularFile(library)) result.put("row_capture_sha256", FileDigests.sha256(library));
        result.put("native_client", client);
        return result;
    }

    /** The jar this class was loaded from, or null for a classes directory. */
    static Path codeSource() {
        try {
            var location = CaptureBuild.class.getProtectionDomain().getCodeSource();
            if (location == null) return null;
            Path path = Path.of(location.getLocation().toURI());
            return Files.isRegularFile(path) ? path : null;
        } catch (URISyntaxException | SecurityException error) {
            return null;
        }
    }

}
