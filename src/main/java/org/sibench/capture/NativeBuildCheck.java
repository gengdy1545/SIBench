package org.sibench.capture;

import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/** The build copy that produced the native client must carry the pinned sources unchanged. */
final class NativeBuildCheck {
    private static final Set<String> SOURCE_SUFFIXES = Set.of(".java", ".c", ".h", ".lua", ".am", ".ac");

    private NativeBuildCheck() {}

    /** Each entry is a pinned source file or a directory searched recursively. */
    static void require(Path pinned, Path build, List<String> entries) throws IOException {
        for (String directory : entries) {
            Path expected = pinned.resolve(directory);
            if (!Files.exists(expected)) throw new IOException("Pinned source is missing: " + directory);
            long checked = 0;
            try (var files = Files.walk(expected)) {
                for (Path file : files.filter(Files::isRegularFile).sorted().toList()) {
                    String name = file.getFileName().toString();
                    if (SOURCE_SUFFIXES.stream().noneMatch(name::endsWith)) continue;
                    Path copy = build.resolve(pinned.relativize(file));
                    if (!Files.isRegularFile(copy)
                            || !BaselineGenerator.sha256(copy).equals(BaselineGenerator.sha256(file)))
                        throw new IOException(
                                "Build copy differs from the pinned source: " + pinned.relativize(file));
                    checked++;
                }
            }
            if (checked == 0) throw new IOException("No source files checked in " + directory);
        }
    }
}
