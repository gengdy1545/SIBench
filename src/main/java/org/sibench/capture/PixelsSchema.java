package org.sibench.capture;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.sibench.schema.SourceSchema;

/** PostgreSQL column types as Pixels types, and the created Pixels tables as Trace tables. */
final class PixelsSchema {
    private static final Pattern TIMESTAMP = Pattern.compile("timestamp(\\(([0-6])\\))? without time zone");
    private static final Pattern SIZED = Pattern.compile("(character varying|character|numeric)\\(([0-9]+(,[0-9]+)?)\\)");

    /** The table spec sent to the Pixels host and the Pixels type of each table's key column. */
    record Spec(String text, Map<String, String> keyTypes) {}

    private PixelsSchema() {}

    static String type(String postgres) throws IOException {
        switch (postgres) {
            case "integer": return "int";
            case "bigint": return "bigint";
            case "smallint": return "smallint";
            case "real": return "float";
            case "double precision": return "double";
            case "date": return "date";
            case "character varying": return "varchar";
            default:
        }
        var timestamp = TIMESTAMP.matcher(postgres);
        if (timestamp.matches()) return "timestamp(" + (timestamp.group(2) == null ? "6" : timestamp.group(2)) + ")";
        var sized = SIZED.matcher(postgres);
        if (sized.matches()) return switch (sized.group(1)) {
            case "character varying" -> "varchar(" + sized.group(2) + ")";
            case "character" -> "char(" + sized.group(2) + ")";
            default -> {
                if (sized.group(3) == null) throw new IOException("numeric needs a declared scale for Pixels CDC: " + postgres);
                yield "decimal(" + sized.group(2) + ")";
            }
        };
        throw new IOException("Unsupported source column type for Pixels: " + postgres);
    }

    /**
     * Declared bound for an unbounded source varchar: the next power of two of the loaded maximum,
     * at least 256 bytes. Pixels truncates longer values, so later source values must stay within it.
     */
    static long bound(long loadedMaximum) {
        long bound = 256;
        while (bound < loadedMaximum) bound <<= 1;
        if (bound > 65535) throw new IllegalArgumentException("Source text exceeds the Pixels varchar limit");
        return bound;
    }

    /** Pixels "name TAB tableId TAB indexId" lines; source tables take their Trace ids, the marker 0. */
    static Map<Long, PixelsTrace.Table> tables(String created, SourceSchema source, Map<String, String> keyTypes, String marker)
            throws IOException {
        var ids = new HashMap<String, Integer>();
        int id = 0;
        for (var table : source.tables()) ids.put(table.name(), ++id);
        ids.put(marker, 0);
        if (!ids.keySet().containsAll(keyTypes.keySet())) throw new IOException("Unknown Pixels source table");
        ids.keySet().retainAll(keyTypes.keySet());
        var result = new HashMap<Long, PixelsTrace.Table>();
        for (String line : created.strip().split("\n")) {
            String[] fields = line.split("\t");
            if (fields.length != 3 || !ids.containsKey(fields[0]) || !keyTypes.containsKey(fields[0]))
                throw new IOException("Unexpected Pixels table: " + line);
            if (result.put(Long.parseLong(fields[1]), new PixelsTrace.Table(fields[0], ids.remove(fields[0]),
                    Long.parseLong(fields[2]), keyTypes.get(fields[0]))) != null) throw new IOException("Duplicate Pixels table id: " + line);
        }
        if (!ids.isEmpty()) throw new IOException("Pixels tables missing: " + ids.keySet());
        return result;
    }
}
