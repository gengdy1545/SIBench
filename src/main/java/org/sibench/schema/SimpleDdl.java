package org.sibench.schema;

import org.sibench.schema.SourceSchema.*;

import java.io.IOException;
import java.util.*;

/** Tokenized grammar limited to the pinned HyBench table/index/sequence declarations. */
final class SimpleDdl {
    record Layout(List<Table> tables, List<Index> secondaryOrder) {}

    private final List<String> tokens;
    private int position;

    private SimpleDdl(String text) throws IOException {
        tokens = tokens(text);
    }

    static List<String> tokens(String text) throws IOException {
        var tokens = new ArrayList<String>();
        for (int i = 0; i < text.length(); ) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (text.startsWith("--", i)) {
                while (i < text.length() && text.charAt(i) != '\n') i++;
                continue;
            }
            if (text.startsWith("/*", i)) {
                int end = text.indexOf("*/", i + 2);
                if (end < 0) throw new IOException("Unterminated DDL comment");
                i = end + 2;
                continue;
            }
            if (c == '\'') {
                int start = i++;
                boolean closed = false;
                while (i < text.length()) {
                    if (text.charAt(i++) != '\'') continue;
                    if (i < text.length() && text.charAt(i) == '\'') {
                        i++;
                        continue;
                    }
                    closed = true;
                    break;
                }
                if (!closed) throw new IOException("Unterminated DDL string");
                tokens.add(text.substring(start, i));
            } else if (Character.isLetterOrDigit(c) || c == '_') {
                int start = i++;
                while (i < text.length()
                        && (Character.isLetterOrDigit(text.charAt(i)) || text.charAt(i) == '_'))
                    i++;
                tokens.add(text.substring(start, i).toLowerCase(Locale.ROOT));
            } else if ("(),;:.+-".indexOf(c) >= 0) {
                tokens.add(Character.toString(c));
                i++;
            } else throw new IOException("Unsupported DDL token: " + c);
        }
        return tokens;
    }

    static Layout read(String ddl, String indexDdl) throws IOException {
        var parser = new SimpleDdl(ddl);
        var tables = new LinkedHashMap<String, Table>();
        while (parser.more()) {
            parser.expect("create", "table");
            String name = parser.identifier();
            parser.expect("(");
            var columns = new ArrayList<Column>();
            var names = new HashSet<String>();
            String primary = null;
            do {
                String column = parser.identifier();
                if (!names.add(column)) throw new IOException("Duplicate DDL column: " + column);
                String type = parser.type();
                boolean pk = parser.take("primary");
                if (pk) {
                    parser.expect("key");
                    if (primary != null) throw new IOException("Multiple primary keys");
                    primary = column;
                }
                boolean nullable = !pk;
                if (parser.take("not")) {
                    parser.expect("null");
                    nullable = false;
                }
                DefaultValue value = DefaultValue.none();
                if (parser.take("default")) {
                    parser.expect("current_timestamp");
                    value = new DefaultValue("current_timestamp", "");
                }
                columns.add(new Column(column, type, nullable, value));
            } while (parser.take(","));
            parser.expect(")", ";");
            if (primary == null) throw new IOException("Missing primary key: " + name);
            if (tables.put(
                            name,
                            new Table(
                                    name,
                                    columns,
                                    List.of(new Index(name + "_pkey", primary, true))))
                    != null) throw new IOException("Duplicate DDL table: " + name);
        }
        parser = new SimpleDdl(indexDdl);
        var indexNames = new HashSet<String>();
        for (var table : tables.values()) indexNames.add(table.indexes().get(0).name());
        var sequences = new LinkedHashMap<String, String>();
        var set = new HashSet<String>();
        var owner = new HashSet<String>();
        var owned = new HashSet<String>();
        var secondaryOrder = new ArrayList<Index>();
        while (parser.more()) {
            if (parser.take("create")) {
                if (parser.take("index")) {
                    String name = parser.identifier();
                    parser.expect("on");
                    String tableName = parser.identifier();
                    parser.expect("(");
                    String column = parser.identifier();
                    parser.expect(")", ";");
                    Table table = required(tables, tableName);
                    if (!indexNames.add(name))
                        throw new IOException("Duplicate DDL index: " + name);
                    if (table.columns().stream().noneMatch(c -> c.name().equals(column)))
                        throw new IOException("Unknown indexed column: " + column);
                    var indexes = new ArrayList<>(table.indexes());
                    Index definition = new Index(name, column, false);
                    indexes.add(definition);
                    secondaryOrder.add(definition);
                    tables.put(tableName, new Table(tableName, table.columns(), indexes));
                } else {
                    parser.expect("sequence", "if", "not", "exists");
                    String sequence = parser.identifier();
                    parser.expect(";");
                    if (sequences.put(sequence, "") != null)
                        throw new IOException("Duplicate sequence: " + sequence);
                }
            } else if (parser.take("select")) {
                parser.expect("setval", "(");
                String sequence = parser.literal();
                parser.expect(",", "(", "select", "max", "(");
                String column = parser.identifier();
                parser.expect(")", "from");
                String table = parser.identifier();
                parser.expect(")", ")", ";");
                if (!sequence.equals(table + "_" + column + "_seq")
                        || !required(tables, table).indexes().get(0).column().equals(column)
                        || !sequences.containsKey(sequence)
                        || !set.add(sequence))
                    throw new IOException("Unsupported source sequence initialization");
            } else {
                parser.expect("alter");
                if (parser.take("table")) {
                    String tableName = parser.identifier();
                    parser.expect("alter", "column");
                    String column = parser.identifier();
                    parser.expect("set", "default", "nextval", "(");
                    String sequence = parser.literal();
                    parser.expect(":", ":", "regclass", ")", ";");
                    Table table = required(tables, tableName);
                    if (!sequences.containsKey(sequence)
                            || !sequence.equals(tableName + "_" + column + "_seq")
                            || !table.indexes().get(0).column().equals(column)
                            || !sequences.get(sequence).isEmpty())
                        throw new IOException("Unsupported source sequence default");
                    sequences.put(sequence, tableName + "." + column);
                    var columns =
                            table.columns().stream()
                                    .map(
                                            c ->
                                                    c.name().equals(column)
                                                            ? new Column(
                                                                    c.name(),
                                                                    c.type(),
                                                                    c.nullable(),
                                                                    DefaultValue.sequence(sequence))
                                                            : c)
                                    .toList();
                    tables.put(tableName, new Table(tableName, columns, table.indexes()));
                } else {
                    parser.expect("sequence");
                    String sequence = parser.identifier();
                    if (!sequences.containsKey(sequence)) throw new IOException("Unknown sequence");
                    if (parser.take("owner")) {
                        parser.expect("to", "postgres", ";");
                        if (!owner.add(sequence)) throw new IOException("Duplicate sequence owner");
                    } else {
                        parser.expect("owned", "by");
                        String table = parser.identifier();
                        parser.expect(".");
                        String column = parser.identifier();
                        parser.expect(";");
                        if (!(table + "." + column).equals(sequences.get(sequence))
                                || !owned.add(sequence))
                            throw new IOException("Sequence ownership mismatch");
                    }
                }
            }
        }
        if (!set.equals(sequences.keySet()) || !owner.equals(set) || !owned.equals(set))
            throw new IOException("Incomplete source sequence declarations");
        return new Layout(List.copyOf(tables.values()), List.copyOf(secondaryOrder));
    }

    private static Table required(Map<String, Table> tables, String name) throws IOException {
        Table table = tables.get(name);
        if (table == null) throw new IOException("Unknown source table: " + name);
        return table;
    }

    private String type() throws IOException {
        String type = next();
        return switch (type) {
            case "int", "integer" -> "integer";
            case "real", "date", "text" -> type;
            case "timestamp" -> "timestamp without time zone";
            case "varchar" -> "character varying";
            case "char" -> {
                expect("(");
                String width = next();
                if (!width.matches("[1-9][0-9]*")) throw new IOException("Invalid CHAR width");
                expect(")");
                yield "character(" + width + ")";
            }
            default -> throw new IOException("Unsupported source column type: " + type);
        };
    }

    private boolean more() {
        return position < tokens.size();
    }

    private String next() throws IOException {
        if (!more()) throw new IOException("Truncated DDL");
        return tokens.get(position++);
    }

    private String identifier() throws IOException {
        String value = next();
        if (!value.matches("[a-z][a-z0-9_]*")) throw new IOException("Unsupported SQL identifier");
        return value;
    }

    private String literal() throws IOException {
        String value = next();
        if (!value.startsWith("'") || !value.endsWith("'"))
            throw new IOException("Expected literal");
        return value.substring(1, value.length() - 1).replace("''", "'");
    }

    private boolean take(String token) {
        if (!more() || !tokens.get(position).equals(token)) return false;
        position++;
        return true;
    }

    private void expect(String... expected) throws IOException {
        for (String token : expected)
            if (!take(token))
                throw new IOException(
                        "Unsupported DDL; expected "
                                + token
                                + ", got "
                                + (more() ? tokens.get(position) : "end"));
    }
}
