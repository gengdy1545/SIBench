package org.sibench.generation;

import java.io.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.logging.Logger;

/** The JDBC subset used by generation and HyBench, over the local native DuckDB host. */
public final class DuckDbHostDriver implements Driver {
    public static final String PREFIX = "jdbc:sibench:duckdb:";
    private static final int MAX_FIELD = 16 * 1024 * 1024;
    private static final int MAX_MESSAGE = 64 * 1024 * 1024;

    static {
        try { DriverManager.registerDriver(new DuckDbHostDriver()); }
        catch (SQLException e) { throw new ExceptionInInitializerError(e); }
    }

    public static Connection open(Path socket) throws SQLException {
        return new Session(socket).connection;
    }

    public static Connection open(Path socket, Path receipts) throws SQLException {
        Session session = new Session(socket);
        try {
            session.recordTo(receipts);
            return session.connection;
        } catch (SQLException failure) {
            try { session.close(); } catch (SQLException close) { failure.addSuppressed(close); }
            throw failure;
        }
    }

    @Override public Connection connect(String url, Properties properties) throws SQLException {
        if (!acceptsURL(url)) return null;
        if (properties.stringPropertyNames().stream().anyMatch(name -> !name.equals("receipts")))
            throw new SQLException("DuckDB host only accepts the receipts connection property");
        Path socket = Path.of(url.substring(PREFIX.length()));
        String receipts = properties.getProperty("receipts");
        if (receipts != null && receipts.isBlank()) throw new SQLException("Empty SQL receipt directory");
        return receipts == null ? open(socket) : open(socket, Path.of(receipts));
    }
    @Override public boolean acceptsURL(String url) { return url != null && url.startsWith(PREFIX); }
    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) { return new DriverPropertyInfo[0]; }
    @Override public int getMajorVersion() { return 1; }
    @Override public int getMinorVersion() { return 0; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public Logger getParentLogger() { return Logger.getLogger("org.sibench.generation"); }

    private static SQLException unsupported(String name) {
        return new SQLFeatureNotSupportedException("DuckDB host JDBC does not support " + name);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(DuckDbHostDriver.class.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private abstract static class Handle implements InvocationHandler {
        final Session session;
        Handle(Session session) { this.session = session; }
        @Override public final Object invoke(Object self, Method method, Object[] arguments) throws Throwable {
            Object[] args = arguments == null ? new Object[0] : arguments;
            synchronized (session) {
                return switch (method.getName()) {
                    case "toString" -> "DuckDB host " + method.getDeclaringClass().getSimpleName();
                    case "hashCode" -> System.identityHashCode(self);
                    case "equals" -> self == args[0];
                    case "unwrap" -> {
                        if (((Class<?>) args[0]).isInstance(self)) yield self;
                        throw unsupported("unwrap " + args[0]);
                    }
                    case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(self);
                    default -> call(method.getName(), args);
                };
            }
        }
        abstract Object call(String method, Object[] args) throws SQLException;
    }

    @FunctionalInterface private interface Request<T> { T run() throws IOException, SQLException; }

    private static final class Session {
        final SocketChannel socket;
        final DataInputStream input;
        final DataOutputStream output;
        final Connection connection;
        boolean autoCommit = true;
        boolean closed;
        Rows active;
        Receipt receipt;

        Session(Path path) throws SQLException {
            SocketChannel opened = null;
            try {
                opened = SocketChannel.open(StandardProtocolFamily.UNIX);
                opened.connect(UnixDomainSocketAddress.of(path));
                socket = opened;
                input = new DataInputStream(new BufferedInputStream(Channels.newInputStream(socket)));
                output = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(socket)));
            } catch (IOException e) {
                if (opened != null) try { opened.close(); } catch (IOException close) { e.addSuppressed(close); }
                throw new SQLNonTransientConnectionException("Cannot connect to DuckDB host: " + path, e);
            }
            connection = proxy(Connection.class, new Handle(this) {
                @Override Object call(String method, Object[] args) throws SQLException {
                    if (method.equals("isClosed")) return closed;
                    if (method.equals("close")) { close(); return null; }
                    requireOpen();
                    return switch (method) {
                        case "createStatement" -> { requireForwardOnly(args, 0); yield new Query(Session.this, null).statement; }
                        case "prepareStatement" -> {
                            requireForwardOnly(args, 1);
                            yield new Query(Session.this, (String) args[0]).statement;
                        }
                        case "getAutoCommit" -> autoCommit;
                        case "setAutoCommit" -> {
                            boolean value = (boolean) args[0];
                            closeActive();
                            exchange(() -> { output.writeByte(5); output.writeBoolean(value); check(); return null; });
                            autoCommit = value; yield null;
                        }
                        case "commit", "rollback" -> {
                            closeActive(); command(method.equals("commit") ? 6 : 7); yield null;
                        }
                        case "getTransactionIsolation" -> Connection.TRANSACTION_REPEATABLE_READ;
                        case "getWarnings" -> null;
                        case "clearWarnings" -> null;
                        default -> throw unsupported("Connection." + method);
                    };
                }
            });
        }

        void requireOpen() throws SQLException {
            if (closed) throw new SQLNonTransientConnectionException("DuckDB connection is closed");
        }
        void recordTo(Path directory) throws SQLException {
            long id, after;
            try (var query = connection.createStatement();
                    var rows = query.executeQuery("SELECT current_connection_id(), current_query_id()")) {
                if (!rows.next()) throw new SQLException("Missing native SQL connection identity");
                id = rows.getLong(1); after = rows.getLong(2);
                if (rows.next()) throw new SQLException("Multiple native SQL connection identities");
            }
            receipt = new Receipt(directory, id, after);
        }
        <T> T exchange(Request<T> request) throws SQLException {
            requireOpen();
            try { return request.run(); }
            catch (IOException e) {
                closed = true;
                if (active != null) active.closed = true;
                try { socket.close(); } catch (IOException close) { e.addSuppressed(close); }
                throw new SQLNonTransientConnectionException("DuckDB host transport failed", e);
            }
        }
        void check() throws IOException, SQLException {
            output.flush();
            int status = input.readUnsignedByte();
            if (status == 1) {
                int code = input.readInt();
                throw new SQLException(readString(input), "HY000", code);
            }
            if (status != 0) throw new IOException("Invalid DuckDB response status");
        }
        void command(int command) throws SQLException {
            exchange(() -> { output.writeByte(command); check(); return null; });
        }
        void closeActive() throws SQLException { if (active != null) active.close(); }
        void close() throws SQLException {
            if (closed) { if (receipt != null) receipt.close(); return; }
            try {
                closeActive(); command(4);
                if (receipt != null) receipt.complete();
            }
            finally {
                closed = true;
                if (active != null) active.closed = true;
                try { socket.close(); }
                catch (IOException e) { throw new SQLException("Cannot close DuckDB socket", e); }
                finally { if (receipt != null) receipt.close(); }
            }
        }
        void execute(Query query, String sql, SortedMap<Integer, Parameter> parameters) throws SQLException {
            // Encode before sending so a local validation failure cannot leave a partial request.
            var bytes = new ByteArrayOutputStream();
            try (var request = new DataOutputStream(bytes)) {
                request.writeByte(1); writeString(request, sql);
                if (parameters.size() > 65536) throw new SQLException("Too many DuckDB parameters");
                request.writeInt(parameters.size());
                for (int i = 1; i <= parameters.size(); i++) {
                    var parameter = parameters.get(i);
                    if (parameter == null) throw new SQLException("Parameter " + i + " is not bound");
                    parameter.write(request);
                    if (bytes.size() > MAX_MESSAGE) throw new SQLException("DuckDB request exceeds transport limit");
                }
            } catch (IOException e) { throw new SQLException("Cannot encode DuckDB request", e); }
            closeActive();
            query.rows = null; query.changed = -1;
            if (receipt != null) receipt.begin();
            try { exchange(() -> {
                output.write(bytes.toByteArray()); check();
                int kind = input.readUnsignedByte();
                if (kind == 0) query.changed = input.readLong();
                else if (kind == 1) {
                    int columns = input.readInt();
                    if (columns <= 0 || columns > 65536) throw new IOException("Invalid column count");
                    var names = new String[columns]; var types = new String[columns];
                    for (int i = 0; i < columns; i++) { names[i] = readString(input); types[i] = readString(input); }
                    query.rows = new Rows(this, query, names, types);
                    active = query.rows; active.batch();
                } else throw new IOException("Invalid query result kind");
                return null;
            }); }
            catch (SQLException failure) { failed(failure); throw failure; }
            if (receipt != null && (query.rows == null || query.rows.ended))
                receipt.end(0, query.rows == null ? query.changed : 0);
        }

        void failed(SQLException failure) {
            // Transport failures have no native outcome; leave the receipt incomplete.
            if (receipt != null && failure.getErrorCode() > 0) {
                try { receipt.end(failure.getErrorCode(), -1); }
                catch (SQLException logging) { failure.addSuppressed(logging); }
            }
        }
    }

    /** Same submission format as the sysbench driver; native facts supply transaction outcomes. */
    private static final class Receipt {
        final BufferedWriter output;
        long operations;
        boolean pending, broken;

        Receipt(Path directory, long connection, long query) throws SQLException {
            try {
                output = Files.newBufferedWriter(directory.resolve("connection-" + connection + ".csv"),
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            } catch (IOException e) { throw new SQLException("Cannot create SQL receipt", e); }
            try { write("event,operation,kind,connection_id,after_query,error_code,rows\n"
                    + "open,0,session," + connection + "," + query + ",0,0\n"); }
            catch (SQLException failure) {
                try { close(); } catch (SQLException close) { failure.addSuppressed(close); }
                throw failure;
            }
        }
        void write(String line) throws SQLException {
            if (broken) throw new SQLException("SQL receipt is incomplete");
            try { output.write(line); output.flush(); }
            catch (IOException e) { broken = true; throw new SQLException("Cannot write SQL receipt", e); }
        }
        void begin() throws SQLException {
            if (pending) throw new SQLException("Previous SQL receipt is incomplete");
            pending = true;
            write("begin," + ++operations + ",query,,,0,0\n");
        }
        void end(int error, long rows) throws SQLException {
            if (!pending) throw new SQLException("SQL receipt has no pending submission");
            write("end," + operations + ",query,,," + error + "," + rows + "\n");
            pending = false;
        }
        void complete() throws SQLException {
            if (pending) throw new SQLException("Cannot complete unfinished SQL receipt");
            write("close," + operations + ",session,,,0,0\n");
        }
        void close() throws SQLException {
            try { output.close(); } catch (IOException e) { throw new SQLException("Cannot close SQL receipt", e); }
        }
    }

    private static void requireForwardOnly(Object[] args, int start) throws SQLException {
        if (args.length == start) return;
        if (args.length == start + 2 && (int) args[start] == ResultSet.TYPE_FORWARD_ONLY
                && (int) args[start + 1] == ResultSet.CONCUR_READ_ONLY) return;
        throw unsupported("statement options (only forward-only, read-only results are supported)");
    }

    private record Parameter(int tag, byte[] value, int width, int scale) {
        static Parameter of(Object value) throws SQLException {
            if (value == null) return new Parameter(0, null, 0, 0);
            if (value instanceof byte[] bytes) return new Parameter(9, bytes.clone(), 0, 0);
            int tag;
            if (value instanceof String) tag = 1;
            else if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) tag = 2;
            else if (value instanceof Float || value instanceof Double) tag = 3;
            else if (value instanceof BigDecimal || value instanceof BigInteger) {
                BigDecimal decimal = value instanceof BigDecimal d ? d : new BigDecimal((BigInteger) value);
                if ((long) decimal.precision() - decimal.scale() > 38)
                    throw new SQLException("DuckDB DECIMAL precision exceeds 38");
                if (decimal.scale() < 0) decimal = decimal.setScale(0);
                int width = Math.max(1, Math.max(decimal.precision(), decimal.scale()));
                if (width > 38) throw new SQLException("DuckDB DECIMAL precision exceeds 38");
                return new Parameter(4, decimal.toPlainString().getBytes(StandardCharsets.UTF_8), width, decimal.scale());
            } else if (value instanceof Boolean) tag = 5;
            else if (value instanceof java.sql.Date || value instanceof LocalDate) tag = 6;
            else if (value instanceof Time || value instanceof LocalTime) {
                if (value instanceof LocalTime time && time.getNano() % 1000 != 0)
                    throw unsupported("time parameters finer than DuckDB TIME microseconds");
                tag = 7;
            }
            else if (value instanceof Timestamp || value instanceof LocalDateTime) {
                int nanos = value instanceof Timestamp t ? t.getNanos() : ((LocalDateTime) value).getNano();
                if (nanos % 1000 != 0) throw unsupported("timestamp parameters finer than DuckDB TIMESTAMP microseconds");
                tag = 8;
            } else throw unsupported("parameter type " + value.getClass().getName());
            return new Parameter(tag, value.toString().getBytes(StandardCharsets.UTF_8), 0, 0);
        }
        void write(DataOutputStream out) throws IOException {
            out.writeByte(tag);
            if (tag == 0) return;
            writeBytes(out, value);
            if (tag == 4) { out.writeByte(width); out.writeByte(scale); }
        }
    }

    private static final class Query extends Handle {
        final Statement statement;
        final String prepared;
        final SortedMap<Integer, Parameter> parameters = new TreeMap<>();
        Rows rows;
        long changed = -1;
        boolean closed;

        Query(Session session, String sql) {
            super(session); prepared = sql;
            statement = sql == null ? proxy(Statement.class, this) : proxy(PreparedStatement.class, this);
        }
        @Override Object call(String method, Object[] args) throws SQLException {
            if (method.equals("isClosed")) return closed || session.closed;
            if (method.equals("close")) {
                if (!closed) { if (rows != null) rows.close(); closed = true; } return null;
            }
            session.requireOpen();
            if (closed) throw new SQLException("Statement is closed");
            switch (method) {
                case "setString", "setInt", "setLong", "setShort", "setByte", "setDouble", "setFloat", "setBoolean",
                     "setBigDecimal", "setBytes", "setDate", "setTime", "setTimestamp", "setObject", "setNull": {
                    if (prepared == null || args.length != 2) throw unsupported(method + " overload");
                    int index = (int) args[0];
                    if (index < 1 || index > 65536) throw new SQLException("Invalid parameter index: " + index);
                    parameters.put(index, Parameter.of(method.equals("setNull") ? null : args[1])); return null;
                }
                case "clearParameters": parameters.clear(); return null;
                case "execute", "executeQuery", "executeUpdate", "executeLargeUpdate": {
                    if (args.length != (prepared == null ? 1 : 0)) throw unsupported(method + " overload");
                    session.execute(this, prepared == null ? (String) args[0] : prepared, parameters);
                    return switch (method) {
                        case "execute" -> rows != null;
                        case "executeQuery" -> {
                            if (rows == null) throw new SQLException("Statement did not return rows");
                            yield rows.result;
                        }
                        default -> {
                            if (rows != null) { rows.close(); throw new SQLException("Statement returned rows"); }
                            if (method.equals("executeLargeUpdate")) yield Long.valueOf(changed);
                            yield Integer.valueOf(exactInt(changed));
                        }
                    };
                }
                case "getResultSet": return rows == null ? null : rows.result;
                case "getUpdateCount": return exactInt(changed);
                case "getLargeUpdateCount": return changed;
                case "getConnection": return session.connection;
                case "getResultSetType": return ResultSet.TYPE_FORWARD_ONLY;
                case "getResultSetConcurrency": return ResultSet.CONCUR_READ_ONLY;
                case "getWarnings": return null;
                case "clearWarnings": return null;
                default: throw unsupported("Statement." + method);
            }
        }
    }

    private static final class Rows extends Handle {
        final Query query;
        final String[] names, types;
        final ResultSet result;
        final ResultSetMetaData metadata;
        byte[][][] values = new byte[0][][];
        int position = -1;
        boolean ended, closed, wasNull;
        long consumed;

        Rows(Session session, Query query, String[] names, String[] types) {
            super(session); this.query = query; this.names = names; this.types = types;
            result = proxy(ResultSet.class, this);
            metadata = proxy(ResultSetMetaData.class, new Handle(session) {
                @Override Object call(String method, Object[] args) throws SQLException {
                    if (method.equals("getColumnCount")) return names.length;
                    int column = column(args[0]);
                    return switch (method) {
                        case "getColumnName", "getColumnLabel" -> names[column];
                        case "getColumnTypeName" -> types[column];
                        case "getColumnType" -> sqlType(types[column]);
                        default -> throw unsupported("ResultSetMetaData." + method);
                    };
                }
            });
        }
        void batch() throws IOException {
            int count = session.input.readInt();
            if (count < 0 || count > 65536 || (long) count * names.length * 4 > MAX_MESSAGE)
                throw new IOException("Invalid DuckDB batch size");
            values = new byte[count][names.length][];
            int size = 4;
            for (int row = 0; row < count; row++) for (int column = 0; column < names.length; column++) {
                values[row][column] = readBytes(session.input, true);
                size += 4 + (values[row][column] == null ? 0 : values[row][column].length);
                if (size > MAX_MESSAGE) throw new IOException("DuckDB batch exceeds transport limit");
            }
            position = -1; ended = count == 0;
        }
        void close() throws SQLException {
            if (closed) return;
            if (session.active == this) {
                if (!session.closed && !ended) session.command(3);
                session.active = null;
                if (!ended && session.receipt != null && session.receipt.pending)
                    session.receipt.end(0, consumed);
            }
            closed = true; values = new byte[0][][];
        }
        int column(Object argument) throws SQLException {
            if (argument instanceof Integer index) {
                if (index >= 1 && index <= names.length) return index - 1;
            } else {
                for (int i = 0; i < names.length; i++) if (names[i].equalsIgnoreCase((String) argument)) return i;
            }
            throw new SQLException("Unknown result column: " + argument);
        }
        @Override Object call(String method, Object[] args) throws SQLException {
            if (method.equals("isClosed")) return closed || session.closed;
            if (method.equals("close")) { close(); return null; }
            session.requireOpen();
            if (closed) throw new SQLException("ResultSet is closed");
            switch (method) {
                case "next":
                    if (ended) return false;
                    if (++position < values.length) { consumed++; return true; }
                    try { session.exchange(() -> { session.output.writeByte(2); session.check(); batch(); return null; }); }
                    catch (SQLException failure) { ended = true; session.failed(failure); throw failure; }
                    if (ended) {
                        if (session.receipt != null) session.receipt.end(0, consumed);
                        return false;
                    }
                    position = 0; consumed++; return true;
                case "getMetaData": return metadata;
                case "getStatement": return query.statement;
                case "findColumn": return column(args[0]) + 1;
                case "wasNull": return wasNull;
                case "getType": return ResultSet.TYPE_FORWARD_ONLY;
                case "getConcurrency": return ResultSet.CONCUR_READ_ONLY;
                default: break;
            }
            if (args.length != 1 || !Set.of("getString", "getObject", "getBytes", "getInt", "getLong", "getDouble",
                    "getFloat", "getShort", "getByte", "getBoolean", "getBigDecimal", "getDate", "getTime", "getTimestamp").contains(method))
                throw unsupported("ResultSet." + method);
            if (position < 0 || position >= values.length) throw new SQLException("ResultSet is not positioned on a row");
            int column = column(args[0]);
            byte[] bytes = values[position][column]; wasNull = bytes == null;
            if (method.equals("getBytes")) return bytes == null ? null : bytes.clone();
            if (!wasNull && types[column].equals("BLOB")) {
                if (method.equals("getObject")) return bytes.clone();
                throw unsupported("BLOB conversion to " + method);
            }
            String value = wasNull ? null : new String(bytes, StandardCharsets.UTF_8);
            try {
                return switch (method) {
                    case "getString" -> value;
                    case "getObject" -> value == null ? null : object(value, types[column]);
                    case "getBoolean" -> value != null && bool(value);
                    case "getInt" -> value == null ? 0 : new BigDecimal(value).intValueExact();
                    case "getLong" -> value == null ? 0L : new BigDecimal(value).longValueExact();
                    case "getShort" -> value == null ? (short) 0 : new BigDecimal(value).shortValueExact();
                    case "getByte" -> value == null ? (byte) 0 : new BigDecimal(value).byteValueExact();
                    case "getDouble" -> value == null ? 0.0 : Double.parseDouble(value);
                    case "getFloat" -> value == null ? 0.0f : Float.parseFloat(value);
                    case "getBigDecimal" -> value == null ? null : new BigDecimal(value);
                    case "getDate" -> value == null ? null : java.sql.Date.valueOf(value);
                    case "getTime" -> value == null ? null : Time.valueOf(value);
                    case "getTimestamp" -> value == null ? null : Timestamp.valueOf(value);
                    default -> throw unsupported(method);
                };
            } catch (IllegalArgumentException | ArithmeticException e) {
                throw new SQLDataException("Cannot convert DuckDB " + types[column] + " with " + method, e);
            }
        }
    }

    private static boolean bool(String value) throws SQLException {
        if (value.equals("true") || value.equals("1")) return true;
        if (value.equals("false") || value.equals("0")) return false;
        throw new SQLDataException("Invalid boolean: " + value);
    }
    private static int exactInt(long value) throws SQLException {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) throw new SQLDataException("Count exceeds JDBC int range");
        return (int) value;
    }
    private static Object object(String value, String type) throws SQLException {
        if (type.startsWith("DECIMAL(")) return new BigDecimal(value);
        return switch (type) {
            case "BOOLEAN" -> bool(value);
            case "TINYINT" -> Byte.valueOf(value);
            case "SMALLINT" -> Short.valueOf(value);
            case "INTEGER", "UTINYINT", "USMALLINT" -> Integer.valueOf(value);
            case "BIGINT", "UINTEGER" -> Long.valueOf(value);
            case "HUGEINT", "UBIGINT", "UHUGEINT" -> new BigInteger(value);
            case "FLOAT" -> Float.valueOf(value);
            case "DOUBLE" -> Double.valueOf(value);
            case "VARCHAR" -> value;
            case "DATE" -> java.sql.Date.valueOf(value);
            case "TIME" -> LocalTime.parse(value);
            case "TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S" -> Timestamp.valueOf(value);
            default -> throw unsupported("getObject for " + type);
        };
    }
    private static int sqlType(String type) throws SQLException {
        if (type.startsWith("DECIMAL(")) return Types.DECIMAL;
        return switch (type) {
            case "BOOLEAN" -> Types.BOOLEAN;
            case "TINYINT" -> Types.TINYINT;
            case "SMALLINT" -> Types.SMALLINT;
            case "INTEGER", "UTINYINT", "USMALLINT" -> Types.INTEGER;
            case "BIGINT", "UINTEGER" -> Types.BIGINT;
            case "HUGEINT", "UBIGINT", "UHUGEINT" -> Types.NUMERIC;
            case "FLOAT" -> Types.FLOAT;
            case "DOUBLE" -> Types.DOUBLE;
            case "VARCHAR" -> Types.VARCHAR;
            case "BLOB" -> Types.BLOB;
            case "DATE" -> Types.DATE;
            case "TIME" -> Types.TIME;
            case "TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S" -> Types.TIMESTAMP;
            default -> throw unsupported("JDBC type for " + type);
        };
    }
    private static String readString(DataInputStream in) throws IOException {
        return new String(readBytes(in, false), StandardCharsets.UTF_8);
    }
    private static byte[] readBytes(DataInputStream in, boolean nullable) throws IOException {
        int size = in.readInt();
        if (nullable && size == -1) return null;
        if (size < 0 || size > MAX_FIELD) throw new IOException("Invalid DuckDB field size");
        byte[] bytes = new byte[size]; in.readFully(bytes); return bytes;
    }
    private static void writeString(DataOutputStream out, String value) throws IOException {
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }
    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_FIELD) throw new IOException("DuckDB field exceeds transport limit");
        out.writeInt(value.length); out.write(value);
    }
}
