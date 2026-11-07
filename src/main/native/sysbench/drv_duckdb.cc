#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <unistd.h>
#include "retryable_error.hpp"

// The pinned C headers use the C11 spelling for thread-local storage.
#define _Thread_local thread_local
extern "C" {
#include "db_driver.h"
}

namespace {
constexpr size_t MAX_FIELD = 16 * 1024 * 1024;
constexpr size_t MAX_MESSAGE = 64 * 1024 * 1024;
using Cell = std::optional<std::string>;
struct Result { std::vector<std::vector<Cell>> rows; };
struct SqlError : std::runtime_error {
    int code;
    SqlError(int code, const std::string &message) : std::runtime_error(message), code(code) {}
};

class Session {
public:
    explicit Session(const char *path) {
        sockaddr_un address {};
        address.sun_family = AF_UNIX;
        if (!path || !*path || strlen(path) >= sizeof(address.sun_path))
            throw std::runtime_error("invalid DuckDB host socket path");
        strcpy(address.sun_path, path);
        socket = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (socket < 0 || ::connect(socket, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
            if (socket >= 0) ::close(socket);
            socket = -1;
            throw std::runtime_error("cannot connect to DuckDB host");
        }
    }
    ~Session() { if (socket >= 0) ::close(socket); if (ledger) fclose(ledger); }

    void Byte(uint8_t value) { if (request.size() == MAX_MESSAGE) throw std::runtime_error("request exceeds transport limit"); request.push_back(value); }
    void Int(uint32_t value) { for (int shift = 24; shift >= 0; shift -= 8) Byte(uint8_t(value >> shift)); }
    void String(const char *value, size_t size) {
        if (size > MAX_FIELD || size + 4 > MAX_MESSAGE - request.size())
            throw std::runtime_error("request field exceeds transport limit");
        Int(uint32_t(size)); request.insert(request.end(), value, value + size);
    }
    void String(const std::string &value) { String(value.data(), value.size()); }
    void Send() {
        size_t sent = 0;
        while (sent < request.size()) {
            auto count = ::send(socket, request.data() + sent, request.size() - sent, MSG_NOSIGNAL);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) throw std::runtime_error("DuckDB host write failed");
            sent += size_t(count);
        }
        request.clear(); received = 0;
        auto status = ReadByte();
        if (status == 1) { int code = int(ReadInt()); throw SqlError(code, ReadString()); }
        if (status != 0) throw std::runtime_error("invalid DuckDB host status");
    }
    uint8_t ReadByte() { uint8_t value; Read(&value, 1); return value; }
    uint32_t ReadInt() { uint32_t value = 0; for (int i = 0; i < 4; i++) value = (value << 8) | ReadByte(); return value; }
    uint64_t ReadLong() { uint64_t value = 0; for (int i = 0; i < 8; i++) value = (value << 8) | ReadByte(); return value; }
    std::string ReadString() { return ReadString(ReadInt()); }
    std::string ReadString(uint32_t size) {
        if (size > MAX_FIELD) throw std::runtime_error("response field exceeds transport limit");
        std::string value(size, '\0'); Read(value.data(), size); return value;
    }
    void Record(const char *event, const char *kind, int error, int64_t rows) {
        if (ledger && fprintf(ledger, "%s,%llu,%s,,,%d,%lld\n", event,
                static_cast<unsigned long long>(operations), kind, error,
                static_cast<long long>(rows)) < 0)
            throw std::runtime_error("cannot write DuckDB client receipt");
    }
    void Execute(const char *sql, size_t size, db_bind_t *parameters, size_t count, db_result_t *result,
                 const char *kind = "query") {
        request.clear(); Byte(1); String(sql, size);
        if (count > 65536) throw std::runtime_error("too many DuckDB parameters");
        Int(uint32_t(count));
        for (size_t i = 0; i < count; i++) Parameter(parameters[i]);
        if (ledger) { operations++; Record("begin", kind, 0, 0); }
        try {
            Send();
            auto returns = ReadByte();
            if (returns == 0) {
                auto rows = ReadLong();
                if (rows > UINT32_MAX) throw std::runtime_error("affected rows exceed sysbench capacity");
                result->nrows = uint32_t(rows); result->nfields = 0;
                result->counter = rows ? SB_CNT_WRITE : SB_CNT_OTHER;
            } else if (returns == 1) {
                auto columns = ReadInt();
                if (columns == 0 || columns > 65536) throw std::runtime_error("invalid result column count");
                for (uint32_t i = 0; i < columns; i++) { ReadString(); ReadString(); }
                auto data = std::make_unique<Result>();
                size_t memory = 0;
                for (;;) {
                    auto rows = ReadInt();
                    if (!rows) break;
                    if (data->rows.size() + rows > UINT32_MAX) throw std::runtime_error("result exceeds sysbench row capacity");
                    for (uint32_t i = 0; i < rows; i++) {
                        if (columns > (MAX_MESSAGE - memory) / sizeof(Cell)) throw std::runtime_error("sysbench result exceeds 64 MiB");
                        memory += columns * sizeof(Cell);
                        std::vector<Cell> row(columns);
                        for (auto &cell : row) {
                            auto size = ReadInt();
                            if (size == UINT32_MAX) continue;
                            if (size > MAX_MESSAGE - memory) throw std::runtime_error("sysbench result exceeds 64 MiB");
                            memory += size;
                            cell = ReadString(size);
                        }
                        data->rows.push_back(std::move(row));
                    }
                    Byte(2); Send();
                }
                result->nrows = uint32_t(data->rows.size()); result->nfields = columns;
                result->counter = SB_CNT_READ; result->ptr = data.release(); result->row.ptr = nullptr;
            } else throw std::runtime_error("invalid DuckDB result kind");
            Record("end", kind, 0, result->nrows);
        } catch (const SqlError &error) { Record("end", kind, error.code, -1); throw; }
    }
    void OpenLedger(const char *directory) {
        if (!directory || !*directory) throw std::runtime_error("duckdb-ledger-dir is required");
        db_result_t result {};
        const char *sql = "SELECT current_connection_id(), current_query_id()";
        Execute(sql, strlen(sql), nullptr, 0, &result);
        std::unique_ptr<Result> data(static_cast<Result *>(result.ptr));
        if (data->rows.size() != 1 || data->rows[0].size() != 2 || !data->rows[0][0] || !data->rows[0][1])
            throw std::runtime_error("missing DuckDB session identity");
        auto connection = std::stoull(*data->rows[0][0]), query = std::stoull(*data->rows[0][1]);
        auto path = std::string(directory) + "/connection-" + std::to_string(connection) + ".csv";
        int fd = open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
        if (fd < 0) throw std::runtime_error("cannot create DuckDB client receipt: " + path);
        ledger = fdopen(fd, "w");
        if (!ledger) { close(fd); throw std::runtime_error("cannot open DuckDB client receipt"); }
        if (fprintf(ledger, "event,operation,kind,connection_id,after_query,error_code,rows\nopen,0,session,%llu,%llu,0,0\n", connection, query) < 0)
            throw std::runtime_error("cannot record DuckDB session identity");
    }
    void RollbackIfActive() {
        Byte(8); Send();
        auto active = ReadByte();
        if (active > 1) throw std::runtime_error("invalid DuckDB transaction status");
        if (active) { db_result_t result {}; Execute("ROLLBACK", 8, nullptr, 0, &result, "rollback"); }
    }
    void Close() {
        Byte(4); Send();
        Record("close", "session", 0, 0);
        if (ledger) { auto file = ledger; ledger = nullptr; if (fclose(file) != 0) throw std::runtime_error("cannot close DuckDB receipt"); }
    }
    std::string error;

private:
    void Read(void *buffer, size_t size) {
        if (size > MAX_MESSAGE - received) throw std::runtime_error("response batch exceeds transport limit");
        received += size;
        size_t offset = 0;
        while (offset < size) {
            auto count = recv(socket, static_cast<char *>(buffer) + offset, size - offset, 0);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) throw std::runtime_error("DuckDB host response truncated");
            offset += size_t(count);
        }
    }
    void Parameter(const db_bind_t &parameter) {
        if (parameter.is_null && *parameter.is_null) { Byte(0); return; }
        if (!parameter.buffer) throw std::runtime_error("missing sysbench parameter buffer");
        switch (parameter.type) {
        case DB_TYPE_TINYINT: Byte(2); String(std::to_string(*static_cast<int8_t *>(parameter.buffer))); break;
        case DB_TYPE_SMALLINT: Byte(2); String(std::to_string(*static_cast<int16_t *>(parameter.buffer))); break;
        case DB_TYPE_INT: Byte(2); String(std::to_string(*static_cast<int32_t *>(parameter.buffer))); break;
        case DB_TYPE_BIGINT: Byte(2); String(std::to_string(*static_cast<int64_t *>(parameter.buffer))); break;
        case DB_TYPE_CHAR: case DB_TYPE_VARCHAR:
            if (!parameter.data_len || *parameter.data_len > parameter.max_len)
                throw std::runtime_error("invalid sysbench string parameter length");
            Byte(1); String(static_cast<char *>(parameter.buffer), *parameter.data_len); break;
        default: throw std::runtime_error("unsupported sysbench parameter type");
        }
    }
    int socket = -1;
    size_t received = 0;
    std::vector<uint8_t> request;
    FILE *ledger = nullptr;
    uint64_t operations = 0;
};

db_error_t Failure(db_conn_t *connection, const std::exception &error) {
    auto session = static_cast<Session *>(connection->ptr);
    if (session) { session->error = error.what(); connection->sql_errmsg = session->error.c_str(); }
    connection->sql_state = "HY000"; connection->sql_errno = -1;
    log_text(LOG_FATAL, "DuckDB driver: %s", error.what());
    return DB_ERROR_FATAL;
}
int Connect(db_conn_t *connection) {
    try {
        auto session = std::make_unique<Session>(sb_get_value_string("duckdb-socket"));
        session->OpenLedger(sb_get_value_string("duckdb-ledger-dir"));
        connection->ptr = session.release(); return 0;
    } catch (const std::exception &error) { Failure(connection, error); return 1; }
}
int Disconnect(db_conn_t *connection) {
    auto session = static_cast<Session *>(connection->ptr);
    int status = 0;
    try { if (session) session->Close(); } catch (const std::exception &error) { Failure(connection, error); status = 1; }
    delete session; connection->ptr = nullptr; connection->sql_errmsg = nullptr; return status;
}
db_error_t Execute(db_conn_t *connection, const char *sql, size_t size, db_bind_t *parameters, size_t count, db_result_t *result) {
    auto session = static_cast<Session *>(connection->ptr);
    connection->sql_errno = 0; connection->sql_state = nullptr; connection->sql_errmsg = nullptr;
    result->ptr = nullptr;
    try { session->Execute(sql, size, parameters, count, result); return DB_ERROR_NONE; }
    catch (const SqlError &error) {
        session->error = error.what(); connection->sql_errno = error.code;
        connection->sql_state = "HY000"; connection->sql_errmsg = session->error.c_str();
        result->counter = SB_CNT_ERROR; result->nrows = 0;
        if (SysbenchRetryableError(error.code, session->error)) {
            try { session->RollbackIfActive(); return DB_ERROR_IGNORABLE; }
            catch (const std::exception &rollback) {
                return Failure(connection, std::runtime_error(std::string(error.what()) + "; recovery failed: " + rollback.what()));
            }
        }
        log_text(LOG_FATAL, "DuckDB query failed: %s", error.what()); return DB_ERROR_FATAL;
    } catch (const std::exception &error) {
        delete static_cast<Result *>(result->ptr); result->ptr = nullptr;
        result->counter = SB_CNT_ERROR; result->nrows = 0;
        return Failure(connection, error);
    }
}
int Prepare(db_stmt_t *statement, const char *query, size_t size) {
    statement->query = strndup(query, size); return statement->query ? 0 : 1;
}
int Bind(db_stmt_t *statement, db_bind_t *parameters, size_t count) {
    if (count > 65536) return 1;
    auto copy = static_cast<db_bind_t *>(malloc(count * sizeof(db_bind_t)));
    if (count && !copy) return 1;
    if (count) memcpy(copy, parameters, count * sizeof(db_bind_t));
    free(statement->bound_param); statement->bound_param = copy; statement->bound_param_len = count; return 0;
}
db_error_t ExecutePrepared(db_stmt_t *statement, db_result_t *result) {
    return Execute(statement->connection, statement->query, strlen(statement->query), statement->bound_param, statement->bound_param_len, result);
}
db_error_t Query(db_conn_t *connection, const char *query, size_t size, db_result_t *result) {
    return Execute(connection, query, size, nullptr, 0, result);
}
int FetchRow(db_result_t *result, db_row_t *row) {
    auto data = static_cast<Result *>(result->ptr);
    auto position = reinterpret_cast<uintptr_t>(row->ptr);
    if (!data || position >= data->rows.size()) return DB_ERROR_IGNORABLE;
    for (size_t i = 0; i < data->rows[position].size(); i++) {
        auto &cell = data->rows[position][i];
        row->values[i].ptr = cell ? cell->data() : nullptr;
        row->values[i].len = cell ? uint32_t(cell->size()) : 0;
    }
    row->ptr = reinterpret_cast<void *>(position + 1); return 0;
}
int FreeResults(db_result_t *result) { delete static_cast<Result *>(result->ptr); result->ptr = nullptr; result->row.ptr = nullptr; return 0; }
int Describe(drv_caps_t *caps) { *caps = {1, 1, 0, 0, 0, 0}; return 0; }
int Initialize() { return 0; }
int CloseStatement(db_stmt_t *) { return 0; }
int UnsupportedBind(db_stmt_t *, db_bind_t *, size_t) { return 1; }
int UnsupportedFetch(db_result_t *) { return 1; }
int UnsupportedReconnect(db_conn_t *) { log_text(LOG_FATAL, "DuckDB capture does not support reconnect"); return 1; }
db_driver_t driver {};
sb_arg_t arguments[3] {};
}

extern "C" int register_driver_duckdb(sb_list_t *drivers) {
    arguments[0].name = "duckdb-socket"; arguments[0].desc = "Local instrumented DuckDB host socket";
    arguments[0].value = ""; arguments[0].type = SB_ARG_TYPE_STRING;
    arguments[1].name = "duckdb-ledger-dir"; arguments[1].desc = "Fresh client receipt directory";
    arguments[1].value = ""; arguments[1].type = SB_ARG_TYPE_STRING;
    driver.sname = "duckdb"; driver.lname = "DuckDB capture host driver"; driver.args = arguments;
    driver.ops.init = Initialize; driver.ops.done = Initialize;
    driver.ops.describe = Describe; driver.ops.connect = Connect; driver.ops.disconnect = Disconnect;
    driver.ops.reconnect = UnsupportedReconnect; driver.ops.prepare = Prepare; driver.ops.bind_param = Bind;
    driver.ops.bind_result = UnsupportedBind; driver.ops.execute = ExecutePrepared; driver.ops.fetch = UnsupportedFetch;
    driver.ops.fetch_row = FetchRow; driver.ops.free_results = FreeResults; driver.ops.close = CloseStatement;
    driver.ops.query = Query;
    SB_LIST_ADD_TAIL(&driver.listitem, drivers); return 0;
}
