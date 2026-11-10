#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/query_result_stream.hpp"

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <thread>

namespace {
using namespace duckdb;
constexpr uint32_t MAX_FIELD = 16 * 1024 * 1024;
constexpr size_t MAX_MESSAGE = 64 * 1024 * 1024;
volatile sig_atomic_t stopping = 0;
void Stop(int) { stopping = 1; }

struct TransportError : std::runtime_error {
	using std::runtime_error::runtime_error;
};

class Wire {
public:
	explicit Wire(int socket) : socket(socket) {}

	bool Command(uint8_t &value) {
		for (;;) {
			auto n = recv(socket, &value, 1, 0);
			if (n >= 0) { received = 1; return n == 1; }
			if (errno != EINTR) throw TransportError("socket read failed");
		}
	}
	uint8_t Byte() { uint8_t value; Read(&value, 1); return value; }
	uint32_t Int() {
		uint8_t bytes[4]; Read(bytes, 4);
		uint32_t value = 0;
		for (auto byte : bytes) value = (value << 8) | byte;
		return value;
	}
	std::string String() {
		auto size = Int();
		if (size > MAX_FIELD) throw TransportError("field exceeds transport limit");
		std::string value(size, '\0'); Read(value.data(), size); return value;
	}
	void Begin() { output.clear(); PutByte(0); }
	void PutByte(uint8_t value) { Reserve(1); output.push_back(char(value)); }
	void PutInt(uint32_t value) {
		for (int shift = 24; shift >= 0; shift -= 8) PutByte(uint8_t(value >> shift));
	}
	void PutLong(uint64_t value) {
		for (int shift = 56; shift >= 0; shift -= 8) PutByte(uint8_t(value >> shift));
	}
	void PutString(const std::string &value) {
		if (value.size() > MAX_FIELD) throw InvalidInputException("result field exceeds transport limit");
		PutInt(uint32_t(value.size())); Reserve(value.size()); output.append(value);
	}
	void Flush() {
		size_t sent = 0;
		while (sent < output.size()) {
			auto n = send(socket, output.data() + sent, output.size() - sent, MSG_NOSIGNAL);
			if (n < 0 && errno == EINTR) continue;
			if (n <= 0) throw TransportError("socket write failed");
			sent += size_t(n);
		}
	}
	void Error(const std::exception &error) {
		output.clear(); PutByte(1);
		ErrorData data(error);
		PutInt(uint32_t(data.Type())); PutString(data.Message()); Flush();
	}

private:
	void Reserve(size_t size) {
		if (size > MAX_MESSAGE - output.size()) throw InvalidInputException("result batch exceeds transport limit");
	}
	void Read(void *buffer, size_t size) {
		if (size > MAX_MESSAGE - received) throw TransportError("request exceeds transport limit");
		received += size;
		size_t offset = 0;
		while (offset < size) {
			auto n = recv(socket, static_cast<char *>(buffer) + offset, size - offset, 0);
			if (n < 0 && errno == EINTR) continue;
			if (n <= 0) throw TransportError("truncated request");
			offset += size_t(n);
		}
	}
	int socket;
	size_t received = 0;
	std::string output;
};

Value Parameter(Wire &wire) {
	auto tag = wire.Byte();
	if (tag == 0) return Value();
	auto value = wire.String();
	switch (tag) {
	case 1: return Value(value);
	case 2: return Value(value).DefaultCastAs(LogicalType::BIGINT, true);
	case 3: return Value(value).DefaultCastAs(LogicalType::DOUBLE, true);
	case 4: {
		auto width = wire.Byte(), scale = wire.Byte();
		if (!width || width > 38 || scale > width) throw TransportError("invalid decimal type");
		return Value(value).DefaultCastAs(LogicalType::DECIMAL(width, scale), true);
	}
	case 5: return Value(value).DefaultCastAs(LogicalType::BOOLEAN, true);
	case 6: return Value(value).DefaultCastAs(LogicalType::DATE, true);
	case 7: return Value(value).DefaultCastAs(LogicalType::TIME, true);
	case 8: return Value(value).DefaultCastAs(LogicalType::TIMESTAMP, true);
	case 9: return Value::BLOB_RAW(value);
	default: throw TransportError("unknown parameter type");
	}
}

struct Session {
	Session(DuckDB &database, int socket) : socket(socket), connection(new Connection(database)) {}
	~Session() {
		Interrupt();
		if (thread.joinable()) thread.join();
		if (socket >= 0) close(socket);
	}

	void Interrupt() {
		std::lock_guard<std::mutex> lock(mutex);
		if (connection) connection->Interrupt();
		if (socket >= 0) shutdown(socket, SHUT_RDWR);
	}
	void Close() {
		result.reset();
		std::lock_guard<std::mutex> lock(mutex);
		connection.reset();
	}
	unique_ptr<DataChunk> Fetch() {
		auto chunk = result->Fetch();
		if (result->HasError()) result->GetErrorObject().Throw();
		return chunk;
	}
	void Batch(Wire &wire, unique_ptr<DataChunk> chunk) {
		wire.PutInt(chunk ? uint32_t(chunk->size()) : 0);
		if (!chunk) { result.reset(); return; }
		for (idx_t row = 0; row < chunk->size(); row++) {
			for (idx_t column = 0; column < chunk->ColumnCount(); column++) {
				auto value = chunk->GetValue(column, row);
				if (value.IsNull()) wire.PutInt(UINT32_MAX);
				else wire.PutString(value.type().id() == LogicalTypeId::BLOB ? StringValue::Get(value) : value.ToString());
			}
		}
	}
	void Execute(Wire &wire) {
		// Read the complete request before allowing SQL errors to keep the session usable.
		auto sql = wire.String();
		auto count = wire.Int();
		if (count > 65536) throw TransportError("too many parameters");
		vector<Value> parameters;
		try {
			for (uint32_t i = 0; i < count; i++) parameters.push_back(Parameter(wire));
		} catch (const std::exception &error) { throw TransportError(error.what()); }
		result.reset();
		if (!auto_commit && !connection->HasActiveTransaction()) connection->BeginTransaction();
		result = make_uniq<QueryResultStream<>>(connection->Submit(sql, parameters));
		auto chunk = Fetch();
		auto returns = result->GetStatementProperties().return_type;
		wire.PutByte(returns == StatementReturnType::QUERY_RESULT ? 1 : 0);
		if (returns == StatementReturnType::QUERY_RESULT) {
			wire.PutInt(uint32_t(result->ColumnCount()));
			for (idx_t i = 0; i < result->ColumnCount(); i++) {
				wire.PutString(result->ColumnName(i).GetIdentifierName());
				wire.PutString(result->GetTypes()[i].ToString());
			}
			Batch(wire, std::move(chunk));
		} else {
			uint64_t changed = 0;
			while (chunk) {
				if (returns == StatementReturnType::CHANGED_ROWS && chunk->size())
					changed += chunk->GetValue(0, 0).GetValue<uint64_t>();
				chunk = Fetch();
			}
			result.reset();
			wire.PutLong(changed);
		}
	}
	void Run() {
		Wire wire(socket);
		try {
			uint8_t command;
			while (wire.Command(command)) {
				try {
					wire.Begin();
					switch (command) {
					case 1: Execute(wire); break;
					case 2:
						if (!result) throw InvalidInputException("no open result");
						Batch(wire, Fetch()); break;
					case 3: result.reset(); break;
					case 4: Close(); wire.Flush(); goto closed;
					case 5: {
						auto value = wire.Byte();
						if (value > 1) throw TransportError("invalid auto-commit value");
						result.reset();
						if (value && !auto_commit && connection->HasActiveTransaction()) connection->Commit();
						auto_commit = value != 0; break;
					}
					case 6: case 7:
						result.reset();
						if (auto_commit) throw InvalidInputException("JDBC transaction control requires auto-commit off");
						if (connection->HasActiveTransaction()) {
							if (command == 6) connection->Commit(); else connection->Rollback();
						}
						break;
					case 8: wire.PutByte(connection->HasActiveTransaction() ? 1 : 0); break;
					default: throw TransportError("unknown command");
					}
					wire.Flush();
				} catch (const TransportError &) { throw; }
				catch (const std::exception &error) { result.reset(); wire.Error(error); }
			}
		} catch (const std::exception &error) {
			std::cerr << "DuckDB session ended: " << error.what() << '\n';
		}
	closed:
		Close();
		{
			std::lock_guard<std::mutex> lock(mutex);
			close(socket); socket = -1;
		}
		done = true;
	}
	int socket;
	std::mutex mutex;
	std::unique_ptr<Connection> connection;
	unique_ptr<QueryResultStream<>> result;
	bool auto_commit = true;
	std::atomic<bool> done {false};
	std::thread thread;
};
} // namespace

int main(int argc, char **argv) {
	if (argc != 5) {
		std::cerr << "Usage: duckdb_host DATA_DIRECTORY SOCKET_PATH MEMORY_LIMIT THREADS\n";
		return 2;
	}
	int listener = -1;
	bool bound = false;
	try {
		sockaddr_un address {};
		address.sun_family = AF_UNIX;
		if (strlen(argv[2]) >= sizeof(address.sun_path)) throw std::runtime_error("socket path is too long");
		strcpy(address.sun_path, argv[2]);
		DBConfig config;
		config.SetOptionByName("storage_compatibility_version", Value("latest"));
		config.SetOptionByName("memory_limit", Value(argv[3]));
		config.SetOptionByName("threads", Value(argv[4]).DefaultCastAs(LogicalType::BIGINT, true));
		if (!std::filesystem::create_directory(argv[1])) throw std::runtime_error("data directory already exists");
		DuckDB database((std::filesystem::path(argv[1]) / "data.duckdb").string(), &config);
		listener = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
		if (listener < 0) throw std::runtime_error("cannot create local socket");
		auto mask = umask(0077);
		auto status = bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address));
		umask(mask);
		if (status < 0) throw std::runtime_error("cannot bind local socket (existing paths are never replaced)");
		bound = true;
		if (listen(listener, 64) < 0) throw std::runtime_error("cannot listen on local socket");
		signal(SIGINT, Stop); signal(SIGTERM, Stop);
		std::vector<std::unique_ptr<Session>> sessions;
		std::cout << "READY\n" << std::flush;
		while (!stopping) {
			sessions.erase(std::remove_if(sessions.begin(), sessions.end(),
			                             [](const std::unique_ptr<Session> &s) { return s->done.load(); }), sessions.end());
			pollfd wait {listener, POLLIN, 0};
			auto ready = poll(&wait, 1, 200);
			if (ready < 0 && errno != EINTR) throw std::runtime_error("local socket poll failed");
			if (ready <= 0 || !(wait.revents & POLLIN)) continue;
			auto client = accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
			if (client < 0) {
				if (errno != EINTR) throw std::runtime_error("local socket accept failed");
				continue;
			}
			std::unique_ptr<Session> pending;
			try { pending = std::make_unique<Session>(database, client); }
			catch (...) { close(client); throw; }
			sessions.push_back(std::move(pending));
			auto session = sessions.back().get();
			session->thread = std::thread([session] { session->Run(); });
		}
		for (auto &session : sessions) session->Interrupt();
		sessions.clear();
		close(listener); listener = -1;
		unlink(argv[2]); bound = false;
		return 0;
	} catch (const std::exception &error) {
		if (listener >= 0) close(listener);
		if (bound) unlink(argv[2]);
		std::cerr << error.what() << '\n';
		return 1;
	}
}
