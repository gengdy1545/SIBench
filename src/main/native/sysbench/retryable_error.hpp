#pragma once

#include <string>
#include "duckdb/common/exception.hpp"

inline bool SysbenchRetryableError(int code, const std::string &message) {
    bool transaction = code == int(duckdb::ExceptionType::TRANSACTION);
    bool constraint = code == int(duckdb::ExceptionType::CONSTRAINT);
    if (!transaction && !constraint) return false;
    auto detail = message;
    const std::string prefix = transaction ? "TransactionContext Error: " : "Constraint Error: ";
    if (detail.rfind(prefix, 0) == 0) detail.erase(0, prefix.size());
    // Commit wraps nonfatal engine errors, including I/O errors, in TransactionException.
    const std::string commit_prefix = "Failed to commit: ";
    bool commit = transaction && detail.rfind(commit_prefix, 0) == 0;
    if (commit) detail.erase(0, commit_prefix.size());
    bool duplicate = detail.rfind("Duplicate key \"", 0) == 0
            || detail.rfind("PRIMARY KEY or UNIQUE constraint violation: duplicate key \"", 0) == 0;
    return (transaction && detail.rfind("Conflict on ", 0) == 0) || ((constraint || commit) && duplicate);
}
