#include "retryable_error.hpp"
#include <iostream>

int main() {
    struct Case { int code; const char *message; bool retry; };
    const Case cases[] = {
        {10, "Failed to commit: Could not write file: No space left on device", false},
        {10, "Failed to commit: NOT NULL constraint failed: items.value", false},
        {10, "Failed to commit: PRIMARY KEY or UNIQUE constraint violation: duplicate key \"42\"", true},
        {10, "Conflict on update!", true},
        {18, "Duplicate key \"id: 42\" violates primary key constraint.", true},
        {18, "PRIMARY KEY or UNIQUE constraint violation: duplicate key \"42\"", true},
        {18, "NOT NULL constraint failed: items.value", false},
        {10, "cannot start a transaction within a transaction", false},
        {28, "Could not write file", false},
        {10, "TransactionContext Error: Conflict on update!", true},
        {18, "Constraint Error: Duplicate key \"id: 42\" violates primary key constraint.", true},
        {10, "TransactionContext Error: Failed to commit: Could not write file 'Conflict on update!'", false},
        {18, "Constraint Error: NOT NULL constraint failed: items.\"Duplicate key\"", false},
    };
    for (const auto &item : cases) {
        if (SysbenchRetryableError(item.code, item.message) != item.retry) {
            std::cerr << "Incorrect retry classification: " << item.message << '\n'; return 1;
        }
    }
    std::cout << "13 native error cases passed\n";
}
