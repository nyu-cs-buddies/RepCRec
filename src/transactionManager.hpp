#pragma once

#include <list>
#include <unordered_map>
#include <vector>

#include "operation.hpp"
#include "site.h"
#include "transaction.hpp"

class TransactionManager {
   private:
    // (variableIdx, time)
    // for recovery
    std::unordered_map<int, int> uncommitedVariable;
    int time;

    std::list<Operation> operations;
    std::unordered_map<int, Transaction> idToTransaction;
    std::list<Operation> blockedOperations;
    // a list of Operation that are blocked due to sites fail
    std::list<Operation> siteFailedOperations;
    std::unordered_map<int, std::list<int>> waitForGraph;
    std::vector<Site> sites;

    // for read-only transactions
    void copyCommitedValue(Transaction &transaction);

    // for debugging
    void dumpDebug();

   public:
    TransactionManager();
    TransactionManager(const std::list<Operation> operations);

    void simulate();
    void detectDeadLock();
    void begin(const Operation &curOperation, bool isReadOnly);
    void read(const Operation &curOperation);
    void write(const Operation &curOperation);
    void commit(const Operation &curOperation);
    void fail(const Operation &curOperation);

    // for recovery
    void recover(const Operation &curOperation);
    void abort(const int transactionToAbort);
    void dump();
};
