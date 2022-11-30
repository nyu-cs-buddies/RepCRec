#pragma once

#include <iostream>
#include <map>
#include <unordered_map>
#include <unordered_set>

using Index = int;
using Value = int;

enum class TransactionStatus { RUNNING = 1, WAITING, ABORTED, COMMITED };

class Transaction {
   public:
    Transaction();
    Transaction(const int id, const int startTime, const bool isReadOnly);
    int id;
    int startTime;
    bool isReadOnly;
    TransactionStatus transactionStatus;
    std::unordered_set<int> affectedVariables;

    std::unordered_map<int, int> readHistory;
    std::unordered_map<int, int> writeHistory;

    // for read-only transaction
    std::map<Index, Value> commitedValCopy;

    friend std::ostream &operator<<(std::ostream &os,
                                    const TransactionStatus &transactionStatus);
    friend std::ostream &operator<<(std::ostream &os, const Transaction tran);
};

std::ostream &operator<<(std::ostream &os,
                         const TransactionStatus &transactionStatus);
std::ostream &operator<<(std::ostream &os, const Transaction tran);
