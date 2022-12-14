#pragma once
#include <iostream>
#include <map>
#include <unordered_set>

#include "lockManager.hpp"
using namespace std;

using Index = int;
using Value = int;

enum class SiteStatus { UP = 1, DOWN };

class Site {
   private:
    int id;
    LockManager lockManager;

   public:
    int failedTime = 0;
    SiteStatus siteStatus;
    map<Index, Value> commitedVal;
    map<Index, Value> curVal;
    unordered_set<int> restrictedReadVariable;
    unordered_set<int> restrictedWriteVariable;
    Site() {}
    Site(const int id);
    void initialize();

    bool read(const int transactionId, const int idx, int& lockHolder,
              int& readVal);
    bool write(const int transactionId, const int idx, const int varVal,
               unordered_set<int>& lockHolders);
    // release lock from this transaction and
    // rollback if the value is modified.
    void abort(const int transactionId);
    void commit(const int transactionId,
                const unordered_set<int>& affectedVariables);
    bool fail(int time);
    bool recover();
    void dumpDebug();
    void dump() const;

    friend ostream& operator<<(ostream& os, const SiteStatus& siteSatus);
};

ostream& operator<<(ostream& os, const SiteStatus& siteSatus);
