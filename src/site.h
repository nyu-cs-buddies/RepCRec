#pragma once
#include <iostream>
#include <map>
#include <unordered_set>
#include "lockManager.h"
using namespace std;

using Index = int;
using Value = int;

enum struct SiteStatus { UP = 1, DOWN };

inline ostream& operator<<(ostream& os, const SiteStatus& siteSatus) {
  switch (siteSatus) {
    case SiteStatus::UP:
      os << "UP";
      break;
    case SiteStatus::DOWN:
      os << "DOWN";
      break;
  }
  return os;
}

class Site {
    public:
        int id;
        int failedTime = 0;
        SiteStatus siteStatus;
        LockManager lockManager;
        map<Index, Value> commitedVal;
        map<Index, Value> curVal;
        unordered_set<int> restrictedReadVariable;
        unordered_set<int> restrictedWriteVariable;

        Site() {}
        Site(const int id);
        void initialize();

        bool read(const int transactionId, const int idx, int& lockHolder, int& readVal);
        bool write(const int transactionId, const int idx, const int varVal, unordered_set<int>& lockHolders);
        void abort(const int transactionId); // release lock from this transaction and rollback if the value is modified.
        void commit(const int transactionId, const unordered_set<int>& affectedVariables);
        bool fail(int time);
        bool recover();
        void dump() const;
        
};