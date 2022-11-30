#pragma once
#include <unordered_set>
#include <unordered_map>
#include <list>
using namespace std;

class ReadLock {
    public:
        bool isShared;
        unordered_set<int> transactionIds;
        ReadLock(){
            isShared = true;
        }
};

class WriteLock {
    public:
        bool isShared;
        int transactionId;
        WriteLock(){
            isShared = false;
        }
};

class LockManager {
    public:
        // transaction's locks for variable
        unordered_map<int, ReadLock> RLockTable;
        unordered_map<int, WriteLock> WLockTable;
        
        void requestRLock(int transactionId, int varIdx, int& lockHolder);
        void requestWLock(const int transactionId, const int varIdx, unordered_set<int>& lockHolders);
        void promoteLock(const int transactionId, const int idx);
        list<int> releaseLock(const int transactionId);
        void releaseAllLock();
        void dump() const;
};

