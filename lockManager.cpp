#include <iostream>
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

        void requestRLock(int transactionId, int varIdx, int& lockHolder){
            // check whether a writelock on it
            if (!WLockTable.count(varIdx)) {
                // provide a RLock
                if (RLockTable.count(varIdx)) {
                    RLockTable[varIdx].transactionIds.insert(transactionId);
                } 
                else {
                    ReadLock readLock;
                    readLock.transactionIds.insert(transactionId);
                    RLockTable[varIdx] = readLock;
                }
                return;
            }
            // else block this transaction
            lockHolder = WLockTable[varIdx].transactionId;
            return;
        }

        void requestWLock(const int transactionId, const int varIdx, unordered_set<int>& lockHolders){
            if (!RLockTable.count(varIdx) && !WLockTable.count(varIdx)) {
                // provide a WLock
                WriteLock writeLock;
                writeLock.transactionId = transactionId;
                WLockTable[varIdx] = writeLock;
                return;
            }
            // else block this transaction
            if (RLockTable.count(varIdx)) {
                auto ids = RLockTable[varIdx].transactionIds;
                lockHolders.insert(ids.begin(), ids.end());
            } else {
                lockHolders.insert(WLockTable[varIdx].transactionId);
            }
            return;
        }


    void promoteLock(const int transactionId, const int idx) {
        RLockTable.erase(idx);
        WriteLock writeLock;
        writeLock.transactionId = transactionId;
        WLockTable[idx] = writeLock;
    }

    list<int> releaseLock(const int transactionId) {
        // check ReadLock
        list<int> tmp;  // use to update ReadLock table
        for (auto& r : RLockTable) {
            r.second.transactionIds.erase(transactionId);
            if (r.second.transactionIds.empty()) {
                tmp.push_back(r.first);
            }
        }

        // clean up RLockTable
        for (const auto& i : tmp) {
            RLockTable.erase(i);
        }

        // check WriteLock
        list<int> modifiedVar;
        for (auto& w : WLockTable) {
            if (w.second.transactionId == transactionId) {
                modifiedVar.push_back(w.first);
            }
        }

        // clean up WLockTable
        for (const auto& i : modifiedVar) {
            WLockTable.erase(i);
        }
        return modifiedVar;
    }

    void releaseAllLock() {
        RLockTable.clear();
        WLockTable.clear();
    }

    void dump() const {
        cout << "RLock Holders: ";
        for (const auto& r : RLockTable) {
            cout << r.first << " : ";
            for (const auto& t : r.second.transactionIds) {
            cout << t << " ";
            }
            cout << " || ";
        }
        cout << endl;

        cout << "WLockHolders: ";
        for (const auto& w : WLockTable) {
            cout << w.first << " : " << w.second.transactionId << " || ";
        }
        cout << endl;
    }
};

