#include <iostream>
#include <string>
#include "site.hpp"
using namespace std;

Site::Site(const int id) : id(id), siteStatus(SiteStatus::UP) { initialize(); }

void Site::initialize() {
    // save even indexed variables (replicated variables)
    for (int i = 2; i <= 20; i += 2) {
        commitedVal[i] = 10 * i;
    }

    // save odd indexed variables (non-replicated variables)
    for (int i = 1; i <= 19; i += 2) {
        if (id == (i % 10) + 1) {
            commitedVal[i] = 10 * i;
        }
    }
}

bool Site::read(const int transactionId, const int idx, int& lockHolder, int& readVal) {
    if (siteStatus == SiteStatus::DOWN || !commitedVal.count(idx)) {
        // site is down or variable does not exit on this site
        return false;
    }

    if (idx % 2 == 0 && restrictedReadVariable.count(idx)) {
        // if the site just recovered, we can not read the replicated variables
        // until they are commited
        return false;
    }

    // request a ReadLock for read variable
    lockManager.requestRLock(transactionId, idx, lockHolder);
    readVal = lockHolder == transactionId ? curVal[idx] : commitedVal[idx];
    return true;
}

bool Site::write(const int transactionId, const int idx, const int varVal, unordered_set<int>& lockHolders) {
    if (siteStatus == SiteStatus::DOWN || !commitedVal.count(idx)) {
        // site is down or variable does not exit on this site
        return false;
    }

    if (restrictedWriteVariable.count(idx)) {
        return false;  
    }

    // request a WLock for write variable
    lockManager.requestWLock(transactionId, idx, lockHolders);

    // if already has a read lock, RLock promotes to WLock
    if (lockHolders.count(transactionId) && lockHolders.size() == 1) {
        // promote RLock to WLock
        lockManager.promoteLock(transactionId, idx);
        lockHolders.clear();
    }
    if (lockHolders.empty()) {
        // allow to write to curVal
        curVal[idx] = varVal;
        return true;
    }

    return false;
}


void Site::abort(const int transactionId) {
    auto modifiedVar = lockManager.releaseLock(transactionId);
    for (const auto& var : modifiedVar) {
        curVal.erase(var);
        restrictedWriteVariable.erase(var);
    }
    return;
}

void Site::commit(const int transactionId, const unordered_set<int>& affectedVariables) {
    lockManager.releaseLock(transactionId);
    for (const auto& affectedVar : affectedVariables) {
        if (curVal.count(affectedVar)) {
            commitedVal[affectedVar] = curVal[affectedVar];
            curVal.erase(affectedVar);
            // if the site just recovered, we need to clean up restrictedReadVariable
            // to make it readable
            restrictedReadVariable.erase(affectedVar);
        }
        restrictedWriteVariable.erase(affectedVar);
    }
}

bool Site::fail(int time) {
    if (siteStatus != SiteStatus::UP) {
        cout << "Site" << id << " is already DOWN!" << endl;
        return false;
    }
    lockManager.releaseAllLock();
    curVal.clear();
    siteStatus = SiteStatus::DOWN;
    failedTime = time;
    return true;
}

bool Site::recover() {
    if (siteStatus != SiteStatus::DOWN) {
        cout << "Site" << id << " is already UP!" << endl;
        return false;
    }
    // initialize variables
    initialize();

    // mark rpelicated variables as restricted
    for (int i = 2; i <= 20; i += 2) {
        restrictedReadVariable.insert(i);
    }
    siteStatus = SiteStatus::UP;
    return true;
}

void Site::dump() const {
    string delim = "";
    cout << "Site " << id << " -";
    for (const auto& v : commitedVal) {
        cout << delim << " x" << v.first << ": " << v.second;
        delim = ",";
    }
    cout << endl;

    // debug purpose
    // cout << "============" << endl;
    // cout << "Current Val" << endl;
    // cout << "============" << endl;
    // delim = "";
    // cout << "site " << id << " -";
    // for (const auto& [idx, val] : curVal) {
    //   cout << delim << " x" << idx << ": " << val;
    //   delim = ",";
    // }
    // cout << endl;
    // cout << "============" << endl;
    // cout << "LockTable" << endl;
    // cout << "============" << endl;
    // lockManager.dump();
    // cout << endl;
}

ostream& operator<<(ostream& os, const SiteStatus& siteSatus) {
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
