#include "transactionManager.hpp"

#include <algorithm>
#include <unordered_set>

using namespace std;

namespace {

bool dfs(unordered_map<int, list<int>> &waitForGraph,
         unordered_set<int> &visited, int curNode, list<int> &path) {
    path.push_back(curNode);
    if (visited.count(curNode)) {
        // find cycle
        return true;
    }
    if (!waitForGraph.count(curNode)) {
        return false;
    }
    visited.insert(curNode);
    for (auto &next : waitForGraph[curNode]) {
        if (dfs(waitForGraph, visited, next, path)) {
            return true;
        }
    }
    path.pop_back();
    return false;
}

bool hasCycle(unordered_map<int, list<int>> &waitForGraph, list<int> &path) {
    for (auto &n : waitForGraph) {
        unordered_set<int> visited;
        if (dfs(waitForGraph, visited, n.first, path)) {
            return true;
        }
    }
    return false;
}
}  // namespace

TransactionManager::TransactionManager() : time(0){};
TransactionManager::TransactionManager(const list<Operation> operations)
    : time(0), operations(operations){};

void TransactionManager::simulate() {
    // Site initialization
    for (int i = 0; i < 10; i++) {
        sites.emplace_back(Site(i + 1));
    }

    while (!operations.empty()) {
        auto curOperation = operations.front();
        operations.pop_front();
        time++;

        switch (curOperation.action) {
            case Action::BEGIN:
                begin(curOperation, false);
                break;
            case Action::BEGINRO:
                begin(curOperation, true);
                break;
            case Action::READ:
                read(curOperation);
                detectDeadLock();
                break;
            case Action::WRITE:
                write(curOperation);
                detectDeadLock();
                break;
            case Action::FAIL:
                fail(curOperation);
                break;
            case Action::RECOVER:
                recover(curOperation);
                break;
            case Action::END:
                commit(curOperation);
                break;
            case Action::DUMP:
                dump();
                break;
        }
    }
    return;
}

void TransactionManager::detectDeadLock() {
    list<int> path;  // store the path where we find a cycle
    if (!hasCycle(waitForGraph, path)) {
        return;
    }
    cout << "Deadlock happens!" << endl;
    // extract the path of the cycle
    auto r = find(path.begin(), path.end(), path.back());
    vector<int> pool(r, path.end());
    pool.pop_back();  // remove the repeated one
    // find the youngest one
    int youngestTime = 0;
    int transactionToAbort = 0;
    for (const auto &id : pool) {
        auto tran = idToTransaction[id];
        if (tran.startTime > youngestTime) {
            youngestTime = tran.startTime;
            transactionToAbort = id;
        }
    }
    abort(transactionToAbort);
    return;
}

void TransactionManager::begin(const Operation &curOperation, bool isReadOnly) {
    Transaction transaction = Transaction(curOperation.transactionId,
                                          curOperation.timeStamp, isReadOnly);
    if (isReadOnly) {
        copyCommitedValue(transaction);
    }
    idToTransaction[transaction.id] = transaction;
    if (isReadOnly) {
        cout << "T" << transaction.id << " begins, and it is read-only" << endl;
    } else {
        cout << "T" << transaction.id << " begins" << endl;
    }
}

void TransactionManager::read(const Operation &curOperation) {
    auto curId = curOperation.transactionId;
    if (idToTransaction[curId].transactionStatus ==
        TransactionStatus::ABORTED) {
        return;
    }

    // check if it is a read-only transaction
    if (idToTransaction[curId].isReadOnly) {
        if (!idToTransaction[curId].commitedValCopy.count(
                curOperation.varIdx)) {
            cout << "T" << curId << " can not read x" << curOperation.varIdx
                 << " since there are no sites avaialbe. "
                 << "T" << curId << " aborts!" << endl;
            idToTransaction[curId].transactionStatus =
                TransactionStatus::ABORTED;
            return;
        }
        cout << "T" << curId << " reads x" << curOperation.varIdx << ": "
             << idToTransaction[curId].commitedValCopy[curOperation.varIdx]
             << endl;
        return;
    }

    int lockHolder = -1;  // only write lock can block this operation
    int readVal = 0;
    bool isRead = false;
    for (auto &site : sites) {
        if (!isRead) {
            isRead = site.read(curId, curOperation.varIdx, lockHolder, readVal);
        }
    }

    if (!isRead) {
        // all sites down
        siteFailedOperations.push_back(curOperation);
        cout << "T" << curId << " can not read x" << curOperation.varIdx
             << " since there are no sites avaialbe." << endl;
        return;
    }

    if (lockHolder != -1 && lockHolder != curId) {
        // this operation is blocked
        blockedOperations.push_back(curOperation);
        waitForGraph[lockHolder].push_back(curId);
        if (idToTransaction[curId].transactionStatus ==
            TransactionStatus::RUNNING) {
            idToTransaction[curId].transactionStatus =
                TransactionStatus::WAITING;
            cout << "T" << curId << " can not read x" << curOperation.varIdx
                 << " since the lock conflicts" << endl;
        }
    } else {
        idToTransaction[curId].transactionStatus = TransactionStatus::RUNNING;
        cout << "T" << curId << " reads x" << curOperation.varIdx << ": "
             << readVal << endl;
        // update read history
        idToTransaction[curId].readHistory[curOperation.varIdx] = time;
    }
    return;
}

void TransactionManager::write(const Operation &curOperation) {
    auto curId = curOperation.transactionId;
    if (idToTransaction[curId].transactionStatus ==
        TransactionStatus::ABORTED) {
        return;
    }

    // check site's availability
    unordered_set<int> lockHolders;
    vector<int> affectedSiteIndexes;
    for (size_t i = 0; i < 10; i++) {
        if (sites[i].write(curId, curOperation.varIdx, curOperation.val,
                           lockHolders)) {
            affectedSiteIndexes.push_back(i + 1);
        }
    }

    // if all sites down
    if (affectedSiteIndexes.empty() && lockHolders.empty()) {
        siteFailedOperations.push_back(curOperation);
        cout << "T" << curId << " can not write x" << curOperation.varIdx
             << " since there are no sites avaialbe." << endl;
        return;
    }

    // if operation is blocked
    if (!lockHolders.empty()) {
        blockedOperations.push_back(curOperation);
        for (const auto &lockHolder : lockHolders) {
            if (lockHolder == curId) {
                continue;
            }

            waitForGraph[lockHolder].push_back(curId);
        }
        if (idToTransaction[curId].transactionStatus ==
            TransactionStatus::RUNNING) {
            idToTransaction[curId].transactionStatus =
                TransactionStatus::WAITING;
            cout << "T" << curId << " can not write x" << curOperation.varIdx
                 << " since the lock conflicts" << endl;
        }
        return;
    }

    // else
    idToTransaction[curId].transactionStatus = TransactionStatus::RUNNING;
    idToTransaction[curId].affectedVariables.insert(curOperation.varIdx);
    cout << "T" << curId << " writes x" << curOperation.varIdx << " as "
         << curOperation.val << ", and affected sites are ";
    for (const auto &siteIndex : affectedSiteIndexes) {
        cout << siteIndex << " ";
    }
    cout << endl;
    // keep tracking uncommited variable
    uncommitedVariable[curOperation.varIdx] = time;
    // update write history
    idToTransaction[curId].writeHistory[curOperation.varIdx] = time;
    return;
}

void TransactionManager::commit(const Operation &curOperation) {
    auto curId = curOperation.transactionId;
    if (idToTransaction[curId].transactionStatus ==
        TransactionStatus::ABORTED) {
        abort(curId);
        return;
    }
    // check if affected variables can commit
    bool ableToCommit = true;
    // check if sites are up
    for (const auto &av : idToTransaction[curId].affectedVariables) {
        if (av % 2 == 0) {
            for (const auto &site : sites) {
                // if write time before fail
                if (site.siteStatus == SiteStatus::DOWN &&
                    site.curVal.count(av)) {
                    ableToCommit = false;
                    break;
                }
            }
        } else if (sites[av % 10].siteStatus == SiteStatus::DOWN ||
                   (sites[av % 10].siteStatus == SiteStatus::UP &&
                    !sites[av % 10].curVal.count(av))) {
            // non-replicated variable
            ableToCommit = false;
        }
    }
    // check if there's write operations before a site failed
    for (const auto &site : sites) {
        for (const auto &idx : idToTransaction[curId].affectedVariables) {
            if (site.restrictedWriteVariable.count(idx)) {
                ableToCommit = false;
            }
            if (idToTransaction[curId].writeHistory[idx] < site.failedTime) {
                ableToCommit = false;
            }
        }
        for (const auto &e : idToTransaction[curId].readHistory) {
            if (site.commitedVal.count(e.first) && time < site.failedTime) {
                ableToCommit = false;
            }
        }
    }
    // check if `siteFailedOperations` contains the operations of this
    // transaction
    auto it = siteFailedOperations.begin();
    while (it != siteFailedOperations.end()) {
        if ((*it).transactionId == curId) {
            ableToCommit = false;
            it = siteFailedOperations.erase(it);
        } else {
            it++;
        }
    }
    // abort
    if (!ableToCommit) {
        abort(curId);
        return;
    }
    // change curValue to commitedValue
    for (auto &site : sites) {
        if (site.siteStatus != SiteStatus::DOWN) {
            site.commit(curId, idToTransaction[curId].affectedVariables);
        }
    }
    idToTransaction[curId].transactionStatus = TransactionStatus::COMMITED;
    cout << "T" << curId << " commits!" << endl;

    // update uncommitedVarialbe
    for (const auto &idx : idToTransaction[curId].affectedVariables) {
        uncommitedVariable.erase(idx);
    }

    // deal with operations which are blocked by this transaction
    // iterate each waiting transaction backward and push_front its related
    // blocked operations to the operations queue
    auto blockedTrans = waitForGraph[curId];
    for (auto i = blockedTrans.rbegin(); i != blockedTrans.rend(); i++) {
        for (auto j = blockedOperations.rbegin(); j != blockedOperations.rend();
             j++) {
            if ((*j).transactionId == (*i)) {
                operations.push_front(*j);
            }
        }
    }
    unordered_set<int> blockedTransSet(blockedTrans.begin(),
                                       blockedTrans.end());
    auto removeUnblockedTrans =
        remove_if(blockedOperations.begin(), blockedOperations.end(),
                  [&](const Operation &o) {
                      return blockedTransSet.count(o.transactionId);
                  });
    blockedOperations.erase(removeUnblockedTrans, blockedOperations.end());

    waitForGraph.erase(curId);
}

void TransactionManager::fail(const Operation &curOperation) {
    if (sites[curOperation.siteId - 1].fail(time)) {
        cout << "Site" << curOperation.siteId << " fails!" << endl;
    }
}

void TransactionManager::recover(const Operation &curOperation) {
    auto curSid = curOperation.siteId;
    if (sites[curSid - 1].recover()) {
        cout << "Site" << curSid << " recovers!" << endl;

        // let this site knows there exists uncommited variables before it
        // failed
        for (const auto &e : uncommitedVariable) {
            // check if this happened before the site failed
            if (e.second > sites[curSid - 1].failedTime) {
                continue;
            }
            if (sites[curSid - 1].commitedVal.count(e.first)) {
                sites[curSid - 1].restrictedWriteVariable.insert(e.first);
            }
        }

        // check invalid read for replicated variables
        for (auto &e : idToTransaction) {
            for (const auto &v : e.second.readHistory) {
                if (sites[curSid - 1].commitedVal.count(v.first) &&
                    v.second < sites[curSid - 1].failedTime) {
                    e.second.transactionStatus = TransactionStatus::ABORTED;
                }
            }
        }

        // update operations queue
        for (auto i = siteFailedOperations.rbegin();
             i != siteFailedOperations.rend(); i++) {
            operations.push_front(*i);
        }
        siteFailedOperations.clear();
    }
}

void TransactionManager::abort(const int transactionToAbort) {
    for (auto &site : sites) {
        site.abort(transactionToAbort);

        for (const auto &var :
             idToTransaction[transactionToAbort].affectedVariables) {
            if (site.restrictedWriteVariable.count(var)) {
                site.restrictedWriteVariable.erase(var);
            }
        }
    }
    idToTransaction.erase(transactionToAbort);
    unordered_set<int> waitedTrans(waitForGraph[transactionToAbort].begin(),
                                   waitForGraph[transactionToAbort].end());
    waitForGraph.erase(transactionToAbort);
    for (auto &e : waitForGraph) {
        e.second.remove(transactionToAbort);
    }

    // add unblocked operations back to operations queue
    for (auto i = blockedOperations.rbegin(); i != blockedOperations.rend();
         i++) {
        if (waitedTrans.count((*i).transactionId)) {
            operations.push_front(*i);
        }
    }
    auto removeUnblockedTrans =
        remove_if(blockedOperations.begin(), blockedOperations.end(),
                  [&](const Operation &o) {
                      return o.transactionId == transactionToAbort ||
                             waitedTrans.count(o.transactionId);
                  });
    blockedOperations.erase(removeUnblockedTrans, blockedOperations.end());

    idToTransaction[transactionToAbort].transactionStatus =
        TransactionStatus::ABORTED;
    cout << "T" << transactionToAbort << " aborts!" << endl;

    // update waitForGraph
    for (auto i = waitForGraph.cbegin(); i != waitForGraph.cend();) {
        if (i->second.size() == 0) {
            waitForGraph.erase(i++);
        } else {
            i++;
        }
    }

    return;
}

void TransactionManager::dumpDebug() {
    cout << endl;
    cout << "Blocked Transactions: ";
    for (const auto &o : blockedOperations) {
        cout << o << " ";
    }
    cout << endl;

    cout << "Wait for graph: " << endl;
    for (const auto &[id, waits] : waitForGraph) {
        cout << "TransId: " << id << ": ";
        for (const auto &w : waits) {
            cout << w << " ";
        }
        cout << endl;
    }
}

void TransactionManager::dump() {
    for (const auto &site : sites) {
        site.dump();
    }

#ifdef DEBUG
    dumpDebug();
#endif

    return;
}

void TransactionManager::copyCommitedValue(Transaction &transaction) {
    for (const auto &site : sites) {
        for (const auto &e : site.commitedVal) {
            if (site.restrictedReadVariable.count(e.first)) {
                continue;
            }
            transaction.commitedValCopy[e.first] = e.second;
        }
    }
}