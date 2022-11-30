#include "transactionManager.hpp"

#include <algorithm>
#include <unordered_set>

namespace {

bool dfs(std::unordered_map<int, std::list<int>> &waitForGraph,
         std::unordered_set<int> &visited, int curNode, std::list<int> &path) {
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

bool hasCycle(std::unordered_map<int, std::list<int>> &waitForGraph,
              std::list<int> &path) {
    for (auto &n : waitForGraph) {
        std::unordered_set<int> visited;
        if (dfs(waitForGraph, visited, n.first, path)) {
            return true;
        }
    }
    return false;
}
}  // namespace

TransactionManager::TransactionManager() : time(0){};
TransactionManager::TransactionManager(const std::list<Operation> operations)
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
    std::list<int> path;  // store the path where we find a cycle
    if (!hasCycle(waitForGraph, path)) {
        return;
    }
    std::cout << "Deadlock happens!" << std::endl;
    // extract the path of the cycle
    auto r = std::find(path.begin(), path.end(), path.back());
    std::vector<int> pool(r, path.end());
    pool.pop_back();  // remove the repeated one
    // find the youngest one
    int youngestTime = 0;
    int transactionToAbort = 0;
    for (const auto &id : pool) {
        auto trans = idToTransaction[id];
        if (trans.startTime > youngestTime) {
            youngestTime = trans.startTime;
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
        std::cout << "T" << transaction.id << " begins, and it is read-only"
                  << std::endl;
    } else {
        std::cout << "T" << transaction.id << " begins" << std::endl;
    }
}

void TransactionManager::read(const Operation &curOperation) {
    if (idToTransaction[curOperation.transactionId].transactionStatus ==
        TransactionStatus::ABORTED) {
        return;
    }

    // check if it is a read-only transaction
    if (idToTransaction[curOperation.transactionId].isReadOnly) {
        if (!idToTransaction[curOperation.transactionId].commitedValCopy.count(
                curOperation.varIdx)) {
            std::cout << "T" << curOperation.transactionId << " can not read x"
                      << curOperation.varIdx
                      << " since there are no sites avaialbe. "
                      << "T" << curOperation.transactionId << " aborts!"
                      << std::endl;
            idToTransaction[curOperation.transactionId].transactionStatus =
                TransactionStatus::ABORTED;
            return;
        }
        std::cout << "T" << curOperation.transactionId << " reads x"
                  << curOperation.varIdx << ": "
                  << idToTransaction[curOperation.transactionId]
                         .commitedValCopy[curOperation.varIdx]
                  << std::endl;
        return;
    }

    int lockHolder = -1;  // only write lock can block this operation
    int readVal = 0;
    bool isRead = false;
    for (auto &site : sites) {
        isRead = isRead || site.read(curOperation.transactionId,
                                     curOperation.varIdx, lockHolder, readVal);
    }

    if (!isRead) {
        // all sites down
        siteFailedOperations.push_back(curOperation);
        std::cout << "T" << curOperation.transactionId << " can not read x"
                  << curOperation.varIdx
                  << " since there are no sites avaialbe." << std::endl;
        return;
    }

    if (lockHolder != -1 && lockHolder != curOperation.transactionId) {
        // this operation is blocked
        blockedOperations.push_back(curOperation);
        waitForGraph[lockHolder].push_back(curOperation.transactionId);
        if (idToTransaction[curOperation.transactionId].transactionStatus ==
            TransactionStatus::RUNNING) {
            idToTransaction[curOperation.transactionId].transactionStatus =
                TransactionStatus::WAITING;
            std::cout << "T" << curOperation.transactionId << " can not read x"
                      << curOperation.varIdx << " since the lock conflicts"
                      << std::endl;
        }
    } else {
        idToTransaction[curOperation.transactionId].transactionStatus =
            TransactionStatus::RUNNING;
        std::cout << "T" << curOperation.transactionId << " reads x"
                  << curOperation.varIdx << ": " << readVal << std::endl;
        // update read history
        idToTransaction[curOperation.transactionId]
            .readHistory[curOperation.varIdx] = time;
    }
    return;
}

void TransactionManager::write(const Operation &curOperation) {
    if (idToTransaction[curOperation.transactionId].transactionStatus ==
        TransactionStatus::ABORTED) {
        return;
    }

    // check site available
    std::unordered_set<int> lockHolders;
    std::vector<int> affectedSiteIndexes;
    for (size_t i = 0; i < 10; i++) {
        if (sites[i].write(curOperation.transactionId, curOperation.varIdx,
                           curOperation.val, lockHolders)) {
            affectedSiteIndexes.push_back(i + 1);
        }
    }
    if (affectedSiteIndexes.empty() && lockHolders.empty()) {
        // all sites down
        siteFailedOperations.push_back(curOperation);
        std::cout << "T" << curOperation.transactionId << " can not write x"
                  << curOperation.varIdx
                  << " since there are no sites avaialbe." << std::endl;
        return;
    }
    if (!lockHolders.empty()) {
        // this operation is blocked
        blockedOperations.push_back(curOperation);
        for (const auto &lockHolder : lockHolders) {
            if (lockHolder == curOperation.transactionId) {
                continue;
            }

            waitForGraph[lockHolder].push_back(curOperation.transactionId);
        }
        if (idToTransaction[curOperation.transactionId].transactionStatus ==
            TransactionStatus::RUNNING) {
            idToTransaction[curOperation.transactionId].transactionStatus =
                TransactionStatus::WAITING;
            std::cout << "T" << curOperation.transactionId << " can not write x"
                      << curOperation.varIdx << " since the lock conflicts"
                      << std::endl;
        }
    } else {
        idToTransaction[curOperation.transactionId].transactionStatus =
            TransactionStatus::RUNNING;
        idToTransaction[curOperation.transactionId].affectedVariables.insert(
            curOperation.varIdx);
        std::cout << "T" << curOperation.transactionId << " writes x"
                  << curOperation.varIdx << " as " << curOperation.val
                  << ", and affected sites are ";
        for (const auto &siteIndex : affectedSiteIndexes) {
            std::cout << siteIndex << " ";
        }
        std::cout << std::endl;
        // keep tracking uncommited variable
        uncommitedVariable[curOperation.varIdx] = time;
        // update write history
        idToTransaction[curOperation.transactionId]
            .writeHistory[curOperation.varIdx] = time;
    }
    return;
}

void TransactionManager::commit(const Operation &curOperation) {
    if (idToTransaction[curOperation.transactionId].transactionStatus ==
        TransactionStatus::ABORTED) {
        abort(curOperation.transactionId);
        return;
    }
    // check every affected variables if they can commit
    bool ableToCommit = true;
    // check if sites with these variables are up
    for (const auto &affectedVariable :
         idToTransaction[curOperation.transactionId].affectedVariables) {
        if (affectedVariable % 2 == 0) {
            for (const auto &site : sites) {
                // if write time before fail
                if (site.siteStatus == SiteStatus::DOWN &&
                    site.curVal.count(affectedVariable)) {
                    ableToCommit = false;
                    break;
                }
            }
        } else if (sites[affectedVariable % 10].siteStatus ==
                       SiteStatus::DOWN ||
                   (sites[affectedVariable % 10].siteStatus == SiteStatus::UP &&
                    !sites[affectedVariable % 10].curVal.count(
                        affectedVariable))) {
            // non-replicated variable
            ableToCommit = false;
        }
    }
    // check if there's write operations before a site failed
    for (const auto &site : sites) {
        for (const auto &idx :
             idToTransaction[curOperation.transactionId].affectedVariables) {
            if (site.restrictedWriteVariable.count(idx)) {
                ableToCommit = false;
            }
            if (idToTransaction[curOperation.transactionId].writeHistory[idx] <
                site.failedTime) {
                ableToCommit = false;
            }
        }
        for (const auto &e :
             idToTransaction[curOperation.transactionId].readHistory) {
            if (site.commitedVal.count(e.first) && time < site.failedTime) {
                ableToCommit = false;
            }
        }
    }
    // check if `siteFailedOperations` contains the operations of this
    // transaction
    auto it = siteFailedOperations.begin();
    while (it != siteFailedOperations.end()) {
        if ((*it).transactionId == curOperation.transactionId) {
            ableToCommit = false;
            it = siteFailedOperations.erase(it);
        } else {
            it++;
        }
    }
    // abort
    if (!ableToCommit) {
        abort(curOperation.transactionId);
        return;
    }
    // change curValue to commitedValue
    for (auto &site : sites) {
        if (site.siteStatus != SiteStatus::DOWN) {
            site.commit(
                curOperation.transactionId,
                idToTransaction[curOperation.transactionId].affectedVariables);
        }
    }
    idToTransaction[curOperation.transactionId].transactionStatus =
        TransactionStatus::COMMITED;
    std::cout << "T" << curOperation.transactionId << " commits!" << std::endl;

    // update uncommitedVarialbe
    for (const auto &idx :
         idToTransaction[curOperation.transactionId].affectedVariables) {
        uncommitedVariable.erase(idx);
    }

    // deal with operations which are blocked by this transaction
    // iterate each waiting transaction backward and push_front its related
    // blocked operations to the operations queue
    auto blockedTrans = waitForGraph[curOperation.transactionId];
    for (auto i = blockedTrans.rbegin(); i != blockedTrans.rend(); i++) {
        for (auto j = blockedOperations.rbegin(); j != blockedOperations.rend();
             j++) {
            if ((*j).transactionId == (*i)) {
                operations.push_front(*j);
            }
        }
    }
    std::unordered_set<int> blockedTransSet(blockedTrans.begin(),
                                            blockedTrans.end());
    auto removeUnblockedTrans =
        std::remove_if(blockedOperations.begin(), blockedOperations.end(),
                       [&](const Operation &o) {
                           return blockedTransSet.count(o.transactionId);
                       });
    blockedOperations.erase(removeUnblockedTrans, blockedOperations.end());

    waitForGraph.erase(curOperation.transactionId);
}

void TransactionManager::fail(const Operation &curOperation) {
    if (sites[curOperation.siteId - 1].fail(time)) {
        std::cout << "Site" << curOperation.siteId << " fails!" << std::endl;
    }
}

void TransactionManager::recover(const Operation &curOperation) {
    if (sites[curOperation.siteId - 1].recover()) {
        std::cout << "Site" << curOperation.siteId << " recovers!" << std::endl;

        // let this site knows there exists uncommited variables before it
        // failed
        for (const auto &e : uncommitedVariable) {
            // check if this happened before the site failed
            if (e.second > sites[curOperation.siteId - 1].failedTime) {
                continue;
            }
            if (sites[curOperation.siteId - 1].commitedVal.count(e.first)) {
                sites[curOperation.siteId - 1].restrictedWriteVariable.insert(
                    e.first);
            }
        }

        // check invalid read for replicated variables
        for (auto &e : idToTransaction) {
            for (const auto &v : e.second.readHistory) {
                if (sites[curOperation.siteId - 1].commitedVal.count(v.first) &&
                    v.second < sites[curOperation.siteId - 1].failedTime) {
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
    std::unordered_set<int> waitedTrans(
        waitForGraph[transactionToAbort].begin(),
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
        std::remove_if(blockedOperations.begin(), blockedOperations.end(),
                       [&](const Operation &o) {
                           return o.transactionId == transactionToAbort ||
                                  waitedTrans.count(o.transactionId);
                       });
    blockedOperations.erase(removeUnblockedTrans, blockedOperations.end());

    idToTransaction[transactionToAbort].transactionStatus =
        TransactionStatus::ABORTED;
    std::cout << "T" << transactionToAbort << " aborts!" << std::endl;

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
    std::cout << std::endl;
    std::cout << "Blocked Transactions: ";
    for (const auto &o : blockedOperations) {
        std::cout << o << " ";
    }
    std::cout << std::endl;

    std::cout << "Wait for graph: " << std::endl;
    for (const auto &[id, waits] : waitForGraph) {
        std::cout << "TransId: " << id << ": ";
        for (const auto &w : waits) {
            std::cout << w << " ";
        }
        std::cout << std::endl;
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