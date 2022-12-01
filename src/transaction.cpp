#include "transaction.hpp"

using namespace std;

Transaction::Transaction(){};
Transaction::Transaction(const int id, const int startTime,
                         const bool isReadOnly)
    : id(id),
      startTime(startTime),
      isReadOnly(isReadOnly),
      transactionStatus(TransactionStatus::RUNNING){};

ostream &operator<<(ostream &os, const TransactionStatus &transactionStatus) {
    switch (transactionStatus) {
        case TransactionStatus::RUNNING:
            os << "RUNNING";
            break;
        case TransactionStatus::WAITING:
            os << "WAITING";
            break;
        case TransactionStatus::ABORTED:
            os << "ABORTED";
            break;
        case TransactionStatus::COMMITED:
            os << "COMMITED";
            break;
    }
    return os;
}

ostream &operator<<(ostream &os, const Transaction tran) {
    os << "id: " << tran.id << " startTime: " << tran.startTime
       << " isReadOnly: " << tran.isReadOnly
       << " status: " << tran.transactionStatus;
    return os;
}
