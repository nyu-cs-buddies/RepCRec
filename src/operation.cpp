#include "operation.hpp"

Operation::Operation()
    : transactionId(-1), varIdx(-1), val(-1), siteId(-1), timeStamp(0){};

std::ostream& operator<<(std::ostream& os, const Action& action) {
    switch (action) {
        case Action::READ:
            os << "READ";
            break;
        case Action::WRITE:
            os << "WRITE";
            break;
        case Action::BEGIN:
            os << "BEGIN";
            break;
        case Action::BEGINRO:
            os << "BEGINRO";
            break;
        case Action::END:
            os << "END";
            break;
        case Action::RECOVER:
            os << "RECOVER";
            break;
        case Action::FAIL:
            os << "FAIL";
            break;
        case Action::DUMP:
            os << "DUMP";
            break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const Operation& op) {
    os << "time: " << op.timeStamp << " Action: " << op.action
       << " transactionId: " << op.transactionId << " varIdx: " << op.varIdx
       << " val: " << op.val << " siteId: " << op.siteId;
    return os;
}
