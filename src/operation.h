#pragma once
#include <iostream>
using namespace std;

enum struct Action {
    READ = 1,
    WRITE,
    BEGIN,
    BEGINRO,
    END,
    RECOVER,
    FAIL,
    DUMP
};

struct Operation {
    Action action;
    int transactionId = -1;
    int varIdx = -1;
    int val = -1;
    int siteId = -1;
    int timeStamp = 0;
};

inline ostream& operator<<(ostream& os, const Action& action) {
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

inline ostream& operator<<(ostream& os, const Operation& op) {
    os << "time: " << op.timeStamp << " Action: " << op.action << " transactionId: " << op.transactionId << " varIdx: " << op.varIdx
    << " val: " << op.val << " siteId: " << op.siteId;
    return os;
}

namespace {
    // R(T3,x4)
    Operation getReadOperation(const std::string& line) {
        Operation operation;
        operation.action = Action::READ;

        auto idx = line.find('T');
        int transactionId = 0;
        while (isdigit(line[++idx])) {
            transactionId = transactionId * 10 + (line[idx] - '0');
        }
        operation.transactionId = transactionId;

        idx = line.find('x');
        int varIdx = 0;
        while (isdigit(line[++idx])) {
            varIdx = varIdx * 10 + (line[idx] - '0');
        }
        operation.varIdx = varIdx;

        return operation;
    }


    // W(T1,x1,100)
    Operation getWriteOperation(const std::string& line) {
        Operation operation;
        operation.action = Action::WRITE;

        auto idx = line.find('T');
        int transactionId = 0;
        while (isdigit(line[++idx])) {
            transactionId = transactionId * 10 + (line[idx] - '0');
        }
        operation.transactionId = transactionId;

        idx = line.find('x');
        int varIdx = 0;
        while (isdigit(line[++idx])) {
            varIdx = varIdx * 10 + (line[idx] - '0');
        }
        operation.varIdx = varIdx;

        int val = 0;
        while (isdigit(line[++idx])) {
            val = val * 10 + (line[idx] - '0');
        }
        operation.val = val;

        return operation;
    }


    // begin(T1)
    Operation getBeginOperation(const std::string& line) {
        Operation operation;
        operation.action = Action::BEGIN;

        auto idx = line.find('T');
        int transactionId = 0;
        while (isdigit(line[++idx])) {
            transactionId = transactionId * 10 + (line[idx] - '0');
        }
        operation.transactionId = transactionId;
        return operation;
    }


    // beginRO(T1)
    Operation getBeginROOperation(const std::string& line) {
        Operation operation;
        operation.action = Action::BEGINRO;

        auto idx = line.find('T');
        int transactionId = 0;
        while (isdigit(line[++idx])) {
            transactionId = transactionId * 10 + (line[idx] - '0');
        }
        operation.transactionId = transactionId;

        return operation;
    }


    // end(T1)

    Operation getEndOperation(const std::string& line) {
        Operation operation;
        operation.action = Action::END;

        auto idx = line.find('T');
        int transactionId = 0;
        while (isdigit(line[++idx])) {
            transactionId = transactionId * 10 + (line[idx] - '0');
        }
        operation.transactionId = transactionId;

        return operation;
    }


    // dump()
    Operation getDumpOperation(const std::string& line) {
        Operation operation;
        operation.action = Action::DUMP;
        return operation;
    }


    // fail(1)
    Operation getFailOperation(const std::string& line) {
        Operation operation;
        operation.action = Action::FAIL;

        auto idx = line.find('(');
        int siteId = 0;
        while (isdigit(line[++idx])) {
            siteId = siteId * 10 + (line[idx] - '0');
        }
        operation.siteId = siteId;

        return operation;
    }


    // recover(1)
    Operation getRecoverOperation(const std::string& line) {
        Operation operation;
        operation.action = Action::RECOVER;

        auto idx = line.find('(');
        int siteId = 0;
        while (isdigit(line[++idx])) {
            siteId = siteId * 10 + (line[idx] - '0');
        }
        operation.siteId = siteId;

        return operation;
    }
}