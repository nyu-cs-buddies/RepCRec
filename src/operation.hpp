#pragma once

#include <iostream>

enum class Action {
    READ = 1,
    WRITE,
    BEGIN,
    BEGINRO,
    END,
    RECOVER,
    FAIL,
    DUMP
};

class Operation {
   public:
    Action action;
    int transactionId;
    int varIdx;
    int val;
    int siteId;
    int timeStamp;

    Operation();
    friend std::ostream& operator<<(std::ostream& os, const Action& action);
    friend std::ostream& operator<<(std::ostream& os, const Operation& op);
};

std::ostream& operator<<(std::ostream& os, const Action& action);
std::ostream& operator<<(std::ostream& os, const Operation& op);
