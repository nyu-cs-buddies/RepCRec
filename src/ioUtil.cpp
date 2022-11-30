#include "ioUtil.h"

#include <fstream>
#include <iostream>
#include <string>

namespace {
/**
 * deal with read operation (ie. R(T1,x1))
 */
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

/**
 * deal with write operation (ie. W(T1,x1,100))
 */
Operation getWrietOperation(const std::string& line) {
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

/**
 * deal with begin operation (ie. begin(T1))
 */
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

/**
 * deal with beginRO operation (ie. beginRO(T1))
 */
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

/**
 * deal with end operation (ie. end(T1))
 */
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

/**
 * deal with dump operation (ie. dump())
 */
Operation getDumpOperation(const std::string& line) {
  Operation operation;
  operation.action = Action::DUMP;
  return operation;
}

/**
 * deal with fail operation (ie. fail(1))
 */
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

/**
 * deal with recover operation (ie. recover(1))
 */
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

}  // namespace

IOUtil::IOUtil(const char* filename) {
  std::ifstream infile(filename);
  std::string line;
  int time = 0;

  while (getline(infile, line)) {
    if (!isalpha(line[0])) {
      continue;
    }
    Operation operation;
    ++time;
    switch (line[0]) {
      case 'R': {
        operation = getReadOperation(line);
      } break;
      case 'W': {
        operation = getWrietOperation(line);
      } break;
      case 'b': {
        auto idx = line.find("(");
        if (idx == 5) {
          operation = getBeginOperation(line);
        } else if (idx == 7) {
          operation = getBeginROOperation(line);
        } else {
          std::cout << "Error: wrong operation." << std::endl;
          exit(1);
        }
      } break;
      case 'e': {
        operation = getEndOperation(line);
      } break;
      case 'd': {
        operation = getDumpOperation(line);
      } break;
      case 'r': {
        operation = getRecoverOperation(line);
      } break;
      case 'f': {
        operation = getFailOperation(line);
      } break;
      default:
        std::cout << "Error: wrong operation." << std::endl;
        exit(1);
        break;
    }
    operation.timeStamp = time;
    operations.push_back(operation);
  }
};